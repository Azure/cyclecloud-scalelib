# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

import json
import uuid
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Tuple

import cyclecloud.api.clusters
import cyclecloud.session
import requests
import urllib3
from cyclecloud.model.NodeCreationRequestModule import NodeCreationRequest
from cyclecloud.model.NodeCreationRequestSetDefinitionModule import (
    NodeCreationRequestSetDefinition,
)
from cyclecloud.model.NodeCreationRequestSetModule import NodeCreationRequestSet
from cyclecloud.model.NodeCreationResultModule import NodeCreationResult
from cyclecloud.model.NodeCreationResultSetModule import NodeCreationResultSet
from cyclecloud.model.NodeListModule import NodeList
from cyclecloud.model.NodeManagementRequestModule import NodeManagementRequest
from cyclecloud.model.NodeManagementResultModule import NodeManagementResult
from cyclecloud.session import ResponseStatus
from requests.structures import CaseInsensitiveDict
from urllib3.exceptions import InsecureRequestWarning

import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.ccbindings.mock import _node_to_ccnode
from hpc.autoscale.codeanalysis import hpcwrap, hpcwrapclass
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import partition

logger = logging.getLogger("cyclecloud.clustersapi")


class ReadOnlyModeException(RuntimeError):
    pass


def notreadonly(method: Callable) -> Callable:
    def readonlywrapper(*args: Any, **kwargs: Any) -> Optional[Any]:
        if args[0].read_only:
            raise ReadOnlyModeException(
                "Can not call {} in read only mode.".format(method.__name__)
            )
        return method(*args, **kwargs)

    return readonlywrapper


@hpcwrapclass
class ClusterBinding(ClusterBindingInterface):
    def __init__(
        self,
        config: Dict,
        session: Any,
        client: Any,
        clusters_module: Any = None,
        read_only: bool = False,
    ) -> None:
        self.config = config
        self.__cluster_name = config["cluster_name"]
        self.session = session
        self.client = client
        self.clusters_module: cyclecloud.api.clusters = cyclecloud.api.clusters
        if clusters_module:
            self.clusters_module = clusters_module  # type: ignore
        self.read_only = read_only
        self._read_only_nodes: Dict[ht.OperationId, List[Dict]] = {}

    @property
    def cluster_name(self) -> ht.ClusterName:
        return self.__cluster_name

    @hpcwrap
    def create_nodes(self, nodes: List[Node]) -> NodeCreationResult:
        if self.read_only:
            ret = NodeCreationResult()
            ret.operation_id = str(uuid.uuid4())
            ret.sets = [NodeCreationResultSet(added=len(nodes))]
            for n in nodes:
                n.exists = True
                n.target_state = ht.NodeStatus("Started")
                n.delayed_node_id.node_id = ht.NodeId("dryrun-" + str(uuid.uuid4()))
            node_records = [_node_to_ccnode(n) for n in nodes]
            self._read_only_nodes[ht.OperationId(ret.operation_id)] = node_records
            return ret

        creation_request = NodeCreationRequest()
        creation_request.sets = []
        # the node attributes aren't hashable, so a string representation
        # is good enough to ensure they are all the same across the list.
        p_nodes_dict = partition(
            nodes,
            lambda n: (
                n.nodearray,
                n.vm_size,
                n.placement_group,
                str(n.node_attribute_overrides),
                n.keep_alive,
            ),
        )

        request_tuples: List[Tuple[Node, NodeCreationRequestSet]] = []

        def _node_key(n: Node) -> Tuple[str, int]:
            try:
                index = int(n.name.split("-")[-1])
                return (n.nodearray, index)
            except ValueError:
                return (n.nodearray, -1)

        for key, p_nodes in p_nodes_dict.items():
            nodearray, vm_size, pg, _, keep_alive = key
            request_set = NodeCreationRequestSet()

            request_set.nodearray = nodearray
            request_set.count = len(p_nodes)
            request_set.placement_group_id = pg
            request_set.definition = NodeCreationRequestSetDefinition()
            request_set.definition.machine_type = vm_size

            if p_nodes[0].node_attribute_overrides:
                request_set.node_attributes = deepcopy(
                    p_nodes[0].node_attribute_overrides
                )

            if keep_alive:
                if not request_set.node_attributes:
                    request_set.node_attributes = {}
                request_set.node_attributes["KeepAlive"] = keep_alive

            first_node = sorted(p_nodes, key=_node_key)[0]

            request_tuples.append((first_node, request_set))

        sorted_tuples = sorted(request_tuples, key=lambda t: _node_key(t[0]))
        for _, request_set in sorted_tuples:
            creation_request.sets.append(request_set)

        creation_request.validate()

        logging.fine(json.dumps(creation_request.to_dict()))

        http_response, result = self.clusters_module.create_nodes(
            self.session, self.cluster_name, creation_request
        )

        self._log_response(http_response, result)

        return result

    @notreadonly
    def deallocate_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[ht.NodeName]] = None,
        node_ids: Optional[List[ht.NodeId]] = None,
        hostnames: Optional[List[ht.Hostname]] = None,
        ip_addresses: Optional[List[ht.IpAddress]] = None,
        custom_filter: str = None,
        request_id: Optional[str] = None,
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter, request_id
        )

        logging.fine(json.dumps(request.to_dict()))

        http_response, result = self.clusters_module.deallocate_nodes(
            self.session, self.cluster_name, request
        )
        self._log_response(http_response, result)
        return result

    @hpcwrap
    def get_cluster_status(self, nodes: bool = False) -> cyclecloud.model.ClusterStatus:
        http_response, result = self.clusters_module.get_cluster_status(
            self.session, self.cluster_name, nodes
        )
        if self.read_only and nodes:
            for nodes_list in self._read_only_nodes.values():
                result.nodes.extend(nodes_list)
        return result

    def get_nodes(
        self,
        operation_id: Optional[ht.OperationId] = None,
        request_id: Optional[ht.RequestId] = None,
    ) -> NodeList:
        if self.read_only and self._read_only_nodes:
            if operation_id:
                nodes = self._read_only_nodes.get(operation_id, [])
            else:
                nodes = []
                for sub_list in self._read_only_nodes.values():
                    nodes.extend(sub_list)
            return NodeList(nodes=nodes)

        http_response, result = self.clusters_module.get_nodes(
            self.session, self.cluster_name, operation_id, request_id
        )
        self._log_response(http_response, result)
        return result

    @notreadonly
    def remove_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[ht.NodeName]] = None,
        node_ids: Optional[List[ht.NodeId]] = None,
        hostnames: Optional[List[ht.Hostname]] = None,
        ip_addresses: Optional[List[ht.IpAddress]] = None,
        custom_filter: str = None,
        request_id: Optional[str] = None,
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter, request_id
        )

        logging.fine(json.dumps(request.to_dict()))

        http_response, result = self.clusters_module.remove_nodes(
            self.session, self.cluster_name, request
        )
        self._log_response(http_response, result)
        return result

    @notreadonly
    def scale(
        self,
        nodearray: ht.NodeArrayName,
        total_core_count: Optional[int] = None,
        total_node_count: Optional[int] = None,
    ) -> NodeCreationResult:

        http_response, result = self.clusters_module.scale(
            self.session,
            self.cluster_name,
            nodearray,
            total_core_count,
            total_node_count,
        )
        self._log_response(http_response, result)
        return result

    @notreadonly
    def shutdown_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[ht.NodeName]] = None,
        node_ids: Optional[List[ht.NodeId]] = None,
        hostnames: Optional[List[ht.Hostname]] = None,
        ip_addresses: Optional[List[ht.IpAddress]] = None,
        custom_filter: str = None,
        request_id: Optional[str] = None,
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter, request_id
        )

        logging.fine(json.dumps(request.to_dict()))

        http_response, result = self.clusters_module.shutdown_nodes(
            self.session, self.cluster_name, request
        )

        self._log_response(http_response, result)
        return result

    @hpcwrap
    def start_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[ht.NodeName]] = None,
        node_ids: Optional[List[ht.NodeId]] = None,
        hostnames: Optional[List[ht.Hostname]] = None,
        ip_addresses: Optional[List[ht.IpAddress]] = None,
        custom_filter: str = None,
        request_id: Optional[str] = None,
    ) -> NodeManagementResult:
        if self.read_only:
            ret = NodeCreationResult()
            ret.operation_id = str(uuid.uuid4())

            node_records: List[Dict]
            if not nodes:

                all_nodes: List[Dict] = []
                for rnodes in self._read_only_nodes.values():
                    all_nodes.extend(rnodes)

                if names:
                    node_records = [n for n in all_nodes if n["Name"] in names]
                elif node_ids:
                    node_records = [n for n in all_nodes if n["NodeId"] in node_ids]
                elif hostnames:
                    node_records = [n for n in all_nodes if n["Hostname"] in node_ids]
                elif ip_addresses:
                    node_records = [n for n in all_nodes if n["PrivateIp"] in node_ids]
                elif custom_filter:
                    raise RuntimeError(
                        "custom_filter is not supported with run_only mode (--dry-run)"
                    )
                elif request_id:
                    node_records = [n for n in all_nodes if n["RequestId"] in node_ids]
                else:
                    raise RuntimeError(
                        "Please specify at least one of nodes, names, node_ids, hostnames, ip_addresses, custom_filter or request_id"
                    )

            else:
                node_records = [_node_to_ccnode(n) for n in nodes]

            ret.sets = [NodeCreationResultSet(added=len(node_records))]

            for n in nodes or []:
                n.exists = True
                n.target_state = ht.NodeStatus("Started")
                if not n.delayed_node_id.node_id:
                    n.delayed_node_id.node_id = ht.NodeId("dryrun-" + str(uuid.uuid4()))

            self._read_only_nodes[ht.OperationId(ret.operation_id)] = node_records
            return ret

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter, request_id
        )

        logging.fine(json.dumps(request.to_dict()))

        http_response, result = self.clusters_module.start_nodes(
            self.session, self.cluster_name, request
        )
        self._log_response(http_response, result)
        return result

    @notreadonly
    def terminate_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[ht.NodeName]] = None,
        node_ids: Optional[List[ht.NodeId]] = None,
        hostnames: Optional[List[ht.Hostname]] = None,
        ip_addresses: Optional[List[ht.IpAddress]] = None,
        custom_filter: str = None,
        request_id: Optional[str] = None,
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter, request_id
        )

        logging.fine(json.dumps(request.to_dict()))

        http_response, result = self.clusters_module.terminate_nodes(
            self.session, self.cluster_name, request
        )
        self._log_response(http_response, result)
        return result

    @notreadonly
    def delete_nodes(self, nodes: List[Node]) -> NodeManagementResult:
        return self.shutdown_nodes(nodes)

    @notreadonly
    def retry_failed_nodes(self) -> NodeManagementResult:
        _request_context = cyclecloud.api.clusters._RequestContext()

        _path_parameters = {}
        _path_parameters["cluster"] = self.cluster_name
        _request_context.path = "/cloud/actions/retry/{cluster}".format(
            **_path_parameters
        )

        _query: Dict = {}
        _headers: Dict = {}

        _body = None

        _responses = []
        _responses.append((200, "object", lambda v: v))

        _status: cyclecloud.session.ResponseStatus
        _status, _response = self.session.request(
            _request_context,
            "POST",
            query=_query,
            headers=_headers,
            body=_body,
            expected_responses=_responses,
        )
        if _status.status_code < 200 or _status.status_code > 299:
            raise RuntimeError(
                "Attempt to retry failed nodes did not succeed: %s" % _response
            )

        return _status, _response

    def _log_response(self, s: ResponseStatus, r: Any) -> None:
        if logging.getLogger().getEffectiveLevel() > logging.DEBUG:
            return

        import inspect

        current_frame = inspect.currentframe()
        caller_frame = inspect.getouterframes(current_frame, 2)
        caller = "[{}]".format(caller_frame[1].function)

        as_json = json.dumps(r.to_dict())

        logging.debug(
            "[%s] Response: Status=%s -> %s", caller, s.status_code, as_json[:100],
        )

        if logging.getLogger().getEffectiveLevel() > logging.FINE:
            return

        logging.fine(
            "[%s] Full response: Status=%s -> %s", caller, s.status_code, as_json,
        )

    @notreadonly
    def _node_management_request(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[ht.NodeName]] = None,
        node_ids: Optional[List[ht.NodeId]] = None,
        hostnames: Optional[List[ht.Hostname]] = None,
        ip_addresses: Optional[List[ht.IpAddress]] = None,
        custom_filter: str = None,
        request_id: Optional[str] = None,
    ) -> NodeManagementRequest:

        args = [
            a
            for a in [nodes, names, node_ids, hostnames, ip_addresses, custom_filter]
            if a
        ]
        if len(args) != 1:
            raise RuntimeError(
                "You must specify one and only one of the following: {}".format(args)
            )

        request = NodeManagementRequest()

        if names:
            request.names = names

        if node_ids:
            request.ids = node_ids

        if hostnames:
            request.hostnames = hostnames

        if ip_addresses:
            request.ip_addresses = ip_addresses

        if custom_filter:
            request.filter = custom_filter

        if nodes:
            request.ids = [n.delayed_node_id.node_id for n in nodes]

        if request_id:
            request.request_id = request_id

        request.validate()

        return request

    def __str__(self) -> str:
        return "ClusterBinding(v=7.9, cluster='{cluster}', url='{url}', username='{username}', verify={verify})".format(
            cluster=self.cluster_name,
            url=str(self.session._config.get("url")),
            username=str(self.session._config.get("username")),
            verify=str(self.session._config.get("verify_certificates")),
        )

    def __repr__(self) -> str:
        return str(self)


def _get_session(config: Dict) -> requests.sessions.Session:
    try:
        retries = 3
        while retries > 0:
            try:
                if not config["verify_certificates"]:
                    urllib3.disable_warnings(InsecureRequestWarning)

                s = requests.session()
                s.auth = (config["username"], config["password"])
                # TODO apparently this does nothing...
                # s.timeout = config["cycleserver"]["timeout"]
                s.verify = config[
                    "verify_certificates"
                ]  # Should we auto-accept unrecognized certs?
                s.headers = CaseInsensitiveDict(
                    {"X-Cycle-Client-Version": "%s-cli:%s" % ("hpc-autoscale", "0.0.0")}
                )

                return s
            except requests.exceptions.SSLError:
                retries = retries - 1
                if retries < 1:
                    raise
    except ImportError:
        raise

    raise AssertionError(
        "Could not connect to CycleCloud. Please see the log for more details."
    )
