# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

import json
from typing import Any, Callable, Dict, List, Optional

import cyclecloud.api.clusters
import hpc.autoscale.hpclogging as logging
import requests
import urllib3
from cyclecloud.model.NodeCreationRequestModule import NodeCreationRequest
from cyclecloud.model.NodeCreationRequestSetDefinitionModule import (
    NodeCreationRequestSetDefinition,
)
from cyclecloud.model.NodeCreationRequestSetModule import NodeCreationRequestSet
from cyclecloud.model.NodeCreationResultModule import NodeCreationResult
from cyclecloud.model.NodeListModule import NodeList
from cyclecloud.model.NodeManagementRequestModule import NodeManagementRequest
from cyclecloud.model.NodeManagementResultModule import NodeManagementResult
from cyclecloud.session import ResponseStatus
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.codeanalysis import hpcwrap, hpcwrapclass
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import partition
from requests.structures import CaseInsensitiveDict
from urllib3.exceptions import InsecureRequestWarning

logger = logging.getLogger("cyclecloud.clustersapi")


class ReadOnlyModeException(RuntimeError):
    pass


def notreadonly(method: Callable) -> Callable:
    def readonlywrapper(*args: Any) -> Optional[Any]:
        if args[0].read_only:
            raise ReadOnlyModeException(
                "Can not call {} in read only mode.".format(method.__name__)
            )
        return method(*args)

    return readonlywrapper


@hpcwrapclass
class ClusterBinding(ClusterBindingInterface):
    def __init__(
        self,
        cluster_name: ht.ClusterName,
        session: Any,
        client: Any,
        clusters_module: Any = None,
        read_only: bool = False,
    ) -> None:
        self.__cluster_name = cluster_name
        self.session = session
        self.client = client
        self.clusters_module: cyclecloud.api.clusters = cyclecloud.api.clusters
        if clusters_module:
            self.clusters_module = clusters_module  # type: ignore
        self.read_only = read_only

    @property
    def cluster_name(self) -> ht.ClusterName:
        return self.__cluster_name

    @hpcwrap
    @notreadonly
    def create_nodes(self, nodes: List[Node]) -> NodeCreationResult:
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
            ),
        )

        for key, p_nodes in p_nodes_dict.items():
            nodearray, vm_size, pg, _ = key
            request_set = NodeCreationRequestSet()

            request_set.nodearray = nodearray
            request_set.count = len(p_nodes)
            request_set.placement_group_id = pg
            request_set.definition = NodeCreationRequestSetDefinition()
            request_set.definition.machine_type = vm_size

            if p_nodes[0].node_attribute_overrides:
                request_set.node_attributes = p_nodes[0].node_attribute_overrides

            creation_request.sets.append(request_set)

        creation_request.validate()
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
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter
        )
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
        return result

    def get_nodes(
        self,
        operation_id: Optional[ht.OperationId] = None,
        request_id: Optional[ht.RequestId] = None,
    ) -> NodeList:

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
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter
        )
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
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter
        )
        http_response, result = self.clusters_module.shutdown_nodes(
            self.session, self.cluster_name, request
        )

        self._log_response(http_response, result)
        return result

    @notreadonly
    def start_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[ht.NodeName]] = None,
        node_ids: Optional[List[ht.NodeId]] = None,
        hostnames: Optional[List[ht.Hostname]] = None,
        ip_addresses: Optional[List[ht.IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter
        )
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
    ) -> NodeManagementResult:

        request = self._node_management_request(
            nodes, names, node_ids, hostnames, ip_addresses, custom_filter
        )
        http_response, result = self.clusters_module.terminate_nodes(
            self.session, self.cluster_name, request
        )
        self._log_response(http_response, result)
        return result

    @notreadonly
    def delete_nodes(self, nodes: List[Node]) -> NodeManagementResult:
        return self.shutdown_nodes(nodes)

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
