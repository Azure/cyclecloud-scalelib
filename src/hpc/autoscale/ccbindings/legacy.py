# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

from typing import Any, Dict, List, Optional

import cyclecloud.api.clusters
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
from requests.structures import CaseInsensitiveDict
from urllib3.exceptions import InsecureRequestWarning

import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import partition

logger = logging.getLogger("cyclecloud.clustersapi")


class ClusterBinding(ClusterBindingInterface):
    def __init__(
        self,
        cluster_name: ht.ClusterName,
        session: Any,
        client: Any,
        clusters_module: Any = None,
    ) -> None:
        self.__cluster_name = cluster_name
        self.session = session
        self.client = client
        self.clusters_module = clusters_module or cyclecloud.api.clusters

    @property
    def cluster_name(self) -> ht.ClusterName:
        return self.__cluster_name

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
        _http_response, result = self.clusters_module.create_nodes(
            self.session, self.cluster_name, creation_request
        )
        return result

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
        _http_response, result = self.clusters_module.deallocate_nodes(
            self.session, self.cluster_name, request
        )
        return result

    def get_cluster_status(self, nodes: bool = False) -> cyclecloud.model.ClusterStatus:
        _http_response, result = self.clusters_module.get_cluster_status(
            self.session, self.cluster_name, nodes
        )
        return result

    def get_nodes(
        self,
        operation_id: Optional[ht.OperationId] = None,
        request_id: Optional[ht.RequestId] = None,
    ) -> NodeList:

        _http_response, result = self.clusters_module.get_nodes(
            self.session, self.cluster_name, operation_id, request_id
        )
        return result

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
        _http_response, result = self.clusters_module.remove_nodes(
            self.session, self.cluster_name, request
        )
        return result

    def scale(
        self,
        nodearray: ht.NodeArrayName,
        total_core_count: Optional[int] = None,
        total_node_count: Optional[int] = None,
    ) -> NodeCreationResult:

        _http_response, result = self.clusters_module.scale(
            self.session,
            self.cluster_name,
            nodearray,
            total_core_count,
            total_node_count,
        )
        return result

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
        _http_response, result = self.clusters_module.shutdown_nodes(
            self.session, self.cluster_name, request
        )
        return result

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
        _http_response, result = self.clusters_module.start_nodes(
            self.session, self.cluster_name, request
        )
        return result

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
        _http_response, result = self.clusters_module.terminate_nodes(
            self.session, self.cluster_name, request
        )
        return result

    def delete_nodes(self, nodes: List[Node]) -> NodeManagementResult:
        raise RuntimeError()

    def _node_management_request(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[ht.NodeName]] = None,
        node_ids: Optional[List[ht.NodeId]] = None,
        hostnames: Optional[List[ht.Hostname]] = None,
        ip_addresses: Optional[List[ht.IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:

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
    # TODO
    raise AssertionError("Please contact support.")
