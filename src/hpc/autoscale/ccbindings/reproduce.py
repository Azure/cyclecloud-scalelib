import json
import os
from typing import Dict, List, Optional

from cyclecloud.model.ClusterStatusModule import ClusterStatus
from cyclecloud.model.NodeCreationResultModule import NodeCreationResult
from cyclecloud.model.NodeListModule import NodeList
from cyclecloud.model.NodeManagementResultModule import NodeManagementResult

from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.hpctypes import (
    ClusterName,
    Hostname,
    IpAddress,
    NodeArrayName,
    NodeId,
    NodeName,
    OperationId,
    RequestId,
)
from hpc.autoscale.node.node import Node


class ReproduceFromResponse(ClusterBindingInterface):
    """
    This class makes it easier to reproduce a bug by mocking the cluster binding cluster_status/get_nodes
    response.
    Simply add {
    "cl
    }
    """

    def __init__(self, config: Dict) -> None:
        self.config = config
        assert (
            "cluster_response" in config
        ), "cluster_response must be specified when using _mock_bindings = { name = 'reproduce' }"
        self.cluster_response = config.get("cluster_response")
        if isinstance(self.cluster_response, str):
            self.cluster_response = json.load(
                open(os.path.expanduser(self.cluster_response))
            )
        self.node_list = config.get("node_list")
        if isinstance(self.node_list, str):
            self.node_list = json.load(open(os.path.expanduser(self.node_list)))

    @property
    def cluster_name(self) -> ClusterName:
        return ClusterName("mock-reproduce-cluster")

    def get_cluster_status(self, nodes: bool = False) -> ClusterStatus:
        ret: ClusterStatus = ClusterStatus.from_dict(self.cluster_response)  # type: ignore
        if ret.nodes is None:
            ret.nodes = []
        return ret

    def get_nodes(
        self,
        operation_id: Optional[OperationId] = None,
        request_id: Optional[RequestId] = None,
    ) -> NodeList:
        if self.config.get(self.node_list):
            return NodeList.from_dict(self.node_list)  # type: ignore
        return NodeList(nodes=[])

    def create_nodes(
        self, nodes: List[Node], request_id: Optional[str] = None
    ) -> NodeCreationResult:
        assert False

    def deallocate_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: Optional[str] = None,
    ) -> NodeManagementResult:
        assert False

    def delete_nodes(self, nodes: List[Node]) -> NodeManagementResult:
        assert False

    def remove_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: Optional[str] = None,
    ) -> NodeManagementResult:
        assert False

    def retry_failed_nodes(self) -> NodeManagementResult:
        assert False

    def scale(
        self,
        nodearray: NodeArrayName,
        total_core_count: Optional[int] = None,
        total_node_count: Optional[int] = None,
    ) -> None:
        assert False

    def start_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> NodeManagementResult:
        assert False

    def shutdown_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: Optional[str] = None,
    ) -> NodeManagementResult:
        assert False

    def terminate_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: Optional[str] = None,
    ) -> NodeManagementResult:
        assert False
