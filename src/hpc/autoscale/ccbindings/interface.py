# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#
import abc
from abc import ABC, abstractproperty
from typing import List, Optional

from cyclecloud.model.ClusterStatusModule import ClusterStatus
from cyclecloud.model.NodeCreationResultModule import NodeCreationResult
from cyclecloud.model.NodeListModule import NodeList
from cyclecloud.model.NodeManagementResultModule import NodeManagementResult
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
from hpc.autoscale.node import node


class ClusterBindingInterface(ABC):
    @abstractproperty
    def cluster_name(self) -> ClusterName:
        ...

    @abc.abstractmethod
    def create_nodes(self, nodes: List[node.Node]) -> NodeCreationResult:
        pass

    @abc.abstractmethod
    def deallocate_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    @abc.abstractmethod
    def get_cluster_status(self, nodes: bool = False) -> ClusterStatus:
        pass

    @abc.abstractmethod
    def get_nodes(
        self,
        operation_id: Optional[OperationId] = None,
        request_id: Optional[RequestId] = None,
    ) -> NodeList:
        pass

    @abc.abstractmethod
    def remove_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    @abc.abstractmethod
    def scale(
        self,
        nodearray: NodeArrayName,
        total_core_count: Optional[int] = None,
        total_node_count: Optional[int] = None,
    ) -> None:
        pass

    @abc.abstractmethod
    def shutdown_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    @abc.abstractmethod
    def start_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    @abc.abstractmethod
    def terminate_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    @abc.abstractmethod
    def delete_nodes(self, nodes: List[node.Node]) -> NodeManagementResult:
        pass
