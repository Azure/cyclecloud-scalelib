# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

import uuid
from copy import deepcopy
from typing import Any, Dict, List, Optional
from uuid import uuid4

import cyclecloud  # noqa
from cyclecloud.model.ClusterNodearrayStatusModule import ClusterNodearrayStatus
from cyclecloud.model.ClusterStatusModule import ClusterStatus
from cyclecloud.model.NodearrayBucketStatusDefinitionModule import (
    NodearrayBucketStatusDefinition,
)
from cyclecloud.model.NodearrayBucketStatusModule import NodearrayBucketStatus
from cyclecloud.model.NodearrayBucketStatusVirtualMachineModule import (
    NodearrayBucketStatusVirtualMachine,
)
from cyclecloud.model.NodeCreationResultModule import NodeCreationResult
from cyclecloud.model.NodeCreationResultSetModule import NodeCreationResultSet
from cyclecloud.model.NodeManagementResultModule import NodeManagementResult

import hpc.autoscale.hpclogging as logging
from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.hpctypes import (
    ClusterName,
    Hostname,
    IpAddress,
    Memory,
    NodeArrayName,
    NodeId,
    NodeName,
    NodeStatus,
    OperationId,
    PlacementGroup,
    RequestId,
    VMSize,
)
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import partition

logger = logging.getLogger("cyclecloud.clustersapi")


@hpcwrapclass
class MockClusterBinding(ClusterBindingInterface):
    def __init__(self, cluster_name: str = "clusty") -> None:
        self.__cluster_name = ClusterName(cluster_name)
        self.nodes: Dict[NodeName, Node] = {}
        self.nodearrays: Dict[NodeArrayName, ClusterNodearrayStatus] = {}
        self.operations: Dict[OperationId, "MockNodeManagementResult"] = {}
        self.max_core_count = 10000
        self.max_count = 1000
        self.region = "westus"
        self.subnet_id = "subnetid1"
        self.state = "Started"
        self.target_state = "Started"

    @property
    def cluster_name(self) -> ClusterName:
        return self.__cluster_name

    def add_nodearray(
        self,
        name: NodeArrayName,
        resources: Dict,
        max_core_count: int = 1_000_000,
        max_count: int = 100_000,
        spot: bool = False,
    ) -> ClusterNodearrayStatus:
        nodearray_status = ClusterNodearrayStatus()
        nodearray_status.buckets = []
        nodearray_status.name = name
        nodearray_status.max_core_count = max_core_count
        nodearray_status.max_count = max_count
        nodearray_status.nodearray = {
            "Name": name,
            "Status": self.state,
            "Region": self.region,
            "SubnetId": self.subnet_id,
            "Configuration": {"autoscale": {"resources": resources}},
            "Interruptible": spot,
        }
        for attr in dir(nodearray_status):
            if attr[0].isalpha() and "count" in attr:
                assert (
                    getattr(nodearray_status, attr) is not None
                ), "{} was not defined".format(attr)

        self.nodearrays[name] = nodearray_status
        return nodearray_status

    def add_bucket(
        self,
        noderray_name: NodeArrayName,
        vm_size: VMSize,
        vcpu_count: int,
        memory: Memory,
        max_count: int,
        available_count: int,
        location: str = "westus2",
        family_consumed_core_count: Optional[int] = None,
        family_quota_core_count: Optional[int] = None,
        family_quota_count: Optional[int] = None,
        regional_consumed_core_count: Optional[int] = None,
        regional_quota_core_count: Optional[int] = None,
        regional_quota_count: Optional[int] = None,
        max_placement_group_size: int = 100,
        placement_groups: Optional[List[str]] = None,
    ) -> NodearrayBucketStatus:
        def pick(a: Optional[int], b: Optional[int]) -> int:
            if a is not None:
                return a
            assert b is not None
            return b

        assert (
            vm_sizes.get_aux_vm_size_info(location, vm_size).vm_family != "unknown"
        ), vm_size
        assert isinstance(memory, Memory)
        if noderray_name not in self.nodearrays:
            raise RuntimeError("Please call add_nodearray first.")

        nodearray_status = self.nodearrays[noderray_name]
        bucket_status = NodearrayBucketStatus()
        bucket_status.bucket_id = str(uuid.uuid4())
        bucket_status.available_count = available_count
        bucket_status.max_count = max_count

        bucket_status.active_count = max_count - available_count
        bucket_status.active_nodes = []
        bucket_status.max_placement_group_size = max_placement_group_size
        bucket_status.family_consumed_core_count = pick(
            family_consumed_core_count, bucket_status.active_count * vcpu_count
        )

        bucket_status.family_quota_count = pick(family_quota_count, available_count)
        bucket_status.family_quota_core_count = pick(
            family_quota_core_count, bucket_status.family_quota_count * vcpu_count
        )

        bucket_status.regional_quota_count = pick(
            regional_quota_count, bucket_status.family_quota_count
        )
        bucket_status.quota_count = bucket_status.family_quota_count

        bucket_status.active_core_count = bucket_status.active_count * vcpu_count
        bucket_status.regional_consumed_core_count = pick(
            regional_consumed_core_count, bucket_status.family_consumed_core_count
        )
        bucket_status.consumed_core_count = bucket_status.family_consumed_core_count
        bucket_status.available_core_count = bucket_status.available_count * vcpu_count
        bucket_status.max_core_count = bucket_status.max_count * vcpu_count
        bucket_status.regional_quota_core_count = pick(
            regional_quota_core_count, bucket_status.family_quota_core_count
        )
        bucket_status.quota_core_count = bucket_status.family_quota_core_count
        bucket_status.max_placement_group_core_size = (
            bucket_status.max_placement_group_size * vcpu_count
        )

        bucket_status.definition = NodearrayBucketStatusDefinition()
        bucket_status.definition.machine_type = vm_size
        bucket_status.virtual_machine = NodearrayBucketStatusVirtualMachine()
        bucket_status.virtual_machine.vcpu_count = vcpu_count
        bucket_status.virtual_machine.memory = memory.convert_to("m").value
        # TODO RDH
        bucket_status.virtual_machine.infiniband = False
        bucket_status.placement_groups = placement_groups

        for attr in dir(bucket_status):
            if attr[0].isalpha() and "count" in attr:
                assert (
                    getattr(bucket_status, attr) is not None
                ), "{} was not defined".format(attr)

        nodearray_status.buckets.append(bucket_status)

        return bucket_status

    def add_node(
        self,
        name: NodeName,
        nodearray: NodeArrayName,
        vm_size: VMSize = None,
        state: NodeStatus = NodeStatus("Started"),
        hostname: Optional[Hostname] = None,
        spot: bool = False,
        placement_group: str = None,
    ) -> Node:
        assert nodearray in self.nodearrays
        nodearray_status = self.nodearrays[nodearray]
        nodearray_record = nodearray_status.nodearray
        bucket = None

        if len(nodearray_status.buckets) == 1:
            bucket = nodearray_status.buckets[0]
        else:
            for b in nodearray_status.buckets:
                if b.definition.machine_type == vm_size:
                    bucket = b
                    break

        if bucket is None:
            raise RuntimeError(
                "More than one bucket found for nodearray {}, please specify a vm_size".format(
                    nodearray
                )
            )

        resources = (
            nodearray_record.get("Configuration", {})
            .get("autoscale", {})
            .get("resources", {})
        )

        vm_size = VMSize(bucket.definition.machine_type)

        _update_bucket_counts(bucket, 1)

        self.nodes[name] = Node(
            node_id=DelayedNodeId(name, node_id=NodeId(str(uuid4()))),
            name=name,
            nodearray=nodearray,
            bucket_id=bucket.bucket_id,
            hostname=hostname,
            private_ip=None,
            vm_size=vm_size,
            location=nodearray_record["Region"],
            spot=spot,
            vcpu_count=bucket.virtual_machine.vcpu_count,
            memory=Memory(bucket.virtual_machine.memory, "g"),
            infiniband=False,
            state=state,
            power_state=state,
            exists=True,
            placement_group=PlacementGroup(placement_group)
            if placement_group
            else None,
            managed=True,
            resources=resources,
        )
        return self.nodes[name]

    def create_nodes(self, new_nodes: List[Node]) -> NodeCreationResult:
        for node in new_nodes:
            assert node.name not in self.nodes, "{} already in {}".format(
                node.name, list(self.nodes)
            )
            self.nodes[node.name] = node.clone()

        result = NodeCreationResult()
        result.sets = []
        b_nodes = partition(new_nodes, lambda n: n.bucket_id)
        for _bucket_id, nodes_per_bucket in b_nodes.items():
            result_set = NodeCreationResultSet()
            result_set.added = len(nodes_per_bucket)
            result.sets.append(result_set)

        result.operation_id = OperationId(str(uuid.uuid4()))

        self.operations[result.operation_id] = MockNodeManagementResult(
            result.operation_id, new_nodes
        )

        return result

    def shutdown_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        assert names
        result = NodeManagementResult()
        result.nodes = []
        for name in names:
            if name in self.nodes:
                result.nodes.append(self.nodes.pop(name))
        result.operation_id = OperationId(str(uuid.uuid4()))
        self.operations[result.operation_id] = result
        return result

    def deallocate_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        raise NotImplementedError()

    def get_cluster_status(self, nodes: bool = False) -> ClusterStatus:
        response = ClusterStatus()
        response.max_core_count = self.max_core_count
        response.max_count = self.max_count
        response.state = self.state
        response.target_state = self.target_state

        response.nodearrays = list(self.nodearrays.values())
        # TODO RDH nodes by bucket
        if nodes:
            response.nodes = [_node_to_ccnode(n) for n in self.nodes.values()]

        return response

    def get_nodes(
        self,
        operation_id: Optional[OperationId] = None,
        request_id: Optional[RequestId] = None,
    ) -> "MockNodeManagementResult":
        # TODO what is the actual error?
        if not operation_id:
            all_nodes: List[Node] = []
            for op in self.operations.values():
                all_nodes.extend(op._nodes)
            return MockNodeManagementResult(OperationId(""), all_nodes)

        if operation_id not in self.operations:
            raise RuntimeError(
                "Operation not found: {} vs {}".format(
                    operation_id, self.operations.keys()
                )
            )
        assert operation_id
        assert operation_id in self.operations
        return self.operations[operation_id]

    def remove_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        raise NotImplementedError()

    def start_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        raise NotImplementedError()

    def terminate_nodes(
        self,
        nodes: Optional[List[Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        raise NotImplementedError()

    def delete_nodes(self, nodes: List[Node]) -> NodeManagementResult:
        raise NotImplementedError()

    def scale(
        self,
        nodearray: NodeArrayName,
        total_core_count: Optional[int] = None,
        total_node_count: Optional[int] = None,
    ) -> None:
        raise NotImplementedError()


NodeRecord = Dict[str, Any]


@hpcwrapclass
class MockNodeManagementResult:
    def __init__(self, operation_id: OperationId, nodes: List[Node]) -> None:
        self.operation_id = operation_id
        if nodes:
            assert isinstance(nodes[0], Node)
        self._nodes = nodes

    @property
    def nodes(self) -> List[NodeRecord]:
        return list([_node_to_ccnode(n) for n in self._nodes])


def _node_to_ccnode(n: Node) -> NodeRecord:
    return {
        "Name": n.name,
        "Template": n.nodearray,
        "MachineType": n.vm_size,
        "Hostname": n.hostname,
        "PrivateIp": n.private_ip,
        "Region": n.location,
        "NodeId": n.delayed_node_id.node_id,
        # TODO eventually test custom core count
        # "CoreCount":
        # "Memory":
        "Status": n.state,
        "PlacementGroupId": n.placement_group,
        "Infiniband": n.infiniband,
        "Configuration": {
            "Configuration": {"autoscale": {"resources": deepcopy(n.resources)}}
        },
    }


def _update_bucket_counts(bucket: NodearrayBucketStatus, num_nodes: int) -> None:
    vcpu_count = bucket.virtual_machine.vcpu_count

    for attr in dir(bucket):
        if not attr[0].isalpha() or "count" not in attr:
            continue

        mag = vcpu_count if "core" in attr else num_nodes
        current_value = getattr(bucket, attr)
        if "consumed" in attr or "active" in attr:
            setattr(bucket, attr, current_value + mag)
        elif "available" in attr:
            setattr(bucket, attr, current_value - mag)
        elif "max" in attr or "quota" in attr:
            pass
        else:
            assert False, attr
    pass
