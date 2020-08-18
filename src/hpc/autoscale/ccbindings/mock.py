# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

import uuid
from copy import deepcopy
from typing import Any, Dict, List, Optional
from uuid import uuid4

from frozendict import frozendict

import cyclecloud  # noqa
import hpc.autoscale.hpclogging as logging
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
from cyclecloud.model.NodeListModule import NodeList
from cyclecloud.model.NodeManagementResultModule import NodeManagementResult
from cyclecloud.model.NodeManagementResultNodeModule import NodeManagementResultNode
from cyclecloud.model.PlacementGroupStatusModule import PlacementGroupStatus
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
NodeRecord = Dict[str, Any]


@hpcwrapclass
class MockClusterBinding(ClusterBindingInterface):
    def __init__(self, cluster_name: str = "clusty") -> None:
        self.__cluster_name = ClusterName(cluster_name)
        self.nodes: Dict[NodeName, Node] = {}
        self.nodearrays: Dict[NodeArrayName, ClusterNodearrayStatus] = {}
        self.operations: Dict[OperationId, "MockNodeManagementResult"] = {}
        self.max_core_count = 10000
        self.max_count = 1000
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
        location: str = "westus2",
        max_core_count: int = 1_000_000,
        max_count: int = 100_000,
        spot: bool = False,
        software_configuration: Dict = {},
    ) -> ClusterNodearrayStatus:
        nodearray_status = ClusterNodearrayStatus()
        nodearray_status.buckets = []
        nodearray_status.name = name
        nodearray_status.max_core_count = max_core_count
        nodearray_status.max_count = max_count
        nodearray_status.nodearray = {
            "Name": name,
            "Status": self.state,
            "Region": location,
            "SubnetId": self.subnet_id,
            "Configuration": {"autoscale": {"resources": resources}},
            "Interruptible": spot,
        }

        nodearray_status.nodearray["Configuration"].update(software_configuration)

        for attr in dir(nodearray_status):
            if attr[0].isalpha() and "count" in attr:
                assert (
                    getattr(nodearray_status, attr) is not None
                ), "{} was not defined".format(attr)

        self.nodearrays[name] = nodearray_status
        return nodearray_status

    def add_bucket(
        self,
        nodearray_name: NodeArrayName,
        vm_size: VMSize,
        max_count: int,
        available_count: int,
        family_consumed_core_count: Optional[int] = None,
        family_quota_core_count: Optional[int] = 1_000_000,
        family_quota_count: Optional[int] = 10_000,
        regional_consumed_core_count: Optional[int] = None,
        regional_quota_core_count: Optional[int] = 1_000_000,
        regional_quota_count: Optional[int] = 10_000,
        max_placement_group_size: int = 100,
        placement_groups: Optional[List[str]] = None,
    ) -> NodearrayBucketStatus:
        def pick(a: Optional[int], b: Optional[int]) -> int:
            if a is not None:
                return a
            assert b is not None
            return b

        if nodearray_name not in self.nodearrays:
            raise RuntimeError("Please call add_nodearray first.")

        nodearray_status = self.nodearrays[nodearray_name]
        location = nodearray_status.nodearray["Region"]

        aux_info = vm_sizes.get_aux_vm_size_info(location, vm_size)

        na_max_count = nodearray_status.max_count
        na_max_core_count = nodearray_status.max_core_count
        if na_max_count * aux_info.vcpu_count < na_max_core_count:
            na_max_core_count = na_max_count * aux_info.vcpu_count
        else:
            na_max_count = na_max_core_count // aux_info.vcpu_count

        assert aux_info.vm_family != "unknown", vm_size

        vcpu_count = aux_info.vcpu_count

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

        bucket_status.family_quota_count = pick(family_quota_count, max_count)
        bucket_status.family_quota_core_count = pick(
            family_quota_core_count, bucket_status.family_quota_count * vcpu_count
        )

        bucket_status.regional_quota_count = pick(regional_quota_count, na_max_count)
        bucket_status.quota_count = bucket_status.family_quota_count

        bucket_status.active_core_count = bucket_status.active_count * vcpu_count
        bucket_status.regional_consumed_core_count = pick(
            regional_consumed_core_count, bucket_status.family_consumed_core_count
        )
        bucket_status.consumed_core_count = bucket_status.family_consumed_core_count
        bucket_status.available_core_count = bucket_status.available_count * vcpu_count
        bucket_status.max_core_count = bucket_status.max_count * vcpu_count
        bucket_status.regional_quota_core_count = pick(
            regional_quota_core_count, na_max_core_count
        )
        bucket_status.quota_core_count = bucket_status.family_quota_core_count
        bucket_status.max_placement_group_core_size = (
            bucket_status.max_placement_group_size * vcpu_count
        )

        assert bucket_status.active_core_count <= bucket_status.max_core_count
        assert bucket_status.active_count <= bucket_status.max_count
        assert (
            bucket_status.family_consumed_core_count
            <= bucket_status.family_quota_core_count
        )
        assert (
            bucket_status.regional_consumed_core_count
            <= bucket_status.regional_quota_core_count
        )

        bucket_status.definition = NodearrayBucketStatusDefinition()
        bucket_status.definition.machine_type = vm_size
        bucket_status.virtual_machine = NodearrayBucketStatusVirtualMachine()
        bucket_status.virtual_machine.vcpu_count = vcpu_count
        bucket_status.virtual_machine.memory = aux_info.memory.convert_to("m").value

        bucket_status.virtual_machine.infiniband = aux_info.infiniband

        bucket_status.placement_groups = []
        for pg in placement_groups or []:
            bucket_status.placement_groups.append(
                PlacementGroupStatus(name=pg, active_core_count=0, active_count=0)
            )

        for attr in dir(bucket_status):
            if attr[0].isalpha() and "count" in attr:
                assert (
                    getattr(bucket_status, attr) is not None
                ), "{} was not defined".format(attr)

        nodearray_status.buckets.append(bucket_status)

        return bucket_status

    def _get_buckets(
        self, location: str, vm_family: str
    ) -> List[NodearrayBucketStatus]:
        ret = []
        for _, cluster_nodearray_status in self.nodearrays.items():

            if not cluster_nodearray_status.nodearray.get("Region").lower() == location:
                continue

            for bucket in cluster_nodearray_status.buckets:
                vm_size = bucket.definition.machine_type
                aux_info = vm_sizes.get_aux_vm_size_info(location, vm_size)
                if aux_info.vm_family == vm_family:
                    ret.append(bucket)
        return ret

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
        bucket.active_nodes.append(name)

        if placement_group is not None:
            placement_group = PlacementGroup(placement_group)
            for pg_status in bucket.placement_groups:
                if pg_status.name == placement_group:
                    pg_status.active_count += 1
                    pg_status.active_core_count += bucket.virtual_machine.vcpu_count

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
            placement_group=placement_group,
            managed=True,
            resources=resources,
            software_configuration=frozendict(
                nodearray_record.get("Configuration", {})
            ),
        )
        op_id = OperationId(str(uuid4()))
        self.operations[op_id] = MockNodeManagementResult(op_id, [self.nodes[name]])

        return self.nodes[name]

    def create_nodes(self, new_nodes: List[Node]) -> NodeCreationResult:
        for node in new_nodes:
            assert node.name not in self.nodes, "{} already in {}".format(
                node.name, list(self.nodes)
            )
            self.nodes[node.name] = node.clone()

            for bucket in self._get_buckets(node.location, node.vm_family):
                if bucket.bucket_id == node.bucket_id:
                    bucket.active_nodes.append(node.name)
                bucket.active_core_count += node.vcpu_count
                bucket.available_core_count -= node.vcpu_count
                bucket.active_count = bucket.active_core_count // node.vcpu_count
                bucket.available_count = bucket.available_core_count // node.vcpu_count

                bucket.family_consumed_core_count += node.vcpu_count
                bucket.consumed_core_count += node.vcpu_count
                bucket.regional_consumed_core_count += node.vcpu_count

        for nodearray in self.nodearrays.values():
            active_count = sum([len(b.active_nodes) for b in nodearray.buckets])
            active_core_count = sum(
                [
                    len(b.active_nodes) * b.virtual_machine.vcpu_count
                    for b in nodearray.buckets
                ]
            )
            available_count = nodearray.max_count - active_count
            available_core_count = nodearray.max_core_count - active_core_count
            for bucket in nodearray.buckets:
                bucket.available_count = min(bucket.available_count, available_count)
                bucket.available_core_count = min(
                    bucket.available_core_count, available_core_count
                )

        result = NodeCreationResult()
        result.sets = []
        b_nodes = partition(new_nodes, lambda n: n.bucket_id)
        for _bucket_id, nodes_per_bucket in b_nodes.items():
            result_set = NodeCreationResultSet()
            result_set.added = len(nodes_per_bucket)
            result.sets.append(result_set)

        result.operation_id = OperationId(str(uuid.uuid4()))

        cloned_nodes = [n.clone() for n in new_nodes]
        for n in new_nodes:
            # TODO add node statuses as constants / util functions.
            n.state = NodeStatus("Allocating")

        self.operations[result.operation_id] = MockNodeManagementResult(
            result.operation_id, cloned_nodes
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
        if not names:
            assert nodes
            names = [n.name for n in nodes]
            nodes = None

        result_nodes: List[Node] = []
        for name in names:
            if name in self.nodes:
                node = self.nodes[name]
                node.state = NodeStatus("Terminating")
                result_nodes.append(node)
        result = MockNodeManagementResult(OperationId(str(uuid.uuid4())), result_nodes)
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
            response.nodes = self.get_nodes().nodes

        return response

    def get_nodes(
        self,
        operation_id: Optional[OperationId] = None,
        request_id: Optional[RequestId] = None,
    ) -> NodeList:

        # TODO what is the actual error?
        if not operation_id:
            all_nodes = _nodes_to_ccnode(list(self.nodes.values()))
            return NodeList(nodes=all_nodes)

        if operation_id not in self.operations:
            raise RuntimeError(
                "Operation not found: {} vs {}".format(
                    operation_id, self.operations.keys()
                )
            )
        assert operation_id
        assert operation_id in self.operations

        mgmt_result = self.operations[operation_id]
        cc_nodes = [
            _node_to_ccnode(self.nodes[n.name])
            for n in mgmt_result.nodes
            if n.name in self.nodes
        ]
        return NodeList(nodes=cc_nodes, operation_id=operation_id)

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
        return self.shutdown_nodes(nodes)

    def scale(
        self,
        nodearray: NodeArrayName,
        total_core_count: Optional[int] = None,
        total_node_count: Optional[int] = None,
    ) -> None:
        raise NotImplementedError()

    def __str__(self) -> str:
        return "MockBindings()"

    def __repr__(self) -> str:
        return str(self)


@hpcwrapclass
class MockNodeManagementResult(NodeManagementResult):
    def __init__(self, operation_id: OperationId, nodes: List[Node]) -> None:

        if nodes:
            assert isinstance(nodes[0], Node)

        self._nodes = nodes
        mgmt_nodes = list(
            [
                NodeManagementResultNode(
                    name=n.name,
                    id=n.delayed_node_id.node_id,
                    status="Error" if n.state == "Failure" else "OK",
                )
                for n in self._nodes
            ]
        )
        NodeManagementResult.__init__(self, nodes=mgmt_nodes, operation_id=operation_id)


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


def _nodes_to_ccnode(nodes: List[Node]) -> List[NodeRecord]:
    return [_node_to_ccnode(n) for n in nodes]


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
