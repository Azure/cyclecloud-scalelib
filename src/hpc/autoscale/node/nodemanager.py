import functools
import logging
from math import ceil
from typing import Callable, Dict, List, Optional, Union
from uuid import uuid4

from cyclecloud.model.NodeCreationResultModule import NodeCreationResult

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.ccbindings import new_cluster_bindings
from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.node import constraints as constraintslib
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.node.bucket import (
    NodeBucket,
    NodeDefinition,
    bucket_candidates,
    node_from_bucket,
)
from hpc.autoscale.node.constraints import NodeConstraint, minimum_space
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.limits import create_bucket_limits, null_bucket_limits
from hpc.autoscale.node.node import BaseNode, Node, UnmanagedNode
from hpc.autoscale.results import AllocationResult, BootupResult
from hpc.autoscale.util import apitrace, partition

logger = logging.getLogger("cyclecloud.buckets")


DefaultValueFunc = Callable[[BaseNode], ht.ResourceTypeAtom]


class NodeManager:
    """
    """

    def __init__(
        self, cluster_bindings: ClusterBindingInterface, node_buckets: List[NodeBucket],
    ) -> None:
        self.__cluster_bindings = cluster_bindings
        self.__node_buckets = node_buckets
        self._node_names = set()
        for node_bucket in node_buckets:
            for node in node_bucket.nodes:
                self._node_names.add(node.name)
        self.__unmanaged_nodes: List[UnmanagedNode] = []
        self.__default_resources: List[_DefaultResource] = []
        # list of nodes a user has 'allocated'.
        # self.new_nodes = []  # type: List[Node]

    @property
    def new_nodes(self) -> List[Node]:
        return [n for n in self.get_nodes() if not n.exists]

    def _add_bucket(self, bucket: NodeBucket) -> None:
        self.__node_buckets.append(bucket)

    @apitrace
    def allocate(
        self,
        constraints: Union[List[constraintslib.Constraint], constraintslib.Constraint],
        node_count: Optional[int] = None,
        core_count: Optional[int] = None,
        memory: Optional[ht.Memory] = None,
        slot_count: Optional[int] = None,
        allow_existing: bool = True,
        all_or_nothing: bool = False,
        assignment_id: Optional[str] = None,
    ) -> AllocationResult:
        if not isinstance(constraints, list):
            constraints = [constraints]

        parsed_constraints = constraintslib.get_constraints(constraints)

        # TODO hacks
        # parsed_constraints.append(constraintslib.NotAllocated())
        candidates_result = bucket_candidates(self.get_buckets(), parsed_constraints)

        remaining = _RemainingToAllocate(node_count, core_count, memory, slot_count)

        allocated_nodes = []
        total_slots_allocated = 0

        if candidates_result:
            for candidate in candidates_result.candidates:
                node_count = self._get_node_count(
                    parsed_constraints,
                    candidate,
                    node_count=remaining.count,
                    core_count=remaining.vcpu_count,
                    slot_count=remaining.slot_count,
                    memory=remaining.memory,
                    all_or_nothing=all_or_nothing,
                    allow_existing=allow_existing,
                )
                if not node_count:
                    logging.debug(
                        "Ignoring candidate %s because it does not have capacity",
                        candidate.nodearray,
                    )
                    continue

                result = self._allocate(
                    candidate,
                    node_count,
                    parsed_constraints,
                    allow_existing,
                    assignment_id,
                )

                if result:
                    allocated_nodes.extend(result.nodes)
                    total_slots_allocated += result.total_slots
                    if remaining.decrement(candidate, result.total_slots) <= 0:
                        break

        if allocated_nodes:
            return AllocationResult(
                "success", nodes=allocated_nodes, slots_allocated=total_slots_allocated
            )

        return AllocationResult(
            "NoAllocationSelected",
            reasons=[
                "Could not allocate based on the selection criteria: {}".format(
                    constraints
                )
            ],
        )

    def _allocate(
        self,
        bucket: NodeBucket,
        count: int,
        constraints: List[constraintslib.NodeConstraint],
        allow_existing: bool = False,
        assignment_id: Optional[str] = None,
    ) -> AllocationResult:

        remaining = count
        assert count > 0
        allocated_nodes = []
        new_nodes = []
        slots_allocated = 0
        available_count_total = self._availabe_count(bucket, allow_existing)

        if remaining > available_count_total:
            return AllocationResult(
                "NoCapacity",
                reasons=[
                    "Not enough {} availability for request: Available {} requested {}".format(
                        bucket.vm_size, available_count_total, count
                    )
                ],
            )

        for node in bucket.nodes:
            if node.closed:
                continue

            satisfied = functools.reduce(lambda a, b: a and b, [c.satisfied_by_node(node) for c in constraints])  # type: ignore
            if satisfied:
                match_result = node.decrement(constraints)
                assert match_result
                slots_allocated += match_result.total_slots
                allocated_nodes.append(node)
                remaining -= 1

        for _ in range(remaining):
            node_name = self._next_node_name(bucket)
            new_node = node_from_bucket(
                bucket,
                exists=False,
                state=ht.NodeStatus("Off"),
                # TODO what about deallocated? Though this is a 'new' node...
                power_state=ht.NodeStatus("Off"),
                placement_group=bucket.placement_group,
                new_node_name=node_name,
            )

            self._apply_defaults(new_node)

            min_space = minimum_space(constraints, new_node)
            if min_space == -1:
                min_space = remaining
            per_node = min(remaining, min_space)

            match_result = new_node.decrement(constraints, per_node, assignment_id)
            assert match_result
            slots_allocated += match_result.total_slots

            new_nodes.append(new_node)
            allocated_nodes.append(new_node)
            if not new_node.exists:
                bucket.decrement(1)

        bucket.nodes.extend(new_nodes)
        self.new_nodes.extend(new_nodes)

        for node in allocated_nodes:
            node._allocated = True

        return AllocationResult(
            "success", allocated_nodes, slots_allocated=slots_allocated
        )

    @apitrace
    def allocate_at_least(
        self,
        bucket: NodeBucket,
        count: Optional[int] = None,
        core_count: Optional[int] = None,
        memory: Optional[ht.Memory] = None,
        all_or_nothing: bool = False,
    ) -> AllocationResult:
        raise NotImplementedError()

    def _get_node_count(
        self,
        constraints: List[NodeConstraint],
        bucket: NodeBucket,
        node_count: Optional[int] = None,
        core_count: Optional[int] = None,
        slot_count: Optional[int] = None,
        memory: Optional[ht.Memory] = None,
        all_or_nothing: bool = False,
        allow_existing: bool = True,
    ) -> int:
        assert (
            bool(node_count) ^ bool(core_count) ^ bool(memory) ^ bool(slot_count)
        ), "Please pick one and only one of count, vcpu_count, slot_count or memory."

        if node_count is None:
            if not core_count:
                if not memory:
                    if not slot_count:
                        raise RuntimeError(
                            "Either count, core_count, slot_count or memory must be specified"
                        )
                    per_node = minimum_space(constraints, bucket.example_node)
                    if per_node > 0:
                        count = int(ceil(slot_count / per_node))
                    else:
                        count = 0
                else:
                    count = int(ceil((memory / bucket.memory).value))
                    assert count > 0
            else:
                count = int(ceil(core_count / float(bucket.vcpu_count)))
                assert count > 0
        else:
            count = node_count

        available_count_total = self._availabe_count(bucket, allow_existing)

        if not all_or_nothing:
            count = min(count, available_count_total)

        if count == 0 or count > available_count_total:
            # TODO report what the user reported
            return 0

        assert count is not None
        return int(count)

    def _availabe_count(self, bucket: NodeBucket, allow_existing: bool) -> int:
        available_count_total = bucket.available_count

        if allow_existing:
            # let's include the unallocated existing nodes
            for node in bucket.nodes:
                if not node._allocated:
                    available_count_total += 1
        return available_count_total

    def _next_node_name(self, bucket: NodeBucket) -> ht.NodeName:
        index = 1
        while True:
            name = ht.NodeName("{}-{}".format(bucket.nodearray, index))
            if name not in self._node_names:
                self._node_names.add(name)
                return name
            index += 1

    @apitrace
    def add_unmanaged_bucket(self, unmanaged_bucket: NodeBucket) -> None:
        self.__node_buckets.append(unmanaged_bucket)

    def get_nodes(self) -> List[Node]:
        # TODO slow
        ret: List[Node] = list(self.__unmanaged_nodes)
        for node_bucket in self.__node_buckets:
            ret.extend(node_bucket.nodes)
        return ret

    def get_new_nodes(self) -> List[Node]:
        return self.new_nodes

    def get_buckets(self) -> List[NodeBucket]:
        return self.__node_buckets

    @apitrace
    def get_nodes_by_operation(self, operation_id: ht.OperationId) -> List[Node]:
        node_list = self.__cluster_bindings.get_nodes(operation_id=operation_id)
        by_name = partition(self.get_nodes(), lambda n: n.name)

        ret = []
        print(
            "found {} nodes for operation {}".format(len(node_list.nodes), operation_id)
        )
        for cc_node in node_list.nodes:
            name = cc_node["Name"]
            if name in by_name:
                ret.append(by_name[name][0])
            else:
                print("Huh? {} not in {}".format(name, by_name))

        return ret

    @apitrace
    def bootup(
        self,
        nodes: Optional[List[Node]] = None,
        request_id: Optional[ht.RequestId] = None,
    ) -> BootupResult:
        nodes = nodes or self.new_nodes
        result: NodeCreationResult = self.__cluster_bindings.create_nodes(nodes)

        for s in result.sets:
            print(s.message)

        for creation_set in result.sets:
            if creation_set.message:
                logging.warn(result.message)

        created_nodes = self.__cluster_bindings.get_nodes(
            operation_id=result.operation_id
        )

        new_node_mappings = {}
        for node in created_nodes.nodes:
            new_node_mappings[node["Name"]] = node

        started_nodes = []
        for offset, node in enumerate(nodes):
            node.delayed_node_id.operation_id = result.operation_id
            node.delayed_node_id.operation_offset = offset
            if node.name in new_node_mappings:
                cc_node = new_node_mappings[node.name]
                node.state = cc_node["Status"]
                node.status = cc_node["Status"]
                node.delayed_node_id.node_id = cc_node["NodeId"]
                started_nodes.append(node)
            else:
                node.state = "Unknown"
                node.status = "Unknown"

        return BootupResult("success", result.operation_id, request_id, started_nodes)

    @apitrace
    def add_default_resource(
        self,
        selection: Union[Dict, List[constraintslib.Constraint]],
        resource_name: str,
        default_value: Union[ht.ResourceTypeAtom, DefaultValueFunc],
    ) -> None:
        if isinstance(default_value, str):
            if default_value.startswith("node."):
                attr = default_value[len("node.") :]  # noqa: E203
                acceptable = [
                    x for x in dir(BaseNode) if x[0].isalpha() and x[0].islower()
                ]
                if attr not in dir(BaseNode):
                    msg = "Invalid node.attribute '{}'. Expected one of {}".format(
                        attr, acceptable
                    )
                    raise RuntimeError(msg)

                def get_from_base_node(node: BaseNode) -> ht.ResourceTypeAtom:
                    return getattr(node, attr)

                default_value = get_from_base_node

        if not isinstance(selection, list):
            # TODO add runtime type checking
            selection = [selection]  # type: ignore

        constraints = constraintslib.get_constraints(selection)

        if not hasattr(default_value, "__call__"):

            def default_value_func(node: BaseNode) -> ht.ResourceTypeAtom:
                # already checked if it has a call
                return default_value  # type: ignore

        else:
            default_value_func = default_value  # type: ignore

        dr = _DefaultResource(constraints, resource_name, default_value_func)
        self.__default_resources.append(dr)
        self._apply_defaults_all()

    def _apply_defaults_all(self) -> None:
        for dr in self.__default_resources:
            for bucket in self.get_buckets():
                dr.apply_default(bucket.example_node)
                bucket.resources.update(bucket.example_node.available)

            for node in self.get_nodes():
                dr.apply_default(node)

    def _apply_defaults(self, node: Node) -> None:
        for dr in self.__default_resources:
            dr.apply_default(node)

    @apitrace
    def deallocate_nodes(self, nodes: List[Node]) -> None:
        self._nodes_operation(nodes, "deallocate_nodes")

    @apitrace
    def delete(self, nodes: List[Node]) -> None:
        self._nodes_operation(nodes, "delete_nodes")

    @apitrace
    def remove_nodes(self, nodes: List[Node]) -> None:
        self._nodes_operation(nodes, "remove_nodes")

    @apitrace
    def shutdown_nodes(self, nodes: List[Node]) -> None:
        self._nodes_operation(nodes, "shutdown_nodes")

    @apitrace
    def start_nodes(self, nodes: List[Node]) -> None:
        self._nodes_operation(nodes, "start_nodes")

    @apitrace
    def terminate_nodes(self, nodes: List[Node]) -> None:
        self._nodes_operation(nodes, "terminate_nodes")

    def _nodes_operation(self, nodes: List[Node], op_name: str) -> None:
        managed_nodes = [node for node in nodes if node.managed]
        unmanaged_node_names = [node.name for node in nodes if not node.managed]

        if unmanaged_node_names:
            logging.warn(
                "The following nodes will not be {} because node.managed=false: {}".format(
                    op_name, unmanaged_node_names
                )
            )

        if not managed_nodes:
            logging.warn("No nodes to {}".format(op_name))
            return
        getattr(self.__cluster_bindings, op_name)(nodes=managed_nodes)

    def __str__(self) -> str:
        attrs = []
        for attr_name in dir(self):
            if not (attr_name[0].isalpha() or attr_name.startswith("_NodeManager")):
                continue

            attr = getattr(self, attr_name)
            if "__call__" not in dir(attr):
                attr_expr = attr_name.replace("_NodeManager", "")
                attrs.append("{}={}".format(attr_expr, attr))
        return "NodeManager({})".format(", ".join(attrs))

    def __repr__(self) -> str:
        return str(self)


@apitrace
def new_node_manager(
    config: dict, existing_nodes: Optional[List[UnmanagedNode]] = None
) -> NodeManager:
    ret = _new_node_manager_79(new_cluster_bindings(config))
    existing_nodes = existing_nodes or []
    by_key: Dict[str, List[Node]] = partition(
        existing_nodes,  # type: ignore
        lambda n: str((n.vcpu_count, n.memory, n.resources)),
    )

    for key, nodes_list in by_key.items():
        a_node = nodes_list[0]
        # create a null definition, limits and bucket for each
        # unique set of unmanaged nodes
        node_def = NodeDefinition(
            nodearray=ht.NodeArrayName("__unmanaged__"),
            bucket_id=ht.BucketId(str(uuid4())),
            vm_size=ht.VMSize("unknown"),
            vm_family=ht.VMFamily("unknown"),
            location=ht.Location("unknown"),
            spot=False,
            subnet=ht.SubnetId("unknown"),
            vcpu_count=a_node.vcpu_count,
            memory=a_node.memory,
            placement_group=None,
            resources=a_node.resources,
        )

        limits = null_bucket_limits(len(nodes_list), a_node.vcpu_count)
        bucket = NodeBucket(node_def, limits, len(nodes_list), nodes_list)

        ret.add_unmanaged_bucket(bucket)

    return ret


def _new_node_manager_79(cluster_bindings: ClusterBindingInterface,) -> NodeManager:
    cluster_status = cluster_bindings.get_cluster_status(nodes=True)
    nodes_list = cluster_bindings.get_nodes()
    all_node_names = [n["Name"] for n in nodes_list.nodes]

    buckets = []

    bucket_limits = create_bucket_limits(cluster_bindings.cluster_name, cluster_status)

    for nodearray_status in cluster_status.nodearrays:
        nodearray = nodearray_status.nodearray
        custom_resources = ht.ResourceDict(
            nodearray.get("Configuration", {}).get("autoscale", {}).get("resources", {})
        )
        spot = nodearray.get("Interruptible", False)

        for bucket in nodearray_status.buckets:
            # TODO move to a util func
            placement_groups = set(
                [nodearray.get("PlacementGroupId")] + (bucket.placement_groups or [])
            )
            for pg in placement_groups:
                vm_size = bucket.definition.machine_type
                vm_family = vm_sizes.get_family(vm_size)

                location = nodearray["Region"]
                subnet = nodearray["SubnetId"]
                vcpu_count = bucket.virtual_machine.vcpu_count
                memory = bucket.virtual_machine.memory

                bucket_id = bucket.bucket_id

                pool_name = nodearray_status.name

                # TODO the bucket has a list of node names
                cc_node_records = [
                    n
                    for n in cluster_status.nodes
                    if n["Name"] in bucket.active_nodes
                    and n.get("PlacementGroupId") == pg
                ]

                nodes = []

                for cc_node_rec in cc_node_records:
                    node_id = ht.NodeId(cc_node_rec["NodeId"])
                    node_name = ht.NodeName(cc_node_rec["Name"])
                    nodearray_name = cc_node_rec["Template"]
                    vm_size_node = cc_node_rec["MachineType"]
                    hostname = cc_node_rec.get("Hostname")
                    private_ip = cc_node_rec.get("PrivateIp")
                    location = cc_node_rec.get("Region")
                    vcpu_count = (
                        cc_node_rec.get("CoreCount")
                        or bucket.virtual_machine.vcpu_count
                    )
                    memory = cc_node_rec.get("Memory") or (
                        bucket.virtual_machine.memory * 1024
                    )
                    placement_group = cc_node_rec.get("PlacementGroupId")
                    infiniband = bool(placement_group)
                    state = cc_node_rec.get("Status")
                    resources = (
                        cc_node_rec.get("Configuration", {})
                        .get("autoscale", {})
                        .get("resources", {})
                    )

                    nodes.append(
                        Node(
                            node_id=DelayedNodeId(node_name, node_id=node_id),
                            name=node_name,
                            nodearray=nodearray_name,
                            bucket_id=bucket_id,
                            vm_size=vm_size_node,
                            vm_family=vm_sizes.get_family(vm_size_node),
                            hostname=hostname,
                            private_ip=private_ip,
                            location=location,
                            spot=spot,
                            vcpu_count=vcpu_count,
                            memory=memory,
                            infiniband=infiniband,
                            state=state,
                            power_state=state,
                            exists=True,
                            placement_group=placement_group,
                            managed=True,
                            resources=resources,
                        )
                    )

                node_def = NodeDefinition(
                    nodearray=pool_name,
                    bucket_id=bucket_id,
                    vm_size=vm_size,
                    vm_family=vm_family,
                    location=location,
                    spot=spot,
                    subnet=subnet,
                    vcpu_count=vcpu_count,
                    memory=ht.Memory(memory, "g"),
                    placement_group=pg,
                    resources=custom_resources,
                )
                key = (bucket.bucket_id, pg)
                assert (
                    key in bucket_limits
                ), "No bucket limits found for bucket_id {} not in {}".format(
                    key, bucket_limits.keys()
                )

                node_bucket = NodeBucket(
                    node_def,
                    limits=bucket_limits[key],
                    max_placement_group_size=bucket.max_placement_group_size,
                    nodes=nodes,
                )
                logging.debug(
                    "Found %s with limits %s", node_bucket, repr(bucket_limits[key])
                )
                buckets.append(node_bucket)

    ret = NodeManager(cluster_bindings, buckets)
    for name in all_node_names:
        ret._node_names.add(name)
    return ret


class _DefaultResource:
    def __init__(
        self,
        selection: List[constraintslib.NodeConstraint],
        resource_name: str,
        default_value_function: DefaultValueFunc,
    ) -> None:
        self.selection = selection
        self.resource_name = resource_name
        self.default_value_function = default_value_function

    def apply_default(self, node: Node) -> None:

        # obviously we don't want to override anything
        if node.resources.get(self.resource_name) is not None:
            return

        # let's create a temporary node to do selection criteria
        from hpc.autoscale.job.computenode import SchedulerNode

        node_name = ht.NodeName("default-node-selection[{}]".format(self.resource_name))
        temp_node = SchedulerNode(node_name, node.resources)

        for criteria in self.selection:
            if not criteria.satisfied_by_node(temp_node):
                return

        # it met all of our criteria, so set the default
        default_value = self.default_value_function(node)
        node.resources[self.resource_name] = default_value
        node.available[self.resource_name] = default_value


class _RemainingToAllocate:
    def __init__(
        self,
        count: Optional[int],
        vcpu_count: Optional[int],
        memory: Optional[ht.Memory],
        slot_count: Optional[int],
    ) -> None:
        self.count = count
        self.vcpu_count = vcpu_count
        self.memory = memory
        self.slot_count = slot_count

    def __int__(self) -> int:
        if self.count is not None:
            return self.count
        if self.vcpu_count is not None:
            return self.vcpu_count
        if self.memory is not None:
            return int(ceil(self.memory.value))
        if self.slot_count is not None:
            return self.slot_count
        raise RuntimeError()

    def decrement(self, bucket: NodeBucket, value: int) -> int:
        if self.count is not None:
            self.count -= value

        elif self.vcpu_count is not None:
            self.vcpu_count -= bucket.vcpu_count * value

        elif self.memory is not None:
            self.memory = max(
                ht.Memory(0, self.memory.magnitude), self.memory - bucket.memory * value
            )

        elif self.slot_count is not None:
            self.slot_count -= value

        return int(self)
