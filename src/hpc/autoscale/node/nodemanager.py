import functools
from copy import deepcopy
from typing import Callable, Dict, List, Optional, Union
from uuid import uuid4

from cyclecloud.model.ClusterStatusModule import ClusterStatus
from cyclecloud.model.NodeCreationResultModule import NodeCreationResult
from cyclecloud.model.PlacementGroupStatusModule import PlacementGroupStatus

import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.ccbindings import new_cluster_bindings
from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.hpclogging import apitrace
from hpc.autoscale.node import constraints as constraintslib
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.node.bucket import (
    NodeBucket,
    NodeDefinition,
    bucket_candidates,
    node_from_bucket,
)
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.limits import (
    BucketLimits,
    _SharedLimit,
    _SpotLimit,
    null_bucket_limits,
)
from hpc.autoscale.node.node import Node, UnmanagedNode, minimum_space
from hpc.autoscale.results import AllocationResult, BootupResult
from hpc.autoscale.util import partition, partition_single

logger = logging.getLogger("cyclecloud.buckets")


DefaultValueFunc = Callable[[Node], ht.ResourceTypeAtom]


@hpcwrapclass
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
        slot_count: Optional[int] = None,
        allow_existing: bool = True,
        all_or_nothing: bool = False,
        assignment_id: Optional[str] = None,
    ) -> AllocationResult:

        if not isinstance(constraints, list):
            constraints = [constraints]

        parsed_constraints = constraintslib.get_constraints(constraints)

        candidates_result = bucket_candidates(self.get_buckets(), parsed_constraints)
        if not candidates_result:
            return AllocationResult(
                "NoCandidatesFound", reasons=candidates_result.reasons,
            )

        allocated_nodes = {}
        total_slots_allocated = 0

        for candidate in candidates_result.candidates:

            if slot_count:
                result = self._allocate_slots(
                    candidate,
                    slot_count,
                    parsed_constraints,
                    allow_existing,
                    assignment_id,
                )

                if not result:
                    continue

                for node in result.nodes:
                    allocated_nodes[node.name] = node

                assert result.total_slots > 0
                total_slots_allocated += result.total_slots

                if total_slots_allocated >= slot_count:
                    break
            else:
                assert node_count is not None
                node_count_floored = self._get_node_count(
                    candidate,
                    node_count - len(allocated_nodes),
                    all_or_nothing,
                    allow_existing,
                )

                if node_count_floored < 1:
                    continue

                result = self._allocate_nodes(
                    candidate,
                    node_count_floored,
                    parsed_constraints,
                    allow_existing,
                    assignment_id,
                )

                if not result:
                    continue

                for node in result.nodes:
                    allocated_nodes[node.name] = node

                total_slots_allocated = len(allocated_nodes)

                if total_slots_allocated >= node_count:
                    break

        if allocated_nodes:
            assert total_slots_allocated
            return AllocationResult(
                "success",
                nodes=list(allocated_nodes.values()),
                slots_allocated=total_slots_allocated,
            )

        return AllocationResult(
            "NoAllocationSelected",
            reasons=[
                "Could not allocate based on the selection criteria: {} all_or_nothing={} allow_existing={}".format(
                    constraints, all_or_nothing, allow_existing
                )
            ],
        )

    def _allocate_slots(
        self,
        bucket: NodeBucket,
        slot_count: int,
        constraints: List[constraintslib.NodeConstraint],
        allow_existing: bool = False,
        assignment_id: Optional[str] = None,
    ) -> AllocationResult:
        remaining = slot_count
        allocated_nodes: Dict[str, Node] = {}

        while remaining > 0:
            alloc_result = self._allocate_nodes(
                bucket, 1, constraints, allow_existing, assignment_id
            )
            if not alloc_result:
                return alloc_result

            assert len(alloc_result.nodes) == 1, alloc_result.nodes
            node = alloc_result.nodes[0]
            allocated_nodes[node.name] = node
            remaining -= alloc_result.total_slots

            while remaining > 0:
                per_node = minimum_space(constraints, node)

                if per_node == 0:
                    break

                if per_node == -1:
                    per_node = 1

                per_node = min(remaining, per_node)

                if node.decrement(
                    constraints, iterations=per_node, assignment_id=assignment_id
                ):
                    remaining -= per_node
                else:
                    break

        return AllocationResult(
            "success",
            nodes=list(allocated_nodes.values()),
            slots_allocated=slot_count - remaining,
        )

    def _allocate_nodes(
        self,
        bucket: NodeBucket,
        count: int,
        constraints: List[constraintslib.NodeConstraint],
        allow_existing: bool = False,
        assignment_id: Optional[str] = None,
    ) -> AllocationResult:

        for constraint in constraints:
            sat_result = constraint.satisfied_by_bucket(bucket)
            assert constraint.satisfied_by_node(bucket.example_node)
            if not sat_result:
                return AllocationResult(sat_result.status, reasons=sat_result.reasons)
            min_space = constraint.minimum_space(bucket.example_node)
            if min_space != -1:
                assert min_space > 0, constraint

        assert count > 0
        allocated_nodes: Dict[str, Node] = {}
        new_nodes = []
        slots_allocated = 0
        available_count_total = self._availabe_count(bucket, allow_existing)

        def remaining() -> int:
            return count - len(allocated_nodes)

        if remaining() > available_count_total:
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

            if constraints:
                satisfied: bool = functools.reduce(lambda a, b: bool(a) and bool(b), [c.satisfied_by_node(node) for c in constraints])  # type: ignore
            else:
                satisfied = True

            if satisfied:
                match_result = node.decrement(constraints, assignment_id=assignment_id)
                assert match_result
                slots_allocated += match_result.total_slots
                allocated_nodes[node.name] = node
                if remaining() <= 0:
                    break

        while remaining() > 0:

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

            assert new_node.vcpu_count == bucket.vcpu_count

            min_space = minimum_space(constraints, new_node)
            if min_space == 0:
                continue
            elif min_space == -1:
                min_space = remaining()
            else:
                for constraint in constraints:
                    res = constraint.satisfied_by_node(new_node)
                    assert res, "{} {} {}".format(res, constraint, new_node.vcpu_count)

            per_node = min(remaining(), min_space)

            assert per_node > 0, "{} {} {} {}".format(
                per_node, remaining(), new_node.resources, constraints
            )

            match_result = new_node.decrement(constraints, per_node, assignment_id)
            assert match_result
            slots_allocated += match_result.total_slots

            new_nodes.append(new_node)

            assert new_node.name not in allocated_nodes

            allocated_nodes[new_node.name] = new_node
            if not new_node.exists:
                bucket.decrement(1)

        bucket.nodes.extend(new_nodes)
        self.new_nodes.extend(new_nodes)

        for node in allocated_nodes.values():
            node._allocated = True

        return AllocationResult(
            "success", list(allocated_nodes.values()), slots_allocated=slots_allocated
        )

    @apitrace
    def allocate_at_least(
        self,
        bucket: NodeBucket,
        count: Optional[int] = None,
        memory: Optional[ht.Memory] = None,
        all_or_nothing: bool = False,
    ) -> AllocationResult:
        raise NotImplementedError()

    def _get_node_count(
        self,
        bucket: NodeBucket,
        node_count: int,
        all_or_nothing: bool = False,
        allow_existing: bool = True,
    ) -> int:

        available_count_total = self._availabe_count(bucket, allow_existing)

        count = node_count
        if not all_or_nothing:
            count = min(node_count, available_count_total)

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
    def add_unmanaged_nodes(self, existing_nodes: List[UnmanagedNode]) -> None:
        by_key: Dict[str, List[Node]] = partition(
            # typing will complain that List[Node] is List[UnmanagedNode]
            # just a limitation of python3's typing
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
                location=ht.Location("unknown"),
                spot=False,
                subnet=ht.SubnetId("unknown"),
                vcpu_count=a_node.vcpu_count,
                memory=a_node.memory,
                placement_group=None,
                resources=deepcopy(a_node.resources),
            )

            limits = null_bucket_limits(len(nodes_list), a_node.vcpu_count)
            bucket = NodeBucket(node_def, limits, len(nodes_list), nodes_list)

        self.__node_buckets.append(bucket)

    def get_nodes(self) -> List[Node]:
        # TODO slow
        ret: List[Node] = []
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

        for cc_node in node_list.nodes:
            name = cc_node["Name"]
            if name in by_name:
                ret.append(by_name[name][0])

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
            logging.info(s.message)

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

    @property
    def cluster_max_core_count(self) -> int:
        assert (
            self.get_buckets()
        ), "We need at least one bucket defined to get cluster limits"
        return self.get_buckets()[0].limits.cluster_max_core_count

    @property
    def cluster_consumed_core_count(self) -> int:
        assert (
            self.get_buckets()
        ), "We need at least one bucket defined to get cluster limits"
        return self.get_buckets()[0].limits.cluster_consumed_core_count

    def get_regional_max_core_count(self, location: Optional[str] = None) -> int:
        for bucket in self.get_buckets():
            if bucket.location == location:
                return self.get_buckets()[0].limits.regional_quota_core_count
        raise RuntimeError(
            "No bucket found in location {} so we do not have the regional limits.".format(
                location
            )
        )

    def get_locations(self) -> List[ht.Location]:
        return list(partition(self.get_buckets(), lambda b: b.location).keys())

    def get_regional_consumed_core_count(self, location: Optional[str] = None) -> int:
        for bucket in self.get_buckets():
            if bucket.location == location:
                return self.get_buckets()[0].limits.regional_consumed_core_count
        raise RuntimeError(
            "No bucket found in location {} so we do not have the regional limits.".format(
                location
            )
        )

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
                acceptable = [x for x in dir(Node) if x[0].isalpha() and x[0].islower()]
                if attr not in dir(Node):
                    msg = "Invalid node.attribute '{}'. Expected one of {}".format(
                        attr, acceptable
                    )
                    raise RuntimeError(msg)

                def get_from_base_node(node: Node) -> ht.ResourceTypeAtom:
                    return getattr(node, attr)

                default_value = get_from_base_node

        if not isinstance(selection, list):
            # TODO add runtime type checking
            selection = [selection]  # type: ignore

        constraints = constraintslib.get_constraints(selection)

        if not hasattr(default_value, "__call__"):

            def default_value_func(node: Node) -> ht.ResourceTypeAtom:
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

        by_name = partition_single(nodes, lambda n: n.name)
        cc_by_name = partition_single(
            self.__cluster_bindings.get_nodes().nodes, lambda n: n["Name"]
        )

        by_bucket_id = partition_single(self.__node_buckets, lambda b: b.bucket_id)

        for name, cc_node in cc_by_name.items():
            if name not in by_name:
                continue
            node = by_name[name]
            node.state = cc_node["Status"]

            if node.state in ["Terminating"]:
                if node.bucket_id not in by_bucket_id:
                    logging.warning(
                        "Unknown bucketid??? %s not in %s", node.bucket_id, by_bucket_id
                    )
                    continue

                bucket = by_bucket_id[node.bucket_id]
                nodes_list = bucket.nodes

                if node not in nodes_list:

                    logging.warning(
                        (
                            "Somehow node {} is not being tracked by bucket {}. "
                            + "Did you try to shutdown/terminate/delete a node twice?"
                        ).format(node, bucket)
                    )
                    continue

                bucket.nodes.remove(node)

    def set_system_default_resources(self) -> None:
        self.add_default_resource({}, "ncpus", "node.vcpu_count")
        self.add_default_resource({}, "pcpus", "node.pcpu_count")
        self.add_default_resource({}, "ngpus", "node.gpu_count")
        self.add_default_resource(
            {}, "memmb", lambda node: node.memory.convert_to("m").value
        )
        self.add_default_resource(
            {}, "memgb", lambda node: node.memory.convert_to("g").value
        )
        self.add_default_resource(
            {}, "memtb", lambda node: node.memory.convert_to("t").value
        )

    def example_node(self, location: str, vm_size: str) -> Node:
        aux_info = vm_sizes.get_aux_vm_size_info(location, vm_size)

        node = Node(
            node_id=DelayedNodeId(ht.NodeName("__example__")),
            name=ht.NodeName("__example__"),
            nodearray=ht.NodeArrayName("__example_nodearray__"),
            bucket_id=ht.BucketId("__example_bucket_id__"),
            hostname=None,
            private_ip=None,
            vm_size=ht.VMSize(vm_size),
            location=ht.Location(location),
            spot=False,
            vcpu_count=aux_info.vcpu_count,
            memory=aux_info.memory,
            infiniband=aux_info.infiniband,
            state=ht.NodeStatus("Off"),
            power_state=ht.NodeStatus("Off"),
            exists=False,
            placement_group=None,
            managed=False,
            resources=ht.ResourceDict({}),
        )
        self._apply_defaults(node)
        return node

    def __str__(self) -> str:
        attrs = []
        for attr_name in dir(self):
            if not (attr_name[0].isalpha() or attr_name.startswith("_NodeManager")):
                continue
            # TODO RDH - we need a better str
            if "core_count" in attr_name:
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
    config: dict,
    existing_nodes: Optional[List[UnmanagedNode]] = None,
    disable_default_resources: bool = False,
) -> NodeManager:

    logging.initialize_logging(config)

    ret = _new_node_manager_79(new_cluster_bindings(config))
    existing_nodes = existing_nodes or []

    for entry in config.get("default_resources", []):
        if set(["select", "name", "value"]) != set(entry.keys()):
            raise RuntimeError(
                "default_resources: Expected select=dict name=str value=str: {}".format(
                    entry
                )
            )

        try:
            assert isinstance(entry["select"], dict)
            assert isinstance(entry["name"], str)
            assert isinstance(entry["value"], str)
        except AssertionError as e:
            raise RuntimeError(
                "default_resources: Expected select=dict name=str value=str: {}".format(
                    e
                )
            )

        ret.add_default_resource(entry["select"], entry["name"], entry["value"])

    if not disable_default_resources:
        ret.set_system_default_resources()

    return ret


def _cluster_limits(cluster_name: str, cluster_status: ClusterStatus) -> _SharedLimit:

    cluster_active_cores = 0

    for cc_node in cluster_status.nodes:
        aux_info = vm_sizes.get_aux_vm_size_info(
            cc_node["Region"], cc_node["MachineType"]
        )
        cluster_active_cores += aux_info.vcpu_count

    return _SharedLimit(
        "Cluster({})".format(cluster_name),
        cluster_active_cores,  # noqa: E128
        cluster_status.max_core_count,
    )


def _new_node_manager_79(cluster_bindings: ClusterBindingInterface,) -> NodeManager:
    cluster_status = cluster_bindings.get_cluster_status(nodes=True)
    nodes_list = cluster_bindings.get_nodes()
    all_node_names = [n["Name"] for n in nodes_list.nodes]

    buckets = []

    cluster_limit = _cluster_limits(cluster_bindings.cluster_name, cluster_status)

    nodearray_limits: Dict[str, _SharedLimit] = {}
    regional_limits: Dict[str, _SharedLimit] = {}
    family_limits: Dict[str, _SharedLimit] = {}

    for nodearray_status in cluster_status.nodearrays:
        nodearray = nodearray_status.nodearray
        region = nodearray["Region"]

        custom_resources = deepcopy(
            ht.ResourceDict(
                nodearray.get("Configuration", {})
                .get("autoscale", {})
                .get("resources", {})
            )
        )

        nodearray_limits[nodearray_status.name] = _SharedLimit(
            "NodeArray({})".format(nodearray_status.name),
            0,
            nodearray_status.max_core_count,  # noqa: E128,
            nodearray_status.max_count,
        )

        spot = nodearray.get("Interruptible", False)

        for bucket in nodearray_status.buckets:
            aux_vm_info = vm_sizes.get_aux_vm_size_info(
                region, bucket.definition.machine_type
            )
            assert isinstance(aux_vm_info, vm_sizes.AuxVMSizeInfo)
            vm_family = aux_vm_info.vm_family

            if region not in regional_limits:
                regional_limits[region] = _SharedLimit(
                    "Region({})".format(region),
                    bucket.regional_consumed_core_count,
                    bucket.regional_quota_core_count,
                )

            if vm_family not in family_limits:
                family_limits[vm_family] = _SharedLimit(
                    "VM Family({})".format(vm_family),
                    bucket.family_consumed_core_count,
                    bucket.family_quota_core_count,
                )

            placement_groups = partition_single(
                bucket.placement_groups, lambda p: p.name
            )

            default_pg = nodearray_status.nodearray.get("PlacementGroupId")

            if default_pg is None:
                placement_groups[None] = None

            elif default_pg not in placement_groups:
                placement_groups[default_pg] = PlacementGroupStatus(
                    active_core_count=0, active_count=0, name=default_pg
                )

            for pg_name, pg_status in placement_groups.items():
                vm_size = bucket.definition.machine_type
                location = nodearray["Region"]
                subnet = nodearray["SubnetId"]
                vcpu_count = bucket.virtual_machine.vcpu_count
                bucket_memory_gb = nodearray.get("Memory") or (
                    bucket.virtual_machine.memory
                )
                bucket_memory = ht.Memory(bucket_memory_gb, "g")

                bucket_id = bucket.bucket_id

                nodearray_name = nodearray_status.name

                # TODO the bucket has a list of node names
                cc_node_records = [
                    n
                    for n in cluster_status.nodes
                    if n["Name"] in bucket.active_nodes
                    and n.get("PlacementGroupId") == pg_name
                ]

                active_cores = len(cc_node_records) * vcpu_count

                nodearray_limits[nodearray_name]._consumed_core_count += active_cores

                if pg_name:
                    max_count = min(bucket.max_placement_group_size, bucket.max_count)
                    max_core_count = max_count * vcpu_count
                    active_count = len(cc_node_records)
                    active_core_count = active_count * vcpu_count

                    available_count = max_count - active_count
                    available_core_count = max_core_count - active_core_count
                else:
                    max_count = bucket.max_count
                    max_core_count = bucket.max_core_count
                    active_count = bucket.active_count
                    active_core_count = bucket.active_core_count
                    available_count = bucket.available_count
                    available_core_count = bucket.available_core_count

                family_limit: Union[_SpotLimit, _SharedLimit] = family_limits[vm_family]
                if nodearray.get("Interruptible"):
                    # enabling spot/interruptible 0's out the family limit response
                    # as the regional limit is supposed to be used in its place,
                    # however that responsibility is on the caller and not the
                    # REST api. For this library we handle that for them.
                    family_limit = _SpotLimit(regional_limits[region])

                bucket_limit = BucketLimits(
                    vcpu_count,
                    regional_limits[region],
                    cluster_limit,
                    nodearray_limits[nodearray_name],
                    family_limit,
                    active_core_count=active_core_count,
                    active_count=active_count,
                    available_core_count=available_core_count,
                    available_count=available_count,
                    max_core_count=max_core_count,
                    max_count=max_count,
                )

                nodes = []

                for cc_node_rec in cc_node_records:
                    node_id = ht.NodeId(cc_node_rec["NodeId"])
                    node_name = ht.NodeName(cc_node_rec["Name"])
                    nodearray_name = cc_node_rec["Template"]
                    vm_size_node = cc_node_rec["MachineType"]
                    hostname = cc_node_rec.get("Hostname")
                    private_ip = cc_node_rec.get("PrivateIp")
                    vcpu_count = (
                        cc_node_rec.get("CoreCount")
                        or bucket.virtual_machine.vcpu_count
                    )
                    node_memory_gb = cc_node_rec.get("Memory") or (
                        bucket.virtual_machine.memory
                    )
                    node_memory = ht.Memory(node_memory_gb, "g")
                    placement_group = cc_node_rec.get("PlacementGroupId")
                    infiniband = bool(placement_group)
                    state = cc_node_rec.get("Status")
                    resources = deepcopy(
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
                            hostname=hostname,
                            private_ip=private_ip,
                            location=location,
                            spot=spot,
                            vcpu_count=vcpu_count,
                            memory=node_memory,
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
                    nodearray=nodearray_name,
                    bucket_id=bucket_id,
                    vm_size=vm_size,
                    location=location,
                    spot=spot,
                    subnet=subnet,
                    vcpu_count=vcpu_count,
                    memory=bucket_memory,
                    placement_group=pg_name,
                    resources=custom_resources,
                )

                node_bucket = NodeBucket(
                    node_def,
                    limits=bucket_limit,
                    max_placement_group_size=bucket.max_placement_group_size,
                    nodes=nodes,
                )
                logging.debug(
                    "Found %s with limits %s", node_bucket, repr(bucket_limit)
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

        for criteria in self.selection:
            if not criteria.satisfied_by_node(node):
                return

        # it met all of our criteria, so set the default
        default_value = self.default_value_function(node)
        node._resources[self.resource_name] = default_value
        node.available[self.resource_name] = default_value
