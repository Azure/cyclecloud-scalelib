import functools
import re
from copy import deepcopy
from typing import Callable, Dict, List, Optional, Tuple, TypeVar, Union

from cyclecloud.model.ClusterStatusModule import ClusterStatus
from cyclecloud.model.NodearrayBucketStatusModule import NodearrayBucketStatus
from cyclecloud.model.NodeCreationResultModule import NodeCreationResult
from cyclecloud.model.NodeListModule import NodeList
from cyclecloud.model.NodeManagementResultModule import NodeManagementResult
from cyclecloud.model.NodeManagementResultNodeModule import NodeManagementResultNode
from cyclecloud.model.PlacementGroupStatusModule import PlacementGroupStatus
from immutabledict import ImmutableOrderedDict
from typing_extensions import Literal

import hpc.autoscale.hpclogging as logging
import hpc.autoscale.util as hpcutil
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
from hpc.autoscale.node.constraints import NodeConstraint, get_constraints
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.limits import (
    BucketLimits,
    _SharedLimit,
    _SpotLimit,
    null_bucket_limits,
)
from hpc.autoscale.node.node import Node, UnmanagedNode, minimum_space
from hpc.autoscale.results import (
    AllocationResult,
    BootupResult,
    DeallocateResult,
    DeleteResult,
    MatchResult,
    RemoveResult,
    SatisfiedResult,
    ShutdownResult,
    StartResult,
    TerminateResult,
)
from hpc.autoscale.util import partition, partition_single

logger = logging.getLogger("cyclecloud.buckets")


DefaultValueFunc = Callable[[Node], ht.ResourceTypeAtom]
ResourceModifier = Literal["add", "subtract", "multiply", "divide", "divide_floor"]


T = TypeVar("T")


@hpcwrapclass
class NodeManager:
    """
    TODO add round robin / scatter shot allocation strategy
    (goes to this ^ when no capacity)
    long term requests

    priority
    scatter
    adaptive (when no capacity, switch to prio)

    """

    def __init__(
        self,
        cluster_bindings: ClusterBindingInterface,
        node_buckets: List[NodeBucket],
    ) -> None:
        self.__cluster_bindings = cluster_bindings
        self.__node_buckets = node_buckets
        self._node_names = {}
        for node_bucket in node_buckets:
            for node in node_bucket.nodes:
                self._node_names[node.name] = True

        self.__default_resources: List[_DefaultResource] = []
        self.__journal: List[_JournalEntry] = []
        self._next_node_name_hook: Optional[Callable[[NodeBucket, int], str]] = None
        # list of nodes a user has 'allocated'.
        # self.new_nodes = []  # type: List[Node]

    @property
    def new_nodes(self) -> List[Node]:
        return [
            n
            for n in self.get_nodes()
            if not n.exists
            and n.state != "Stopping"
            and not (n.state == "Deallocated" and not n._allocated)
        ]

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
        node_namer: Optional[Callable[[], str]] = None,
    ) -> AllocationResult:
        if isinstance(constraints, dict):
            constraints = [constraints]
            
        if not allow_existing:
            constraints = get_constraints([{"node.exists": False}]) + constraints

        if int(node_count or 0) <= 0 and int(slot_count or 0) <= 0:
            return AllocationResult(
                "NothingRequested",
                reasons=[
                    "Either node_count or slot_count must be defined as a positive integer."
                ],
            )

        if not isinstance(constraints, list):
            constraints = [constraints]

        parsed_constraints = constraintslib.get_constraints(constraints)

        candidates_result = bucket_candidates(self.get_buckets(), parsed_constraints)
        if not candidates_result:
            return AllocationResult(
                "NoCandidatesFound",
                reasons=candidates_result.reasons,
            )

        logging.debug(
            "Candidates for job %s: %s", assignment_id, candidates_result.candidates
        )

        allocated_nodes = {}
        total_slots_allocated = 0

        additional_reasons = []

        for candidate in candidates_result.candidates:

            if slot_count and slot_count > 0:
                assert slot_count - total_slots_allocated > 0, "%s - %s <= 0" % (
                    slot_count,
                    total_slots_allocated,
                )
                result = self._allocate_slots(
                    candidate,
                    slot_count - total_slots_allocated,
                    parsed_constraints,
                    allow_existing,
                    all_or_nothing,
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
                    all_or_nothing=all_or_nothing,
                    allow_existing=allow_existing,
                )

                if node_count_floored < 1:
                    additional_reasons.append(
                        "Bucket {} does not have capacity: Required={} Available={}".format(
                            candidate, node_count, candidate.available_count
                        )
                    )
                    continue

                result = self._allocate_nodes(
                    candidate,
                    node_count_floored,
                    -1,
                    parsed_constraints,
                    allow_existing,
                    assignment_id,
                )

                if not result:
                    if hasattr(result, "reasons"):
                        additional_reasons.extend(result.reasons)
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
            ]
            + additional_reasons,
        )

    def _allocate_slots(
        self,
        bucket: NodeBucket,
        slot_count: int,
        constraints: List[constraintslib.NodeConstraint],
        allow_existing: bool = False,
        all_or_nothing: bool = False,
        assignment_id: Optional[str] = None,
    ) -> AllocationResult:
        remaining = slot_count
        allocated_nodes: Dict[str, Tuple[Node, Node]] = {}
        alloc_result: Optional[AllocationResult] = None
        reasons = []

        assert remaining > 0

        while remaining > 0:
            min_count = minimum_space(constraints, bucket.example_node)
            assert min_count, "%s -> %s" % (constraints, bucket.example_node.resources)
            # -1 is returned when the constraints have no say in how many could fit
            # like, say, if the only constraint was that a flag was true
            if min_count <= -1:
                min_count = remaining
            elif min_count == 0:
                reasons.append(
                    "Constraints dictate that no slots will fit on a node in {}".format(
                        bucket
                    )
                )
                for cons in constraints:
                    res = cons.satisfied_by_bucket(bucket)
                    if not res:
                        reasons.extend(res.reasons)
                break

            alloc_result = self._allocate_nodes(
                bucket,
                1,
                # min(remaining, min_count),
                remaining,
                constraints,
                allow_existing=True,
                assignment_id=assignment_id,
                commit=False,
            )

            if not alloc_result:
                reasons.extend(alloc_result.reasons)
                break

            for node in alloc_result.nodes:
                allocated_nodes[node.name] = (node, node)

            remaining -= alloc_result.total_slots
            # TODO hack -
            if not all_or_nothing:
                self._commit(bucket, list(allocated_nodes.values()))

        if all_or_nothing and remaining > 0:
            self._rollback(bucket)
            return AllocationResult("OutOfCapacity", reasons=[""])

        # allocated at least one slot
        if remaining < slot_count:
            commited_nodes = self._commit(bucket, list(allocated_nodes.values()))
            return AllocationResult(
                "success",
                nodes=commited_nodes,
                slots_allocated=slot_count - remaining,
            )
        self._rollback(bucket)
        if alloc_result:
            return alloc_result
        return AllocationResult("Failed", reasons=reasons)

    def _allocate_nodes(
        self,
        bucket: NodeBucket,
        count: int,
        total_iterations: int,
        constraints: List[constraintslib.NodeConstraint],
        allow_existing: bool = False,
        assignment_id: Optional[str] = None,
        commit: bool = True,
    ) -> AllocationResult:
        ret = self.__allocate_nodes(
            bucket,
            count,
            total_iterations,
            constraints,
            allow_existing,
            assignment_id,
            commit,
        )
        assert bucket.available_count >= 0, bucket
        return ret

    def __allocate_nodes(
        self,
        bucket: NodeBucket,
        count: int,
        total_iterations: int,
        constraints: List[constraintslib.NodeConstraint],
        allow_existing: bool = False,
        assignment_id: Optional[str] = None,
        commit: bool = True,
    ) -> AllocationResult:
        for node in bucket.nodes:
            assert node.placement_group == bucket.placement_group

        if not allow_existing and bucket.available_count < 1:
            return AllocationResult(
                "OutOfCapacity",
                reasons=["No more capacity for bucket {}".format(bucket)],
            )

        assert count > 0
        allocated_nodes: Dict[str, Tuple[Node, Node]] = {}
        new_nodes = []
        slots_allocated = 0
        available_count_total = self._available_count(bucket, allow_existing)

        def remaining_slots() -> int:
            if total_iterations > 0:
                return total_iterations - slots_allocated
            return count - len(allocated_nodes)

        initial_slot_count = remaining_slots()

        def remaining_nodes() -> int:
            return count - len(allocated_nodes)

        def _per_node(node: Node, constraints: List[NodeConstraint]) -> int:
            min_space = minimum_space(constraints, node)

            if min_space == -1:
                min_space = remaining_slots()
            else:
                for constraint in constraints:
                    res = constraint.satisfied_by_node(node)
                    assert res, "{} {} {}".format(res, constraint, node.vcpu_count)
                    m = constraint.minimum_space(node)
                    assert (
                        m != 0
                    ), "{} satisfies node {} but minimum_space was {}".format(
                        constraint, node.name, m
                    )

            per_node = 1 if total_iterations < 0 else min(remaining_slots(), min_space)
            if per_node == 0:
                for constraint in constraints:
                    res = constraint.satisfied_by_node(node)
                    assert res, "{} {} {}".format(res, constraint, node.vcpu_count)
                    m = constraint.minimum_space(node)
                    assert (
                        m != 0
                    ), "{} satisfies node {} but minimum_space was {}".format(
                        constraint, node.name, m
                    )

            assert per_node > 0, "{} {} {} {}".format(
                per_node, remaining_slots(), node.resources, constraints
            )
            return per_node

        assert remaining_slots() > 0

        if remaining_nodes() > available_count_total:
            return AllocationResult(
                "NoCapacity",
                reasons=[
                    "Not enough {} availability for request: Available {} requested {}. See {}".format(
                        bucket.vm_size,
                        available_count_total,
                        count - len(allocated_nodes),
                        bucket.limits,
                    )
                ],
            )

        for node in bucket.nodes:

            if node.closed:
                continue

            satisfied: Union[SatisfiedResult, bool]

            if constraints:
                for c in constraints:

                    result = c.satisfied_by_node(node)
                    if not result:
                        satisfied = result

                satisfied: bool = functools.reduce(lambda a, b: bool(a) and bool(b), [c.satisfied_by_node(node) for c in constraints])  # type: ignore
            else:
                satisfied = True

            if satisfied:
                
                do_decrement = node.state == "Deallocated" and not node.assignments

                per_node = _per_node(node, constraints)
                journal_entry = _JournalEntry(
                    node, constraints, per_node, assignment_id
                )
                match_result = journal_entry.precommit()
                assert match_result
                slots_allocated += match_result.total_slots

                self.__journal.append(journal_entry)
                allocated_nodes[node.name] = (node, node)

                if do_decrement:
                    bucket.decrement(1)

                if remaining_slots() <= 0:
                    break
        
        while remaining_slots() > 0 and bucket.available_count > 0:
            node_name = self._next_node_name(bucket)
            new_node = node_from_bucket(
                bucket,
                exists=False,
                state=ht.NodeStatus("Off"),
                target_state=ht.NodeStatus("Off"),
                # TODO what about deallocated? Though this is a 'new' node...
                power_state=ht.NodeStatus("Off"),
                placement_group=bucket.placement_group,
                new_node_name=node_name,
            )
            self._apply_defaults(new_node)

            assert new_node.vcpu_count == bucket.vcpu_count

            per_node = _per_node(new_node, constraints)

            match_result = new_node.decrement(constraints, per_node, assignment_id)

            assert match_result
            slots_allocated += match_result.total_slots

            new_nodes.append(new_node)

            assert new_node.name not in allocated_nodes

            allocated_nodes[new_node.name] = (new_node, new_node)
            if not new_node.exists:
                bucket.decrement(1)

        if remaining_slots() == initial_slot_count:
            return AllocationResult(
                "InsufficientResources",
                reasons=[
                    "Could not allocate {} slots for bucket {}".format(
                        remaining_slots(), bucket
                    )
                ],
            )

        if not allocated_nodes:
            raise RuntimeError(
                "Empty but successful node allocation for {} with constraints {}".format(
                    bucket, constraints
                )
            )

        # assert len(allocated_nodes) <= count

        if len(allocated_nodes) < count:
            msg = "Could only allocate {}/{} nodes".format(len(allocated_nodes), count)
            return AllocationResult("InsufficientCapacity", reasons=[msg])

        if commit:
            self._commit(bucket, list(allocated_nodes.values()))

        allocated_result_nodes = [n[1] for n in allocated_nodes.values()]

        return AllocationResult(
            "success", allocated_result_nodes, slots_allocated=slots_allocated
        )

    def _commit(
        self, bucket: NodeBucket, allocated_nodes: List[Tuple[Node, Node]]
    ) -> List[Node]:
        # TODO put this logic into the bucket.

        self.__journal = []

        by_name = partition(bucket.nodes, lambda n: n.name)
        new_nodes = [n[0] for n in allocated_nodes if not n[0].exists]

        for new_node in new_nodes:
            if new_node.name not in by_name:
                bucket.add_nodes([new_node])
                by_name[new_node.name] = [new_node]

        for node in new_nodes:
            self._node_names[node.name] = True

        bucket.commit()
        return [n[1] for n in allocated_nodes]

    def _rollback(self, bucket: NodeBucket) -> None:
        for entry in self.__journal:
            entry.rollback()

        self.__journal = []
        bucket.rollback()
        for name in list(self._node_names.keys()):
            if not self._node_names[name]:
                self._node_names.pop(name)

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

        available_count_total = self._available_count(bucket, allow_existing)
        if all_or_nothing:
            count = node_count
            if count > available_count_total:
                return 0
        else:
            count = min(node_count, available_count_total)

        if count == 0 or count > available_count_total:
            # TODO report what the user reported
            return 0

        assert count is not None
        return int(count)

    def _available_count(self, bucket: NodeBucket, allow_existing: bool) -> int:
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
            if self._next_node_name_hook:
                name = ht.NodeName(self._next_node_name_hook(bucket, index))
            else:
                name = ht.NodeName("{}-{}".format(bucket.nodearray, index))
            if name not in self._node_names:
                self._node_names[name] = False
                return name
            index += 1

    def set_node_name_hook(self, hook: Callable[[NodeBucket, int], str]) -> None:
        self._next_node_name_hook = hook

    @apitrace
    def add_unmanaged_nodes(self, existing_nodes: List[UnmanagedNode]) -> None:

        by_key: Dict[
            Tuple[Optional[ht.PlacementGroup], ht.BucketId], List[Node]
        ] = partition(
            # typing will complain that List[Node] is List[UnmanagedNode]
            # just a limitation of python3's typing
            existing_nodes,  # type: ignore
            lambda n: (n.placement_group, n.bucket_id),
        )

        buckets = partition_single(
            self.__node_buckets, lambda b: (b.placement_group, b.bucket_id)
        )

        for key, nodes_list in by_key.items():
            if key in buckets:
                buckets[key].add_nodes(nodes_list)
                continue

            placement_group, bucket_id = key

            a_node = nodes_list[0]
            # create a null definition, limits and bucket for each
            # unique set of unmanaged nodes
            node_def = NodeDefinition(
                nodearray=ht.NodeArrayName("__unmanaged__"),
                bucket_id=ht.BucketId(bucket_id),
                vm_size=ht.VMSize("unknown"),
                location=ht.Location("unknown"),
                spot=False,
                subnet=ht.SubnetId("unknown"),
                vcpu_count=a_node.vcpu_count,
                gpu_count=a_node.gpu_count,
                memory=a_node.memory,
                placement_group=placement_group,
                resources=ht.ResourceDict(dict(deepcopy(a_node.resources))),
                software_configuration=ImmutableOrderedDict(
                    a_node.software_configuration
                ),
            )

            limits = null_bucket_limits(len(nodes_list), a_node.vcpu_count)

            bucket = NodeBucket(
                node_def, limits, len(nodes_list), nodes_list, artificial=True
            )

            self.__node_buckets.append(bucket)
            bucket.add_nodes(nodes_list)

        self._apply_defaults_all()

    def get_nodes(self) -> List[Node]:
        # TODO slow
        ret: List[Node] = []
        for node_bucket in self.__node_buckets:
            ret.extend(node_bucket.nodes)
        return ret

    def get_non_failed_nodes(self) -> List[Node]:
        ret: List[Node] = []
        for node_bucket in self.__node_buckets:
            ret.extend([n for n in node_bucket.nodes if n.state != "Failed"])
        return ret

    def get_failed_nodes(self) -> List[Node]:
        ret: List[Node] = []
        for node_bucket in self.__node_buckets:
            ret.extend([n for n in node_bucket.nodes if n.state == "Failed"])
        return ret

    def get_new_nodes(self) -> List[Node]:
        return self.new_nodes

    def get_buckets(self) -> List[NodeBucket]:
        return self.__node_buckets

    def get_buckets_by_id(self) -> Dict[ht.BucketId, NodeBucket]:
        return partition_single(self.get_buckets(), lambda b: b.bucket_id)

    @apitrace
    def get_nodes_by_request_id(self, request_id: ht.RequestId) -> List[Node]:
        relevant_node_list = self.__cluster_bindings.get_nodes(
            request_id=request_id
        )
        return self._get_nodes_by_id(relevant_node_list)

    @apitrace
    def get_nodes_by_operation(self, operation_id: ht.OperationId) -> List[Node]:
        relevant_node_list = self.__cluster_bindings.get_nodes(
            operation_id=operation_id
        )
        return self._get_nodes_by_id(relevant_node_list)

    def _get_nodes_by_id(self, relevant_node_list: NodeList) -> List[Node]:
        relevant_node_names = [n["Name"] for n in relevant_node_list.nodes]
        updated_cluster_status = self.__cluster_bindings.get_cluster_status(True)

        updated_cc_nodes = partition_single(
            updated_cluster_status.nodes, lambda n: n["Name"]
        )

        nodes_by_name = partition_single(self.get_nodes(), lambda n: n.name)

        ret = []

        for name in relevant_node_names:
            if name in nodes_by_name:
                node = nodes_by_name[name]
                if name not in updated_cc_nodes:
                    logging.warning("%s no longer exists.", name)
                    node.exists = False
                    node.state = ht.NodeStatus("Off")
                    node.target_state = ht.NodeStatus("Off")
                else:
                    cc_node = updated_cc_nodes[name]
                    node.state = ht.NodeStatus(cc_node["Status"])
                    node.target_state = ht.NodeStatus(cc_node["TargetState"])
                    if node.delayed_node_id.node_id:
                        assert node.delayed_node_id.node_id == cc_node["NodeId"]
                    else:
                        logging.info(
                            "Found nodeid for %s -> %s", node, cc_node["NodeId"]
                        )
                        assert cc_node["NodeId"]
                        node.delayed_node_id.node_id = cc_node["NodeId"]
                    ret.append(node)

        return ret

    @apitrace
    def bootup(
        self,
        nodes: Optional[List[Node]] = None,
        request_id_start: Optional[ht.RequestId] = None,
        request_id_create: Optional[ht.RequestId] = None,
    ) -> BootupResult:
        nodes = nodes or self.new_nodes
        request_ids = [x for x in [request_id_start, request_id_create] if x]
        if not nodes:
            return BootupResult(
                "success",
                ht.OperationId(""),
                request_ids,
                reasons=["No new nodes required or created."],
            )

        nodes_to_start = [n for n in nodes if n.target_state == "Deallocated"]
        nodes_to_create = [n for n in nodes if n.target_state != "Deallocated"]
        booted_nodes = []

        operation_id: str = ""

        if nodes_to_start:
            start_result: NodeManagementResult = self.__cluster_bindings.start_nodes(
                nodes_to_start, request_id=request_id_start
            )
            operation_id = start_result.operation_id

            started_nodes = self.get_nodes_by_operation(start_result.operation_id)

            started_node_mappings: Dict[str, Node] = partition_single(
                started_nodes, lambda n: n.name
            )

            for offset, node in enumerate(started_nodes):
                node.delayed_node_id.operation_id = start_result.operation_id
                node.delayed_node_id.operation_offset = offset
                if node.name in started_node_mappings:
                    booted_nodes.append(node)
                else:
                    node.state = ht.NodeStatus("Unknown")

        if nodes_to_create:

            create_result: NodeCreationResult = self.__cluster_bindings.create_nodes(
                nodes_to_create, request_id=request_id_create
            )

            if operation_id:
                operation_id = operation_id + "," + create_result.operation_id
            else:
                operation_id = create_result.operation_id

            for s in create_result.sets:
                if s.message:
                    logging.warning(s.message)
                else:
                    logging.info("Create %d nodes", s.added)

            created_nodes = self.get_nodes_by_operation(create_result.operation_id)

            created_node_mappings: Dict[str, Node] = partition_single(
                created_nodes, lambda n: n.name
            )

            for offset, node in enumerate(created_nodes):
                node.delayed_node_id.operation_id = create_result.operation_id
                node.delayed_node_id.operation_offset = offset
                if node.name in created_node_mappings:
                    booted_nodes.append(node)
                else:
                    node.state = ht.NodeStatus("Unknown")

        return BootupResult(
            "success", ht.OperationId(operation_id), request_ids, booted_nodes
        )

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
        modifier: Optional[ResourceModifier] = None,
        modifier_magnitude: Optional[Union[int, float]] = None,
        allow_none: bool = True,
    ) -> None:
        assert resource_name
        if isinstance(default_value, str):
            if default_value.startswith("node."):
                attr = default_value[len("node.") :]  # noqa: E203

                if attr.startswith("resources."):
                    alias = attr[len("resources.") :]  # noqa: E203

                    def get_from_node_resources(node: Node) -> ht.ResourceTypeAtom:

                        if alias.endswith(".value"):
                            rname = alias[: -len(".value")]
                            value = node.resources.get(rname)
                        else:
                            rname = alias
                            value = node.resources.get(rname)

                        if value is None:
                            msg: str = (
                                "Could not define default resource name=%s with alias=%s"
                                + " for node/bucket %s because the node/bucket did not define %s as a resource"
                            )
                            logging.warning(
                                msg,
                                attr,
                                alias,
                                node,
                                alias,
                            )
                            value = ""

                        if alias.endswith(".value"):
                            return getattr(value, "value")

                        return value

                    default_value = get_from_node_resources
                elif attr.startswith("software_configuration."):
                    alias = attr[len("software_configuration.") :]  # noqa: E203

                    def get_from_node_software_configuration(
                        node: Node,
                    ) -> ht.ResourceTypeAtom:
                        value = node.software_configuration.get(alias)
                        if value is None:
                            msg: str = (
                                "Could not define default resource name=%s with alias=%s"
                                + " for node/bucket %s because the node/bucket did not define %s"
                                + " in its software_configuration"
                            )
                            logging.warning(
                                msg,
                                attr,
                                alias,
                                node,
                                alias,
                            )
                            value = ""

                        return value

                    default_value = get_from_node_software_configuration
                else:
                    acceptable = [
                        x for x in dir(Node) if x[0].isalpha() and x[0].islower()
                    ]
                    if attr not in dir(Node):
                        msg = "Invalid node.attribute '{}'. Expected one of {}".format(
                            attr, acceptable
                        )
                        raise RuntimeError(msg)

                    default_value = GetFromBaseNode(attr)

        if not isinstance(selection, list):
            # TODO add runtime type checking
            selection = [selection]  # type: ignore

        constraints = constraintslib.get_constraints(selection)

        default_value_expr = str(default_value)
        if not hasattr(default_value, "__call__"):

            def default_value_func(node: Node) -> ht.ResourceTypeAtom:
                # already checked if it has a call
                ret = default_value
                if isinstance(default_value, str):
                    if default_value.startswith("`") and default_value.endswith("`"):
                        expr = default_value[1:-1]
                        return eval(
                            "(lambda: {})()".format(expr), {"node": node.clone()}
                        )
                    elif re.match("size::[0-9a-zA-Z]+", default_value):
                        ret = ht.Size.value_of(default_value)
                    elif re.match("memory::[0-9a-zA-Z]+", default_value):
                        ret = ht.Memory.value_of(default_value)
                return ret  # type: ignore

        else:
            default_value_func = default_value  # type: ignore

        dr = _DefaultResource(
            constraints,
            resource_name,
            default_value_func,
            default_value_expr,
            modifier,
            modifier_magnitude,
            allow_none,
        )
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

    def add_placement_group(
        self, pg_name: ht.PlacementGroup, bucket: NodeBucket
    ) -> None:
        by_key = partition(
            self.__node_buckets, lambda b: (b.nodearray, b.placement_group)
        )

        key = (bucket.nodearray, pg_name)

        if key in by_key:
            logging.warning(
                "Bucket with nodearray=%s, vm_size=%s, location=%s and placement group %s already exists. Ignoring",
                bucket.nodearray,
                bucket.vm_size,
                bucket.location,
                pg_name,
            )
            return

        self.__node_buckets.append(bucket.clone_with_placement_group(pg_name))

    def get_placement_groups(self, bucket: NodeBucket) -> List[ht.PlacementGroup]:
        return [b.placement_group for b in self.__node_buckets if b.nodearray == bucket.nodearray and b.placement_group]

    @apitrace
    def deallocate_nodes(self, nodes: List[Node]) -> DeallocateResult:
        return self._nodes_operation(
            nodes, self.__cluster_bindings.deallocate_nodes, DeallocateResult
        )

    @apitrace
    def delete(self, nodes: List[Node]) -> DeleteResult:
        return self._nodes_operation(
            nodes, self.__cluster_bindings.delete_nodes, DeleteResult
        )

    @apitrace
    def remove_nodes(self, nodes: List[Node]) -> RemoveResult:
        return self._nodes_operation(
            nodes, self.__cluster_bindings.shutdown_nodes, RemoveResult
        )

    @apitrace
    def shutdown_nodes(self, nodes: List[Node]) -> ShutdownResult:
        return self._nodes_operation(
            nodes, self.__cluster_bindings.shutdown_nodes, ShutdownResult
        )

    @apitrace
    def start_nodes(self, nodes: List[Node], request_id: Optional[str] = None) -> StartResult:
        return self._nodes_operation(
            nodes, lambda n: self.__cluster_bindings.start_nodes(n, request_id=request_id), StartResult
        )

    @apitrace
    def terminate_nodes(self, nodes: List[Node]) -> TerminateResult:
        return self._nodes_operation(
            nodes, self.__cluster_bindings.terminate_nodes, TerminateResult
        )

    @property
    def cluster_bindings(self) -> ClusterBindingInterface:
        return self.__cluster_bindings

    def _nodes_operation(
        self,
        nodes: List[Node],
        function: Callable[[List[Node]], NodeManagementResult],
        ctor: Callable[[str, ht.OperationId, Optional[List[ht.RequestId]], List[Node]], T],
    ) -> T:
        managed_nodes = [node for node in nodes if node.managed]
        unmanaged_node_names = [node.name for node in nodes if not node.managed]
        op_name = function.__name__

        if unmanaged_node_names:
            logging.warning(
                "The following nodes will not be {} because node.managed=false: {}".format(
                    op_name, unmanaged_node_names
                )
            )

        if not managed_nodes:
            logging.warning("No nodes to {}".format(op_name))
            return ctor("success", ht.OperationId(""), None, [])

        result: NodeManagementResult = function(managed_nodes)

        by_name = partition_single(nodes, lambda n: n.name, strict=False)
        mgmt_by_name: Dict[ht.NodeName, NodeManagementResultNode]
        mgmt_by_name = partition_single(result.nodes, lambda n: n.name)

        affected_nodes: List[Node] = []

        # force the node.state to be updated
        self.get_nodes_by_operation(result.operation_id)

        for name, mgmt_node in mgmt_by_name.items():
            assert isinstance(mgmt_node, NodeManagementResultNode)

            if name not in by_name:
                continue

            node = by_name[name]
            if mgmt_node.status != "OK":
                logging.warning("%s was unaffected by call %s", node, function.__name__)
                continue

            affected_nodes.append(node)

            if node.target_state in ["Terminated", "Off"]:
                self._remove_node_internally(node)

        return ctor(
            "success", ht.OperationId(result.operation_id), None, affected_nodes
        )

    def _remove_node_internally(self, node: Node) -> None:
        by_bucket_id_and_pg = partition_single(
            self.__node_buckets, lambda b: (b.bucket_id, b.placement_group)
        )

        key = (node.bucket_id, node.placement_group)

        if key not in by_bucket_id_and_pg:
            logging.warning(
                "Unknown bucketid/placement_group??? %s not in %s",
                key,
                by_bucket_id_and_pg,
            )
            return

        bucket = by_bucket_id_and_pg[key]
        nodes_list = bucket.nodes

        if node not in nodes_list:

            logging.warning(
                (
                    "Somehow {} is not being tracked by bucket {}. "
                    + "Did you try to shutdown/terminate/delete a node twice?"
                ).format(node, bucket)
            )
            return

        bucket.nodes.remove(node)

    def set_system_default_resources(self) -> None:
        self.add_default_resource({}, "ncpus", "node.vcpu_count")
        self.add_default_resource({}, "pcpus", "node.pcpu_count")
        self.add_default_resource({}, "ngpus", "node.gpu_count")
        self.add_default_resource({}, "memb", MemoryDefault("b"))
        self.add_default_resource({}, "memkb", MemoryDefault("k"))
        self.add_default_resource({}, "memmb", MemoryDefault("m"))
        self.add_default_resource({}, "memgb", MemoryDefault("g"))
        self.add_default_resource({}, "memtb", MemoryDefault("t"))
        
        self.add_default_resource({}, "nodearray", "node.nodearray")
        self.add_default_resource(
            {}, "ccnodeid", lambda n: n.delayed_node_id.node_id
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
            instance_id=None,
            vm_size=ht.VMSize(vm_size),
            location=ht.Location(location),
            spot=False,
            vcpu_count=aux_info.vcpu_count,
            memory=aux_info.memory,
            infiniband=aux_info.infiniband,
            state=ht.NodeStatus("Off"),
            target_state=ht.NodeStatus("Off"),
            power_state=ht.NodeStatus("Off"),
            exists=False,
            placement_group=None,
            managed=False,
            resources=ht.ResourceDict({}),
            software_configuration=ImmutableOrderedDict({}),
            keep_alive=False,
        )
        self._apply_defaults(node)
        return node

    def __str__(self) -> str:
        attrs = []
        for attr_name in dir(self):
            if not (attr_name[0].isalpha() or attr_name.startswith("_NodeManager")):
                continue

            if "core_count" in attr_name:
                continue

            attr = getattr(self, attr_name)
            if "__call__" not in dir(attr):
                attr_expr = attr_name.replace("_NodeManager", "")
                attrs.append("{}={}".format(attr_expr, attr))
        return "NodeManager({})".format(", ".join(attrs))

    def __repr__(self) -> str:
        return str(self)

    def to_dict(self) -> Dict:
        ret = {}
        for attr_name in dir(self):
            if not (attr_name[0].isalpha() or attr_name.startswith("_NodeManager")):
                continue

            attr = getattr(self, attr_name)
            if "__call__" not in dir(attr):
                attr_expr = attr_name.replace("_NodeManager", "")

                if hasattr(attr, "to_dict"):
                    attr_value = attr.to_dict()
                else:
                    attr_value = str(attr)

                ret[attr_expr] = attr_value
        return ret


class GetFromBaseNode:
    def __init__(self, attr: str) -> None:
        self.attr = attr

    def __call__(self, node: Node) -> ht.ResourceTypeAtom:
        return getattr(node, self.attr)

    def __repr__(self) -> str:
        return "node.{}".format(self.attr)


class MemoryDefault:
    def __init__(self, mag: ht.MemoryMagnitude):
        self.mag = mag

    def __call__(self, node: Node) -> ht.ResourceTypeAtom:
        return node.memory.convert_to(self.mag)

    def __repr__(self) -> str:
        return "node.memory[{}]".format(self.mag)


@apitrace
def new_node_manager(
    config: dict,
    existing_nodes: Optional[List[UnmanagedNode]] = None,
    disable_default_resources: bool = True,
) -> NodeManager:

    logging.initialize_logging(config)

    ret = _new_node_manager_79(new_cluster_bindings(config), config)
    existing_nodes = existing_nodes or []

    if not disable_default_resources or not config.get(
        "disable_default_resources", False
    ):
        ret.set_system_default_resources()

    for entry in config.get("default_resources", []):

        try:
            assert isinstance(entry["select"], dict)
            assert isinstance(entry["name"], str)
            assert isinstance(entry["value"], (str, int, float, bool))
        except AssertionError as e:
            raise RuntimeError(
                "default_resources: Expected select=dict name=str value=str|int|float|bool: {}".format(
                    e
                )
            )
        modifier = None
        for op in ["add", "subtract", "multiply", "divide", "divide_floor"]:
            if op in entry:
                if modifier:
                    raise RuntimeError(
                        "Can not support more than one modifier for default resources at this time. {}".format(
                            entry
                        )
                    )
                modifier = op

        ret.add_default_resource(
            entry["select"],
            entry["name"],
            entry["value"],
            modifier,
            entry.get(modifier, None),
        )

    return ret


def _cluster_limits(cluster_name: str, cluster_status: ClusterStatus) -> _SharedLimit:

    cluster_active_cores = 0

    nodearray_regions = {}
    for nodearray_status in cluster_status.nodearrays:
        nodearray_regions[nodearray_status.name] = nodearray_status.nodearray["Region"]

    for cc_node in cluster_status.nodes:
        region = nodearray_regions[cc_node["Template"]]
        aux_info = vm_sizes.get_aux_vm_size_info(region, cc_node["MachineType"])
        cluster_active_cores += aux_info.vcpu_count

    return _SharedLimit(
        "Cluster({})".format(cluster_name),
        cluster_active_cores,
        cluster_status.max_core_count,  # noqa: E128
    )


def _new_node_manager_79(
    cluster_bindings: ClusterBindingInterface,
    autoscale_config: Dict,
) -> NodeManager:
    cluster_status = cluster_bindings.get_cluster_status(nodes=True)
    nodes_list = cluster_bindings.get_nodes()

    # to make it trivial to mimic 'onprem' nodes by simply filtering them out
    # of the response.
    mimic_on_prem = autoscale_config.get("_mimic_on_prem", [])
    if mimic_on_prem:
        nodes_list.nodes = [
            n for n in nodes_list.nodes if n["Name"] not in mimic_on_prem
        ]
        cluster_status.nodes = [
            n for n in cluster_status.nodes if n["Name"] not in mimic_on_prem
        ]

    assume_standalone_hostnames = autoscale_config.get(
        "assume_standalone_hostnames", False
    )

    all_node_names = [n["Name"] for n in nodes_list.nodes]

    buckets = []

    cluster_limit = _cluster_limits(cluster_bindings.cluster_name, cluster_status)

    filtered_nodes = []

    for cc_node in nodes_list.nodes:
        if "TargetState" not in cc_node:
            logging.error("Node %s created without a TargetState.", cc_node.get("Name"))
            logging.error(
                "You may attempt to start manually, but ignoring for autoscale."
            )
            continue
        else:
            filtered_nodes.append(cc_node)

    cc_nodes_by_template = partition(nodes_list.nodes, lambda n: n["Template"])

    nodearray_limits: Dict[str, _SharedLimit] = {}
    regional_limits: Dict[str, _SharedLimit] = {}
    regional_spot_limits: Dict[str, _SharedLimit] = {}
    family_limits: Dict[str, _SharedLimit] = {}

    for nodearray_status in cluster_status.nodearrays:
        nodearray = nodearray_status.nodearray
        nodearray_name = nodearray_status.name

        is_autoscale_disabled = (
            not nodearray.get("Configuration", {})
            .get("autoscale", {})
            .get("enabled", True)
        )

        if is_autoscale_disabled:
            logging.fine(
                "Ignoring nodearray %s because autoscale.enabled=false",
                nodearray_name,
            )
            continue

        region = nodearray["Region"]

        custom_resources = deepcopy(
            ht.ResourceDict(
                nodearray.get("Configuration", {})
                .get("autoscale", {})
                .get("resources", {})
            )
        )
        active_na_core_count = 0
        active_na_count = 0
        for cc_node in cc_nodes_by_template.get(nodearray_name, []):

            reached_final_state = cc_node["TargetState"] == cc_node["Status"]
            booting = cc_node["TargetState"] == "Started"
            if reached_final_state and not booting:
                continue
            aux_vm_info = vm_sizes.get_aux_vm_size_info(region, cc_node["MachineType"])
            active_na_count += 1
            active_na_core_count += aux_vm_info.vcpu_count

        nodearray_limits[nodearray_name] = _SharedLimit(
            "NodeArray({})".format(nodearray_name),
            active_na_core_count,
            nodearray_status.max_core_count,  # noqa: E128,
            active_na_count,
            nodearray_status.max_count,
        )

        spot = nodearray.get("Interruptible", False)

        for bucket in nodearray_status.buckets:
            if not bucket.valid:
                continue

            vcpu_count = bucket.virtual_machine.vcpu_count
            gpu_count = bucket.virtual_machine.gpu_count
            assert gpu_count is not None

            aux_vm_info = vm_sizes.get_aux_vm_size_info(
                region, bucket.definition.machine_type
            )
            assert isinstance(aux_vm_info, vm_sizes.AuxVMSizeInfo)
            vm_family = aux_vm_info.vm_family
            if vm_family == "unknown":
                vm_family = bucket.definition.machine_type

            # bugfix: If spot instances came first, their regional limits were imposed on
            # other buckets.
            if nodearray.get("Interruptible"):
                target_regional_limits = regional_spot_limits
            else:
                target_regional_limits = regional_limits

            if region not in target_regional_limits:
                target_regional_limits[region] = _SharedLimit(
                    "Region({})".format(region),
                    bucket.regional_consumed_core_count,
                    bucket.regional_quota_core_count,
                )

            if vm_family not in family_limits and not spot:
                family_limits[vm_family] = _SharedLimit(
                    "VM Family({})".format(vm_family),
                    bucket.family_consumed_core_count,
                    bucket.family_quota_core_count,
                )

            placement_groups = partition_single(
                bucket.placement_groups, lambda p: p.name
            )

            default_pgs = (
                autoscale_config.get("nodearrays", {})
                .get("default", {})
                .get("placement_groups", [])
            )
            nodearray_pgs = (
                autoscale_config.get("nodearrays", {})
                .get(nodearray_name, {})
                .get("placement_groups", [])
            )
            hardcoded_pg = nodearray_status.nodearray.get("PlacementGroupId")
            predefined_pgs = default_pgs + nodearray_pgs
            if hardcoded_pg:
                predefined_pgs.append(hardcoded_pg)

            for predef_pg in predefined_pgs:
                if predef_pg not in placement_groups:
                    placement_groups[predef_pg] = PlacementGroupStatus(
                        active_core_count=0, active_count=0, name=predef_pg
                    )

            # We allow a non-placement grouped bucket. We simply set it to none here and
            # downstream understands this
            if None not in placement_groups:
                placement_groups[None] = None

            assert placement_groups

            for pg_name, pg_status in placement_groups.items():
                placement_group_limit = None
                if pg_name:
                    placement_group_limit = _SharedLimit(
                        "PlacementGroup({})".format(pg_name),
                        consumed_core_count=pg_status.active_core_count,
                        max_core_count=bucket.max_placement_group_size * vcpu_count,
                    )

                vm_size = bucket.definition.machine_type
                location = nodearray["Region"]
                subnet = nodearray["SubnetId"]

                bucket_memory_gb = nodearray.get("Memory") or (
                    bucket.virtual_machine.memory
                )
                bucket_memory = ht.Memory(bucket_memory_gb, "g")

                bucket_id = bucket.bucket_id

                for n in nodes_list.nodes:
                    ov = autoscale_config.get("_node-overrides", {})
                    n.update(ov.get(n.get(""), {}))

                # TODO the bucket has a list of node names
                cc_node_records = [
                    n
                    for n in nodes_list.nodes
                    if n["Template"] == nodearray_name
                    and n["MachineType"] == bucket.definition.machine_type
                    and (n["TargetState"] == "Started" or n["Status"] == "Deallocated")
                    and n.get("PlacementGroupId") == pg_name
                ]

                if nodearray.get("Interruptible"):
                    # enabling spot/interruptible 0's out the family limit response
                    # as the regional limit is supposed to be used in its place,
                    # however that responsibility is on the caller and not the
                    # REST api. For this library we handle that for them.
                    family_limit = _SpotLimit(target_regional_limits[region])
                else:
                    family_limit: Union[_SpotLimit, _SharedLimit] = family_limits[
                        vm_family
                    ]

                na_limit = nodearray_limits[nodearray_name]
                # bug - unfortunately CycleCloud does not give an accurate 'available_count'
                # here. It excludes nodes in the process of being terminated. While this 
                # was originally intentional, combined with the bug where CC ignores the 
                # nodearray limit when starting deallocated nodes, we need to guard against that.
                # Our nodearray_limits already adds in nodes that are in the process of terminating / stopping,
                # so flooring the effective avail count gets us the correct behavior. 
                avail_count_with_na = min(na_limit._available_count(vcpu_count), bucket.available_count)
                avail_core_count_with_na = min(na_limit._available_count(1), bucket.available_core_count)
                bucket_limit = BucketLimits(
                    vcpu_count,
                    target_regional_limits[region],
                    cluster_limit,
                    na_limit,
                    family_limit,
                    placement_group_limit,
                    active_core_count=bucket.active_core_count,
                    active_count=bucket.active_count,
                    available_core_count=avail_core_count_with_na,
                    available_count=avail_count_with_na,
                    max_core_count=bucket.max_core_count,
                    max_count=bucket.max_count,
                )

                nodes = []

                for cc_node_rec in cc_node_records:
                    if cc_node_rec.get("TargetState") not in ["Started", "Deallocated"]:
                        logging.fine(
                            "Ignoring CycleCloud node %(Name)s (%(NodeId)s) with TargetState=%(TargetState)s"
                            % cc_node_rec
                        )
                        continue
                    node = _node_from_cc_node(
                        cc_node_rec, bucket, region, assume_standalone_hostnames
                    )
                    nodes.append(node)

                node_def = NodeDefinition(
                    nodearray=nodearray_name,
                    bucket_id=bucket_id,
                    vm_size=vm_size,
                    location=location,
                    spot=spot,
                    subnet=subnet,
                    vcpu_count=vcpu_count,
                    gpu_count=gpu_count,
                    memory=bucket_memory,
                    placement_group=pg_name,
                    resources=custom_resources,
                    software_configuration=ImmutableOrderedDict(
                        nodearray.get("Configuration", {})
                    ),
                )

                def nodes_key(n: Node) -> Tuple[str, int]:

                    try:
                        name, index = n.name.rsplit("-", 1)
                        return (name, int(index))
                    except Exception:
                        return (n.name, 0)

                node_bucket = NodeBucket(
                    node_def,
                    limits=bucket_limit,
                    max_placement_group_size=bucket.max_placement_group_size,
                    nodes=sorted(nodes, key=nodes_key),
                )

                logging.debug(
                    "Found %s with limits %s", node_bucket, repr(bucket_limit)
                )
                buckets.append(node_bucket)

    ret = NodeManager(cluster_bindings, buckets)
    for name in all_node_names:
        ret._node_names[name] = True
    return ret


def _node_from_cc_node(
    cc_node_rec: dict,
    bucket: NodearrayBucketStatus,
    region: ht.Location,
    assume_standalone_hostnames: bool = False,
) -> Node:
    node_id = ht.NodeId(cc_node_rec["NodeId"])
    node_name = ht.NodeName(cc_node_rec["Name"])
    nodearray_name = cc_node_rec["Template"]
    vm_size_node = cc_node_rec["MachineType"]
    software_configuration = ImmutableOrderedDict(cc_node_rec.get("Configuration", {}))
    hostname = cc_node_rec.get("Hostname")
    private_ip = cc_node_rec.get("PrivateIp")

    is_standalone_dns = (
        software_configuration.get("cyclecloud", {})
        .get("hosts", {})
        .get("standalone_dns", {})
        .get("enabled", True)
    )
    if is_standalone_dns and assume_standalone_hostnames and private_ip:
        ip_bytes = [int(x) for x in private_ip.split(".")]
        assumed_hostname = (
            "ip-" + ("".join(["{0:0{1}x}".format(x, 2) for x in ip_bytes])).upper()
        )
        if hostname != assumed_hostname:
            logging.debug(
                f"Assuming hostname {assumed_hostname} (currently is {hostname}) for {node_name}"
            )
            hostname = assumed_hostname

    instance_id = cc_node_rec.get("InstanceId")
    vcpu_count = cc_node_rec.get("CoreCount") or bucket.virtual_machine.vcpu_count
    node_memory_gb = cc_node_rec.get("Memory") or (bucket.virtual_machine.memory)

    node_memory = ht.Memory(node_memory_gb, "g")
    placement_group = cc_node_rec.get("PlacementGroupId")
    infiniband = bool(placement_group)

    state = ht.NodeStatus(str(cc_node_rec.get("Status")))
    target_state = ht.NodeStatus(str(cc_node_rec.get("TargetState")))
    resources = deepcopy(
        cc_node_rec.get("Configuration", {}).get("autoscale", {}).get("resources", {})
    )

    return Node(
        node_id=DelayedNodeId(node_name, node_id=node_id),
        name=node_name,
        nodearray=nodearray_name,
        bucket_id=bucket.bucket_id,
        vm_size=vm_size_node,
        hostname=hostname,
        private_ip=private_ip,
        instance_id=instance_id,
        location=region,
        spot=cc_node_rec.get("Interruptible", False),
        vcpu_count=vcpu_count,
        memory=node_memory,
        infiniband=infiniband,
        state=state,
        target_state=target_state,
        power_state=state,
        exists=target_state == "Started",
        placement_group=placement_group,
        managed=True,
        resources=resources,
        software_configuration=software_configuration,
        keep_alive=cc_node_rec.get("KeepAlive", False),
    )


class _DefaultResource:
    def __init__(
        self,
        selection: List[constraintslib.NodeConstraint],
        resource_name: str,
        default_value_function: DefaultValueFunc,
        default_value_expr: str,
        modifier: Optional[ResourceModifier],
        modifier_magnitude: Optional[Union[int, float, ht.Memory]],
        allow_none: bool,
    ) -> None:
        self.selection = selection
        self.resource_name = resource_name
        self.default_value_function = default_value_function
        self.default_value_expr = default_value_expr
        assert not (
            bool(modifier) ^ bool(modifier_magnitude is not None)
        ), "Please specify both modifier and modifier_magnitude"
        self.modifier = modifier
        self.modifier_magnitude = modifier_magnitude
        self.allow_none = allow_none

    def apply_default(self, node: Node) -> None:

        # obviously we don't want to override anything
        if node.resources.get(self.resource_name) is not None:
            return

        for criteria in self.selection:
            if not criteria.satisfied_by_node(node):
                return

        # it met all of our criteria, so set the default
        default_value = self.default_value_function(node)

        if default_value is None and not self.allow_none:
            logging.fine(
                "Ignoring default value None for resource %s", self.resource_name
            )
            return

        if self.modifier and default_value is not None:
            if not isinstance(default_value, (float, int, ht.Memory)):
                raise RuntimeError(
                    "Can't modify a default resource if the value is not an int, float or memory expression."
                    + "select={} name={} value={} {}={}".format(
                        self.selection,
                        self.resource_name,
                        default_value,
                        self.modifier,
                        self.modifier_magnitude,
                    )
                )
            assert self.modifier_magnitude
            try:
                if self.modifier == "add":
                    default_value = default_value + self.modifier_magnitude  # type: ignore
                elif self.modifier == "subtract":
                    default_value = default_value - self.modifier_magnitude  # type: ignore
                elif self.modifier == "multiply":
                    default_value = default_value * self.modifier_magnitude  # type: ignore
                elif self.modifier == "divide":
                    default_value = default_value / self.modifier_magnitude  # type: ignore
                elif self.modifier == "divide_floor":
                    default_value = default_value // self.modifier_magnitude  # type: ignore
                else:
                    raise RuntimeError("Unsupported modifier {}".format(self))
            except Exception as e:
                logging.error(
                    "Could not modify default value: 'Error={}' select={} name={} value={} {}={}".format(
                        e,
                        self.selection,
                        self.resource_name,
                        default_value,
                        self.modifier,
                        self.modifier_magnitude,
                    )
                )
                return

        node._resources[self.resource_name] = default_value
        node.available[self.resource_name] = default_value

    def __str__(self) -> str:
        return "DefaultResource(select={}, name={}, value={})".format(
            self.selection, self.resource_name, self.default_value_expr
        )

    def __repr__(self) -> str:
        return str(self)


class _JournalEntry:
    def __init__(
        self,
        node: Node,
        constraints: List[NodeConstraint],
        per_node: int,
        assignment_id: Optional[str],
    ) -> None:
        self.node = node
        self.constraints = constraints
        self.per_node = per_node
        self.assignment_id = assignment_id
        self.invoked = False
        self.rollback_node: Optional[Node] = None

    def precommit(self) -> MatchResult:
        assert not self.invoked
        assert not self.rollback_node
        self.rollback_node = self.node.clone()
        result = self.node.decrement(
            self.constraints, self.per_node, self.assignment_id
        )
        self.invoked = True
        return result

    def rollback(self) -> None:
        assert self.invoked
        assert self.rollback_node
        self.node.update(self.rollback_node)
        self.rollback_node = None

    def __repr__(self) -> str:
        return "JournalEntry({}, {}, {}, {})".format(
            self.node, self.per_node, self.assignment_id, self.constraints
        )
