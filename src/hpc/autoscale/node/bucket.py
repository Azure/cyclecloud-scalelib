import typing
from copy import deepcopy
from typing import Any, Dict, List, Optional

from immutabledict import ImmutableOrderedDict

from hpc.autoscale import hpctypes as ht
from hpc.autoscale import util
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.node import constraints as constraintslib  # noqa: F401
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.limits import BucketLimits
from hpc.autoscale.results import CandidatesResult, Result
from hpc.autoscale.util import partition

if typing.TYPE_CHECKING:
    from hpc.autoscale.node.node import Node


@hpcwrapclass
class NodeDefinition:
    """
    Can be serialized into a node request
    """

    def __init__(
        self,
        nodearray: ht.NodeArrayName,
        bucket_id: ht.BucketId,
        vm_size: ht.VMSize,
        location: ht.Location,
        spot: bool,
        subnet: ht.SubnetId,
        vcpu_count: int,
        memory: ht.Memory,
        placement_group: Optional[ht.PlacementGroup],
        resources: ht.ResourceDict,
        software_configuration: ImmutableOrderedDict,
    ) -> None:
        assert nodearray is not None
        self.nodearray = nodearray
        assert bucket_id is not None
        self.bucket_id = bucket_id
        assert vm_size is not None
        self.vm_size = vm_size
        assert location is not None
        self.location = location
        assert spot is not None
        self.spot = spot
        assert subnet is not None
        self.subnet = subnet
        assert vcpu_count is not None
        self.vcpu_count = vcpu_count
        assert memory is not None
        assert isinstance(memory, ht.Memory), "expected Memory, got {}".format(
            type(memory)
        )
        self.memory = memory
        self.placement_group = placement_group
        assert resources is not None
        self.resources = deepcopy(resources)
        assert software_configuration is not None
        self.software_configuration = software_configuration

    def __str__(self) -> str:
        attr_exprs: List[str] = []
        for attr_name in dir(self):
            if attr_name[0].isalpha():
                attr = getattr(self, attr_name)
                attr_exprs.append("{}={}".format(attr_name, repr(attr)))
        return "NodeDefinition({})".format(", ".join(attr_exprs))

    def __repr__(self) -> str:
        return str(self)


@hpcwrapclass
class NodeBucket:
    """
    combination of a definition plus the allocation constraints and existing nodes
    """

    def __init__(
        self,
        definition: NodeDefinition,
        limits: BucketLimits,
        max_placement_group_size: int,
        nodes: List["Node"],
        artificial: bool = False,
    ) -> None:
        # example node to be used to see if your job would match this
        self.__definition = definition

        assert limits
        self.limits = limits
        self.max_placement_group_size = max_placement_group_size
        self.priority = 0
        # list of nodes cyclecloud currently says are in this bucket
        self.nodes = nodes
        self.__decrement_counter = 0
        example_node_name = ht.NodeName("{}-0".format(definition.nodearray))
        self._artificial = artificial

        # TODO infiniband
        from hpc.autoscale.node.node import Node

        self.__example = Node(
            node_id=DelayedNodeId(example_node_name),
            name=example_node_name,
            nodearray=definition.nodearray,
            bucket_id=definition.bucket_id,
            hostname=None,
            private_ip=None,
            vm_size=definition.vm_size,
            location=self.location,
            spot=definition.spot,
            vcpu_count=self.vcpu_count,
            memory=self.memory,
            infiniband=False,
            state=ht.NodeStatus("Off"),
            power_state=ht.NodeStatus("Off"),
            exists=False,
            placement_group=definition.placement_group,
            managed=False,
            resources=self.resources,
            software_configuration=definition.software_configuration,
            keep_alive=False,
        )

    def decrement(self, count: int = 1) -> None:
        assert (
            self.available_count - count >= 0
        ), "Requested too many nodes: %s > %s" % (count, self.available_count)
        assert self.family_available_count >= 0
        self.__decrement_counter += count
        # self.limits.decrement(self.vcpu_count, count)

    def increment(self, count: int = 1) -> None:
        return self.decrement(-count)

    def commit(self) -> None:
        to_dec = self.__decrement_counter
        self.__decrement_counter = 0
        self.limits.decrement(to_dec)

    def rollback(self) -> None:
        self.__decrement_counter = 0

    @property
    def available_count(self) -> int:
        # non-pg buckets return -1
        pg_available = self.limits.placement_group_available_count

        if pg_available < 0:
            pg_available = 2 ** 32

        return (
            min(
                self.limits.available_count,
                self.limits.regional_available_count,
                self.limits.cluster_available_count,
                self.limits.nodearray_available_count,
                self.limits.family_available_count,
                pg_available,
            )
            - self.__decrement_counter
        )

    @available_count.setter
    def available_count(self, value: int) -> None:
        assert value >= 0, "{} < 0".format(value)
        self.limits.decrement(value)

    @property
    def family_available_count(self) -> int:
        return self.limits.family_available_count - self.__decrement_counter

    @property
    def location(self) -> ht.Location:
        return self.__definition.location

    @property
    def spot(self) -> bool:
        return self.__definition.spot

    @property
    def bucket_id(self) -> ht.BucketId:
        return self.__definition.bucket_id

    @property
    def memory(self) -> ht.Memory:
        return self.__definition.memory

    @property
    def placement_group(self) -> Optional[ht.PlacementGroup]:
        return self.__definition.placement_group

    @property
    def resources(self) -> ht.ResourceDict:
        return self.__definition.resources

    @property
    def subnet(self) -> ht.SubnetId:
        return self.__definition.subnet

    @property
    def vcpu_count(self) -> int:
        return self.__definition.vcpu_count

    @property
    def vm_size(self) -> ht.VMSize:
        return self.__definition.vm_size

    @property
    def vm_family(self) -> ht.VMFamily:
        return self.__example.vm_family

    @property
    def vm_capabilities(self) -> Dict[str, Any]:
        return self.__example.vm_capabilities

    @property
    def pcpu_count(self) -> int:
        return self.__example.pcpu_count

    @property
    def gpu_count(self) -> int:
        return self.__example.gpu_count

    @property
    def cores_per_socket(self) -> int:
        return self.__example.cores_per_socket

    @property
    def nodearray(self) -> ht.NodeArrayName:
        return self.__definition.nodearray

    @property
    def example_node(self) -> "Node":
        return self.__example

    @property
    def software_configuration(self) -> Dict:
        return self.__definition.software_configuration

    def add_nodes(self, nodes: List["Node"]) -> None:
        new_by_id = partition(nodes, lambda n: n.delayed_node_id.transient_id)
        cur_by_id = partition(self.nodes, lambda n: n.delayed_node_id.transient_id)

        filtered = []
        for new_id, new_nodes in new_by_id.items():
            if new_id not in cur_by_id:
                filtered.append(new_nodes[0])

        new_by_hostname = partition(filtered, lambda n: n.hostname_or_uuid)
        cur_by_hostname = partition(self.nodes, lambda n: n.hostname_or_uuid)

        for new_hostname, new_nodes in new_by_hostname.items():
            if new_hostname not in cur_by_hostname:
                self.nodes.append(new_nodes[0])

    def __str__(self) -> str:
        if self.placement_group:
            return "NodeBucket({}, available={}, pg={}, size={})".format(
                self.nodearray, self.available_count, self.placement_group, self.vm_size
            )
        return "NodeBucket({}, available={}, size={}, id={})".format(
            self.nodearray, self.available_count, self.vm_size, self.bucket_id
        )

    def __repr__(self) -> str:
        return str(self)


def bucket_candidates(
    candidates: List["NodeBucket"], constraints: List["constraintslib.NodeConstraint"],
) -> CandidatesResult:
    if not candidates:
        return CandidatesResult("NoBucketsDefined", child_results=[],)

    for c in candidates:
        assert isinstance(c, NodeBucket)

    satisfied_buckets: List["NodeBucket"] = []
    allocation_failures = []

    bucket_weights = []
    for n, bucket in enumerate(candidates):
        bucket_weights.append((bucket, float(len(candidates) - n)))

    for constraint in constraints:
        bucket_weights = constraint.weight_buckets(bucket_weights)

    sorted_bucket_weights = sorted(bucket_weights, key=lambda t: -t[1])
    sorted_candidates = [t[0] for t in sorted_bucket_weights]

    for bucket in sorted_candidates:
        reasons: List[Result] = []
        is_unsatisfied = False
        raw_scores: List[int] = []
        for constraint in constraints:
            # TODO reason
            result = constraint.satisfied_by_bucket(bucket)
            if not result:
                is_unsatisfied = True
                if hasattr(result, "reasons"):
                    reasons.append(result)
                break
            if result.score is not None:
                raw_scores.append(result.score)

        # as a tie breaker, the number of open nodes
        # num_open_nodes = len([n for n in bucket.nodes if not n.closed])
        # raw_scores.append(num_open_nodes)

        if is_unsatisfied:
            allocation_failures.extend(reasons)
        else:
            satisfied_buckets.append(bucket)

    if satisfied_buckets:
        for cc in constraints:
            for b in satisfied_buckets:
                assert cc.satisfied_by_bucket(b)
                assert cc.satisfied_by_node(b.example_node)
        artificial = []
        actual = []
        for c in satisfied_buckets:
            if c._artificial:
                artificial.append(c)
            else:
                actual.append(c)
        return CandidatesResult("success", candidates=artificial + actual)

    return CandidatesResult("CompoundFailure", child_results=allocation_failures)


def node_from_bucket(
    bucket: "NodeBucket",
    new_node_name: ht.NodeName,
    state: ht.NodeStatus,
    power_state: ht.NodeStatus,
    hostname: Optional[ht.Hostname] = None,
    placement_group: Optional[ht.PlacementGroup] = None,
    exists: bool = True,
) -> "Node":
    if hostname is None:
        hostname = ht.Hostname(util.uuid("hostname"))
    from hpc.autoscale.node.node import Node

    return Node(
        node_id=DelayedNodeId(new_node_name),
        name=new_node_name,
        nodearray=bucket.nodearray,
        bucket_id=bucket.bucket_id,
        vm_size=bucket.vm_size,
        hostname=hostname,
        private_ip=None,
        location=bucket.location,
        spot=bucket.spot,
        vcpu_count=bucket.vcpu_count,
        memory=bucket.memory,
        infiniband=False,  # TODO
        state=state,
        power_state=power_state,
        exists=exists,
        placement_group=placement_group,
        managed=True,
        resources=ht.ResourceDict(bucket.resources),
        software_configuration=bucket.software_configuration,
        keep_alive=False,
    )
