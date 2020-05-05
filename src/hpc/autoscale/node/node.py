import re
from abc import ABC
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

from frozendict import frozendict

import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.codeanalysis import hpcwrap
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.node.constraints import NodeConstraint
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.results import MatchResult

# state is added by default because it also has a setter
# property and most tools get confused by this
QUERYABLE_PROPERTIES: List[str] = ["state", "exists", "placement_group"]


def nodeproperty(function: Callable) -> property:
    QUERYABLE_PROPERTIES.append(function.__name__)
    return property(function)


class Node(ABC):
    def __init__(
        self,
        node_id: DelayedNodeId,
        name: ht.NodeName,
        nodearray: ht.NodeArrayName,
        bucket_id: ht.BucketId,
        hostname: Optional[ht.Hostname],
        private_ip: Optional[ht.IpAddress],
        vm_size: ht.VMSize,
        location: ht.Location,
        spot: bool,
        vcpu_count: int,
        memory: ht.Memory,
        infiniband: bool,
        state: ht.NodeStatus,
        power_state: ht.NodeStatus,
        exists: bool,
        placement_group: Optional[ht.PlacementGroup],
        managed: bool,
        resources: ht.ResourceDict,
    ) -> None:
        self.__name = name
        self.__nodearray = nodearray
        self.__bucket_id = bucket_id
        self.__vm_size = vm_size
        self.__hostname = hostname
        self.__private_ip = private_ip
        self.__location = location
        self.__spot = spot
        self.__vcpu_count = vcpu_count
        assert isinstance(memory, ht.Memory)
        self.__memory = memory
        self.__infiniband = infiniband

        self._resources = resources or ht.ResourceDict({})
        self.__available = deepcopy(self._resources)

        self.__state = state
        self.__exists = exists
        self.__placement_group = None
        # call the setter for extra validation
        self.placement_group = placement_group
        self.__power_state = power_state
        self.__managed = managed
        self.__version = "7.9"
        self.__node_id = node_id
        self._allocated: bool = False
        self.__closed = False
        self._node_index: Optional[int] = None
        if "-" in name:
            try:
                self._node_index = int(self.name.rsplit("-")[-1])
            except ValueError:
                pass
        self.__metadata: Dict = {}
        self.__node_attribute_overrides: Dict = {}
        self.__assignments: Set[str] = set()

        self.__aux_vm_info = vm_sizes.get_aux_vm_size_info(location, vm_size)

    @property
    def required(self) -> bool:
        return self._allocated

    @required.setter
    def required(self, value: bool) -> None:
        self._allocated = value

    @property
    def name(self) -> ht.NodeName:
        return self.__name

    @nodeproperty
    def nodearray(self) -> ht.NodeArrayName:
        return self.__nodearray

    @nodeproperty
    def bucket_id(self) -> ht.BucketId:
        return self.__bucket_id

    @nodeproperty
    def vm_size(self) -> ht.VMSize:
        return self.__vm_size

    @nodeproperty
    def vm_family(self) -> ht.VMFamily:
        return ht.VMFamily(self.__aux_vm_info.vm_family)

    @nodeproperty
    def hostname(self) -> Optional[ht.Hostname]:
        return self.__hostname

    @property
    def hostname_required(self) -> ht.Hostname:
        if self.hostname is None:
            raise AssertionError("null hostname")
        return ht.Hostname(str(self.hostname))

    @nodeproperty
    def private_ip(self) -> Optional[ht.IpAddress]:
        return self.__private_ip

    @nodeproperty
    def location(self) -> ht.Location:
        return self.__location

    @nodeproperty
    def spot(self) -> bool:
        return self.__spot

    @nodeproperty
    def vcpu_count(self) -> int:
        return self.__vcpu_count

    @nodeproperty
    def memory(self) -> ht.Memory:
        return self.__memory

    @nodeproperty
    def infiniband(self) -> bool:
        return self.__infiniband

    @property
    def state(self) -> ht.NodeStatus:
        return self.__state

    @state.setter
    def state(self, value: ht.NodeStatus) -> None:
        self.__state = value

    @property
    def exists(self) -> bool:
        return self.__exists

    @exists.setter
    def exists(self, value: bool) -> None:
        self.__exists = value

    @property
    def placement_group(self) -> Optional[ht.PlacementGroup]:
        return self.__placement_group

    @placement_group.setter
    def placement_group(self, value: Optional[ht.PlacementGroup]) -> None:
        if isinstance(value, str) and not value:
            value = None

        if self.__placement_group and value != self.__placement_group:
            if self.exists:
                raise RuntimeError(
                    "Can not change the placement group of an existing node: {} old={} new={}".format(
                        self, self.__placement_group, value
                    )
                )
        if value:
            if not re.match("^[a-zA-Z0-9_-]+$", value):
                raise RuntimeError(
                    "Invalid placement_group - must only contain letters, numbers, '-' or '_'"
                )
        self.__placement_group = value

    def set_placement_group_escaped(
        self, value: Optional[ht.PlacementGroup]
    ) -> Optional[ht.PlacementGroup]:
        if value:
            value = ht.PlacementGroup(re.sub("[^a-zA-z0-9-_]", "_", value))
        self.placement_group = value
        return self.placement_group

    @property
    def resources(self) -> ht.ResourceDict:
        return frozendict(self._resources)

    @property
    def managed(self) -> bool:
        return self.__managed

    @managed.setter
    def managed(self, value: bool) -> None:
        self.__managed = value

    @nodeproperty
    def version(self) -> str:
        return self.__version

    @property
    def delayed_node_id(self) -> DelayedNodeId:
        return self.__node_id

    @property
    def vm_capabilities(self) -> Dict[str, Any]:
        return self.__aux_vm_info.capabilities

    @nodeproperty
    def pcpu_count(self) -> int:
        return self.__aux_vm_info.pcpu_count

    @nodeproperty
    def gpu_count(self) -> int:
        return self.__aux_vm_info.gpu_count

    @nodeproperty
    def cores_per_socket(self) -> int:
        return self.__aux_vm_info.cores_per_socket

    @property
    def metadata(self) -> Dict:
        """
            Convenience: this is not used by the library at all,
            but allows the user to assign custom metadata to the nodes
            during allocation process. See results.DefaultContextHandler
            for an example.
        """
        return self.__metadata

    @property
    def node_attribute_overrides(self) -> Dict:
        """
            Override attributes for the Cloud.Node attributes created in
            Cyclecloud
        """
        if self.exists:
            return frozendict(self.__node_attribute_overrides)
        return self.__node_attribute_overrides

    def clone(self) -> "Node":
        ret = Node(
            node_id=self.__node_id.clone(),
            name=self.name,
            nodearray=self.nodearray,
            bucket_id=self.bucket_id,
            hostname=self.hostname,
            private_ip=self.private_ip,
            vm_size=self.vm_size,
            location=self.location,
            spot=self.spot,
            vcpu_count=self.vcpu_count,
            memory=self.memory,
            infiniband=self.infiniband,
            state=self.state,
            power_state=self.state,
            exists=self.exists,
            placement_group=self.placement_group,
            managed=self.managed,
            resources=ht.ResourceDict(deepcopy(self._resources)),
        )
        ret.available.update(self.available)
        return ret

    @property
    def closed(self) -> bool:
        return self.__closed

    @closed.setter
    def closed(self, value: bool) -> None:
        if value:
            self.__closed = value
        elif self.__closed:
            raise RuntimeError("Can not unclose a job.")

    @property
    def available(self) -> dict:
        return self.__available

    def decrement(
        self,
        constraints: List[NodeConstraint],
        iterations: int = 1,
        assignment_id: Optional[str] = None,
    ) -> MatchResult:
        """
        Assigns assignment_id if and only if the host has available resources. If successful, this method will decrement resources.
        """
        if self.closed:
            return MatchResult("NodeClosed", node=self, slots=iterations,)

        assignment_id = assignment_id or str(uuid4())

        reasons: List[str] = []
        is_unsatisfied = False
        for constraint in constraints:
            result = constraint.satisfied_by_node(self)
            if not result:
                is_unsatisfied = True
                # TODO need to propagate reason. Maybe a constraint result object?
                if hasattr(result, "reasons"):
                    reasons.extend(result.reasons)

        if is_unsatisfied:
            # TODO log why things are rejected at fine detail
            return MatchResult(
                "NodeRejected", node=self, slots=iterations, reasons=reasons,
            )

        min_space = minimum_space(constraints, self)
        assert isinstance(min_space, int)
        assert isinstance(iterations, int)

        if min_space == -1:
            min_space = iterations

        to_pack = min(iterations, min_space)

        for constraint in constraints:
            for i in range(to_pack):
                assert constraint.do_decrement(
                    self
                ), "calculated minimum space of {} but failed at index {}".format(
                    to_pack, i
                )

        self._allocated = True
        self.__assignments.add(assignment_id)
        return MatchResult("success", node=self, slots=to_pack)

    def assign(self, assignment_id: str) -> None:
        self.__assignments.add(assignment_id)

    @property
    def assignments(self) -> Set[str]:
        return self.__assignments

    def update(self, snode: "Node") -> None:
        for attr, new_value in snode.available.items():
            current_value = self.available.get(attr)
            if current_value != new_value:
                logging.warning(
                    "Updating %s.%s: %s->%s", self, attr, current_value, new_value,
                )
        self.available.update(snode.available)
        self.required = self.required or snode.required
        self.__assignments.update(snode.assignments)

    def __str__(self) -> str:
        hostname = self.hostname if self.exists else "..."
        return "Node({}, {})".format(self.name, hostname)

    def __repr__(self) -> str:
        hostname = self.hostname if self.exists else "..."
        return "Node({}, {}, {})".format(self.name, hostname, self.available)


class UnmanagedNode(Node):
    def __init__(
        self,
        hostname: str,
        resources: Optional[dict] = None,
        vm_size: Optional[ht.VMSize] = None,
        location: Optional[ht.Location] = None,
        vcpu_count: Optional[int] = None,
        memory: Optional[ht.Memory] = None,
        placement_group: Optional[ht.PlacementGroup] = None,
    ) -> None:
        resources = resources or ht.ResourceDict({})
        if vm_size:
            assert (
                vm_size and location
            ), "You must specify location when specifying vm_size"
        vm_size = vm_size or ht.VMSize("unknown")
        location = location or ht.Location("unknown")
        aux = vm_sizes.get_aux_vm_size_info(location, vm_size)
        Node.__init__(
            self,
            node_id=DelayedNodeId(ht.NodeName(hostname)),
            name=ht.NodeName(hostname),
            nodearray=ht.NodeArrayName("unknown"),
            bucket_id=ht.BucketId("unknown"),
            hostname=ht.Hostname(hostname),
            private_ip=None,
            vm_size=vm_size,
            location=location,
            spot=False,
            vcpu_count=aux.vcpu_count,
            memory=aux.memory,
            infiniband=False,
            state=ht.NodeStatus("running"),
            power_state=ht.NodeStatus("running"),
            exists=True,
            placement_group=placement_group,
            managed=False,
            resources=ht.ResourceDict(resources),
        )
        assert self.exists

    def __str__(self) -> str:
        return "Unmanaged{}".format(Node.__str__(self))

    def __repr__(self) -> str:
        return "Unmanaged{}".format(Node.__repr__(self))


@hpcwrap
def minimum_space(constraints: List[NodeConstraint], node: "Node") -> int:
    min_space = None if constraints else 1

    for constraint in constraints:
        # TODO not sure about how to handle this
        constraint_min_space = constraint.minimum_space(node)
        assert constraint_min_space is not None

        if constraint_min_space > -1:
            if min_space is None:
                min_space = constraint_min_space
            min_space = min(min_space, constraint_min_space)

    if min_space is None:
        min_space = -1

    return min_space
