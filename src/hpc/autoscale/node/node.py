import logging
from abc import ABC, abstractproperty
from copy import deepcopy
from typing import Dict, List, Optional, Set
from uuid import uuid4

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.constraints import NodeConstraint, minimum_space
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.results import MatchResult


class BaseNode(ABC):
    @abstractproperty
    def required(self) -> bool:
        ...

    @abstractproperty
    def name(self) -> ht.NodeName:
        ...

    @abstractproperty
    def nodearray(self) -> ht.NodeArrayName:
        ...

    @abstractproperty
    def bucket_id(self) -> ht.BucketId:
        ...

    @abstractproperty
    def vm_size(self) -> ht.VMSize:
        ...

    @abstractproperty
    def vm_family(self) -> ht.VMFamily:
        ...

    @abstractproperty
    def hostname(self) -> Optional[ht.Hostname]:
        ...

    @abstractproperty
    def private_ip(self) -> Optional[ht.IpAddress]:
        ...

    @abstractproperty
    def location(self) -> ht.Location:
        ...

    @abstractproperty
    def spot(self) -> bool:
        ...

    @abstractproperty
    def vcpu_count(self) -> int:
        ...

    @abstractproperty
    def memory(self) -> ht.Memory:
        ...

    @abstractproperty
    def infiniband(self) -> bool:
        ...

    @abstractproperty
    def state(self) -> ht.NodeStatus:
        ...

    @abstractproperty
    def exists(self) -> bool:
        ...

    @abstractproperty
    def placement_group(self) -> Optional[ht.PlacementGroup]:
        ...

    @abstractproperty
    def resources(self) -> ht.ResourceDict:
        ...

    @abstractproperty
    def managed(self) -> bool:
        ...

    @abstractproperty
    def delayed_node_id(self) -> DelayedNodeId:
        ...


class Node(BaseNode):
    def __init__(
        self,
        node_id: DelayedNodeId,
        name: ht.NodeName,
        nodearray: ht.NodeArrayName,
        bucket_id: ht.BucketId,
        hostname: Optional[ht.Hostname],
        private_ip: Optional[ht.IpAddress],
        vm_size: ht.VMSize,
        vm_family: ht.VMFamily,
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
        self.__vm_family = vm_family
        self.__hostname = hostname
        self.__private_ip = private_ip
        self.__location = location
        self.__spot = spot
        self.__vcpu_count = vcpu_count
        self.__memory = memory
        self.__infiniband = infiniband
        self.__resources = resources or ht.ResourceDict({})
        self.__available = deepcopy(self.__resources)

        self.__available["__vcpu_count"] = self.vcpu_count
        self.__available["__memory_mb"] = self.memory.convert_to("m").value

        self.__state = state
        self.__exists = exists
        self.__placement_group = placement_group
        self.__power_state = power_state
        self.__managed = managed
        self.__version = "7.9"
        self.__node_id = node_id
        self._allocated = False
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

    @property
    def required(self) -> bool:
        return self._allocated

    @required.setter
    def required(self, value: bool) -> None:
        assert value
        self._allocated = value

    @property
    def name(self) -> ht.NodeName:
        return self.__name

    @property
    def nodearray(self) -> ht.NodeArrayName:
        return self.__nodearray

    @property
    def bucket_id(self) -> ht.BucketId:
        return self.__bucket_id

    @property
    def vm_size(self) -> ht.VMSize:
        return self.__vm_size

    @property
    def vm_family(self) -> ht.VMFamily:
        return self.__vm_family

    @property
    def hostname(self) -> Optional[ht.Hostname]:
        return self.__hostname

    @property
    def hostname_required(self) -> ht.Hostname:
        if self.hostname is None:
            raise AssertionError("null hostname")
        return ht.Hostname(str(self.hostname))

    @property
    def private_ip(self) -> Optional[ht.IpAddress]:
        return self.__private_ip

    @property
    def location(self) -> ht.Location:
        return self.__location

    @property
    def spot(self) -> bool:
        return self.__spot

    @property
    def vcpu_count(self) -> int:
        return self.__vcpu_count

    @property
    def memory(self) -> ht.Memory:
        return self.__memory

    @property
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

    @property
    def placement_group(self) -> Optional[ht.PlacementGroup]:
        return self.__placement_group

    @property
    def resources(self) -> ht.ResourceDict:
        return self.__resources

    @property
    def managed(self) -> bool:
        return self.__managed

    @managed.setter
    def managed(self, value: bool) -> None:
        self.__managed = value

    @property
    def version(self) -> str:
        return self.__version

    @property
    def delayed_node_id(self) -> DelayedNodeId:
        return self.__node_id

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
        return self.__node_attribute_overrides

    def clone(self) -> "Node":
        return Node(
            node_id=self.__node_id.clone(),
            name=self.name,
            nodearray=self.nodearray,
            bucket_id=self.bucket_id,
            hostname=self.hostname,
            private_ip=self.private_ip,
            vm_size=self.vm_size,
            vm_family=self.vm_family,
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
            resources=ht.ResourceDict(deepcopy(self.resources)),
        )

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
                logging.debug(
                    "Updating %s.%s: %s->%s", self, attr, current_value, new_value,
                )
        self.available.update(snode.available)
        self.required = self.required or snode.required
        self.__assignments.update(snode.assignments)

    def __str__(self) -> str:
        hostname = self.hostname if self.exists else "..."
        return "Node({}, {})".format(self.name, hostname)

    def __repr__(self) -> str:
        return str(self)


class UnmanagedNode(Node):
    def __init__(
        self,
        hostname: str,
        resources: Optional[dict] = None,
        vcpu_count: Optional[int] = None,
        memory: Optional[ht.Memory] = None,
    ) -> None:
        resources = resources or ht.ResourceDict({})
        Node.__init__(
            self,
            node_id=DelayedNodeId(ht.NodeName(hostname)),
            name=ht.NodeName(hostname),
            nodearray=ht.NodeArrayName("unknown"),
            bucket_id=ht.BucketId("unknown"),
            hostname=ht.Hostname(hostname),
            private_ip=None,
            vm_size=ht.VMSize("unknown"),
            vm_family=ht.VMFamily("unknown"),
            location=ht.Location("unknown"),
            spot=False,
            vcpu_count=vcpu_count or 1,
            memory=memory or ht.Memory(0, "m"),
            infiniband=False,
            state=ht.NodeStatus("running"),
            power_state=ht.NodeStatus("running"),
            exists=True,
            placement_group=None,
            managed=False,
            resources=ht.ResourceDict(resources),
        )
        assert self.exists

    def __str__(self) -> str:
        return "Unmanaged{}".format(Node.__str__(self))

    def __repr__(self) -> str:
        return "Unmanaged{}".format(Node.__repr__(self))
