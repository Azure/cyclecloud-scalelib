import re
from abc import ABC
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

from immutabledict import ImmutableOrderedDict

import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.codeanalysis import hpcwrap
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.node.constraints import NodeConstraint
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.results import MatchResult

# state is added by default because it also has a setter
# property and most tools get confused by this
QUERYABLE_PROPERTIES: List[str] = [
    "state",
    "exists",
    "instance_id",
    "keep_alive",
    "placement_group",
    "create_time_unix",
    "last_match_time_unix",
    "delete_time_unix",
    "create_time_remaining",
]


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
        instance_id: Optional[ht.InstanceId],
        vm_size: ht.VMSize,
        location: ht.Location,
        spot: bool,
        vcpu_count: int,
        memory: ht.Memory,
        infiniband: bool,
        state: ht.NodeStatus,
        target_state: ht.NodeStatus,
        power_state: ht.NodeStatus,
        exists: bool,
        placement_group: Optional[ht.PlacementGroup],
        managed: bool,
        resources: ht.ResourceDict,
        software_configuration: ImmutableOrderedDict,
        keep_alive: bool,
        gpu_count: Optional[int] = None,
    ) -> None:
        self.__name = name
        self.__nodearray = nodearray
        self.__bucket_id = bucket_id
        self.__vm_size = vm_size
        self.__hostname = hostname
        self.__private_ip = private_ip
        self.__instance_id = instance_id
        self.__location = location
        self.__spot = spot
        self.__vcpu_count = vcpu_count
        assert isinstance(memory, ht.Memory), type(memory)
        self.__memory = memory
        self.__infiniband = infiniband

        self._resources = resources or ht.ResourceDict({})
        self.__available = deepcopy(self._resources)

        self.__state = state
        self.__target_state = target_state
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
        self.__marked_for_deletion = False
        if "-" in name:
            try:
                self._node_index = int(self.name.rsplit("-")[-1])
            except ValueError:
                pass
        self.__metadata: Dict = {}
        self.__node_attribute_overrides: Dict = {}
        self.__assignments: Set[str] = set()

        self.__aux_vm_info = vm_sizes.get_aux_vm_size_info(location, vm_size)
        self.__software_configuration = software_configuration

        self.__create_time = self.__last_match_time = self.__delete_time = 0.0
        self.__create_time_remaining = self.__idle_time_remaining = 0.0
        self.__keep_alive = keep_alive
        self.__gpu_count = (
            gpu_count if gpu_count is not None else self.__aux_vm_info.gpu_count
        )

    @property
    def required(self) -> bool:
        """Is this node required by either a job or other reasons, like being marked keep-alive=true"""
        return self.__keep_alive or self._allocated or bool(self.assignments)

    @required.setter
    def required(self, value: bool) -> None:
        self._allocated = value

    @property
    def keep_alive(self) -> bool:
        """Is this node protected by CycleCloud to prevent it from being terminated."""
        return self.__keep_alive

    @keep_alive.setter
    def keep_alive(self, v: bool) -> None:
        if self.exists:
            raise RuntimeError(
                "You can not change keep_alive on an existing node. {}".format(self)
            )
        self.__keep_alive = v

    @nodeproperty
    def name(self) -> ht.NodeName:
        """Name of the node in CycleCloud, e.g. `execute-1`"""
        return self.__name

    @nodeproperty
    def nodearray(self) -> ht.NodeArrayName:
        """NodeArray name associated with this node, e.g. `execute`"""
        return self.__nodearray

    @nodeproperty
    def bucket_id(self) -> ht.BucketId:
        """UUID for this combination of NodeArray, VM Size and Placement Group"""
        return self.__bucket_id

    @nodeproperty
    def vm_size(self) -> ht.VMSize:
        """Azure VM Size of this node, e.g. `Standard_F2`"""
        return self.__vm_size

    @nodeproperty
    def vm_family(self) -> ht.VMFamily:
        """Azure VM Family of this node, e.g. `standardFFamily`"""
        return ht.VMFamily(self.__aux_vm_info.vm_family)

    @nodeproperty
    def hostname(self) -> Optional[ht.Hostname]:
        if not self.__hostname:
            return self.__hostname
        return ht.Hostname(self.__hostname.split(".")[0])

    @nodeproperty
    def fqdn(self) -> Optional[ht.Hostname]:
        return self.__hostname

    @nodeproperty
    def hostname_or_uuid(self) -> Optional[ht.Hostname]:
        """Hostname or a UUID. Useful when partitioning a mixture of real and potential nodes by hostname"""
        return ht.Hostname(self.hostname or self.delayed_node_id.transient_id)

    @property
    def hostname_required(self) -> ht.Hostname:
        """
        Type safe hostname property - it will always return a hostname or an error.
        """
        if self.hostname is None:
            raise AssertionError("null hostname")
        return ht.Hostname(str(self.hostname))

    @nodeproperty
    def private_ip(self) -> Optional[ht.IpAddress]:
        """Private IP address of the node, if it has one."""
        return self.__private_ip

    @nodeproperty
    def location(self) -> ht.Location:
        """Azure location for this VM, e.g. `westus2`"""
        return self.__location

    @nodeproperty
    def spot(self) -> bool:
        """If true, this node is taking advantage of unused capacity for a cheaper rate"""
        return self.__spot

    @nodeproperty
    def vcpu_count(self) -> int:
        """Virtual CPU Count"""
        return self.__vcpu_count

    @nodeproperty
    def memory(self) -> ht.Memory:
        """Amount of memory, as reported by the Azure API. OS reported memory will differ."""
        return self.__memory

    @nodeproperty
    def infiniband(self) -> bool:
        """Does the VM Size of this node support infiniband"""
        return self.__infiniband

    @property
    def state(self) -> ht.NodeStatus:
        # TODO list out states
        """State of the node, as reported by CycleCloud."""
        return self.__state

    @state.setter
    def state(self, value: ht.NodeStatus) -> None:
        self.__state = value

    @property
    def target_state(self) -> ht.NodeStatus:
        # TODO list out states
        """State of the node, as reported by CycleCloud."""
        return self.__target_state

    @target_state.setter
    def target_state(self, value: ht.NodeStatus) -> None:
        self.__target_state = value

    @property
    def exists(self) -> bool:
        """Has this node actually been created in CycleCloud yet"""
        return self.__exists

    @exists.setter
    def exists(self, value: bool) -> None:
        self.__exists = value

    @nodeproperty
    def colocated(self) -> bool:
        """Will this node be put into a VMSS with a placement group, allowing infiniband"""
        return bool(self.placement_group)

    @property
    def placement_group(self) -> Optional[ht.PlacementGroup]:
        """If set, this node is put into a VMSS where all nodes with the same placement group are tightly coupled"""
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
        """
        Immutable dictionary (str->value) of the node's resources.
        """
        # for interactive modes
        from hpc.autoscale.clilib import ShellDict

        if isinstance(self._resources, ShellDict):
            return self._resources  # type: ignore
        return ImmutableOrderedDict(self._resources)

    @property
    def managed(self) -> bool:
        """Is this a CycleCloud managed node or not. Typically only onprem nodes have this set as false"""
        return self.__managed

    @managed.setter
    def managed(self, value: bool) -> None:
        self.__managed = value

    @nodeproperty
    def version(self) -> str:
        """Internal version property to handle upgrades"""
        return self.__version

    @property
    def delayed_node_id(self) -> DelayedNodeId:
        return self.__node_id

    @property
    def instance_id(self) -> Optional[ht.InstanceId]:
        """Azure VM Instance Id, if the node has a backing VM."""
        return self.__instance_id

    @instance_id.setter
    def instance_id(self, v: ht.InstanceId) -> None:
        self.__instance_id = v

    @property
    def vm_capabilities(self) -> Dict[str, Any]:
        """Dictionary of raw reported VM capabilities by the Azure VM SKU api"""
        return self.__aux_vm_info.capabilities

    @nodeproperty
    def pcpu_count(self) -> int:
        """Physical CPU count"""
        return self.__aux_vm_info.pcpu_count

    @nodeproperty
    def gpu_count(self) -> int:
        """GPU Count"""
        return self.__gpu_count

    @nodeproperty
    def cores_per_socket(self) -> int:
        """CPU cores per CPU socket"""
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
            return ImmutableOrderedDict(self.__node_attribute_overrides)
        return self.__node_attribute_overrides

    @property
    def create_time_unix(self) -> float:
        """When was this node created, in unix time"""
        return self.__create_time

    @create_time_unix.setter
    def create_time_unix(self, value: float) -> None:
        self.__create_time = value

    @nodeproperty
    def create_time(self) -> datetime:
        """When was this node created, as a datetime"""
        return datetime.fromtimestamp(self.create_time_unix)

    @property
    def create_time_remaining(self) -> float:
        """How much time is remaining for this node to reach the `Ready` state"""
        if self.state == "Ready":
            return -1
        return self.__create_time_remaining

    @create_time_remaining.setter
    def create_time_remaining(self, value: float) -> None:
        self.__create_time_remaining = max(0, value)

    @property
    def idle_time_remaining(self) -> float:
        """How much time is remaining for this node to be assigned a job or be marked ready for termination"""
        if self.assignments:
            return -1
        return self.__idle_time_remaining

    @idle_time_remaining.setter
    def idle_time_remaining(self, value: float) -> None:
        self.__idle_time_remaining = max(-1, value)

    @property
    def last_match_time_unix(self) -> float:
        """The last time this node was matched with a job, in unix time."""
        return self.__last_match_time

    @last_match_time_unix.setter
    def last_match_time_unix(self, value: float) -> None:
        self.__last_match_time = value

    @nodeproperty
    def last_match_time(self) -> datetime:
        """The last time this node was matched with a job, as datetime."""
        return datetime.fromtimestamp(self.last_match_time_unix)

    @property
    def delete_time_unix(self) -> float:
        """When was this node deleted, in unix time"""
        return self.__delete_time

    @delete_time_unix.setter
    def delete_time_unix(self, value: float) -> None:
        self.__delete_time = value

    @nodeproperty
    def delete_time(self) -> datetime:
        """When was this node deleted, as datetime"""
        return datetime.fromtimestamp(self.delete_time_unix or 0)

    @property
    def marked_for_deletion(self) -> bool:
        return self.__marked_for_deletion

    @marked_for_deletion.setter
    def marked_for_deletion(self, v: bool) -> None:
        self.__marked_for_deletion = v

    def clone(self) -> "Node":
        ret = Node(
            node_id=self.__node_id.clone(),
            name=self.name,
            nodearray=self.nodearray,
            bucket_id=self.bucket_id,
            hostname=self.hostname_or_uuid,
            private_ip=self.private_ip,
            instance_id=self.instance_id,
            vm_size=self.vm_size,
            location=self.location,
            spot=self.spot,
            vcpu_count=self.vcpu_count,
            memory=self.memory,
            infiniband=self.infiniband,
            state=self.state,
            target_state=self.target_state,
            power_state=self.state,
            exists=self.exists,
            placement_group=self.placement_group,
            managed=self.managed,
            resources=ht.ResourceDict(deepcopy(self._resources)),
            software_configuration=deepcopy(self.software_configuration),
            keep_alive=self.__keep_alive,
        )
        ret.available.update(deepcopy(self.available))
        ret.metadata.update(deepcopy(self.metadata))

        if not self.exists:
            ret.node_attribute_overrides.update(deepcopy(self.node_attribute_overrides))

        # for a_id in self.assignments:
        #     ret.assign(a_id)

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
                ), "calculated minimum space of {} but failed at index {} {} {}".format(
                    to_pack, i, constraint, constraint.satisfied_by_node(self),
                )

        self._allocated = True

        self.assign(assignment_id)

        return MatchResult("success", node=self, slots=to_pack)

    def assign(self, assignment_id: str) -> None:
        self.__assignments.add(assignment_id)

    @property
    def assignments(self) -> Set[str]:
        return self.__assignments

    @property
    def software_configuration(self) -> Dict:
        overrides = self.node_attribute_overrides
        if overrides and overrides.get("Configuration"):
            ret: Dict = {}
            ret.update(self.__software_configuration)
            ret.update(overrides["Configuration"])
        else:
            ret = self.__software_configuration

        if self.exists:
            return ImmutableOrderedDict(ret)

        return ret

    def update(self, snode: "Node") -> None:
        for attr, new_value in snode.available.items():
            current_value = self.available.get(attr)
            if current_value != new_value:
                level = (
                    logging.FINE
                    if current_value is None or snode.assignments
                    else logging.WARNING
                )
                logging.log(
                    level,
                    "Updating %s.%s: %s->%s",
                    self,
                    attr,
                    current_value,
                    new_value,
                )
        self.available.update(snode.available)
        self._resources.update(snode.resources)
        # TODO RDH test coverage
        self.required = self.required or snode.required or bool(snode.assignments)
        self.__assignments.update(snode.assignments)
        self.metadata.update(deepcopy(snode.metadata))

    def shellify(self) -> None:
        from hpc.autoscale.clilib import ShellDict

        self._resources = ShellDict(self._resources)  # type: ignore
        self.__available = ShellDict(self.__available)  # type: ignore
        self.__metadata = ShellDict(self.__metadata)  # type: ignore

    def _is_example_node(self) -> bool:
        return self.name == "{}-0".format(self.nodearray)

    def __str__(self) -> str:
        if self.name.endswith("-0"):
            return "NodeBucket(nodearray={}, vm_size={}, pg={})".format(
                self.nodearray, self.vm_size, self.placement_group
            )
        hostname = self.hostname if self.exists else "..."
        node_id = self.delayed_node_id.node_id
        if node_id:
            return "Node({}, {}, {}, {})".format(
                self.name, hostname, self.vm_size, node_id
            )
        return "Node({}, {}, {}, {})".format(
            self.name, hostname, self.vm_size, self.placement_group
        )

    def __repr__(self) -> str:
        hostname = self.hostname if self.exists else "..."
        return "Node({}, {}, {})".format(self.name, hostname, self.placement_group)

    def __lt__(self, node: Any) -> int:
        return node.hostname_or_uuid < self.hostname_or_uuid

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "hostname": self.hostname,
            "private-ip": self.private_ip,
            "instance-id": self.instance_id,
            "nodearray": self.nodearray,
            "placement_group": self.placement_group,
            "node-id": self.delayed_node_id.node_id,
            "operation-id": self.delayed_node_id.operation_id,
            "vm-size": self.vm_size,
            "location": self.location,
            "job-ids": list(self.assignments),
            "resources": dict(self.resources),
            "software-configuration": dict(self.software_configuration),
            "available": dict(self.available),
            "bucket-id": self.bucket_id,
            "metadata": dict(self.metadata),
            "spot": self.spot,
            "state": self.state,
            "exists": self.exists,
            "managed": self.managed,
        }

    @staticmethod
    def from_dict(d: Dict) -> "Node":
        hostname = ht.Hostname(d["hostname"])
        name = ht.NodeName(d["name"])
        node_id = ht.NodeId(d["node-id"])
        nodearray = ht.NodeArrayName(d["nodearray"])
        vm_size = ht.VMSize(d["vm-size"])
        location = ht.Location(d["location"])
        aux_info = vm_sizes.get_aux_vm_size_info(location, vm_size)
        job_ids = d.get("job-ids", [])

        resources = ht.ResourceDict(d.get("resources", {}))
        available = ht.ResourceDict(d.get("available", {}))

        for key, value in list(resources.items()):
            if not isinstance(value, str):
                continue
            if value.startswith("size::"):
                resources[key] = ht.Size.value_of(value)
            elif value.startswith("memory::"):
                resources[key] = ht.Memory.value_of(value)

        for key, value in list(available.items()):
            if not isinstance(value, str):
                continue
            if value.startswith("size::"):
                available[key] = ht.Size.value_of(value)
            elif value.startswith("memory::"):
                available[key] = ht.Memory.value_of(value)
        if d.get("memory"):
            memory: ht.Memory = ht.Memory.value_of(d["memory"])  # type: ignore
        else:
            memory = aux_info.memory
        ret = Node(
            node_id=DelayedNodeId(name, node_id),
            name=name,
            nodearray=nodearray,
            bucket_id=ht.BucketId(d["bucket-id"]),
            hostname=hostname,
            private_ip=ht.IpAddress(d["private-ip"]),
            instance_id=ht.InstanceId(d["instance-id"]),
            vm_size=vm_size,
            location=location,
            spot=d.get("spot", False),
            vcpu_count=d.get("vcpu-count", aux_info.vcpu_count),
            memory=memory,
            infiniband=aux_info.infiniband,
            state=ht.NodeStatus(d.get("state", "unknown")),
            target_state=ht.NodeStatus(d.get("state", "unknown")),
            power_state=ht.NodeStatus("on"),
            exists=d.get("exists", True),
            placement_group=d.get("placement_group"),
            managed=d.get("managed", False),
            resources=resources,
            software_configuration=d.get("software-configuration"),
            keep_alive=d.get("keep-alive", False),
        )

        for job_id in job_ids:
            ret.assign(job_id)

        ret.available.update(d.get("available", {}))
        ret.metadata.update(d.get("metadata", {}))

        return ret


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
            instance_id=None,
            vm_size=vm_size,
            location=location,
            spot=False,
            vcpu_count=vcpu_count or aux.vcpu_count,
            memory=memory or aux.memory,
            infiniband=False,
            state=ht.NodeStatus("running"),
            target_state=ht.NodeStatus("running"),
            power_state=ht.NodeStatus("running"),
            exists=True,
            placement_group=placement_group,
            managed=False,
            resources=ht.ResourceDict(resources),
            software_configuration=ImmutableOrderedDict({}),
            keep_alive=True,
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
