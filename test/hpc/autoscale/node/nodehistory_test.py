from hashlib import md5
from typing import List, Optional

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodehistory import SQLiteNodeHistory


class EasyNode(Node):
    def __init__(
        self,
        name: ht.NodeName,
        node_id: Optional[DelayedNodeId] = None,
        nodearray: ht.NodeArrayName = ht.NodeArrayName("execute"),
        bucket_id: Optional[ht.BucketId] = None,
        hostname: Optional[ht.Hostname] = None,
        private_ip: Optional[ht.IpAddress] = None,
        instance_id: Optional[ht.InstanceId] = None,
        vm_size: ht.VMSize = ht.VMSize("Standard_F4"),
        location: ht.Location = ht.Location("westus"),
        spot: bool = False,
        vcpu_count: int = 4,
        memory: ht.Memory = ht.Memory.value_of("8g"),
        infiniband: bool = False,
        state: ht.NodeStatus = ht.NodeStatus("Off"),
        target_state: ht.NodeStatus = ht.NodeStatus("Off"),
        power_state: ht.NodeStatus = ht.NodeStatus("off"),
        exists: bool = False,
        placement_group: Optional[ht.PlacementGroup] = None,
        managed: bool = True,
        resources: ht.ResourceDict = ht.ResourceDict({}),
        software_configuration: dict = {},
        keep_alive: bool = False,
        gpu_count: Optional[int] = None,
    ) -> None:

        Node.__init__(
            self,
            name=name,
            nodearray=nodearray,
            hostname=hostname,
            node_id=node_id or DelayedNodeId(name),
            bucket_id=bucket_id or "b1",
            private_ip=private_ip,
            instance_id=instance_id,
            vm_size=vm_size,
            location=location,
            spot=spot,
            vcpu_count=vcpu_count,
            memory=memory,
            infiniband=infiniband,
            state=state,
            target_state=target_state,
            power_state=power_state,
            exists=exists,
            placement_group=placement_group,
            managed=managed,
            resources=resources,
            software_configuration=software_configuration,
            keep_alive=keep_alive,
            gpu_count=gpu_count,
        )


def new_booting_node(
    name: str,
    nodearray: str = "execute",
    keep_alive=False,
    state: str = "Acquiring",
    instance_id_suffix: str = "",
) -> Node:
    name = ht.NodeName(name)
    nodearray = ht.NodeArrayName(nodearray)
    return EasyNode(
        name=name,
        nodearray=nodearray,
        hostname=ht.Hostname("host" + name),
        node_id=DelayedNodeId(
            name=name,
            node_id=ht.NodeId(md5(name.encode()).hexdigest()),
            operation_id="op-1",
            operation_offset=0,
        ),
        exists=True,
        infiniband=False,
        instance_id=ht.InstanceId(
            md5(f"inst-{name}".encode()).hexdigest() + instance_id_suffix
        ),
        keep_alive=keep_alive,
        state=ht.NodeStatus(state),
        target_state=ht.NodeStatus("Started"),
        power_state=ht.NodeStatus("on"),
    )


class SQLiteNodeHistoryMockClock(SQLiteNodeHistory):
    def __init__(self, path: str = "nodehistory.db", read_only: bool = False) -> None:
        super().__init__(path, read_only)

        self.mock_now = 0.0

    def now(self) -> float:
        return self.mock_now


def test_ready_time() -> None:
    db = SQLiteNodeHistoryMockClock(":memory:")
    db.mock_now = 1000

    def nodes(state: str, instance_id_suffix: str = "") -> List[Node]:
        return [
            new_booting_node("e-1", state=state, instance_id_suffix=instance_id_suffix),
            new_booting_node("e-2", state=state, instance_id_suffix=instance_id_suffix),
            new_booting_node("e-3", state=state, instance_id_suffix=instance_id_suffix),
        ]

    # create 3 nodes at t=1000
    db.update(nodes("Acquiring"))
    assert 0 == len(db.find_booting(for_at_least=1800))
    db.mock_now += 1801
    assert db.now() == db.mock_now

    # at t=2801, the 3 nodes have now failed to boot
    assert 3 == len(db.find_booting(for_at_least=1800))
    db.mock_now = 1000

    # quick test that we can turn back time
    assert 0 == len(db.find_booting(for_at_least=1800))

    # now at t=4000, they again have timed out
    db.mock_now += 3000
    assert 3 == len(db.find_booting(for_at_least=1800))
    # change their state to Ready and the timeout does not apply
    db.update(nodes("Ready"))

    assert 0 == len(db.find_booting(for_at_least=1800))

    # BUG: if the node reverted to acquiring or any non-Ready state,
    # we would report the node as a boot timeout
    # however, since this node at least one time was ready, it should be fine.
    db.update(nodes("Acquiring"))
    assert 0 == len(db.find_booting(for_at_least=1800))

    # ok, so if the node has been stuck in acquiring for all this time,
    # we STILL do not consider it timed out, unless the instance id changed
    db.mock_now += 3000
    assert 0 == len(db.find_booting(for_at_least=1800))

    # the instance id changed... which RESTARTS the timer!
    db.update(nodes("Acquiring", instance_id_suffix="-2"))
    assert 0 == len(db.find_booting(for_at_least=1800))

    # Now they really have timed out
    db.mock_now += 3000
    assert 3 == len(db.find_booting(for_at_least=1800))
