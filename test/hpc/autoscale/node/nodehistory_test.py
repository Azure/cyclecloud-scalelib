from hashlib import md5
from typing import List, Optional

import pytest
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
            # Note nodeid needs to be a uuid like instance_id, so use the same suffix
            # when generating it here.
            node_id=ht.NodeId(md5((name + instance_id_suffix).encode()).hexdigest()),
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


def new_history_node(
    node_id: str, hostname: Optional[str] = None, instance_id: Optional[str] = None
) -> Node:
    name = ht.NodeName("history-node")
    return EasyNode(
        name=name,
        node_id=DelayedNodeId(name, ht.NodeId(node_id)),
        hostname=ht.Hostname(hostname) if hostname is not None else None,
        instance_id=ht.InstanceId(instance_id) if instance_id is not None else None,
        exists=True,
        state=ht.NodeStatus("Ready"),
    )


def test_quoted_hostname() -> None:
    db = SQLiteNodeHistoryMockClock(":memory:")
    db.mock_now = 1000
    node = EasyNode(
        name=ht.NodeName("quoted"),
        node_id=new_booting_node("quoted").delayed_node_id,
        hostname=ht.Hostname("O'Brien"),
        exists=True,
    )

    db.update([node])

    assert list(db.conn.execute("SELECT hostname FROM nodes")) == [("o'brien",)]


@pytest.mark.parametrize("field", ["hostname", "instance_id"])
@pytest.mark.parametrize("injection", [False, True])
def test_reported_strings_are_data(field: str, injection: bool) -> None:
    db = SQLiteNodeHistoryMockClock(":memory:")
    db.mock_now = 10000
    victim = new_history_node("victim", "healthy")
    db.update([victim])
    victim_row = db.conn.execute(
        "SELECT * FROM nodes WHERE node_id=?", ("victim",)
    ).fetchone()
    value = "O'Brien"
    if injection:
        prefix = "z', " if field == "hostname" else "z', 'x', "
        value = (
            prefix + "1, 1, 0, null, 1), ('victim', 'x', 'pwned', 1, 1, 0, null, 1) --"
        )
    attacker = new_history_node("attacker", **{field: value})
    sibling = new_history_node("sibling", "co-batched")

    db.update([victim, attacker, sibling])

    assert (
        db.conn.execute("SELECT * FROM nodes WHERE node_id=?", ("victim",)).fetchone()
        == victim_row
    )
    attacker_row = db.conn.execute(
        "SELECT hostname, instance_id FROM nodes WHERE node_id=?", ("attacker",)
    ).fetchone()
    assert attacker_row[0 if field == "hostname" else 1] == value.lower()
    assert db.conn.execute("SELECT count(*) FROM nodes").fetchone() == (3,)
    assert db.find_booting() == []
    assert db.find_unmatched() == []
    assert db.find_ignored() == []
    db.decorate([victim, sibling])
    assert victim.idle_time_remaining == 300
    assert sibling.create_time_unix == db.mock_now


@pytest.mark.parametrize("node_id", ["x' OR 1=1 --", 'x" OR 1=1 --'])
def test_quoted_node_id_operations(node_id: str) -> None:
    db = SQLiteNodeHistoryMockClock(":memory:")
    db.mock_now = 10000
    quoted = new_history_node(node_id.lower(), "quoted")
    victim = new_history_node("victim", "healthy")
    db.update([quoted, victim])
    db.decorate([quoted])
    assert quoted.create_time_unix == db.mock_now

    db.mark_ignored([quoted])
    assert [row[0] for row in db.find_ignored()] == [node_id.lower()]
    db.mark_ignored([victim])
    db.unmark_ignored([quoted])
    assert [row[0] for row in db.find_ignored()] == ["victim"]

    db.mock_now += 10
    db.update([victim])
    assert db.conn.execute(
        "SELECT delete_time FROM nodes WHERE node_id=?", (node_id.lower(),)
    ).fetchone() == (db.mock_now,)
    assert db.conn.execute(
        "SELECT delete_time FROM nodes WHERE node_id=?", ("victim",)
    ).fetchone() == (None,)


def test_legacy_string_normalization() -> None:
    db = SQLiteNodeHistoryMockClock(":memory:")
    db.mock_now = 10000
    db.update(
        [new_history_node("MiXeD", "HoSt", "InStAnCe"), new_history_node("missing")]
    )
    assert list(
        db.conn.execute(
            "SELECT node_id, hostname, instance_id, delete_time FROM nodes ORDER BY node_id"
        )
    ) == [("missing", "none", "none", None), ("mixed", "host", "instance", None)]
    mixed = new_history_node("MiXeD")
    db.mark_ignored([mixed])
    assert db.find_ignored() == []


@pytest.mark.parametrize(
    "block_size,node_count", [(None, 26), ("1", 3), ("25", 51), ("10000", 1100)]
)
def test_parameter_batches(
    monkeypatch: pytest.MonkeyPatch, block_size: Optional[str], node_count: int
) -> None:
    if block_size is None:
        monkeypatch.delenv("SCALELIB_SQLITE_INSERT_BLOCK", raising=False)
    else:
        monkeypatch.setenv("SCALELIB_SQLITE_INSERT_BLOCK", block_size)
    db = SQLiteNodeHistoryMockClock(":memory:")
    db.mock_now = 10000
    nodes = [new_history_node("node-{}".format(index)) for index in range(node_count)]

    db.update(nodes)
    db.decorate(nodes)
    assert all(node.create_time_unix == db.mock_now for node in nodes)
    assert db.conn.execute("SELECT count(*) FROM nodes").fetchone() == (node_count,)

    db.mock_now += 10
    db.update(nodes[:1])
    assert db.conn.execute(
        "SELECT count(*) FROM nodes WHERE delete_time=?", (db.mock_now,)
    ).fetchone() == (node_count - 1,)
    db.mock_now += 11
    db.retire_records(timeout=10)
    assert list(db.conn.execute("SELECT node_id FROM nodes")) == [("node-0",)]


@pytest.mark.parametrize("block_size", ["0", "-1"])
def test_invalid_insert_block_size(
    monkeypatch: pytest.MonkeyPatch, block_size: str
) -> None:
    monkeypatch.setenv("SCALELIB_SQLITE_INSERT_BLOCK", block_size)
    db = SQLiteNodeHistoryMockClock(":memory:")
    with pytest.raises(
        ValueError, match="SCALELIB_SQLITE_INSERT_BLOCK must be positive"
    ):
        db.update([new_history_node("node")])


def test_empty_and_read_only_operations() -> None:
    db = SQLiteNodeHistoryMockClock(":memory:")
    db.update([])
    db.decorate([])
    db.mark_ignored([])
    db.unmark_ignored([])
    db.read_only = True
    db.update([new_history_node("node", "O'Brien")])
    db.retire_records()
    assert list(db.conn.execute("SELECT * FROM nodes")) == []


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
    actual = len(db.find_booting(for_at_least=1800))
    assert 3 == actual
