import logging
from typing import Any

import pytest

from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.hpctypes import Memory
from hpc.autoscale.node.node import BaseNode
from hpc.autoscale.node.nodemanager import NodeManager, new_node_manager
from hpc.autoscale.results import (
    DefaultContextHandler,
    register_result_handler,
    unregister_all_result_handlers,
)
from hpc.autoscale.util import partition

logging.basicConfig(
    filename="/tmp/log.txt", filemode="w", format="%(message)s", level=logging.DEBUG
)


def setup_function(function: Any) -> None:
    register_result_handler(DefaultContextHandler("[{}]".format(function.__name__)))


def teardown_function(function: Any) -> None:
    unregister_all_result_handlers()


@pytest.fixture
def bindings() -> MockClusterBinding:
    return _bindings()


def _bindings() -> MockClusterBinding:
    bindings = MockClusterBinding("clusty")
    bindings.add_nodearray("htc", {"nodetype": "A", "pcpus": 2})
    bindings.add_bucket(
        "htc",
        "Standard_F4s",
        4,
        Memory(8, "g"),
        max_count=20,
        available_count=10,
        family_consumed_core_count=0,
        family_quota_count=20,
    )

    bindings.add_nodearray("hpc", {"nodetype": "B", "pcpus": 4})
    bindings.add_bucket(
        "hpc",
        "Standard_F8s",
        4,
        Memory(8, "g"),
        max_count=20,
        available_count=10,
        family_consumed_core_count=0,
        family_quota_count=20,
    )

    return bindings


@pytest.fixture
def node_mgr(bindings: MockClusterBinding) -> NodeManager:
    return _node_mgr(bindings)


def _node_mgr(bindings: MockClusterBinding) -> NodeManager:
    return new_node_manager({"_mock_bindings": bindings})


def test_empty_cluster() -> None:
    node_mgr = _node_mgr(MockClusterBinding("clusty"))
    assert node_mgr.bootup()

    result = node_mgr.allocate({}, node_count=1)
    assert not result


def test_single_alloc(node_mgr: NodeManager) -> None:
    result = node_mgr.allocate({"node.nodearray": "htc"}, node_count=1)
    assert result and len(result.nodes) == 1
    assert result.nodes[0].nodearray == "htc"


def test_multi_alloc(node_mgr: NodeManager) -> None:
    result = node_mgr.allocate({"node.nodearray": "htc"}, node_count=10)
    assert result and len(result.nodes) == 10
    assert result.nodes[0].nodearray == "htc"


def test_over_allocate(node_mgr: NodeManager) -> None:
    assert node_mgr.allocate({"node.nodearray": "htc"}, node_count=1)
    # can't allocate 10, because there are only 9 left
    assert not node_mgr.allocate(
        {"node.nodearray": "htc"}, node_count=10, all_or_nothing=True
    )

    result = node_mgr.allocate(
        {"node.nodearray": "htc"}, node_count=10, all_or_nothing=False
    )
    assert result and len(result.nodes) == 9
    assert result.nodes[0].nodearray == "htc"


def test_multi_array_alloc(bindings: MockClusterBinding) -> None:
    result = _node_mgr(bindings).allocate(
        {"node.nodearray": ["htc", "hpc"], "exclusive": True}, node_count=20
    )
    assert result and len(result.nodes) == 20
    assert set(["htc", "hpc"]) == set([n.nodearray for n in result.nodes])


def test_packing(node_mgr: NodeManager) -> None:
    node_mgr.add_default_resource({}, "ncpus", 4)

    result = node_mgr.allocate({"node.nodearray": "htc", "ncpus": 1}, slot_count=2)
    assert result, str(result)
    assert len(result.nodes) == 1, result.nodes
    assert result.nodes[0].name == "htc-1"
    assert result.nodes[0].resources["ncpus"] == 4
    assert result.nodes[0].available["ncpus"] == 2, result.nodes[0].available["ncpus"]
    assert len(node_mgr.new_nodes) == 1, len(node_mgr.new_nodes)
    result = node_mgr.allocate({"node.nodearray": "htc", "ncpus": 1}, slot_count=4)
    assert result
    assert len(result.nodes) == 2, result.nodes
    assert result.nodes[0].name == "htc-1"
    assert result.nodes[1].name == "htc-2"
    assert len(node_mgr.new_nodes) == 2
    assert len(set([n.name for n in node_mgr.new_nodes])) == 2
    result = node_mgr.allocate({"node.nodearray": "htc", "ncpus": 1}, slot_count=2)
    assert len(result.nodes) == 1
    assert result.nodes[0].name == "htc-2"

    assert len(node_mgr.new_nodes) == 2


def test_or_ordering() -> None:
    register_result_handler(DefaultContextHandler("[test_or_ordering]"))
    for ordering in [["A", "B"], ["B", "A"]]:
        node_mgr = _node_mgr(_bindings())

        result = node_mgr.allocate(
            {"nodetype": ordering, "exclusive": True}, node_count=15
        )

        assert result and len(result.nodes) == 15
        assert set(["htc", "hpc"]) == set([n.nodearray for n in result.nodes])

        by_array = partition(result.nodes, lambda n: n.resources["nodetype"])
        assert len(by_array[ordering[0]]) == 10
        assert len(by_array[ordering[1]]) == 5


def test_vm_family_limit(bindings: MockClusterBinding) -> None:
    bindings = MockClusterBinding("clusty")
    bindings.add_nodearray("htc", {"nodetype": "A"})
    bindings.add_bucket(
        "htc",
        "Standard_F4",
        vcpu_count=4,
        memory=Memory(8, "g"),
        available_count=20,
        max_count=20,
        family_quota_count=30,
        family_quota_core_count=120,
        family_consumed_core_count=0,
    )
    bindings.add_bucket(
        "htc",
        "Standard_F2",
        vcpu_count=2,
        memory=Memory(4, "g"),
        available_count=20,
        max_count=20,
        family_quota_count=30,
        family_quota_core_count=120,
        family_consumed_core_count=0,
    )
    nm = _node_mgr(bindings)
    result = nm.allocate({}, node_count=100, all_or_nothing=False)
    assert len(result.nodes) == 40
    pass
    # assert len(result.nodes) == 1
    # assert result.nodes[0].vm_size == "Standard_F2"

    # result = nm.allocate({}, vcpu_count=100000, all_or_nothing=False)
    # assert result and len(result.nodes) == 30
    # result = nm.allocate({"node.vm_sizex": "Standard_F4s"}, node_count=10, all_or_nothing=False)
    # assert not result
    # result = nm.allocate({}, node_count=20, all_or_nothing=False)
    # assert result and len(result.nodes) == 10
    # assert set(["htc"]) == set([n.nodearray for n in result.nodes])


def test_mock_bindings(bindings: MockClusterBinding) -> None:
    ctx = register_result_handler(DefaultContextHandler("[test]"))
    for x in _node_mgr(bindings).get_buckets():
        assert x.family_available_count == 20
        assert x.available_count == 10

        x.decrement(2)
        assert x.family_available_count == 18
        assert x.available_count == 8

        x.increment(1)
        assert x.family_available_count == 19
        assert x.available_count == 9

        # make sure we go back to 20, otherwise it will fail.
        x.increment(1)
        assert x.family_available_count == 20
        assert x.available_count == 10

    bucket1, bucket2 = _node_mgr(bindings).get_buckets()
    assert bucket1.family_available_count == 20
    assert bucket2.family_available_count == 20
    bucket1.decrement(1)
    assert bucket1.family_available_count == 19
    assert bucket2.family_available_count == 19
    bucket1.increment(1)
    assert bucket1.family_available_count == 20
    assert bucket2.family_available_count == 20

    ctx.set_context("[failure]")
    nm = _node_mgr(bindings)
    assert 20 == len(nm.allocate({}, node_count=20).nodes)

    b = MockClusterBinding()
    b.add_nodearray("haspgs", {})
    b.add_bucket(
        "haspgs",
        "Standard_F4",
        4,
        Memory(8, "g"),
        100,
        100,
        max_placement_group_size=20,
        placement_groups=["pg0", "pg1"],
    )
    # make sure we take the max_placement_group_size (20) into account
    # and that we have the non-pg and 2 pg buckets.
    nm = _node_mgr(b)
    no_pg, pg0, pg1 = sorted(nm.get_buckets(), key=lambda b: b.placement_group or "")
    assert no_pg.available_count == 100
    assert pg0.available_count == 20
    assert pg1.available_count == 20

    # let's add a node to pg0 (100 - 1, 20 - 1, 20)
    b.add_node("haspgs-pg0-1", "haspgs", "Standard_F4", placement_group="pg0")

    nm = _node_mgr(b)
    no_pg, pg0, pg1 = sorted(nm.get_buckets(), key=lambda b: b.placement_group or "")
    assert no_pg.available_count == 99
    assert pg0.available_count == 19
    assert pg1.available_count == 20

    # let's add a node to pg1 (100 - 2, 20 - 1, 20 - 1)
    b.add_node("haspgs-pg1-1", "haspgs", "Standard_F4", placement_group="pg1")

    nm = _node_mgr(b)
    no_pg, pg0, pg1 = sorted(nm.get_buckets(), key=lambda b: b.placement_group or "")
    assert no_pg.available_count == 98
    assert pg0.available_count == 19
    assert pg1.available_count == 19

    # let's add 90 htc nodes so that our pg available counts are floored
    # by the overall available_count
    for i in range(90):
        b.add_node("haspgs-{}".format(i + 1), "haspgs", "Standard_F4")

    nm = _node_mgr(b)
    no_pg, pg0, pg1 = sorted(nm.get_buckets(), key=lambda b: b.placement_group or "")
    assert no_pg.available_count == 8
    assert pg0.available_count == 8
    assert pg1.available_count == 8

    # lastly, add a nother node to a pg and see that all of avail go down
    b.add_node("haspgs-pg1-2", "haspgs", "Standard_F4", placement_group="pg1")
    nm = _node_mgr(b)
    no_pg, pg0, pg1 = sorted(nm.get_buckets(), key=lambda b: b.placement_group or "")
    assert no_pg.available_count == 7
    assert pg0.available_count == 7
    assert pg1.available_count == 7


def test_default_resources() -> None:

    # set a global default
    node_mgr = _node_mgr(_bindings())

    for bucket in node_mgr.get_buckets():
        assert "vcpus" not in bucket.resources

    node_mgr.add_default_resource({}, "vcpus", 1)

    for bucket in node_mgr.get_buckets():
        assert 1 == bucket.resources["vcpus"]

    node_mgr.add_default_resource({}, "vcpus", 2)

    for bucket in node_mgr.get_buckets():
        assert 1 == bucket.resources["vcpus"]

    b = _bindings()
    b.add_nodearray("other", {"nodetype": "C"})
    b.add_bucket("other", "Standard_F4", 10, Memory.value_of("100g"), 1, 1)

    # a few specific with finally applying a global default
    node_mgr = _node_mgr(b)

    node_mgr.add_default_resource({"nodetype": "A"}, "vcpus", 2)
    node_mgr.add_default_resource({"nodetype": "B"}, "vcpus", "node.vcpu_count")
    node_mgr.add_default_resource({}, "vcpus", lambda node: node.vcpu_count // 2)

    by_nodetype = partition(node_mgr.get_buckets(), lambda b: b.resources["nodetype"])
    assert by_nodetype.get("A")[0].resources["vcpus"] == 2
    assert by_nodetype.get("B")[0].resources["vcpus"] == 4
    assert by_nodetype.get("C")[0].resources["vcpus"] == 5

    # use a BaseNode function, which is essentially the same as the next
    node_mgr = _node_mgr(_bindings())
    node_mgr.add_default_resource({}, "vcpus", BaseNode.vcpu_count)
    assert by_nodetype.get("A")[0].resources["vcpus"] == 2
    assert by_nodetype.get("B")[0].resources["vcpus"] == 4

    # use a node reference
    node_mgr = _node_mgr(_bindings())
    node_mgr.add_default_resource({}, "vcpus", "node.vcpu_count")

    by_nodetype = partition(node_mgr.get_buckets(), lambda b: b.resources["nodetype"])
    assert by_nodetype.get("A")[0].resources["vcpus"] == 4
    assert by_nodetype.get("B")[0].resources["vcpus"] == 4


# def test_top_level_limits(node_mgr: NodeManager) -> None:
#     assert node_mgr.cluster_max_core_count == 20
