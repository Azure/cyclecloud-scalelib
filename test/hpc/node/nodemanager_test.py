from typing import Any, List

import pytest
from hypothesis import given, settings
from hypothesis import strategies as s

from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.node.node import Node, UnmanagedNode
from hpc.autoscale.node.nodemanager import NodeManager, new_node_manager
from hpc.autoscale.results import (
    DefaultContextHandler,
    register_result_handler,
    unregister_all_result_handlers,
)
from hpc.autoscale.util import partition


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
        max_count=20,
        available_count=10,
        family_consumed_core_count=0,
        family_quota_count=20,
    )

    bindings.add_nodearray("hpc", {"nodetype": "B", "pcpus": 4})
    bindings.add_bucket(
        "hpc",
        "Standard_F8s",
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
    node_mgr = _node_mgr(bindings)
    hpc, htc = node_mgr.get_buckets()
    if hpc.nodearray == "htc":
        hpc, htc = htc, hpc

    assert hpc.vm_family == htc.vm_family

    assert hpc.available_count == 10
    assert htc.available_count == 10
    result = node_mgr.allocate(
        {"node.nodearray": ["htc", "hpc"], "exclusive": True}, node_count=20
    )

    assert result and len(result.nodes) == 15
    assert hpc.available_count == 0
    assert htc.available_count == 0
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
    bindings = MockClusterBinding()
    bindings.add_nodearray("array-a", {"nodetype": "A"})
    bindings.add_bucket("array-a", "Standard_F4", 10, 10)
    bindings.add_nodearray("array-b", {"nodetype": "B"})
    bindings.add_bucket("array-b", "Standard_F4s", 10, 10)

    register_result_handler(DefaultContextHandler("[test_or_ordering]"))
    for ordering in [["A", "B"], ["B", "A"]]:
        node_mgr = _node_mgr(bindings)
        hi, lo = node_mgr.get_buckets()

        if hi.resources["nodetype"] != ordering[0]:
            hi, lo = lo, hi

        assert hi.available_count == 10
        assert lo.available_count == 10
        result = node_mgr.allocate(
            {
                "or": [{"nodetype": ordering[0]}, {"nodetype": ordering[1]}],
                "exclusive": True,
            },
            node_count=15,
        )
        assert hi.available_count == 0
        assert lo.available_count == 5
        assert result

        by_array = partition(result.nodes, lambda n: n.resources["nodetype"])
        assert len(by_array[ordering[0]]) == 10
        assert len(by_array[ordering[1]]) == 5


def test_choice_ordering() -> None:
    bindings = MockClusterBinding()
    bindings.add_nodearray("array-a", {"nodetype": "A"})
    bindings.add_bucket("array-a", "Standard_F4", 10, 10)
    bindings.add_nodearray("array-b", {"nodetype": "B"})
    bindings.add_bucket("array-b", "Standard_F4s", 10, 10)

    register_result_handler(DefaultContextHandler("[test_or_ordering]"))
    for ordering in [["A", "B"], ["B", "A"]]:
        node_mgr = _node_mgr(bindings)
        hi, lo = node_mgr.get_buckets()

        if hi.resources["nodetype"] != ordering[0]:
            hi, lo = lo, hi

        assert hi.available_count == 10
        assert lo.available_count == 10
        result = node_mgr.allocate(
            {"nodetype": ordering, "exclusive": True,}, node_count=15,  # noqa: E231
        )
        assert hi.available_count == 0
        assert lo.available_count == 5
        assert result

        by_array = partition(result.nodes, lambda n: n.resources["nodetype"])
        assert len(by_array[ordering[0]]) == 10
        assert len(by_array[ordering[1]]) == 5


def test_vm_family_limit(bindings: MockClusterBinding) -> None:
    bindings = MockClusterBinding("clusty")
    bindings.add_nodearray("htc", {"nodetype": "A"})
    bindings.add_bucket(
        "htc",
        "Standard_F4",
        available_count=20,
        max_count=20,
        family_quota_count=30,
        family_quota_core_count=120,
        family_consumed_core_count=0,
    )
    bindings.add_bucket(
        "htc",
        "Standard_F2",
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
    hpc, htc = _node_mgr(bindings).get_buckets()
    if hpc.nodearray != "hpc":
        hpc, htc = htc, hpc
    assert hpc.nodearray == "hpc"
    assert htc.nodearray == "htc"

    assert hpc.family_available_count == 10
    assert hpc.available_count == 10

    assert hpc.family_available_count == 10
    assert htc.family_available_count == 20

    hpc.decrement(1)
    assert hpc.family_available_count == 9
    assert htc.family_available_count == 18
    hpc.increment(1)
    assert hpc.family_available_count == 10
    assert htc.family_available_count == 20

    ctx.set_context("[failure]")
    nm = _node_mgr(bindings)

    b = MockClusterBinding()
    b.add_nodearray("haspgs", {})
    b.add_bucket(
        "haspgs",
        "Standard_F4",
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
    b.add_bucket("other", "Standard_F16", 1, 1)

    # a few specific with finally applying a global default
    node_mgr = _node_mgr(b)

    node_mgr.add_default_resource({"nodetype": "A"}, "vcpus", 2)
    node_mgr.add_default_resource({"nodetype": "B"}, "vcpus", "node.vcpu_count")
    node_mgr.add_default_resource({}, "vcpus", lambda node: node.vcpu_count - 2)

    by_nodetype = partition(node_mgr.get_buckets(), lambda b: b.resources["nodetype"])
    assert by_nodetype.get("A")[0].resources["vcpus"] == 2
    assert by_nodetype.get("B")[0].resources["vcpus"] == 8
    assert by_nodetype.get("C")[0].resources["vcpus"] == 14

    # use a Node function, which is essentially the same as the next
    node_mgr = _node_mgr(_bindings())
    node_mgr.add_default_resource({}, "vcpus", Node.vcpu_count)
    assert by_nodetype.get("A")[0].resources["vcpus"] == 2
    assert by_nodetype.get("B")[0].resources["vcpus"] == 8

    # use a node reference
    node_mgr = _node_mgr(_bindings())
    node_mgr.add_default_resource({}, "vcpus", "node.vcpu_count")

    by_nodetype = partition(node_mgr.get_buckets(), lambda b: b.resources["nodetype"])
    assert by_nodetype.get("A")[0].resources["vcpus"] == 4
    assert by_nodetype.get("B")[0].resources["vcpus"] == 8


@given(
    s.integers(1, 3),
    s.integers(1, 3),
    s.lists(
        s.integers(0, len(vm_sizes.VM_SIZES["southcentralus"]) - 1),
        min_size=9,
        max_size=9,
        unique=True,
    ),
    s.lists(s.integers(1, 25), min_size=1, max_size=10,),
    s.lists(s.integers(1, 32), min_size=20, max_size=20,),
    s.lists(s.booleans(), min_size=20, max_size=20,),
    s.lists(s.booleans(), min_size=20, max_size=20,),
    s.lists(s.integers(1, 2 ** 31), min_size=10, max_size=10,),
)
@settings(deadline=None)
def test_slot_count_hypothesis(
    num_arrays: int,
    num_buckets: int,
    vm_indices: List[int],
    magnitudes: List[int],
    ncpus_per_job: List[int],
    slots_or_nodes: List[bool],
    exclusivity: List[bool],
    shuffle_seeds: List[int],
) -> None:

    # construct a dc with num_buckets x num_arrays
    # use vm_indices to figure out which vms to pick
    def next_node_mgr(existing_nodes: List[Node]) -> NodeManager:
        bindings = MockClusterBinding()
        for_region = list(vm_sizes.VM_SIZES["southcentralus"].keys())

        for n in range(num_arrays):
            nodearray = "nodearray{}".format(n)
            bindings.add_nodearray(nodearray, {}, location="southcentralus")
            for b in range(num_buckets):
                vm_size = for_region[vm_indices[n * num_buckets + b]]
                bindings.add_bucket(
                    nodearray, vm_size, max_count=10, available_count=10,
                )

        return _node_mgr(bindings)

    # create len(job_iters) jobs with ncpus_per_job
    def make_requests(node_mgr: NodeManager) -> None:
        for n, mag in enumerate(magnitudes):
            node_count = None if slots_or_nodes[n] else mag
            slot_count = None if node_count else mag
            node_mgr.allocate(
                {"ncpus": ncpus_per_job[n], "exclusive": exclusivity[n]},
                node_count=node_count,
                slot_count=slot_count,
            )

    node_mgr = next_node_mgr([])
    assert len(node_mgr.get_buckets()) == num_buckets * num_arrays

    for bucket in node_mgr.get_buckets():
        assert bucket.resources["ncpus"] >= 1
        assert bucket.resources["ncpus"] == bucket.vcpu_count
        assert bucket.location == "southcentralus"

    make_requests(node_mgr)

    # let's take the previous existing nodes
    # and feed them into the next dc, simulating a repeating cron
    # to see that we get the same demand regardless of existing nodes
    existing_nodes = list(node_mgr.get_nodes())

    base_nodes = [UnmanagedNode(n.hostname, n.resources) for n in existing_nodes]

    for sseed in shuffle_seeds:
        import random

        random.seed(sseed)
        random.shuffle(existing_nodes)

        node_mgr = next_node_mgr(existing_nodes)

        existing_nodes = [
            UnmanagedNode(n.hostname, n.resources) for n in existing_nodes
        ]
        assert len(existing_nodes) == len(base_nodes)
        assert 0 == len(node_mgr.get_new_nodes())
        result = node_mgr.allocate({}, node_count=1)
        if result:
            assert 1 == len(node_mgr.get_new_nodes())


# def test_top_level_limits(node_mgr: NodeManager) -> None:
#     assert node_mgr.cluster_max_core_count == 20

if __name__ == "__main__":
    test_slot_count_hypothesis()
