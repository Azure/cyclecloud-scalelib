from typing import List, Optional

import pytest

from hpc.autoscale import results as resultslib
from hpc.autoscale import util
from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.job.demandcalculator import DemandCalculator, new_demand_calculator
from hpc.autoscale.job.job import Job
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.node.constraints import (
    ExclusiveNode,
    InAPlacementGroup,
    get_constraints,
)
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodehistory import NullNodeHistory
from hpc.autoscale.node.nodemanager import NodeManager
from hypothesis import given, settings
from hypothesis import strategies as s

util.set_uuid_func(util.IncrementingUUID())


def setup_function(function) -> None:
    resultslib.register_result_handler(
        resultslib.DefaultContextHandler("[{}]".format(function.__name__))
    )


def teardown_function(function) -> None:
    resultslib.unregister_all_result_handlers()


def test_no_buckets():
    node_mgr = NodeManager(MockClusterBinding(), [])
    dc = DemandCalculator(
        node_mgr, NullNodeHistory(), singleton_lock=util.NullSingletonLock()
    )
    result = dc._add_job(Job("1", {"ncpus": 2}))
    assert not result
    assert "NoBucketsDefined" == result.status


def test_one_bucket_match_mpi():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_bucket("twocpus", "Standard_F2", 100, 100, placement_groups=["pg0"])
    dc = _new_dc(bindings)
    result = dc._add_job(_mpi_job(nodes=2, resources={"ncpus": 2}))
    _assert_success(result, ["twocpus"] * 2)


def test_one_bucket_match_htc():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 4})
    bindings.add_bucket("twocpus", "Standard_F2", 100, 100)
    dc = _new_dc(bindings)
    result = dc._add_job(_htc_job(resources={"ncpus": 2}))
    _assert_success(result, ["twocpus"])


def test_one_bucket_match_htc_reuse():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_bucket("twocpus", "Standard_F2", 100, 100)
    dc = _new_dc(bindings)

    result = dc._add_job(_htc_job(job_name="1", resources={"ncpus": 1}))
    _assert_success(result, ["twocpus"])
    result = dc._add_job(_htc_job(job_name="2", resources={"ncpus": 1}))
    _assert_success(result, ["twocpus"])


def test_one_bucket_no_match_mpi() -> None:
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_bucket("twocpus", "Standard_F2", 100, 100)
    dc = _new_dc(bindings)
    result = dc._add_job(_mpi_job(resources={"ncpus": 3}))
    assert not result
    # TODO doesn't say why,,,
    # _assert_insufficient_resource(result)


def test_one_bucket_no_match_htc():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_bucket("twocpus", "Standard_F2", 100, 100)
    dc = _new_dc(bindings)
    result = dc._add_job(_htc_job(resources={"ncpus": 3}))
    _assert_insufficient_resource(result)


def test_two_buckets_one_viable_match_mpi(pgbindings):
    dc = _new_dc(pgbindings)
    result = dc._add_job(_mpi_job(resources={"ncpus": 3}))
    _assert_success(result, ["hpc4"])


def test_two_buckets_one_viable_match_mpi_pg(pgbindings):
    dc = _new_dc(pgbindings)
    result = dc._add_job(_mpi_job(resources={"ncpus": 3}, nodes=2))
    _assert_success(result, ["hpc4"] * 2)


def test_two_buckets_one_viable_match_htc(nopgbindings):
    dc = _new_dc(nopgbindings)

    result = dc._add_job(_htc_job(resources={"ncpus": 3}, slot_count=5))
    _assert_success(result, ["fourcpus"] * 5)


def test_two_buckets_mixed_job_type(mixedbindings):
    dc = _new_dc(mixedbindings)

    result = dc._add_job(_htc_job(resources={"ncpus": 3}, slot_count=5))
    _assert_success(result, ["htc"] * 5)


def test_two_buckets_one_viable_partial_match_htc(mixedbindings):
    dc = _new_dc(mixedbindings)

    result = dc._add_job(_mpi_job(resources={"ncpus": 2}, nodes=2))
    _assert_success(result, ["hpc"] * 2)
    result = dc._add_job(_htc_job(resources={"ncpus": 3}, slot_count=5))
    _assert_success(result, ["htc"] * 5)


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
    s.lists(s.integers(1, 2 ** 31), min_size=10, max_size=10,),
)
@settings(deadline=None)
def test_iterations_hypothesis(
    num_arrays: int,
    num_buckets: int,
    vm_indices: List[int],
    job_iters: List[int],
    ncpus_per_job: List[int],
    shuffle_seeds: List[int],
) -> None:

    # construct a dc with num_buckets x num_arrays
    # use vm_indices to figure out which vms to pick
    def next_dc(existing_nodes: List[Node]) -> DemandCalculator:
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

        return _new_dc(bindings)

    # create len(job_iters) jobs with ncpus_per_job
    def jobs_list() -> List[Job]:
        jobs = []
        for n, job_i in enumerate(job_iters):
            jobs.append(Job(str(n), {"ncpus": ncpus_per_job[n]}, iterations=job_i))
        return jobs

    dcbase = next_dc([])
    assert len(dcbase.node_mgr.get_buckets()) == num_buckets * num_arrays

    for bucket in dcbase.node_mgr.get_buckets():
        assert bucket.resources["ncpus"] >= 1
        assert bucket.resources["ncpus"] == bucket.vcpu_count
        assert bucket.location == "southcentralus"

    for job in jobs_list():
        dcbase.add_job(job)
    base_demand = dcbase.finish()

    # let's take the previous existing nodes
    # and feed them into the next dc, simulating a repeating cron
    # to see that we get the same demand regardless of existing nodes
    existing_nodes = list(base_demand.new_nodes)
    for sseed in shuffle_seeds:
        import random

        random.seed(sseed)
        random.shuffle(existing_nodes)

        dci = next_dc(existing_nodes)

        for job in jobs_list():
            dci.add_job(job)

        demandi = dci.get_demand()
        assert len(demandi.new_nodes) == len(base_demand.new_nodes), "{} != {}".format(
            demandi.new_nodes[0].resources, base_demand.new_nodes[1].resources
        )
        existing_nodes = list(demandi.new_nodes)


def test_bug100(mixedbindings) -> None:
    dcalc = _new_dc(mixedbindings)

    # # 100 cores
    dcalc.add_job(
        Job(
            "tc-10",
            {"node.nodearray": "htc", "ncpus": 1, "exclusive": False},
            iterations=10,
        )
    )
    demand = dcalc.finish()

    assert len(demand.new_nodes) == 3


def _assert_success(result, bucket_names, index_start=1):
    result == str(result)
    assert "success" == result.status

    node_names = []
    index = index_start
    last_name = None
    for name in bucket_names:
        if name != last_name:
            index = 1
        else:
            index += 1
        node_names.append("{}-{}".format(name, index))
        last_name = name
    assert len(node_names) == len(result.nodes)
    assert node_names == [n.name for n in result.nodes]


def _assert_insufficient_resource(result):
    assert not result
    for child_result in result.child_results:
        assert "InsufficientResource" == child_result.status, str(result)


def _assert_no_capacity(result):
    assert not result
    assert "NoCapacity" == result.status


@pytest.fixture
def mixedbindings():
    bindings = MockClusterBinding()
    bindings.add_nodearray("hpc", {"ncpus": 2})
    bindings.add_nodearray("htc", {"ncpus": 4})
    bindings.add_bucket("hpc", "Standard_F2", 100, 100, placement_groups=["pg0"])
    bindings.add_bucket("htc", "Standard_F2", 100, 100)
    return bindings


@pytest.fixture
def pgbindings():
    bindings = MockClusterBinding()
    bindings.add_nodearray("hpc2", {"ncpus": 2})
    bindings.add_nodearray("hpc4", {"ncpus": 4})
    bindings.add_bucket("hpc2", "Standard_F2", 100, 100, placement_groups=["pg0"])
    bindings.add_bucket("hpc4", "Standard_F4", 100, 100, placement_groups=["pg0"])
    return bindings


@pytest.fixture
def nopgbindings():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_nodearray("fourcpus", {"ncpus": 4})
    bindings.add_bucket("twocpus", "Standard_F2", 100, 100)
    bindings.add_bucket("fourcpus", "Standard_F2", 100, 100)
    return bindings


def _new_dc(bindings: MockClusterBinding, existing_nodes: Optional[List[Node]] = None):

    existing_nodes = existing_nodes or []
    return new_demand_calculator(
        config={"_mock_bindings": bindings},
        existing_nodes=existing_nodes,
        node_history=NullNodeHistory(),
        singleton_lock=util.NullSingletonLock(),
    )


def _mpi_job(job_name="1", nodes=1, placeby="pg", resources=None):
    resources = resources or {"ncpus": 2}
    constraints = get_constraints([resources])
    constraints.append(InAPlacementGroup())
    constraints.append(ExclusiveNode())
    return Job(job_name, constraints=constraints, node_count=nodes, colocated=True,)


def _htc_job(job_name="1", slot_count=1, resources=None):
    resources = resources or {"ncpus": 2}
    return Job(job_name, resources, iterations=slot_count)


if __name__ == "__main__":
    test_iterations_hypothesis()
