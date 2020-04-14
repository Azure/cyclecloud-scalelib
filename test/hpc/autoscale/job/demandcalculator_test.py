import pytest

from hpc.autoscale import results as resultslib
from hpc.autoscale import util
from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.hpctypes import Memory
from hpc.autoscale.job.demandcalculator import DemandCalculator, new_demand_calculator
from hpc.autoscale.job.job import Job
from hpc.autoscale.node.constraints import (
    ExclusiveNode,
    InAPlacementGroup,
    get_constraints,
)
from hpc.autoscale.node.nodehistory import NullNodeHistory
from hpc.autoscale.node.nodemanager import NodeManager

util.set_uuid_func(util.IncrementingUUID())


def setup_function(function) -> None:
    resultslib.register_result_handler(
        resultslib.DefaultContextHandler("[{}]".format(function.__name__))
    )


def teardown_function(function) -> None:
    resultslib.unregister_all_result_handlers()


def test_no_buckets():
    node_mgr = NodeManager(MockClusterBinding(), [])
    dc = DemandCalculator(node_mgr, NullNodeHistory())
    result = dc._add_job(Job("1", {"ncpus": 2}))
    assert not result
    assert "NoBucketsDefined" == result.status


def test_one_bucket_match_mpi():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_bucket(
        "twocpus", "Standard_F2", 2, Memory(8, "g"), 100, 100, placement_groups=["pg0"]
    )
    dc = _new_dc(bindings)
    result = dc._add_job(_mpi_job(nodes=2, resources={"ncpus": 2}))
    _assert_success(result, ["twocpus"] * 2)


def test_one_bucket_match_htc():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 4})
    bindings.add_bucket("twocpus", "Standard_F2", 4, Memory(8, "g"), 100, 100)
    dc = _new_dc(bindings)
    result = dc._add_job(_htc_job(resources={"ncpus": 2}))
    _assert_success(result, ["twocpus"])


def test_one_bucket_match_htc_reuse():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_bucket("twocpus", "Standard_F2", 2, Memory(8, "g"), 100, 100)
    dc = _new_dc(bindings)

    result = dc._add_job(_htc_job(job_name="1", resources={"ncpus": 1}))
    _assert_success(result, ["twocpus"])
    result = dc._add_job(_htc_job(job_name="2", resources={"ncpus": 1}))
    _assert_success(result, ["twocpus"])


def test_one_bucket_no_match_mpi() -> None:
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_bucket("twocpus", "Standard_F2", 2, Memory(8, "g"), 100, 100)
    dc = _new_dc(bindings)
    result = dc._add_job(_mpi_job(resources={"ncpus": 3}))
    assert not result
    # TODO doesn't say why,,,
    # _assert_insufficient_resource(result)


def test_one_bucket_no_match_htc():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_bucket("twocpus", "Standard_F2", 2, Memory(8, "g"), 100, 100)
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
    bindings.add_bucket(
        "hpc", "Standard_F2", 2, Memory(8, "g"), 100, 100, placement_groups=["pg0"]
    )
    bindings.add_bucket("htc", "Standard_F2", 4, Memory(16, "g"), 100, 100)
    return bindings


@pytest.fixture
def pgbindings():
    bindings = MockClusterBinding()
    bindings.add_nodearray("hpc2", {"ncpus": 2})
    bindings.add_nodearray("hpc4", {"ncpus": 4})
    bindings.add_bucket(
        "hpc2", "Standard_F2", 2, Memory(8, "g"), 100, 100, placement_groups=["pg0"]
    )
    bindings.add_bucket(
        "hpc4", "Standard_F4", 4, Memory(16, "g"), 100, 100, placement_groups=["pg0"]
    )
    return bindings


@pytest.fixture
def nopgbindings():
    bindings = MockClusterBinding()
    bindings.add_nodearray("twocpus", {"ncpus": 2})
    bindings.add_nodearray("fourcpus", {"ncpus": 4})
    bindings.add_bucket("twocpus", "Standard_F2", 2, Memory(8, "g"), 100, 100)
    bindings.add_bucket("fourcpus", "Standard_F2", 4, Memory(16, "g"), 100, 100)
    return bindings


def _new_dc(bindings: MockClusterBinding):
    return new_demand_calculator(
        config={"_mock_bindings": bindings},
        existing_nodes=[],
        node_history=NullNodeHistory(),
    )


def _mpi_job(job_name="1", nodes=1, placeby="pg", resources=None):
    resources = resources or {"ncpus": 2}
    job_constraints = get_constraints([resources])
    job_constraints.append(InAPlacementGroup())
    job_constraints.append(ExclusiveNode())
    return Job(
        job_name, job_constraints=job_constraints, node_count=nodes, colocated=True,
    )


def _htc_job(job_name="1", slot_count=1, resources=None):
    resources = resources or {"ncpus": 2}
    return Job(job_name, resources, iterations=slot_count)
