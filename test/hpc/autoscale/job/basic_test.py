import inspect
from typing import List, Optional

import pytest

from hpc.autoscale import results
from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.example.basic import BasicDriver, autoscale_driver
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.node import Node


def setup_function(function) -> None:
    SchedulerNode.ignore_hostnames = True
    results.register_result_handler(
        results.DefaultContextHandler("[{}]".format(function.__name__))
    )


def teardown_function(function) -> None:
    results.unregister_all_result_handlers()


@pytest.fixture
def bindings():
    return _bindings()


def _bindings():
    mock_bindings = MockClusterBinding()
    mock_bindings.add_nodearray(
        "htc2", {"slots": 2, "customer_htc_flag": True},
    )
    mock_bindings.add_nodearray(
        "htc4", {"slots": 4, "customer_htc_flag": True},
    )
    mock_bindings.add_bucket("htc2", "Standard_F2", 10, 8)
    mock_bindings.add_bucket("htc4", "Standard_F4", 5, 4)
    return mock_bindings


def test_basic_integration():
    def run_test(scheduler_nodes, jobs, unmatched, matched, new, mock_bindings=None):
        current_frame = inspect.currentframe()
        caller_frame = inspect.getouterframes(current_frame, 2)
        context = "[line:{}]".format(caller_frame[1][2])
        results.HANDLERS[0].set_context(context)
        SubTest(
            scheduler_nodes, jobs, mock_bindings, matched, unmatched, new
        ).run_test()

    def snodes():
        return [SchedulerNode("ip-010A0005", {"slots": 4})]

    def _xjob(jobid, constraints=None):
        constraints = constraints or [{"slots": 1}]
        if not isinstance(constraints, list):
            constraints = [constraints]
        constraints += [{"exclusive": True}]

        return Job(jobid, constraints=constraints)

    def _job(jobid, constraints=None, t=1):
        constraints = constraints or [{"slots": 1}]
        if not isinstance(constraints, list):
            constraints = [constraints]
        return Job(jobid, constraints=constraints, iterations=t)

    # fmt: off
    run_test(snodes(), [],                       unmatched=1, matched=0, new=0)  # noqa
    run_test(snodes(), [_xjob("1")],             unmatched=0, matched=1, new=0)  # noqa
    run_test(snodes(), [_xjob("1"), _xjob("2")], unmatched=0, matched=2, new=1)  # noqa      

    run_test(snodes(),
        [_xjob("1", {"customer_htc_flag": False}),  # noqa
         _xjob("2", {"customer_htc_flag": False})],  # noqa
                                                unmatched=1, matched=0, new=0)  # noqa

    run_test(
        snodes(),
        [_xjob("1", {"customer_htc_flag": True}),
            _xjob("2", {"customer_htc_flag": True})],
                                                unmatched=1, matched=2, new=2)  # noqa

    # ok, now let's make that scheduler node something CC is managing

    def run_test_b(*args, **kwargs):
        mock_bindings = _bindings()
        mock_bindings.add_node("htc2-500", "htc2", "F2")
        run_test(*args, **kwargs)

    run_test_b(snodes(), [],                      unmatched=1, matched=0, new=0)  # noqa
    run_test_b(snodes(), [_xjob("1")],            unmatched=0, matched=1, new=0)  # noqa
    run_test_b(snodes(), [_xjob("1"), _xjob("2")],unmatched=0, matched=2, new=1)  # noqa

    run_test_b(snodes(), [_job("1", t=4)],        unmatched=0, matched=1, new=0)  # noqa

    run_test_b(snodes(), [_job("1", {"slots": 4}, t=3,)],
                                                  unmatched=0, matched=3, new=2)  # noqa

    # fmt: on


class SubTest:
    def __init__(
        self,
        scheduler_nodes: Optional[List[Node]] = None,
        jobs: Optional[List[Job]] = None,
        mock_bindings: Optional[MockClusterBinding] = None,
        matched: Optional[int] = None,
        unmatched: Optional[int] = None,
        new_nodes: Optional[int] = None,
    ):

        self.scheduler_nodes = scheduler_nodes or []
        self.jobs = jobs or []
        self.mock_bindings = mock_bindings or _bindings()
        self.expected_matched = matched or 0
        self.expected_unmatched = unmatched or 0
        self.expected_new_nodes = new_nodes or 0

    def run_test(self):
        assert len(set([job.name for job in self.jobs])) == len(
            self.jobs
        ), "duplicate job id"
        driver = BasicDriver(self.jobs, self.scheduler_nodes)

        config = {"_mock_bindings": self.mock_bindings, "lock_file": None}

        demand_result = autoscale_driver(config, driver)

        msg = str(demand_result.compute_nodes)
        act_unmatched = len(demand_result.unmatched_nodes)

        assert self.expected_unmatched == act_unmatched, msg

        assert self.expected_matched == len(demand_result.matched_nodes), msg

        assert self.expected_new_nodes == len(demand_result.new_nodes), msg

        return demand_result

    def __str__(self) -> str:
        return "SubTest"

    def __repr__(self) -> str:
        return str(self)
