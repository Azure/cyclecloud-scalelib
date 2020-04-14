from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.node.constraints import (
    ExclusiveNode,
    MinResourcePerNode,
    get_constraint,
)


def test_minimum_space():
    c = MinResourcePerNode("pcpus", 1)
    assert 1 == c.minimum_space(SchedulerNode("nodey", {"pcpus": 1}))
    assert 2 == c.minimum_space(SchedulerNode("nodey", {"pcpus": 2}))
    snode = SchedulerNode("nodey", {"pcpus": 2})
    assert 1 == ExclusiveNode().minimum_space(snode)
    snode.assign("1")
    assert 0 == ExclusiveNode().minimum_space(snode)


def test_parsing():
    assert (
        MinResourcePerNode("pcpus", 2).to_dict()
        == get_constraint({"pcpus": 2}).to_dict()
    )
    assert isinstance(get_constraint({"pcpus": 2}), MinResourcePerNode)
