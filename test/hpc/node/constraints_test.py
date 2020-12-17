from typing import Dict

from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.node.constraints import (
    BaseNodeConstraint,
    ExclusiveNode,
    InAPlacementGroup,
    MinResourcePerNode,
    Never,
    NodePropertyConstraint,
    NodeResourceConstraint,
    Or,
    SharedConsumableConstraint,
    SharedConsumableResource,
    SharedNonConsumableConstraint,
    SharedNonConsumableResource,
    XOr,
    get_constraint,
    get_constraints,
    register_parser,
)
from hpc.autoscale.node.node import (
    QUERYABLE_PROPERTIES,
    Node,
    UnmanagedNode,
    minimum_space,
)
from hpc.autoscale.results import SatisfiedResult


def setup_module() -> None:
    SchedulerNode.ignore_hostnames = True


def test_minimum_space() -> None:
    c = MinResourcePerNode("pcpus", 1)
    assert 1 == c.minimum_space(SchedulerNode("", {"pcpus": 1}))
    assert 2 == c.minimum_space(SchedulerNode("", {"pcpus": 2}))
    snode = SchedulerNode("", {"pcpus": 2})
    assert -1 == ExclusiveNode(assignment_id="1").minimum_space(snode)
    snode.assign("1")
    assert 0 == ExclusiveNode(assignment_id="1").minimum_space(snode)


def test_min_resource_per_node() -> None:
    assert (
        MinResourcePerNode("pcpus", 2).to_dict()
        == get_constraint({"pcpus": 2}).to_dict()
    )

    c = get_constraint({"pcpus": 2})
    assert isinstance(c, MinResourcePerNode)
    assert 0 == c.minimum_space(SchedulerNode(""))
    try:
        assert not c.do_decrement(SchedulerNode(""))
        assert False
    except RuntimeError:
        pass

    s = SchedulerNode("has-pcpus", {"pcpus": 4})
    assert s.available["pcpus"] == 4

    assert c.do_decrement(s)
    assert s.available["pcpus"] == 2
    assert s.resources["pcpus"] == 4
    assert not c.satisfied_by_node(SchedulerNode("no-blah-define"))
    assert not c.satisfied_by_node(SchedulerNode("wrong-blah-define", {"pcpus": 1}))
    assert c.satisfied_by_node(SchedulerNode("min-blah-define", {"pcpus": 2}))
    assert c.satisfied_by_node(SchedulerNode("more-blah-define", {"pcpus": 100}))


def test_node_property_constraint() -> None:
    assert (
        NodePropertyConstraint("vcpu_count", 2).to_dict()
        == get_constraint({"node.vcpu_count": 2}).to_dict()
    )
    assert isinstance(get_constraint({"node.vcpu_count": 2}), NodePropertyConstraint)
    for attr in dir(Node):
        if not attr[0].lower():
            continue
        try:
            get_constraint({"node.{}".format(attr): 2})
        except ValueError:
            assert attr not in QUERYABLE_PROPERTIES

    c = get_constraint({"node.vcpu_count": 2})
    assert -1 == c.minimum_space(SchedulerNode(""))
    assert c.do_decrement(SchedulerNode(""))


def test_node_resource_constraint() -> None:
    assert (
        NodeResourceConstraint("blah", "A").to_dict()
        == get_constraint({"blah": ["A"]}).to_dict()
    )
    c = get_constraint({"blah": ["A"]})
    assert isinstance(c, NodeResourceConstraint)
    assert -1 == c.minimum_space(SchedulerNode(""))
    assert c.do_decrement(SchedulerNode(""))
    assert not c.satisfied_by_node(SchedulerNode("no-blah-define"))
    assert not c.satisfied_by_node(SchedulerNode("wrong-blah-define", {"blah": "B"}))
    assert c.satisfied_by_node(SchedulerNode("wrong-blah-define", {"blah": "A"}))


def test_exclusive_node_parsing() -> None:
    assert (
        ExclusiveNode(True).to_dict() == get_constraint({"exclusive": True}).to_dict()
    )
    assert (
        ExclusiveNode(True).to_dict() == get_constraint({"exclusive": "true"}).to_dict()
    )
    assert ExclusiveNode(True).to_dict() == get_constraint({"exclusive": 1}).to_dict()
    # assert (
    #     ExclusiveNode(False).to_dict()
    #     == get_constraint({"exclusive": "faLse"}).to_dict()
    # )
    # assert ExclusiveNode(False).to_dict() == get_constraint({"exclusive": 0}).to_dict()
    # assert (
    #     ExclusiveNode(False).to_dict() == get_constraint({"exclusive": False}).to_dict()
    # )

    try:
        get_constraint({"exclusive": "asdf"})
        assert False
    except RuntimeError:
        pass


def test_job_excl() -> None:
    s = SchedulerNode("")
    # typical exclusive behavior - one task per job per node
    job_excl = get_constraint({"exclusive": True})
    assert job_excl.job_exclusive
    assert isinstance(job_excl, ExclusiveNode)
    assert job_excl.satisfied_by_node(s)
    assert -1 == job_excl.minimum_space(s)
    assert job_excl.do_decrement(s)

    s.assign("1")
    job_excl.assignment_id = "1"
    # can't put the same jobid on the same node twice
    assert not job_excl.satisfied_by_node(s)
    assert not job_excl.do_decrement(s)
    assert s.closed
    assert 0 == job_excl.minimum_space(s)


def test_task_excl() -> None:
    s = SchedulerNode("")

    # now to test tack exclusive, where multiple tasks from the same
    # job can run on the same machine
    task_excl = get_constraint({"exclusive_task": True})
    assert not task_excl.job_exclusive
    assert isinstance(task_excl, ExclusiveNode)
    assert task_excl.satisfied_by_node(s)
    assert -1 == task_excl.minimum_space(s)
    assert task_excl.do_decrement(s)

    s.assign("1")
    task_excl.assignment_id = "1"
    assert task_excl.satisfied_by_node(s)
    assert task_excl.do_decrement(s)

    assert s.closed

    assert -1 == task_excl.minimum_space(s)

    # cfalse = get_constraint({"exclusive": False})
    # cfalse.assignment_id = "2"
    # assert isinstance(cfalse, ExclusiveNode)
    # assert s.assignments
    # # s is already assign to something
    # assert not cfalse.satisfied_by_node(s)
    # assert 0 == cfalse.minimum_space(s)
    # try:
    #     cfalse.do_decrement(s)
    #     assert False
    # except RuntimeError:
    #     pass

    # s = SchedulerNode("")
    # assert cfalse.satisfied_by_node(s)
    # assert -1 == cfalse.minimum_space(s)
    # assert cfalse.do_decrement(s)


def test_in_a_placement_group() -> None:
    c = InAPlacementGroup()
    n1 = UnmanagedNode("ip-123", vm_size="Standard_F4", location="southcentralus")
    n2 = UnmanagedNode(
        "ip-234",
        vm_size="Standard_F4",
        location="southcentralus",
        placement_group="pg0",
    )
    assert not c.satisfied_by_node(n1)
    assert c.satisfied_by_node(n2)
    assert 0 == c.minimum_space(n1)
    assert -1 == c.minimum_space(n2)
    assert not c.do_decrement(n1)
    assert c.do_decrement(n2)


def test_or() -> None:
    assert (
        Or(
            NodeResourceConstraint("blah", "A"), NodeResourceConstraint("blah", "B")
        ).to_dict()
        == get_constraint({"or": [{"blah": ["A"]}, {"blah": ["B"]}]}).to_dict()
    )

    or_expr = {"or": [{"blah": ["A"]}, {"blah": ["B"]}]}
    assert isinstance(get_constraint(or_expr), Or)
    c = get_constraint({"node.vcpu_count": 2})
    assert -1 == c.minimum_space(SchedulerNode(""))
    assert c.do_decrement(SchedulerNode(""))


def test_xor() -> None:
    assert (
        XOr(
            NodeResourceConstraint("blah", "A"), NodeResourceConstraint("blah", "B")
        ).to_dict()
        == get_constraint({"xor": [{"blah": ["A"]}, {"blah": ["B"]}]}).to_dict()
    )

    xor_expr = {"xor": [{"blah": ["A"]}, {"blah": ["B"]}]}
    assert isinstance(get_constraint(xor_expr), XOr)

    c = XOr(NodeResourceConstraint("blah", "A"), NodeResourceConstraint("blah", "B"))
    assert not c.satisfied_by_node(SchedulerNode(""))
    assert not c.satisfied_by_node(SchedulerNode("", {"blah": ["A", "B"]}))
    assert c.satisfied_by_node(SchedulerNode("", {"blah": "A"}))
    assert c.satisfied_by_node(SchedulerNode("", {"blah": "B"}))
    assert c.do_decrement(SchedulerNode("", {"blah": "A"}))


# def test_non_exclusive_mixed() -> None:
#     simple = get_constraints([{"ncpus": 1}])
#     complex = get_constraints([{"ncpus": 1, "exclusive": False}])
#     node = SchedulerNode("", {"ncpus": 4})
#     assert minimum_space(simple, node) == 4
#     assert minimum_space(simple, node) == minimum_space(complex, node)


def test_memory() -> None:
    c = get_constraints([{"memgb": 1}])
    node = SchedulerNode("test", {"memgb": 4.0})
    m = minimum_space(c, node)
    assert isinstance(m, int)
    assert m == 4


def test_register_parser() -> None:
    class SimpleConstraint(BaseNodeConstraint):
        def __init__(self, name: str = "defaultname") -> None:
            self.name = name

        def satisfied_by_node(self, node: Node) -> SatisfiedResult:
            return SatisfiedResult("success", self, node)

        def to_dict(self) -> Dict:
            return {"custom-parser": self.name}

        @staticmethod
        def from_dict(d: Dict) -> "SimpleConstraint":
            assert "custom-parser" in d
            return SimpleConstraint(d["custom-parser"])

        def __str__(self) -> str:
            return "SimpleConstraint({})".format(self.name)

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, SimpleConstraint):
                return False
            return self.name == other.name

    register_parser("custom-parser", SimpleConstraint.from_dict)

    assert get_constraints([{"custom-parser": "a"}]) == [SimpleConstraint("a")]


def test_never() -> None:
    c = Never("my message")
    node = SchedulerNode("test", {"memgb": 4.0})
    assert not c.satisfied_by_node(node)
    assert c.satisfied_by_node(node).reasons == ["my message"]

    c = get_constraint({"never": "my other message"})
    assert isinstance(c, Never)
    assert c.message == "my other message"


def test_shared_constraint() -> None:
    qres = SharedConsumableResource("qres", "queue", 100, 100)
    cons = SharedConsumableConstraint([qres], 40)
    node = SchedulerNode("tux", {})

    assert cons.satisfied_by_node(node)
    assert cons.do_decrement(node)
    assert qres.current_value == 60

    assert cons.satisfied_by_node(node)
    assert cons.do_decrement(node)
    assert qres.current_value == 20

    assert not cons.satisfied_by_node(node)
    assert not cons.do_decrement(node)
    assert qres.current_value == 20

    cons = SharedConsumableConstraint([qres], 20)
    assert cons.satisfied_by_node(node)
    assert cons.do_decrement(node)
    assert qres.current_value == 0

    qres = SharedNonConsumableResource("qres", "queue", "abc")
    cons = SharedNonConsumableConstraint(qres, "abc")
    assert cons.satisfied_by_node(node)
    assert cons.do_decrement(node)
    assert qres.current_value == "abc"

    cons = SharedNonConsumableConstraint(qres, "xyz")
    assert not cons.satisfied_by_node(node)
    assert not cons.do_decrement(node)
    assert qres.current_value == "abc"

    global_qres = SharedConsumableResource("qres", "queue", 100, 100)
    queue_qres = SharedConsumableResource("qres", "queue", 50, 50)

    qcons = SharedConsumableConstraint([global_qres, queue_qres], 30)
    assert qcons.satisfied_by_node(node)
    assert qcons.do_decrement(node)
    assert global_qres.current_value == 70
    assert queue_qres.current_value == 20

    assert not qcons.satisfied_by_node(node)
    assert not qcons.do_decrement(node)
    assert global_qres.current_value == 70
    assert queue_qres.current_value == 20
