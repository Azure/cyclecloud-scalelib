from typing import Dict

from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.node.constraints import (
    BaseNodeConstraint,
    ExclusiveNode,
    InAPlacementGroup,
    MinResourcePerNode,
    NodePropertyConstraint,
    NodeResourceConstraint,
    Or,
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


def test_minimum_space():
    c = MinResourcePerNode("pcpus", 1)
    assert 1 == c.minimum_space(SchedulerNode("nodey", {"pcpus": 1}))
    assert 2 == c.minimum_space(SchedulerNode("nodey", {"pcpus": 2}))
    snode = SchedulerNode("nodey", {"pcpus": 2})
    assert 1 == ExclusiveNode().minimum_space(snode)
    snode.assign("1")
    assert 0 == ExclusiveNode().minimum_space(snode)


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


def test_exclusive_node() -> None:
    assert (
        ExclusiveNode(True).to_dict() == get_constraint({"exclusive": True}).to_dict()
    )
    assert (
        ExclusiveNode(True).to_dict() == get_constraint({"exclusive": "true"}).to_dict()
    )
    assert ExclusiveNode(True).to_dict() == get_constraint({"exclusive": 1}).to_dict()
    assert (
        ExclusiveNode(False).to_dict()
        == get_constraint({"exclusive": "faLse"}).to_dict()
    )
    assert ExclusiveNode(False).to_dict() == get_constraint({"exclusive": 0}).to_dict()
    assert (
        ExclusiveNode(False).to_dict() == get_constraint({"exclusive": False}).to_dict()
    )

    try:
        get_constraint({"exclusive": "asdf"})
        assert False
    except RuntimeError:
        pass

    s = SchedulerNode("ip-123")

    ctrue = get_constraint({"exclusive": True})
    assert isinstance(ctrue, ExclusiveNode)
    assert ctrue.satisfied_by_node(s)
    assert 1 == ctrue.minimum_space(s)
    assert ctrue.do_decrement(s)

    s.assign("1")
    assert not ctrue.satisfied_by_node(s)
    try:
        ctrue.do_decrement(s)
        assert False
    except RuntimeError:
        pass

    assert s.closed

    assert 0 == ctrue.minimum_space(s)

    cfalse = get_constraint({"exclusive": False})
    assert isinstance(cfalse, ExclusiveNode)
    assert not cfalse.satisfied_by_node(s)
    assert 0 == cfalse.minimum_space(s)
    try:
        cfalse.do_decrement(s)
        assert False
    except RuntimeError:
        pass

    s = SchedulerNode("")
    assert cfalse.satisfied_by_node(s)
    assert -1 == cfalse.minimum_space(s)
    assert cfalse.do_decrement(s)


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


def test_non_exclusive_mixed() -> None:
    simple = get_constraints([{"ncpus": 1}])
    complex = get_constraints([{"ncpus": 1, "exclusive": False}])
    node = SchedulerNode("test", {"ncpus": 4})
    assert minimum_space(simple, node) == 4
    assert minimum_space(simple, node) == minimum_space(complex, node)


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
            return SatisfiedResult("success")

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
