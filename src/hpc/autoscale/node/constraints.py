import typing
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import hpc  # noqa: F401
import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.codeanalysis import hpcwrap, hpcwrapclass
from hpc.autoscale.hpctypes import ResourceType
from hpc.autoscale.results import SatisfiedResult

ConstraintDict = typing.NewType("ConstraintDict", Dict[Any, Any])
# typing.NewType("ConstraintDict", typing.Dict[str, ResourceType])
Constraint = Union["NodeConstraint", ConstraintDict]


if typing.TYPE_CHECKING:
    from hpc.autoscale.node.node import Node, BaseNode
    from hpc.autoscale.node.bucket import NodeBucket


# TODO split by job and node constraints (job being a subclass of node constraint)
class NodeConstraint(ABC):
    @abstractmethod
    def satisfied_by_bucket(self, bucket: "NodeBucket") -> SatisfiedResult:
        raise RuntimeError()

    @abstractmethod
    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        raise RuntimeError()

    @abstractmethod
    def do_decrement(self, node: "Node") -> bool:
        raise RuntimeError()

    def minimum_space(self, node: "Node") -> int:
        return -1

    @abstractmethod
    def to_dict(self) -> dict:
        raise RuntimeError()

    @abstractmethod
    def __str__(self) -> str:
        ...

    def __repr__(self) -> str:
        return str(self)


class BaseNodeConstraint(NodeConstraint):
    def satisfied_by_bucket(self, bucket: "NodeBucket") -> SatisfiedResult:
        return self.satisfied_by_node(bucket.example_node)

    def do_decrement(self, node: "Node") -> bool:
        return True


@hpcwrapclass
class NodeResourceConstraint(BaseNodeConstraint):
    def __init__(self, attr: str, *values: ht.ResourceTypeAtom) -> None:
        self.attr = attr
        self.values: List[ResourceType] = list(values)

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        if self.attr not in node.available:
            # TODO log
            return SatisfiedResult(
                "UndefinedResource",
                self,
                node,
                [
                    "Resource[name={}] not defined Node[name={} hostname={}]".format(
                        self.attr, node.name, node.hostname
                    )
                ],
            )
        target = node.available[self.attr]

        if target not in self.values:
            if len(self.values) > 1:
                msg = "Resource[name={} value={}] is not one of the options {} for node[name={} attr={}]".format(
                    self.attr, target, self.values, node.name, self.attr,
                )
            else:
                msg = "Resource[name={} value={}] != node[name={} {}={}]".format(
                    self.attr, target, node.name, self.attr, self.values[0]
                )

            return SatisfiedResult("InvalidOption", self, node, [msg],)
        return SatisfiedResult(
            "success", self, node, score=len(self.values) - self.values.index(target),
        )

    def __str__(self) -> str:
        if len(self.values) == 1:
            return "(Node.{} == {})".format(self.attr, repr(self.values[0]))
        return "(Node.{} in {})".format(self.attr, self.values)

    def __repr__(self) -> str:
        return "NodeConstraint" + str(self)

    def to_dict(self) -> dict:
        return {self.attr: self.values}


@hpcwrapclass
class MinResourcePerNode(BaseNodeConstraint):
    def __init__(self, attr: str, value: Union[int, float]) -> None:
        self.attr = attr
        self.value = value

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:

        if self.attr not in node.available:
            # TODO log
            msg = "Resource[name={}] is not defined for Node[name={}]".format(
                self.attr, node.name
            )
            return SatisfiedResult("UndefinedResource", self, node, [msg],)

        try:
            if node.available[self.attr] >= self.value:
                return SatisfiedResult("success", self, node,)
        except TypeError as e:
            logging.warning(
                "Could not evaluate %s >= %s because they are different types: %s",
                node.available[self.attr],
                self.value,
                e,
            )

        msg = "Resource[name={} value={}] < Node[name={} value={}]".format(
            self.attr, self.value, node.name, node.available[self.attr],
        )
        return SatisfiedResult("InsufficientResource", self, node, reasons=[msg],)

    def do_decrement(self, node: "Node") -> bool:
        assert (
            self.attr in node.available
        ), "Resource[name={}] not in Node[name={}, hostname={}] for constraint {}".format(
            self.attr, node.name, node.hostname, str(self)
        )
        remaining = node.available[self.attr]

        # TODO type checking here.
        if remaining < self.value:
            raise RuntimeError(
                "Attempted to allocate more {} than is available for node {}: {} < {} ({})".format(
                    self.attr, node.name, remaining, self.value, str(self),
                )
            )
        node.available[self.attr] = remaining - self.value
        return True

    def minimum_space(self, node: "Node") -> int:
        if self.attr not in node.available:
            return 0
        available = node.available[self.attr]
        return available // self.value

    def __str__(self) -> str:
        return "(Node.{} >= {})".format(self.attr, self.value)

    def __repr__(self) -> str:
        return "NodeConstraint" + str(self)

    def to_dict(self) -> dict:
        return ConstraintDict({self.attr: self.value})


@hpcwrapclass
class ExclusiveNode(BaseNodeConstraint):
    def __init__(self, is_exclusive: bool = True) -> None:
        self.is_exclusive = is_exclusive

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        if self.is_exclusive and bool(node.assignments):
            msg = "Job is exclusive and Node[name={} hostname={}] already has a match".format(
                node.name, node.hostname
            )
            return SatisfiedResult("ExclusiveRequirementFailed", self, node, [msg],)
        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: "Node") -> bool:
        node.closed = self.is_exclusive
        return True

    def minimum_space(self, node: "Node") -> int:
        if self.is_exclusive and bool(node.assignments):
            return 0
        return 1

    def to_dict(self) -> dict:
        return ConstraintDict({"exclusive": self.is_exclusive})

    def __str__(self) -> str:
        return "NodeConstraint(exclusive={})".format(self.is_exclusive)


@hpcwrapclass
class InAPlacementGroup(BaseNodeConstraint):
    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        if node.placement_group:
            return SatisfiedResult("success", self, node,)
        msg = "Node[name={} hostname={}] is not in a placement group".format(
            node.name, node.hostname
        )
        return SatisfiedResult("NotInAPlacementGroup", self, node, [msg],)

    def to_dict(self) -> dict:
        return ConstraintDict({"class": InAPlacementGroup.__class__.__name__})

    def __str__(self) -> str:
        return "InAPlacementGroup()"


@hpcwrapclass
class Or(BaseNodeConstraint):
    def __init__(self, *constraints: Union[NodeConstraint, ConstraintDict]) -> None:
        #         if len(constraints) == 1 and isinstance(constraints[0], list):
        #             constraints = constraints[0]
        if len(constraints) <= 1:
            raise AssertionError("Or expression requires at least 2 constraints")
        self.constraints = get_constraints(list(constraints))

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        reasons = []
        for n, c in enumerate(self.constraints):
            result = c.satisfied_by_node(node)

            if result:
                return SatisfiedResult(
                    "success", self, node, score=len(self.constraints) - n,
                )

            if hasattr(result, "reasons"):
                reasons.extend(result.reasons)

        return SatisfiedResult("CompoundFailure", self, node, reasons)

    def do_decrement(self, node: "Node") -> bool:
        for c in self.constraints:
            result = c.satisfied_by_node(node)

            if result:
                return c.do_decrement(node)

        return False

    def __str__(self) -> str:
        return " or ".join([str(c) for c in self.constraints])

    def to_dict(self) -> dict:
        return {"or": [jc.to_dict() for jc in self.constraints]}


@hpcwrapclass
class And(BaseNodeConstraint):
    def __init__(self, *constraints: Constraint) -> None:
        #         if len(constraints) == 1 and isinstance(constraints[0], list):
        #             constraints = constraints[0]
        self.constraints = get_constraints(list(constraints))

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:

        for c in self.constraints:
            result = c.satisfied_by_node(node)
            if not result:
                return result

        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: "Node") -> bool:
        for c in self.constraints:
            if c.satisfied_by_node(node):
                assert c.do_decrement(node)
        return True

    def __str__(self) -> str:
        return " and ".join([str(c) for c in self.constraints])

    def __repr__(self) -> str:
        return str(self)

    def to_dict(self) -> dict:
        return {"and": [jc.to_dict() for jc in self.constraints]}


@hpcwrapclass
class Not(BaseNodeConstraint):
    def __init__(self, condition: Union[NodeConstraint, ConstraintDict]) -> None:
        self.condition = get_constraints([condition])[0]

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:

        result = self.condition.satisfied_by_node(node)
        # TODO ugly
        status = "success" if not result else "not(success)"
        return SatisfiedResult(status, self, node, [str(self)])

    def do_decrement(self, node: "Node") -> bool:
        return self.condition.do_decrement(node)

    def to_dict(self) -> dict:
        return {"not": self.condition.to_dict()}

    def __str__(self) -> str:
        return "Not({})".format(self.condition)


@hpcwrapclass
class NodePropertyConstraint(BaseNodeConstraint):
    def __init__(self, attr: str, *values: ht.ResourceTypeAtom) -> None:

        self.attr = attr

        if len(values) == 1 and isinstance(values[0], list):
            self.values: List[Optional[ht.ResourceTypeAtom]] = values[0]
        else:
            self.values = list(values)

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:

        if self.attr not in dir(node):
            msg = "Property[name={}] not defined Node[name={} hostname={}]".format(
                self.attr, node.name, node.hostname
            )
            logging.error(msg)
            return SatisfiedResult("UndefinedNodeProperty", self, node, [msg],)

        target = getattr(node, self.attr)
        if target not in self.values:
            assert target != "htc"
            if len(self.values) > 1:
                msg = "Property[name={} value={}] is not one of the options {} for node[name={} attr={}]".format(
                    self.attr, target, self.values, node.name, self.attr,
                )
            else:
                msg = "Property[name={} value={}] != node[name={} {}={}]".format(
                    self.attr, target, node.name, self.attr, self.values[0]
                )

            return SatisfiedResult("InvalidOption", self, node, [msg],)

        # our score is our inverted index - i.e. the first element is the highest score
        score = len(self.values) - self.values.index(target)
        return SatisfiedResult("success", self, node, score=score,)

    def __str__(self) -> str:
        if len(self.values) == 1:
            return "(Node.{} == {})".format(self.attr, repr(self.values[0]))
        return "(Node.{} in {})".format(self.attr, self.values)

    def __repr__(self) -> str:
        return "NodeConstraint" + str(self)

    def to_dict(self) -> dict:
        return {self.attr: self.values}


@hpcwrapclass
class NotAllocated(BaseNodeConstraint):
    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:

        if node._allocated:
            return SatisfiedResult("AlreadyAllocated", self, node)

        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: "Node") -> bool:
        node._allocated = True
        return True

    def minimum_space(self, node: "BaseNode") -> int:
        return 0

    def to_dict(self) -> dict:
        raise RuntimeError()


def _parse_node_property_constraint(
    attr: str, value: Union[ResourceType, Constraint]
) -> NodePropertyConstraint:

    node_attr = attr[5:]

    if isinstance(value, (float, int, str, bool, type(None))):
        return NodePropertyConstraint(node_attr, value)

    elif isinstance(value, list):

        for n, choice in enumerate(value):

            if not isinstance(choice, (float, int, str, bool, type(None))):
                msg = "Expected string, int or boolean for '{}' but got {} at index {}".format(
                    attr, choice, n
                )
                raise RuntimeError(msg)
        if not isinstance(value, list):
            value = [value]  # type: ignore
        return NodePropertyConstraint(node_attr, *value)  # type: ignore

    msg = "Expected string, int or boolean for '{}' but got {}".format(attr, value)
    raise RuntimeError(msg)


@hpcwrap
def new_job_constraint(
    attr: str, value: Union[ResourceType, Constraint], in_alias: bool = False
) -> NodeConstraint:

    if attr == "not":
        not_cons = get_constraint(value)
        job_cons = new_job_constraint("_", not_cons)
        return Not(job_cons)

    if attr.startswith("node."):
        assert isinstance(
            value, (str, int, float, bool, list)
        ), "Expected str, int, float or bool. Got {}".format(type(value))
        return _parse_node_property_constraint(attr, value)

    if isinstance(value, str):
        return NodeResourceConstraint(attr, value)

    elif isinstance(value, bool):
        # TODO - not sure if this is the way to handle this.

        if attr == "exclusive":
            return ExclusiveNode(value)

        return NodeResourceConstraint(attr, value)

    elif isinstance(value, list):

        if attr == "or":
            child_values: List[NodeConstraint] = []
            for child in value:
                child_values.append(And(*get_constraints([child])))
            return Or(*child_values)

        elif attr == "and":
            child_values = []
            for child in value:
                child_values.extend(get_constraints(child))
            return And(*child_values)

        return NodeResourceConstraint(attr, *value)

    elif isinstance(value, int) or isinstance(value, float):
        return MinResourcePerNode(attr, value)

    elif value is None:
        raise RuntimeError("None is not an allowed value. For attr {}".format(attr))

    else:

        raise RuntimeError(
            "Not handled - attr {} of type {} - {}".format(attr, type(value), value)
        )

    assert False


@hpcwrap
def get_constraint(constraint_expression: Constraint) -> NodeConstraint:
    ret = get_constraints([constraint_expression])
    assert len(ret) == 1
    return ret[0]


@hpcwrap
def get_constraints(constraint_expressions: List[Constraint],) -> List[NodeConstraint]:
    #     if isinstance(constraint_expressions, dict):
    #         constraint_expressions = [constraint_expressions]

    if isinstance(constraint_expressions, tuple):
        constraint_expressions = list(constraint_expressions)

    assert isinstance(constraint_expressions, list), type(constraint_expressions)

    job_constraints = []

    for constraint_expression in constraint_expressions:
        if isinstance(constraint_expression, NodeConstraint):
            job_constraints.append(constraint_expression)
        elif isinstance(constraint_expression, list):
            pass
        else:
            for attr, value in constraint_expression.items():
                # TODO
                c = new_job_constraint(attr, value)  # type: ignore
                assert c is not None
                job_constraints.append(c)

    return job_constraints


_COMPLETE = True
