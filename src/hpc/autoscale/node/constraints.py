import typing
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from uuid import uuid4

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
    from hpc.autoscale.node.node import Node
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

    def get_children(self) -> Iterable["NodeConstraint"]:
        return []

    @abstractmethod
    def __str__(self) -> str:
        ...

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: object) -> bool:
        if not hasattr(other, "to_dict"):
            return False

        return self.to_dict() == getattr(other, "to_dict")()


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

            return SatisfiedResult(
                "UndefinedResource",
                self,
                node,
                [
                    "Resource[name={}] not defined for Node[name={} hostname={}]".format(
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
        # so we want
        score = len(self.values) - self.values.index(target) + 1
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
class MinResourcePerNode(BaseNodeConstraint):
    def __init__(self, attr: str, value: Union[int, float, ht.Memory]) -> None:
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
                "For attribute %s: Could not evaluate %s >= %s because they are different types: %s",
                self.attr,
                node.available[self.attr],
                self.value,
                e,
            )

        msg = "Resource[name={} value={}] < Node[name={} value={}]".format(
            self.attr, self.value, node.name, node.available[self.attr],
        )
        return SatisfiedResult("InsufficientResource", self, node, reasons=[msg],)

    def do_decrement(self, node: "Node") -> bool:
        if self.attr not in node.available:
            msg = "Resource[name={}] not in Node[name={}, hostname={}] for constraint {}".format(
                self.attr, node.name, node.hostname, str(self)
            )
            raise RuntimeError(msg)

        remaining = node.available[self.attr]

        # TODO type checking here.
        if remaining < self.value:
            raise RuntimeError(
                "Attempted to allocate more {} than is available for {}: {} < {} ({})".format(
                    self.attr, node.name, remaining, self.value, str(self),
                )
            )
        node.available[self.attr] = remaining - self.value
        return True

    def minimum_space(self, node: "Node") -> int:
        if self.attr not in node.available:
            return 0
        available = node.available[self.attr]
        return int(available // self.value)

    def __str__(self) -> str:
        return "(Node.{} >= {})".format(self.attr, self.value)

    def __repr__(self) -> str:
        return "NodeConstraint" + str(self)

    def to_dict(self) -> dict:
        return ConstraintDict({self.attr: self.value})


@hpcwrapclass
class ExclusiveNode(BaseNodeConstraint):
    def __init__(
        self,
        is_exclusive: bool = True,
        job_exclusive: bool = True,
        assignment_id: str = "",
    ) -> None:
        self.is_exclusive = is_exclusive
        assert self.is_exclusive, "Only exclusive=true is supported at this time"
        self.assignment_id = assignment_id or str(uuid4())
        self.job_exclusive = job_exclusive

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        # TODO clean up
        if node.assignments or node.closed:

            if self.job_exclusive or self.assignment_id not in node.assignments:
                msg = "[name={} hostname={}] already has an exclusive job: {}".format(
                    node.name, node.hostname, node.assignments
                )
                return SatisfiedResult("ExclusiveRequirementFailed", self, node, [msg],)
        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: "Node") -> bool:
        # if node.closed:
        #     # the job may not be assigned yet, or we may be packing more than one
        #     # instance of the job on the machine
        #     if (not self.is_exclusive) or self.assignment_id not in node.assignments:
        #         raise RuntimeError(
        #             "Can not call ExclusiveNode.do_decrement on a closed node! %s %s" % (node, node.assignments)
        #         )
        #     # we already marked this node as exclusive
        #     assert node.closed
        # elif self.is_exclusive:
        #     node.closed = True
        #     assert not node.assignments
        if self.is_exclusive:
            if self.assignment_id in node.assignments:
                if self.job_exclusive:
                    return False
            node.closed = True
        return True

    def minimum_space(self, node: "Node") -> int:
        assert self.assignment_id
        assert node.vcpu_count > 0
        if node.assignments:
            if self.assignment_id not in node.assignments:
                return 0
            elif self.job_exclusive:
                return 0
        return -1

    def to_dict(self) -> dict:
        if self.job_exclusive:
            return {"exclusive": self.is_exclusive}
        return {"exclusive_task": self.is_exclusive}

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

    def minimum_space(self, node: "Node") -> int:
        if not node.placement_group:
            return 0
        return -1

    def do_decrement(self, node: "Node") -> bool:
        return bool(node.placement_group)

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
        reasons: List[str] = []
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
                ret = c.do_decrement(node)
                if ret > 0:
                    return ret

        return False

    def get_children(self) -> Iterable[NodeConstraint]:
        return self.constraints

    def __str__(self) -> str:
        return " or ".join([str(c) for c in self.constraints])

    def to_dict(self) -> dict:
        return {"or": [jc.to_dict() for jc in self.constraints]}


@hpcwrapclass
class XOr(BaseNodeConstraint):
    def __init__(self, *constraints: Union[NodeConstraint, ConstraintDict]) -> None:
        if len(constraints) <= 1:
            raise AssertionError("XOr expression requires at least 2 constraints")
        self.constraints = get_constraints(list(constraints))

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        reasons: List[str] = []
        xor_result: Optional[SatisfiedResult] = None

        for n, c in enumerate(self.constraints):
            expr_result = c.satisfied_by_node(node)

            if expr_result:
                # true ^ true == false

                if xor_result:
                    msg = "Multiple expressions evaluated as true. See below:\n\t{}\n\t{}".format(
                        xor_result.message, expr_result.message
                    )
                    return SatisfiedResult("XORFailed", self, node, [msg] + reasons,)
                # assign the first true expression as the final result
                xor_result = expr_result
            elif hasattr(expr_result, "reasons"):
                # if this does end up failing for all expressions, keep
                # track of the set of reasons
                reasons.extend(expr_result.reasons)

        if xor_result:
            return xor_result

        return SatisfiedResult("CompoundFailure", self, node, reasons)

    def do_decrement(self, node: "Node") -> bool:
        xor_result: Union[bool, "SatisfiedResult"] = False
        first_matched_constraint: Optional[NodeConstraint] = None

        for c in self.constraints:
            expr_result = c.satisfied_by_node(node)
            if expr_result:
                if xor_result:
                    raise AssertionError(
                        "XOr expression is invalid but do_decrement was still called."
                    )
                xor_result = expr_result
                assert first_matched_constraint is None
                first_matched_constraint = c

        if xor_result and first_matched_constraint:
            return first_matched_constraint.do_decrement(node)

        return False

    def get_children(self) -> Iterable[NodeConstraint]:
        return self.constraints

    def __str__(self) -> str:
        return " xor ".join([str(c) for c in self.constraints])

    def to_dict(self) -> dict:
        return {"xor": [jc.to_dict() for jc in self.constraints]}


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

    def get_children(self) -> Iterable[NodeConstraint]:
        return self.constraints

    def minimum_space(self, node: "Node") -> int:
        m = -1
        for child in self.get_children():
            child_min = child.minimum_space(node)
            if child_min == -1:
                continue
            if m == -1:
                m = child_min
            m = min(m, child_min)
        return m

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

    def get_children(self) -> Iterable[NodeConstraint]:
        return [self.condition]

    def __str__(self) -> str:
        return "Not({})".format(self.condition)


@hpcwrapclass
class NodePropertyConstraint(BaseNodeConstraint):
    def __init__(
        self, attr: str, *values: typing.Union[None, ht.ResourceTypeAtom]
    ) -> None:
        self.attr = attr
        from hpc.autoscale.node.node import QUERYABLE_PROPERTIES

        if attr not in QUERYABLE_PROPERTIES:
            msg = "Property[name={}] not defined for Node".format(self.attr)
            logging.error(msg)
            raise ValueError("UndefinedNodeProperty: " + msg)

        if len(values) == 1 and isinstance(values[0], list):
            self.values: List[Optional[ht.ResourceTypeAtom]] = values[0]
        else:
            self.values = list(values)

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        target = getattr(node, self.attr)

        if target not in self.values:

            if len(self.values) > 1:
                msg = "Property[name={} value={}] is not one of the options {} for node[name={} attr={}]".format(
                    self.attr, target, self.values, node.name, self.attr,
                )
            else:
                msg = "Property[name={} value={}] != node[name={} {}={}]".format(
                    self.attr, self.values[0], node.name, self.attr, target
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

    def minimum_space(self, node: "Node") -> int:
        return 0

    def to_dict(self) -> dict:
        raise RuntimeError()


class Never(BaseNodeConstraint):
    """
    If you need to insert a constraint that rejects every node.
    """

    def __init__(self, message: str) -> None:
        self.__message = message

    @property
    def message(self) -> str:
        return self.__message

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        return SatisfiedResult("Never", self, node, reasons=[self.message])

    def do_decrement(self, node: "Node") -> bool:
        return False

    def to_dict(self) -> dict:
        return {"never": self.message}

    def __str__(self) -> str:
        return "Never({})".format(self.message)


class ReadOnlyAlias(BaseNodeConstraint):
    def __init__(self, alias: str, resource_name: str) -> None:
        self.alias = alias
        self.resource_name = resource_name

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        if self.resource_name not in node.resources:
            return SatisfiedResult(
                "MissingResourceForAlias",
                self,
                node,
                reasons=[
                    "{} does not have resource {}".format(node, self.resource_name)
                ],
            )
        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: "Node") -> bool:
        node.available[self.alias] = node.resources[self.resource_name]
        return True

    def minimum_space(self, node: "Node") -> int:
        return -1

    def __str__(self) -> str:
        return "Alias({}, {})".format(self.alias, self.resource_name)

    def to_dict(self) -> dict:
        return {"class": self.__class__.__name__, self.alias: self.resource_name}


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
    attr: str, value: Union[ResourceType, Constraint, list], in_alias: bool = False
) -> NodeConstraint:

    if attr in _CUSTOM_PARSERS:
        return _CUSTOM_PARSERS[attr]({attr: value})

    if isinstance(value, NodeConstraint):
        return value

    if attr in ["exclusive", "exclusive_task"]:
        if isinstance(value, str) and value.isdigit():
            value = int(value)

        if isinstance(value, str):
            if value.lower() not in ["true", "false"]:
                raise RuntimeError("Unexpected value for exclusive: {}".format(value))
            value = value.lower() == "true"
        elif isinstance(value, int):
            value = bool(value)
        if not isinstance(value, bool):
            raise RuntimeError(
                "Unexpected value for exclusive: {} {}".format(value, type(format))
            )
        # exclusive_tasks = multiple tasks from the same job run on the same node
        # exclusive = only a single task from the same job can run on the same node
        job_exclusive = attr == "exclusive"
        return ExclusiveNode(is_exclusive=value, job_exclusive=job_exclusive)

    if attr == "not":
        not_cons = get_constraint(value)
        job_cons = new_job_constraint("_", not_cons)
        return Not(job_cons)

    if attr == "never":
        return Never(str(value))

    if attr.startswith("node."):
        assert value is None or isinstance(
            value, (str, int, float, bool, list)
        ), "Expected str, int, float or bool. Got {}".format(type(value))
        return _parse_node_property_constraint(attr, value)

    if isinstance(value, str):
        return NodeResourceConstraint(attr, value)

    elif isinstance(value, bool):
        return NodeResourceConstraint(attr, value)

    elif isinstance(value, list):

        if attr in ["or", "xor"]:
            child_values: List[NodeConstraint] = []
            for child in value:
                and_cons = get_constraints([child])
                # we only need the And object if there is actually
                # more than one constraint
                if len(and_cons) > 1:
                    child_values.append(And(*and_cons))
                else:
                    child_values.append(and_cons[0])
            if attr == "or":
                return Or(*child_values)
            return XOr(*child_values)

        elif attr == "and":
            child_values = []
            for child in value:
                child_values.extend(get_constraints(child))
            return And(*child_values)

        return NodeResourceConstraint(attr, *value)

    elif (
        isinstance(value, int)
        or isinstance(value, float)
        or isinstance(value, ht.Memory)
    ):
        return MinResourcePerNode(attr, value)

    elif value is None:
        raise RuntimeError("None is not an allowed value. For attr {}".format(attr))

    else:
        raise RuntimeError(
            "Not handled - attr {} of type {} - {}".format(attr, type(value), value)
        )


@hpcwrap
def get_constraint(constraint_expression: Constraint) -> NodeConstraint:
    ret = get_constraints([constraint_expression])
    assert len(ret) == 1, "Expression=%s len(ret)==%s" % (
        constraint_expression,
        len(ret),
    )
    return ret[0]


@hpcwrap
def get_constraints(constraint_expressions: List[Constraint],) -> List[NodeConstraint]:
    if isinstance(constraint_expressions, dict):
        constraint_expressions = [constraint_expressions]

    if isinstance(constraint_expressions, tuple):
        constraint_expressions = list(constraint_expressions)

    assert isinstance(constraint_expressions, list), type(constraint_expressions)

    constraints = []

    for constraint_expression in constraint_expressions:
        if isinstance(constraint_expression, NodeConstraint):
            constraints.append(constraint_expression)
        elif isinstance(constraint_expression, list):
            # TODO... what?
            pass
        else:
            for attr, value in constraint_expression.items():
                # TODO
                c = new_job_constraint(attr, value)
                assert c is not None
                constraints.append(c)

    return constraints


_CUSTOM_PARSERS: Dict[str, Callable[[Dict], NodeConstraint]] = {}


def register_parser(
    custom_attribute: str, parser: Callable[[Dict], NodeConstraint]
) -> None:
    assert isinstance(custom_attribute, str)
    assert hasattr(parser, "__call__")
    _CUSTOM_PARSERS[custom_attribute] = parser
