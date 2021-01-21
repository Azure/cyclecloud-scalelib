import re
import typing
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from uuid import uuid4

import hpc  # noqa: F401
import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.codeanalysis import hpcwrap, hpcwrapclass
from hpc.autoscale.hpctypes import ResourceType
from hpc.autoscale.results import SatisfiedResult

ConstraintDict = typing.NewType("ConstraintDict", Dict[Any, Any])

Constraint = Union["NodeConstraint", ConstraintDict]


if typing.TYPE_CHECKING:
    from hpc.autoscale.node.node import Node
    from hpc.autoscale.node.bucket import NodeBucket


# TODO split by job and node constraints (job being a subclass of node constraint)
class NodeConstraint(ABC):
    def weight_buckets(
        self, bucket_weights: List[Tuple["NodeBucket", float]]
    ) -> List[Tuple["NodeBucket", float]]:
        return bucket_weights

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
    """
    These are constraints that filter out which node is matched based on
    read-only resources.

    ```json
    {"custom_string1": "custom_value"}
    ```
    ```json
    {"custom_string2": ["custom_value1", "custom_value2"]}
    ```

    For read-only integers you can programmatically call NodeResourceConstraint("custom_int", 16)
    or use a list of integers

    ```json
    {"custom_integer": [16, 32]}
    ```

    For shorthand, you can combine the above expressions

    ```json
    {
        "custom_string1": "custom_value",
        "custom_string2": ["custom_value1", "custom_value2"],
        "custom_integer": [16, 32]
    }
    ```
    """

    def __init__(self, attr: str, *values: ht.ResourceTypeAtom) -> None:
        self.attr = attr
        self.values: List[ResourceType] = list(values)
        self.weight = 100

    def weight_buckets(
        self, bucket_weights: List[Tuple["NodeBucket", float]]
    ) -> List[Tuple["NodeBucket", float]]:
        ret: List[Tuple["NodeBucket", float]] = []

        for bucket, b_weight in bucket_weights:
            new_weight = 0.0
            if self.attr not in bucket.example_node.available:
                ret.append((bucket, 0.0))
                continue
            target = bucket.example_node.available[self.attr]

            for n, value in enumerate(self.values):
                if isinstance(value, str) and isinstance(target, str):
                    matches = fnmatch(target, value)
                else:
                    matches = target == value
                if matches:
                    new_weight = (
                        float(len(self.values) * self.weight - n * self.weight)
                        + b_weight
                    )
                    break

            ret.append((bucket, new_weight))

        return ret

    def _satisfied(self, node: "Node", value: ht.ResourceTypeAtom) -> Optional[str]:
        target = node.available[self.attr]

        if value != target:
            if len(self.values) > 1:
                return "Resource[name={} value={}] is not one of the options {} for node[name={} attr={}]".format(
                    self.attr, target, self.values, node.name, self.attr,
                )
            else:
                return "Resource[name={} value={}] != node[name={} {}={}]".format(
                    self.attr, target, node.name, self.attr, self.values[0]
                )
        return None

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

        for n, value in enumerate(self.values):
            if isinstance(value, str) and isinstance(target, str):
                matches = fnmatch(target, value)
            else:
                matches = target == value
            if matches:
                score = len(self.values) - n
                return SatisfiedResult("success", self, node, score=score,)

        if len(self.values) > 1:
            msg = "Resource[name={} value={}] is not one of the options {} for node[name={} attr={}]".format(
                self.attr, target, self.values, node.name, self.attr,
            )
        else:
            msg = "Resource[name={} value={}] != node[name={} {}={}]".format(
                self.attr, target, node.name, self.attr, self.values[0]
            )

        return SatisfiedResult("InvalidOption", self, node, [msg],)

    def __str__(self) -> str:
        if len(self.values) == 1:
            return "(Node.{} == {})".format(self.attr, repr(self.values[0]))
        return "(Node.{} in {})".format(self.attr, self.values)

    def __repr__(self) -> str:
        return "NodeResourceConstraint" + str(self)

    def to_dict(self) -> dict:
        # just to minimize formatted output
        if len(self.values) == 1:
            return {self.attr: self.values[0]}
        return {self.attr: self.values}


@hpcwrapclass
class MinResourcePerNode(BaseNodeConstraint):
    """
    Filters for nodes that have at least a certain amount of a resource left to
    allocate.

    ```json
    {"ncpus": 1}
    ```
    ```json
    {"mem": "memory::4g"}
    ```
    ```json
    {"ngpus": 4}
    ```
    Or, shorthand for combining the above into one expression
    ```json
    {"ncpus": 1, "mem": "memory::4g", "ngpus": 4}
    ```
    """

    def __init__(self, attr: str, value: Union[int, float, ht.Size]) -> None:
        self.attr = attr
        self.value = value
        assert isinstance(self.value, (int, float, ht.Size))

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:

        if self.attr not in node.available:
            # TODO log
            msg = "Resource[name={}] is not defined for Node[name={}]".format(
                self.attr, node.name
            )
            return SatisfiedResult("UndefinedResource", self, node, [msg],)

        try:
            target = node.available[self.attr]

            assert isinstance(target, (int, float, ht.Size)), "%s => %s" % (
                target,
                type(target),
            )

            if target >= self.value:
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

        if self.value == 0:
            return 2 ** 31

        return int(available // self.value)

    def __str__(self) -> str:
        return "(Node.{} >= {})".format(self.attr, self.value)

    def __repr__(self) -> str:
        return "NodeMinResourceConstraint" + str(self)

    def to_dict(self) -> dict:
        return ConstraintDict({self.attr: self.value})


@hpcwrapclass
class ExclusiveNode(BaseNodeConstraint):
    """
    Defines whether, when allocating, if a node will exclusively run this job.
    ```json
    {"exclusive": true}
    ```
    -> One and only one iteration of the job can run on this node.

    ```json
    {"exclusive-task": true}
    ```
    -> One or more iterations of the same job can run on this node.
    """

    def __init__(
        self,
        is_exclusive: bool = True,
        job_exclusive: bool = True,
        assignment_id: str = "",
    ) -> None:
        self.is_exclusive = is_exclusive
        self.assignment_id = assignment_id or str(uuid4())
        self.job_exclusive = job_exclusive

    def weight_buckets(
        self, bucket_weights: List[Tuple["NodeBucket", float]]
    ) -> List[Tuple["NodeBucket", float]]:
        return bucket_weights

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        if not self.is_exclusive:
            return SatisfiedResult("success", self, node)

        # TODO clean up
        if node.assignments or node.closed:

            if self.job_exclusive or self.assignment_id not in node.assignments:
                msg = "[name={} hostname={}] already has an exclusive job: {}".format(
                    node.name, node.hostname, node.assignments
                )
                return SatisfiedResult("ExclusiveRequirementFailed", self, node, [msg],)
        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: "Node") -> bool:
        if not self.is_exclusive:
            return True

        if self.is_exclusive:
            if self.assignment_id in node.assignments:
                if self.job_exclusive:
                    return False
            node.closed = True
        return True

    def minimum_space(self, node: "Node") -> int:
        if not self.is_exclusive:
            return -1

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
    """
    Ensures that all nodes allocated will be in any placement group or not. Typically
    this is most useful to prevent a job from being allocated to a node in a placement group.

    ```json
    {"in-a-placement-group": true}
    ```
    ```json
    {"in-a-placement-group": false}
    ```
    """

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
        return ConstraintDict({"in-a-placement-group": True})

    def __str__(self) -> str:
        return "InAPlacementGroup()"


@hpcwrapclass
class Or(BaseNodeConstraint):
    """
    Logical 'or' for matching a set of child constraints. Given a list of child constraints,
    the first constraint that matches is the one used to decrement the node. No
    further constraints are considered after the first child constraint has been satisfied.
    For example, say we want to use a GPU instance if we can get a spot instance, otherwise
    we want to use a non-spot CPU instance.
    ```json
    {"or": [{"node.vm_size": "Standard_NC6", "node.spot": true},
            {"node.vm_size": "Standard_F16", "node.spot": false}]
    }
    ```
    """

    def __init__(self, *constraints: Union[NodeConstraint, ConstraintDict]) -> None:
        if len(constraints) <= 1:
            raise AssertionError("Or expression requires at least 2 constraints")
        self.constraints = get_constraints(list(constraints))
        self.weight = 100

    def weight_buckets(
        self, bucket_weights: List[Tuple["NodeBucket", float]]
    ) -> List[Tuple["NodeBucket", float]]:
        ret: List[Tuple["NodeBucket", float]] = []
        for bucket, current_weight in bucket_weights:
            new_weight = None
            for n, c in enumerate(self.constraints):
                result = c.satisfied_by_bucket(bucket)
                if result:
                    new_weight = (
                        float(len(self.constraints) - n) * self.weight + current_weight
                    )
                    break

            ret.append((bucket, new_weight or 0.0))

        return ret

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
                return c.do_decrement(node)

        return False

    def minimum_space(self, node: "Node") -> int:
        for c in self.constraints:
            result = c.satisfied_by_node(node)

            if result:
                return c.minimum_space(node)
        return 0

    def get_children(self) -> Iterable[NodeConstraint]:
        return self.constraints

    def __str__(self) -> str:
        return " or ".join([str(c) for c in self.constraints])

    def to_dict(self) -> dict:
        return {"or": [jc.to_dict() for jc in self.constraints]}


@hpcwrapclass
class XOr(BaseNodeConstraint):
    """
    Similar to the or operation, however one and only one of the child constraints may satisfy
    the node. Here is a trivial example where we have a failover for allocating to a second
    region but we ensure that only one of them is valid at a time.
    ```json
    {"xor": [{"node.location": "westus2"},
             {"node.location": "eastus"}]
    }
    ```
    """

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

    def minimum_space(self, node: "Node") -> int:
        xor_result: Optional[SatisfiedResult] = None
        successful_constraint: Optional[NodeConstraint] = None

        for n, c in enumerate(self.constraints):
            expr_result = c.satisfied_by_node(node)

            if expr_result:
                # true ^ true == false

                if xor_result:
                    return 0
                # assign the first true expression as the final result
                xor_result = expr_result
                successful_constraint = c

        if xor_result and successful_constraint:
            return successful_constraint.minimum_space(node)

        return 0

    def get_children(self) -> Iterable[NodeConstraint]:
        return self.constraints

    def __str__(self) -> str:
        return " xor ".join([str(c) for c in self.constraints])

    def to_dict(self) -> dict:
        return {"xor": [jc.to_dict() for jc in self.constraints]}


@hpcwrapclass
class And(BaseNodeConstraint):
    """
    The logical 'and' operator ensures that all of its child constraints are met.
    ```json
    {"and": [{"ncpus": 1}, {"mem": "memory::4g"}]}
    ```
    Note that and is implied when combining multiple resource definitions in the same
    dictionary. e.g. the following have identical semantic meaning, the latter being
    shorthand for the former.

    ```json
    {"and": [{"ncpus": 1}, {"mem": "memory::4g"}]}
    ```
    ```json
    {"ncpus": 1, "mem": "memory::4g"}
    ```
    """

    def __init__(self, *constraints: Constraint) -> None:
        #         if len(constraints) == 1 and isinstance(constraints[0], list):
        #             constraints = constraints[0]
        self.constraints = get_constraints(list(constraints))

    def weight_buckets(
        self, bucket_weights: List[Tuple["NodeBucket", float]]
    ) -> List[Tuple["NodeBucket", float]]:
        ret = bucket_weights
        for constraint in self.constraints:
            ret = constraint.weight_buckets(ret)
        return ret

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

            if child_min == 0:
                return child_min

            if m == -1:
                m = child_min

            m = min(m, child_min)
        return m

    def __str__(self) -> str:
        return " and ".join([str(c) for c in self.constraints])

    def __repr__(self) -> str:
        return str(self)

    def to_dict(self) -> dict:
        working_constraint: Dict = {}
        all_constraints = [working_constraint]

        for jc in self.constraints:
            jc_dict = jc.to_dict()
            new_working_constraint_required = False
            for key in jc_dict:
                if key in working_constraint:
                    new_working_constraint_required = True
                    break
            if new_working_constraint_required:
                working_constraint = {}
                all_constraints.append(working_constraint)
            working_constraint.update(jc_dict)

        if len(all_constraints) == 1:
            return all_constraints[0]

        return {"and": all_constraints}


@hpcwrapclass
class Not(BaseNodeConstraint):
    """
    Logical 'not' operator negates the single child constraint.

    Only allocate machines with GPUs
    ```json
    {"not": {"node.gpu_count": 0}}
    ```
    Only allocate machines with no GPUs available
    ```json
    {"not": {"ngpus": 1}}
    ```
    """

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
    """
    Similar to NodeResourceConstraint, but these are constraints based purely
    on the read only node properties, i.e. those starting with 'node.'
    ```json
    {"node.vm_size": ["Standard_F16", "Standard_E32"]}
    ```
    ```json
    {"node.location": "westus2"}
    ```
    ```json
    {"node.pcpu_count": 44}
    ```
    Note that the last example does not allocate 44 node.pcpu_count, but simply
    matches nodes that have a pcpu_count of exactly 44.
    """

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

    def _get_target_value(
        self, node: "Node"
    ) -> typing.Union[None, ht.ResourceTypeAtom]:
        return getattr(node, self.attr)

    def weight_buckets(
        self, bucket_weights: List[Tuple["NodeBucket", float]]
    ) -> List[Tuple["NodeBucket", float]]:
        ret: List[Tuple["NodeBucket", float]] = []
        for bucket, weight in bucket_weights:
            for n, value in enumerate(self.values):
                err_msg = self._satisfied(bucket.example_node, value)
                if err_msg:
                    new_weight = 0.0
                else:
                    new_weight = float(len(self.values) - n)
                ret.append((bucket, new_weight))
        return ret

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        err_msgs = []
        for n, value in enumerate(self.values):
            err_msg = self._satisfied(node, value)
            if not err_msg:
                score = len(self.values) - n
                return SatisfiedResult("success", self, node, score=score,)
            err_msgs.append(err_msg)

        return SatisfiedResult("InvalidOption", self, node, err_msgs,)

    def _satisfied(
        self, node: "Node", value: typing.Union[None, ht.ResourceTypeAtom]
    ) -> Optional[str]:
        target = getattr(node, self.attr)

        if isinstance(value, str) and isinstance(target, str):
            if fnmatch(target, value):
                return None
        elif value == target:
            return None

        if len(self.values) > 1:
            return "Property[name={} value={}] is not one of the options {} for node[name={} attr={}]".format(
                self.attr, target, self.values, node.name, self.attr,
            )
        else:
            return "Property[name={} value={}] != node[name={} {}={}]".format(
                self.attr, self.values[0], node.name, self.attr, target
            )

    def __str__(self) -> str:
        if len(self.values) == 1:
            return "(Node.{} == {})".format(self.attr, repr(self.values[0]))
        return "(Node.{} in {})".format(self.attr, self.values)

    def __repr__(self) -> str:
        return "NodePropertyConstraint" + str(self)

    def to_dict(self) -> dict:
        return {"node.{}".format(self.attr): self.values}


@hpcwrapclass
class NotAllocated(BaseNodeConstraint):
    """
    Deprecated, do not use.
    """

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
    Rejects every node. Most useful when generating a complex node constraint
    that cannot be determined to be satisfiable until it is generated.
    For example, say a scheduler supports an 'excluded_users' list for scheduler specific
    "projects". When constructing a set of constraints you may realize that this user will
    never be able to run a job on a node with that project.
    ```json
    {"or":
        [{"project": "open"},
         {"project": "restricted",
          "never": "User is denied access to this project"}
        ]
    }
    ```
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
        return {
            "class": "hpc.autoscale.constraints.ReadOnlyAlias",
            self.alias: self.resource_name,
        }


class SharedResource(ABC):
    def __init__(self, resource_name: str, source: str, current_value: Any) -> None:
        self.resource_name = resource_name
        self.source = source
        self.current_value = current_value

    @property
    @abstractmethod
    def is_consumable(self) -> bool:
        ...

    def __lt__(self, o: object) -> bool:
        if isinstance(o, SharedResource):
            return self.current_value < o.current_value
        return self.current_value < o

    def __le__(self, o: object) -> bool:
        if isinstance(o, SharedResource):
            return self.current_value <= o.current_value
        return self.current_value <= o

    def __eq__(self, o: object) -> bool:
        if isinstance(o, SharedResource):
            return (
                self.resource_name == o.resource_name
                and self.current_value == o.current_value
            )
        return self.current_value < o


class SharedConsumableResource(SharedResource):
    def __init__(
        self,
        resource_name: str,
        source: str,
        initial_value: Union[int, float, "SharedConsumableResource"],
        current_value: Union[int, float, "SharedConsumableResource"],
    ) -> None:
        super().__init__(resource_name, source, current_value)
        self.initial_value = initial_value

    @property
    def is_consumable(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "Shared({}={}/{}, source={})".format(
            self.resource_name, self.current_value, self.initial_value, self.source
        )


class SharedNonConsumableResource(SharedResource):
    @property
    def is_consumable(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "Shared({}={}, source={})".format(
            self.resource_name, self.current_value, self.source
        )


class SharedConstraint(BaseNodeConstraint):
    ...


class SharedConsumableConstraint(SharedConstraint):
    """
    Represent a shared consumable resource, for example a queue
    quota or number of licenses. Please use the SharedConsumableResource
    object to represent this resource.

    While there is a json representation of this object, it is up to the
    author to create the SharedConsumableResources programmatically so
    programmatic creation of this constraint is recommended.
    ```python
    # global value
    SHARED_RESOURCES = [SharedConsumableConstraint(resource_name="licenses",
                                                   source="/path/to/license_limits",
                                                   initial_value=1000,
                                                   current_value=1000)]

    def make_constraint(value: int) -> SharedConsumableConstraint:
        return SharedConsumableConstraint(SHARED_RESOURCES, value)
    ```
    """

    def __init__(
        self,
        shared_resources: List[SharedConsumableResource],
        amount: Union[int, float],
    ) -> None:
        self.shared_resources = shared_resources
        self.amount = amount

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        for shared_resource in self.shared_resources:
            if self.amount > shared_resource.current_value:
                return SatisfiedResult(
                    "SharedLimitReached",
                    self,
                    node,
                    reasons=[
                        "Insufficiant shared resource {} ({}/{}): Requested {} > {}".format(
                            shared_resource.resource_name,
                            shared_resource.current_value,
                            shared_resource.initial_value,
                            self.amount,
                            shared_resource.current_value,
                        )
                    ],
                )
        return SatisfiedResult("success", self, node)

    def do_decrement(self, node: "Node") -> bool:
        for shared_resource in self.shared_resources:
            if self.amount > shared_resource.current_value:
                return False

        for shared_resource in self.shared_resources:
            shared_resource.current_value -= self.amount

        return True

    def __str__(self) -> str:
        current = ",".join(
            [
                "{}/{}".format(r.current_value, r.initial_value)
                for r in self.shared_resources
            ]
        )
        sources = ",".join(
            [
                ",".join(r.source) if not isinstance(r.source, str) else r.source
                for r in self.shared_resources
            ]
        )

        return "Shared(name={}, source={}, current={}, request={})".format(
            self.shared_resources[0].resource_name, sources, current, self.amount,
        )

    def to_dict(self) -> Dict:
        return {
            "shared-consumable-constraint": {
                "resource-name": self.shared_resources[0].resource_name,
                "sources": [r.source for r in self.shared_resources],
                "initial-values": [r.initial_value for r in self.shared_resources],
                "current-value": [r.current_value for r in self.shared_resources],
                "requested": self.amount,
            }
        }

    @staticmethod
    def from_dict(d: Dict) -> "SharedConsumableConstraint":
        inner = d["shared-consumable-constraint"]

        initial_values = inner["initial-value"]
        current_values = inner["current-value"]
        sources = inner["source"]
        requested = inner["requested"]

        shared_resources = []
        for initial_value, current_value, source in zip(
            initial_values, current_values, sources
        ):

            assert isinstance(
                initial_value, (int, float)
            ), "Expected initial-value to be a float or int"
            assert isinstance(
                current_value, (int, float)
            ), "Expected current-value to be a float or int"
            assert isinstance(
                requested, (int, float)
            ), "Expected requested to be a float or int"

            shared_res = SharedConsumableResource(
                inner["resource-name"], source, initial_value, current_value
            )
            shared_resources.append(shared_res)

        return SharedConsumableConstraint(shared_resources, requested)


class SharedNonConsumableConstraint(SharedConstraint):
    """
    Similar to a SharedConsumableConstraint, except that the resource that is shared
    is not consumable (like a string etc). Please use the SharedNonConsumableResource
    object to represent this resource.

    While there is a json representation of this object, it is up to the
    author to create the SharedNonConsumableResource programmatically so
    programmatic creation of this constraint is recommended.
    ```python
    # global value
    SHARED_RESOURCES = [SharedNonConsumableResource(resource_name="prodversion",
                                                    source="/path/to/prod_version",
                                                    current_value="1.2.3")]

    def make_constraint(value: str) -> SharedConstraint:
        return SharedConstraint(SHARED_RESOURCES, value)
    ```
    """

    def __init__(
        self, shared_resource: SharedNonConsumableResource, target: Any
    ) -> None:
        self.shared_resource = shared_resource
        self.target = target

    def satisfied_by_node(self, node: "Node") -> SatisfiedResult:
        if self.shared_resource.current_value == self.target:
            return SatisfiedResult("success", self, node)

        return SatisfiedResult(
            "InvalidSharedValue",
            self,
            node,
            reasons=[
                "Shared value does not match requested value: {} != {}".format(
                    self.shared_resource.current_value, self.target
                )
            ],
        )

    def do_decrement(self, node: "Node") -> bool:
        if self.target != self.shared_resource.current_value:
            return False

        return True

    def __str__(self) -> str:
        return "Shared(name={}, source={}, current={}, request={})".format(
            self.shared_resource.resource_name,
            self.shared_resource.source,
            self.shared_resource.current_value,
            self.target,
        )

    def to_dict(self) -> Dict:
        return {
            "shared-non-consumable-constraint": {
                "resource-name": self.shared_resource.resource_name,
                "source": self.shared_resource.source,
                "current-value": self.shared_resource.current_value,
                "requested": self.target,
            }
        }

    @staticmethod
    def from_dict(d: Dict) -> "SharedNonConsumableConstraint":
        raise RuntimeError(
            "SharedNonConsumableConstraint uses shared resources and cannot be recreated from json"
        )


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

    if attr in ["exclusive", "exclusive_task", "exclusive-task"]:
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

        if re.match("size::[0-9a-zA-Z]+", value):
            value = value[len("size::") :]
            size = ht.Size.value_of(value)
            return MinResourcePerNode(attr, size)

        elif re.match("memory::[0-9a-zA-Z]+", value):
            value = value[len("memory::") :]
            size = ht.Memory.value_of(value)
            return MinResourcePerNode(attr, size)

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

        if attr.startswith("node."):
            return _parse_node_property_constraint(attr, value)

        return NodeResourceConstraint(attr, *value)

    elif (
        isinstance(value, int)
        or isinstance(value, float)
        or isinstance(value, ht.Size)
        or isinstance(value, ht.Memory)
    ):
        return MinResourcePerNode(attr, value)

    elif value is None:
        return NodeResourceConstraint(attr, value)
        # raise RuntimeError("None is not an allowed value. For attr {}".format(attr))

    else:
        raise RuntimeError(
            "Unsupported constraint value type - attr {} of type {} - {}".format(
                attr, type(value), value
            )
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
            raise RuntimeError(
                "A list was unexpected: {} in {}".format(
                    constraint_expression, constraint_expressions
                )
            )
        else:
            and_constraints = []
            for attr, value in constraint_expression.items():
                # TODO
                c = new_job_constraint(attr, value)
                assert c is not None
                and_constraints.append(c)
            if len(and_constraints) > 1:
                constraints.append(And(*and_constraints))
            else:
                constraints.extend(and_constraints)

    return constraints


_CUSTOM_PARSERS: Dict[str, Callable[[Dict], NodeConstraint]] = {
    "in-a-placement-group": lambda d: InAPlacementGroup(),
    "shared-consumable-constraint": SharedConsumableConstraint.from_dict,
    "shared-non-consumable-constraint": SharedNonConsumableConstraint.from_dict,
}


def register_parser(
    custom_attribute: str, parser: Callable[[Dict], NodeConstraint]
) -> None:
    assert isinstance(custom_attribute, str)
    assert hasattr(parser, "__call__")
    _CUSTOM_PARSERS[custom_attribute] = parser
