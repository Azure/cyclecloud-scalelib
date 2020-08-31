import typing
from typing import Any, Dict, Iterable, List, NewType, Optional, Union

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.node import nodemanager
from hpc.autoscale.node.constraints import get_constraints
from hpc.autoscale.results import AllocationResult, CandidatesResult

if typing.TYPE_CHECKING:
    from hpc.autoscale.node.node import Node  # noqa: F401
    from hpc.autoscale.node.bucket import NodeBucket  # noqa: F401

PackingStrategyType = NewType("PackingStrategyType", str)


class PackingStrategy:
    PACK = PackingStrategyType("pack")
    SCATTER = PackingStrategyType("scatter")

    @classmethod
    def is_valid(cls, strat: str) -> bool:
        return strat in [cls.PACK, cls.SCATTER]


_default_job_id = 0


@hpcwrapclass
class Job:
    def __init__(
        self,
        name: Optional[Union[str, ht.JobId]] = None,
        constraints: Any = None,
        iterations: int = 1,
        node_count: int = 0,
        colocated: bool = False,
        packing_strategy: Optional[PackingStrategyType] = None,
        executing_hostnames: Optional[List[ht.Hostname]] = None,
    ) -> None:
        if not name:
            global _default_job_id
            _default_job_id = _default_job_id + 1
            name = ht.JobId(str(_default_job_id))

        if packing_strategy is not None:
            assert PackingStrategy.is_valid(
                packing_strategy
            ), "Invalid packing_strategy {}".format(packing_strategy)
            self.__packing_strategy = packing_strategy
        elif node_count > 0:
            self.__packing_strategy = PackingStrategy.SCATTER
        else:
            self.__packing_strategy = PackingStrategy.PACK

        self.__name = ht.JobId(name)
        self.__iterations = iterations
        self.__iterations_remaining = iterations
        self.__node_count = node_count
        self.__nodes_remaining = node_count
        self.__colocated = colocated
        self.__metadata: Dict[str, Any] = {}

        if constraints is None:
            constraints = []

        if not isinstance(constraints, list):
            constraints = [constraints]

        self._constraints = get_constraints(constraints)

        def update_assignment_id(constraints: Iterable[Any]) -> None:
            for constraint in constraints:
                assert constraint is not None
                if hasattr(constraint, "assignment_id"):
                    constraint.assignment_id = self.name
                update_assignment_id(constraint.get_children())

        update_assignment_id(self._constraints)

        self.__executing_hostnames = executing_hostnames or []

    @property
    def name(self) -> ht.JobId:
        return self.__name

    @property
    def packing_strategy(self) -> PackingStrategyType:
        return self.__packing_strategy

    @packing_strategy.setter
    def packing_strategy(self, packing_strategy: PackingStrategyType) -> None:
        assert PackingStrategy.is_valid(packing_strategy)
        self.__packing_strategy = packing_strategy

    @property
    def executing_hostnames(self) -> List[ht.Hostname]:
        return self.__executing_hostnames

    @executing_hostnames.setter
    def executing_hostnames(self, value: List[ht.Hostname]) -> None:
        self.__executing_hostnames = value

    @property
    def iterations(self) -> int:
        return self.__iterations

    @property
    def iterations_remaining(self) -> int:
        return self.__iterations_remaining

    @iterations_remaining.setter
    def iterations_remaining(self, value: int) -> None:
        self.__iterations_remaining = value

    @property
    def node_count(self) -> int:
        return self.__node_count

    @property
    def colocated(self) -> bool:
        return self.__colocated

    def do_allocate(
        self,
        node_mgr: nodemanager.NodeManager,
        allow_existing: bool,
        all_or_nothing: bool,
    ) -> AllocationResult:

        if self.__node_count > 0:

            return node_mgr.allocate(
                self._constraints,
                node_count=self.__node_count,
                allow_existing=allow_existing,
                all_or_nothing=self.__colocated,
                assignment_id=self.name,
            )

        return node_mgr.allocate(
            self._constraints,
            slot_count=self.iterations_remaining,
            allow_existing=allow_existing,
            all_or_nothing=all_or_nothing,
            assignment_id=self.name,
        )

    def bucket_candidates(self, candidates: List["NodeBucket"]) -> CandidatesResult:
        from hpc.autoscale.node import bucket

        return bucket.bucket_candidates(candidates, self._constraints)

    def add_constraint(self, constraint: typing.Any) -> None:
        if not isinstance(constraint, list):
            constraint = [constraint]
        parsed_cons = get_constraints(constraint)
        self._constraints.extend(parsed_cons)

    @property
    def metadata(self) -> Dict[str, Any]:
        return self.__metadata

    def __str__(self) -> str:
        if self.executing_hostnames:
            return "({}, {})".format(self.name, self.executing_hostnames)
        return "({}, {})".format(self.name, "idle")

    def __repr__(self) -> str:
        return "{}{}".format(self.__class__.__name__, str(self))

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "constraints": [jc.to_dict() for jc in self._constraints],
            "iterations": self.iterations,
            "iterations-remaining": self.iterations_remaining,
            "node-count": self.node_count,
            "colocated": self.__colocated,
            "packing-strategy": self.__packing_strategy,
            "executing-hostnames": self.__executing_hostnames,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Job":

        job = Job(
            name=d["name"],
            constraints=d["constraints"],
            iterations=d.get("iterations", 1),
            node_count=d.get("node-count", 1),
            colocated=d.get("colocated", False),
            packing_strategy=d.get("packing-strategy"),
            executing_hostnames=d.get("executing-hostnames"),
        )
        if "iterations-remaining" in d:
            job.iterations_remaining = d["iterations-remaining"]

        job.metadata.update(d.get("metadata", {}))

        return job
