import typing
from typing import Any, List, NewType, Optional, Union

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
        job_constraints: Any = None,
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

        if job_constraints is None:
            job_constraints = []
        if not isinstance(job_constraints, list):
            job_constraints = [job_constraints]
        job_constraints = get_constraints(job_constraints)
        self._job_constraints = job_constraints
        for jc in self._job_constraints:
            assert jc is not None
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

    def do_allocate(
        self,
        node_mgr: nodemanager.NodeManager,
        allow_existing: bool,
        all_or_nothing: bool,
    ) -> AllocationResult:

        if self.__node_count > 0:

            return node_mgr.allocate(
                self._job_constraints,
                node_count=self.__node_count,
                allow_existing=allow_existing,
                all_or_nothing=self.__colocated,
                assignment_id=self.name,
            )

        return node_mgr.allocate(
            self._job_constraints,
            node_count=1,
            allow_existing=allow_existing,
            all_or_nothing=all_or_nothing,
            assignment_id=self.name,
        )

    def bucket_candidates(self, candidates: List["NodeBucket"]) -> CandidatesResult:
        from hpc.autoscale.node import bucket

        return bucket.bucket_candidates(candidates, self._job_constraints)

    def __str__(self) -> str:
        if self.executing_hostnames:
            return "({}, {})".format(self.name, self.executing_hostnames)
        return "({}, {})".format(self.name, "idle")

    def __repr__(self) -> str:
        return "{}{}".format(self.__class__.__name__, str(self))

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "iterations": self.iterations,
            "node-count": self.node_count,
            "colocated": self.__colocated,
            "constraints": [jc.to_dict() for jc in self._job_constraints],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Job":
        return cls(d["name"], d["task-count"], d["constraints"])
