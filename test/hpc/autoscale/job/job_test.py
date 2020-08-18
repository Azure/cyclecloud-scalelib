import random
import string
from typing import Any, Dict, Optional

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.job.job import Job
from hypothesis import given
from hypothesis.strategies import SearchStrategy


def _cmp(a: Job, b: Job) -> bool:
    assert a.executing_hostnames == b.executing_hostnames
    assert a.iterations == b.iterations
    assert a.iterations_remaining == b.iterations_remaining
    assert a.metadata == b.metadata
    assert a.name == b.name
    assert a.node_count == b.node_count
    assert a.packing_strategy == b.packing_strategy
    assert a._constraints == b._constraints

    return True


def job() -> SearchStrategy[Job]:
    class JobStrategy(SearchStrategy):
        """
        Generate a random schedulernode with a random set of resources,
        job ids etc
        """

        def __repr__(self) -> str:
            return "JobStrategy()"

        def do_draw(self, data: Any) -> ht.VMSize:
            import hypothesis.internal.conjecture.utils as d

            idx = d.integer_range(data, 0, 1_000_000_000)
            r = random.Random(idx)

            def draw_value(rtype_draw: Optional[int] = None) -> Optional[Any]:
                if rtype_draw is None:
                    rtype_draw = r.randint(0, 4)

                if rtype_draw == 0:
                    return r.randint(0, 100)
                elif rtype_draw == 1:
                    return r.random() * 100
                elif rtype_draw == 2:

                    def draw_letter():
                        return r.choice(string.ascii_letters)

                    return "".join([draw_letter() for n in range(r.randint(0, 100))])
                elif rtype_draw == 3:
                    return r.random() < 0.5
                else:
                    list_length = r.randint(0, 10)
                    list_type = r.randint(0, 3)  # exclude lists
                    return [draw_value(list_type) for _ in range(list_length)]

            job_id = "j-o-b_-{}".format(r.randint(1, 1000000))
            constraints: Dict[str, Optional[Any]] = {}
            num_resources = r.randint(0, 10)
            for n in range(num_resources):
                cname = "cons-{}".format(n)
                constraints[cname] = draw_value()

            job = Job(
                job_id,
                constraints,
                iterations=r.randint(0, 100),
                node_count=r.randint(0, 100),
                colocated=r.random() < 0.5,
                packing_strategy=r.choice(["pack", "scatter", None]),
                executing_hostnames=None
                if r.random() < 0.5
                else [draw_value(2) for _ in range(r.randint(0, 5))],
            )

            job.iterations_remaining -= r.randint(0, job.iterations)
            for n in range(r.randint(0, 5)):
                job.metadata["meta-{}".format(n)] = draw_value()

            return job

    return JobStrategy()


@given(job())
def test_job_json(a: Job):
    b = Job.from_dict(a.to_dict())
    assert _cmp(a, b)
