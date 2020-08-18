import random
import string
from typing import Any, Dict, Optional

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.job.schedulernode import SchedulerNode
from hypothesis import given
from hypothesis.strategies import SearchStrategy


def _cmp(a: SchedulerNode, b: SchedulerNode) -> bool:
    assert a.assignments == b.assignments
    assert a.available == b.available
    assert a.bucket_id == b.bucket_id
    assert a.closed == b.closed
    assert a.cores_per_socket == b.cores_per_socket
    # TODO RDH
    # assert a.delayed_node_id == b.delayed_node_id
    assert a.exists == b.exists
    assert a.gpu_count == b.gpu_count
    assert a.hostname == b.hostname
    assert a.infiniband == b.infiniband
    assert a.location == b.location
    assert a.managed == b.managed
    assert a.memory == b.memory
    assert a.metadata == b.metadata
    assert a.name == b.name
    assert a.node_attribute_overrides == b.node_attribute_overrides
    assert a.nodearray == b.nodearray
    assert a.pcpu_count == b.pcpu_count
    assert a.placement_group == b.placement_group
    assert a.private_ip == b.private_ip
    assert a.required == b.required
    assert a.resources == b.resources
    assert a.software_configuration == b.software_configuration
    assert a.spot == b.spot
    assert a.state == b.state
    assert a.vcpu_count == b.vcpu_count
    assert a.version == b.version
    assert a.vm_capabilities == b.vm_capabilities
    assert a.vm_family == b.vm_family
    assert a.vm_size == b.vm_size
    return True


def schedulernode() -> SearchStrategy[SchedulerNode]:
    class SchedulerNodeStrategy(SearchStrategy):
        """
        Generate a random schedulernode with a random set of resources,
        job ids etc
        """

        def __repr__(self) -> str:
            return "SchedulerNodeStrategy()"

        def do_draw(self, data: Any) -> ht.VMSize:
            import hypothesis.internal.conjecture.utils as d

            idx = d.integer_range(data, 0, 1_000_000_000)
            r = random.Random(idx)

            def draw_value() -> Optional[Any]:
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
                    return None

            hostname = "n-o-d-e_-{}".format(r.randint(1, 1000000))
            resources: Dict[str, Optional[Any]] = {}
            num_resources = r.randint(0, 10)
            for n in range(num_resources):
                rname = "res-{}".format(n)
                resources[rname] = draw_value()

            node = SchedulerNode(ht.Hostname(hostname), resources)

            for job_id in range(r.randint(0, 10)):
                node.assign(str(job_id))

            num_meta = r.randint(0, 10)
            for n in range(num_meta):
                mname = "meta-{}".format(n)
                node.metadata[mname] = draw_value()

            for rname, rvalue in node.resources.items():
                if r.random() > 0.5:
                    if isinstance(rvalue, int):
                        node.available[rname] = rvalue - r.randint(0, rvalue + 1)
                    elif isinstance(rvalue, float):
                        node.available[rname] = rvalue * r.random()
                    elif isinstance(rvalue, bool):
                        node.available[rname] = rvalue ^ (r.random() < 0.5)
                    elif rvalue is None:
                        # in theory you can change a null resource
                        # into an actual value as available
                        if r.random() < 0.25:
                            node.available[rname] = draw_value()

            return node

    return SchedulerNodeStrategy()


@given(schedulernode())
def test_schedulernode_json(a: SchedulerNode):
    b = SchedulerNode.from_dict(a.to_dict())
    assert _cmp(a, b)
