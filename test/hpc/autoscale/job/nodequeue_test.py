from hpc.autoscale.job.nodequeue import NodeQueue
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import EarlyBailoutResult


class HostnameNodeQueue(NodeQueue):
    def node_priority(self, node: Node) -> int:
        return ord(node.hostname)


class NoCPUs(NodeQueue):
    def early_bailout(self, node: Node) -> EarlyBailoutResult:
        if node.available["ncpus"] == 0:
            return EarlyBailoutResult("NoCPUs", node, ["No more ncpus"])
        return EarlyBailoutResult("success")


def test_node_queue() -> None:

    # ordinal of hostname
    host = HostnameNodeQueue()
    host.push(SchedulerNode("a", {"ncpus": 4}))
    host.push(SchedulerNode("b", {"ncpus": 8}))
    host.push(SchedulerNode("c", {"ncpus": 2}))

    assert [n.hostname for n in host] == ["c", "b", "a"]
    assert [n.hostname for n in host.reversed()] == ["a", "b", "c"]

    # use ncpus - default behavior
    cpus = NodeQueue()
    cpus.push(SchedulerNode("a", {"ncpus": 4}))
    cpus.push(SchedulerNode("b", {"ncpus": 8}))
    cpus.push(SchedulerNode("c", {"ncpus": 2}))

    assert [n.hostname for n in cpus] == ["b", "a", "c"]
    assert [n.hostname for n in cpus.reversed()] == ["c", "a", "b"]
    b, a, c = list(cpus)
    b.available["ncpus"] = 3

    # rebalancing is expensive, so only do it on update() call
    assert [n.hostname for n in cpus] == ["b", "a", "c"]
    cpus.update()
    assert [n.hostname for n in cpus] == ["a", "b", "c"]

    bailout = NoCPUs()
    a = SchedulerNode("a", {"ncpus": 4})
    bailout.push(a)
    assert bailout.early_bailout(a)
    assert bailout.early_bailout(a).status == "success"

    a.available["ncpus"] = 0
    assert not bailout.early_bailout(a)
    assert bailout.early_bailout(a).status == "NoCPUs"
    assert bailout.early_bailout(a).node == a
    assert bailout.early_bailout(a).reasons == ["No more ncpus"]
