import heapq
from typing import Iterator, List, Tuple

from hpc.autoscale.node.node import Node
from hpc.autoscale.results import EarlyBailoutResult


def default_prioritizer(n: Node) -> int:
    return getattr(n, "available").get("ncpus", 0)


def default_early_bailout(node: Node) -> EarlyBailoutResult:
    return EarlyBailoutResult("success")


class NodeQueue:
    def __init__(self) -> None:
        self._heap: List[Tuple[int, Node]] = []
        heapq.heapify(self._heap)

    def node_priority(self, node: Node) -> int:
        return node.available.get("ncpus", 0)

    def early_bailout(self, node: Node) -> EarlyBailoutResult:
        return EarlyBailoutResult("success")

    def push(self, node: Node) -> None:
        if node in self:
            return
        heap_item = (self.node_priority(node), node)
        heapq.heappush(self._heap, heap_item)

    def __iter__(self) -> Iterator[Node]:
        for _, n in iter(heapq.nlargest(len(self._heap), self._heap)):
            yield n

    def reversed(self) -> Iterator[Node]:
        for _, n in iter(heapq.nsmallest(len(self._heap), self._heap)):
            yield n

    def update(self) -> None:
        self._heap = [(self.node_priority(n), n) for n in self]
        heapq.heapify(self._heap)

    def empty(self) -> bool:
        return len(self) == 0

    def __repr__(self) -> str:
        return str([repr(x) for x in self])

    def __str__(self) -> str:
        return str([str(x) for x in self])

    def __len__(self) -> int:
        return len(self._heap)
