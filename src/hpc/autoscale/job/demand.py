from typing import List

from hpc.autoscale.node.node import Node


class DemandResult:
    """
    Product of DemandCalculator.
    """

    def __init__(
        self,
        new_nodes: List[Node],
        matched_nodes: List[Node],
        unmatched_nodes: List[Node],
    ) -> None:
        self.new_nodes = new_nodes
        self.matched_nodes = matched_nodes
        self.unmatched_nodes = unmatched_nodes
        self.compute_nodes = matched_nodes + unmatched_nodes

    def __str__(self) -> str:
        return "DemandResult(new_nodes={}, matched={}, unmatched={})".format(
            self.new_nodes, self.matched_nodes, self.unmatched_nodes
        )

    def __repr__(self) -> str:
        return str(self)
