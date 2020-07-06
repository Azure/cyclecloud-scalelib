from typing import List

from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.node.node import Node


@hpcwrapclass
class DemandResult:
    """
    Product of DemandCalculator.
    """

    def __init__(
        self,
        new_nodes: List[Node],
        matched_nodes: List[Node],
        unmatched_nodes: List[Node],
        failed_nodes: List[Node],
    ) -> None:
        self.new_nodes = new_nodes
        self.matched_nodes = matched_nodes
        self.unmatched_nodes = unmatched_nodes
        self.failed_nodes = failed_nodes
        self.compute_nodes = matched_nodes + unmatched_nodes
        for node in self.failed_nodes:
            if node not in self.compute_nodes:
                self.compute_nodes.append(node)

    def __str__(self) -> str:
        return "DemandResult(new_nodes={}, matched={}, unmatched={}, failed={})".format(
            self.new_nodes, self.matched_nodes, self.unmatched_nodes, self.failed_nodes
        )

    def __repr__(self) -> str:
        return str(self)
