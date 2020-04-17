import json
from typing import Any, List, Optional, Tuple, Union

import hpc.autoscale.hpclogging as logging
from hpc.autoscale.hpclogging import apitrace
from hpc.autoscale.hpctypes import OperationId
from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.job import Job, PackingStrategy
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodehistory import (
    NodeHistory,
    NullNodeHistory,
    SQLiteNodeHistory,
)
from hpc.autoscale.node.nodemanager import NodeManager, new_node_manager
from hpc.autoscale.results import AllocationResult, BootupResult, Result


class DemandCalculator:
    """
        This class is responsible for calculating what the demand for nodes is based on the Jobs fed into it.

        NOTE: This class itself will NOT actually invoke the REST api calls required to scale up or down nodes,
        but instead gives the user the information required to decide what actions to take.

        See the examples for more information.

    """

    def __init__(
        self, node_mgr: NodeManager, node_history: NodeHistory = NullNodeHistory(),
    ) -> None:
        assert isinstance(node_mgr, NodeManager)
        self.node_mgr = node_mgr
        self.node_history = node_history
        self.__scheduler_nodes = dict(
            [(node.hostname, node) for node in self.node_mgr.get_nodes()]
        )
        self.__set_buffer_delayed_invocations: List[Tuple[Any, ...]] = []

    @apitrace
    def add_jobs(self, jobs: List[Job]) -> None:
        for job in jobs:
            self._add_job(job)

    @apitrace
    def add_job(self, job: Job) -> None:
        assert isinstance(job, Job)
        self._add_job(job)

    def _add_job(self, job: Job) -> Result:

        if job.packing_strategy == PackingStrategy.SCATTER:
            result = self._add_scatter(job)
        else:
            result = self._pack_job(job)
        return result

    def _add_scatter(self, job: Job) -> Result:
        """
        1) will it ever fit? - check num nodes with any capacity
        2) does it have the proper resources? bucket.match(job.resources)
        3) order them
        4) tell the bucket to allocate X nodes - let the bucket figure out what is new and what is not.
        """
        slots_to_allocate = job.iterations_remaining
        # available_buckets = self.node_mgr.get_buckets()
        # candidates_result = job.bucket_candidates(available_buckets)

        # if not candidates_result:
        #     # TODO log or something
        #     logging.warn("There are no resources to scale up for job %s", job)
        #     return candidates_result

        # failure_reasons: List[str] = []

        # for candidate in candidates_result.candidates:
        allocated_nodes: List[Node] = []
        failure_reasons = self._handle_allocate(job, allocated_nodes, True)
        if failure_reasons:
            return AllocationResult(
                "CompoundFailure",
                reasons=failure_reasons,
                slots_allocated=slots_to_allocate,
            )
        else:
            return AllocationResult(
                "success", nodes=allocated_nodes, slots_allocated=slots_to_allocate
            )

        return AllocationResult("CompoundFailure", reasons=failure_reasons)

    def _pack_job(self, job: Job) -> Result:
        """
        1) will it ever fit? - check num nodes with any capacity
        2) does it have the proper resources? bucket.match(job.resources)
        3) order them
        4) tell the bucket to allocate X nodes - let the bucket figure out what is new and what is not.
        """
        # TODO break non-exclusive
        allocated_nodes = []
        slots_to_allocate = job.iterations_remaining
        assert job.iterations_remaining > 0

        for snode in self.__scheduler_nodes.values():
            # TODO
            if snode.state == "Failed":
                continue

            if snode.closed:
                continue

            node_added = False

            while job.iterations_remaining > 0:
                # TODO this is ugly and hokey RDH
                still_valid = True
                for constraint in job._job_constraints:
                    still_valid = still_valid and constraint.satisfied_by_node(snode)

                # TODO hokey
                if not still_valid:
                    break

                add_job_result = snode.decrement(
                    job._job_constraints, job.iterations_remaining, job.name
                )

                if not add_job_result:
                    break

                if not node_added:
                    allocated_nodes.append(snode)
                    node_added = True

                job.iterations_remaining -= add_job_result.total_slots

                if job.iterations_remaining <= 0:
                    break

        if job.iterations_remaining == 0:
            return AllocationResult(
                "success", nodes=allocated_nodes, slots_allocated=slots_to_allocate
            )

        available_buckets = self.node_mgr.get_buckets()

        candidates_result = job.bucket_candidates(available_buckets)

        if not candidates_result:
            # TODO log or something
            logging.warn("There are no resources to scale up for job %s", job)
            return candidates_result

        failure_reasons = self._handle_allocate(
            job, allocated_nodes, all_or_nothing=False
        )

        # we have allocated at least some tasks
        if job.iterations_remaining < job.iterations:
            assert allocated_nodes
            return AllocationResult(
                "success", nodes=allocated_nodes, slots_allocated=slots_to_allocate
            )

        return AllocationResult("Failed", reasons=failure_reasons)

    def _handle_allocate(
        self, job: Job, allocated_nodes_out: List[Node], all_or_nothing: bool,
    ) -> Optional[List[str]]:

        failure_reasons: List[str] = []
        max_loop_iters = 1_000_000
        while job.iterations_remaining > 0:
            max_loop_iters -= 1
            assert max_loop_iters > 0, "Caught in an infinite loop!"

            result = job.do_allocate(
                self.node_mgr, all_or_nothing=all_or_nothing, allow_existing=True,
            )

            if not result:
                failure_reasons.extend(result.reasons)
                return failure_reasons

            job.iterations_remaining -= result.total_slots
            for node in result.nodes:
                self.__scheduler_nodes[node.hostname] = node
            allocated_nodes_out.extend(result.nodes)

        return None

    def get_compute_nodes(self) -> List[Node]:
        return list(self.__scheduler_nodes.values())

    @apitrace
    def finish(self) -> DemandResult:
        # for nodearray, vm_size, count, placement_group_id in self.__set_buffer_delayed_invocations:
        #     self.__set_buffer(nodearray, vm_size, count, placement_group_id)
        self.node_history.update(self.__scheduler_nodes.values())
        return self.get_demand()

    @apitrace
    def get_demand(self) -> DemandResult:
        return self._get_demand()

    def _get_demand(self) -> DemandResult:
        required_nodes = [
            snode for snode in self.__scheduler_nodes.values() if snode.required
        ]

        for node in self.node_mgr.get_new_nodes():
            assert node.required

        unrequired_nodes = [
            snode for snode in self.__scheduler_nodes.values() if not snode.required
        ]

        return DemandResult(
            list(self.node_mgr.get_new_nodes()), required_nodes, unrequired_nodes,
        )

    @apitrace
    def find_unmatched_for(
        self, at_least: float = 300, unmatched_nodes: Optional[List[Node]] = None,
    ) -> List[Node]:
        unmatched_nodes = unmatched_nodes or self.get_demand().unmatched_nodes
        by_hostname = dict([(n.hostname, n) for n in unmatched_nodes])
        ret = []
        for hostname, idle_time in self.node_history.find_unmatched(
            for_at_least=at_least
        ):
            if hostname in by_hostname and idle_time > at_least:
                ret.append(by_hostname[hostname])
        return ret

    @apitrace
    def delete(self, nodes: Optional[List[Node]] = None) -> None:
        nodes = nodes if nodes is not None else self.get_demand().unmatched_nodes
        if not nodes:
            logging.info("No nodes to delete.")
            return

        logging.debug("deleting %s", [n.name for n in nodes])
        return self.node_mgr.delete(nodes)

    @apitrace
    def bootup(self, nodes: Optional[List[Node]] = None) -> BootupResult:
        nodes = nodes if nodes is not None else self.get_demand().new_nodes
        if not nodes:
            logging.info("No nodes to bootup.")
            return BootupResult("success", OperationId(""), None)

        logging.debug("booting up %s", [n.name for n in nodes])
        return self.node_mgr.bootup(nodes)

    @apitrace
    def update_scheduler_nodes(self, scheduler_nodes: List[SchedulerNode]) -> None:
        for new_snode in scheduler_nodes:
            if new_snode.hostname not in self.__scheduler_nodes:
                logging.debug(
                    "Found new node[hostname=%s] that does not exist in CycleCloud",
                    new_snode.hostname,
                )
                self.__scheduler_nodes[new_snode.hostname] = new_snode
                # TODO inform bucket catalog?
            else:
                old_snode = self.__scheduler_nodes[new_snode.hostname]
                logging.debug(
                    "Found new node[hostname=%s] that does not exist in CycleCloud",
                    new_snode.hostname,
                )
                old_snode.update(new_snode)

    def __str__(self) -> str:
        attrs = []
        for attr_name in dir(self):
            if not (
                attr_name[0].isalpha() or attr_name.startswith("_DemandCalculator")
            ):
                continue

            attr = getattr(self, attr_name)
            if "__call__" not in dir(attr):
                attr_expr = attr_name.replace("_DemandCalculator", "")
                attrs.append("{}={}".format(attr_expr, attr))
        return "DemandCalculator({})".format(", ".join(attrs))

    def __repr__(self) -> str:
        return str(self)


@apitrace
def new_demand_calculator(
    config: Union[str, dict],
    existing_nodes: Optional[List[SchedulerNode]] = None,
    node_mgr: Optional[NodeManager] = None,
    node_history: Optional[NodeHistory] = None,
) -> DemandCalculator:

    if isinstance(config, str):
        with open(config) as fr:
            config_dict = json.load(fr)
    else:
        config_dict = config

    existing_nodes = existing_nodes or []
    if isinstance(config, str):
        with open(config) as fr:
            config = json.load(fr)

    if node_mgr is None:
        node_mgr = new_node_manager(config_dict)

    node_history = node_history or SQLiteNodeHistory()

    dc = DemandCalculator(node_mgr, node_history)
    dc.update_scheduler_nodes(existing_nodes)
    return dc
