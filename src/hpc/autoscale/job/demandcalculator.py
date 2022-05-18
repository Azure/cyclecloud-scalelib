from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import hpc.autoscale.hpclogging as logging
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.hpclogging import apitrace
from hpc.autoscale.hpctypes import OperationId
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.job import Job, PackingStrategy
from hpc.autoscale.job.nodequeue import NodeQueue
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodehistory import (
    NodeHistory,
    NullNodeHistory,
    SQLiteNodeHistory,
)
from hpc.autoscale.node.nodemanager import NodeManager, new_node_manager
from hpc.autoscale.results import AllocationResult, BootupResult, DeleteResult, Result
from hpc.autoscale.util import (
    NullSingletonLock,
    SingletonLock,
    load_config,
    new_singleton_lock,
    partition_single,
)


@hpcwrapclass
class DemandCalculator:
    """
    This class is responsible for calculating what the demand for nodes is based on the Jobs fed into it.

    NOTE: This class itself will NOT actually invoke the REST api calls required to scale up or down nodes,
    but instead gives the user the information required to decide what actions to take.

    See the examples for more information.

    """

    def __init__(
        self,
        config: Dict,
        node_mgr: NodeManager,
        node_history: NodeHistory = NullNodeHistory(),
        node_queue: Optional[NodeQueue] = None,
        singleton_lock: Optional[SingletonLock] = None,
    ) -> None:
        self.config = config
        assert isinstance(node_mgr, NodeManager)
        self.node_mgr = node_mgr
        self.node_history = node_history

        if node_queue is None:
            node_queue = NodeQueue()

        self.__scheduler_nodes_queue: NodeQueue = node_queue

        for node in self.node_mgr.get_non_failed_nodes():
            self.__scheduler_nodes_queue.push(node)

        self.__set_buffer_delayed_invocations: List[Tuple[Any, ...]] = []
        failed_nodes = self.node_mgr.get_nodes()
        self.node_history.decorate(
            list(self.__scheduler_nodes_queue) + failed_nodes, config
        )

        if not singleton_lock:
            singleton_lock = new_singleton_lock({})
        self.__singleton_lock = singleton_lock
        logging.debug(
            "Calculating demand using the following pre-existing nodes: %s",
            [n.name for n in self.__scheduler_nodes_queue],
        )

    @apitrace
    def add_jobs(self, jobs: List[Job]) -> None:
        for job in jobs:
            self._add_job(job)

    @apitrace
    def add_job(self, job: Job) -> None:
        assert isinstance(job, Job)
        self._add_job(job)
        self.__scheduler_nodes_queue.update()

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

        allocated_nodes: List[Node] = []
        failure_reasons = self._handle_allocate(
            job, allocated_nodes, all_or_nothing=True
        )
        if failure_reasons:
            return AllocationResult(
                "CompoundFailure",
                reasons=failure_reasons,
                slots_allocated=slots_to_allocate,
            )
        else:
            assert len(allocated_nodes) == job.node_count, "{} != {}".format(
                len(allocated_nodes), job.node_count
            )
            return AllocationResult(
                "success", nodes=allocated_nodes, slots_allocated=slots_to_allocate
            )

    def _pack_job(self, job: Job) -> Result:
        """
        1) will it ever fit? - check num nodes with any capacity
        2) does it have the proper resources? bucket.match(job.resources)
        3) order them
        4) tell the bucket to allocate X nodes - let the bucket figure out what is new and what is not.
        """
        # TODO break non-exclusive
        allocated_nodes: List[Node] = []
        slots_to_allocate = job.iterations_remaining
        assert job.iterations_remaining > 0

        # available_buckets = self.node_mgr.get_buckets()
        # # I don't want to fill up the log with rejecting placement groups
        # # so just filter them here
        # filter_by_colocated = [
        #     b for b in available_buckets if bool(b.placement_group) == job.colocated
        # ]
        # candidates_result = job.bucket_candidates(filter_by_colocated)

        # if not candidates_result:
        #     logging.warning("There are no resources to scale up for job %s", job)
        #     logging.warning("See below:")
        #     for child_result in candidates_result.child_results or []:
        #         logging.warning("    %s", child_result.message)
        #     return candidates_result

        # logging.debug("Candidates for job %s: %s", job.name, candidates_result.candidates)

        failure_reasons = self._handle_allocate(
            job, allocated_nodes, all_or_nothing=False
        )

        # we have allocated at least some tasks
        if allocated_nodes:
            assert allocated_nodes
            return AllocationResult(
                "success", nodes=allocated_nodes, slots_allocated=slots_to_allocate
            )

        return AllocationResult("Failed", reasons=failure_reasons)

    def _handle_allocate(
        self, job: Job, allocated_nodes_out: List[Node], all_or_nothing: bool,
    ) -> Optional[List[str]]:
        result = job.do_allocate(
            self.node_mgr, all_or_nothing=all_or_nothing, allow_existing=True,
        )

        if not result:
            return result.reasons

        for node in result.nodes:
            if not node.exists and node.metadata.get("__demand_allocated") is None:
                self.__scheduler_nodes_queue.push(node)
                node.metadata["__demand_allocated"] = True
        allocated_nodes_out.extend(result.nodes)

        return None

    def get_compute_nodes(self) -> List[Node]:
        return list(self.__scheduler_nodes_queue)

    @apitrace
    def finish(self) -> DemandResult:
        # for nodearray, vm_size, count, placement_group_id in self.__set_buffer_delayed_invocations:
        #     self.__set_buffer(nodearray, vm_size, count, placement_group_id)
        return self.get_demand()

    @apitrace
    def update_history(self) -> None:
        failed_nodes = self.node_mgr.get_failed_nodes()
        self.node_history.update(list(self.__scheduler_nodes_queue) + failed_nodes)

    @apitrace
    def get_demand(self) -> DemandResult:
        return self._get_demand()

    def _get_demand(self) -> DemandResult:
        required_nodes = [
            snode for snode in self.__scheduler_nodes_queue if snode.required
        ]

        unrequired_nodes = [
            snode for snode in self.__scheduler_nodes_queue if not snode.required
        ]

        return DemandResult(
            list(self.node_mgr.get_new_nodes()),
            required_nodes,
            unrequired_nodes,
            self.node_mgr.get_failed_nodes(),
        )

    @apitrace
    def find_unmatched_for(
        self,
        at_least: Union[Callable[[Node], float], float, int] = 300,
        unmatched_nodes: Optional[List[Node]] = None,
    ) -> List[Node]:
        if isinstance(at_least, (int, float)):
            at_least_value = at_least
            at_least = lambda node: at_least_value  # qa: ignore
        unmatched_nodes = unmatched_nodes or self.get_demand().unmatched_nodes
        by_id = dict([(n.delayed_node_id.node_id, n) for n in unmatched_nodes])
        ret = []
        for node_id, hostname, idle_time in self.node_history.find_unmatched(
            for_at_least=-1
        ):
            node = by_id.get(node_id)
            if not node:
                continue
            at_least_delta = at_least(node)
            omega = self.node_history.now() - at_least_delta
            if idle_time < omega:
                if node.assignments:
                    continue
                ret.append(node)
        return ret

    @apitrace
    def find_booting(
        self,
        at_least: Union[Callable[[Node], float], float, int] = 1800,
        booting_nodes: Optional[List[Node]] = None,
    ) -> List[Node]:
        if not booting_nodes:
            booting_nodes = self.node_mgr.get_nodes()

        if isinstance(at_least, (int, float)):
            at_least_value = at_least
            at_least = lambda node: at_least_value

        # filter out nodes that have converged.
        booting_nodes = [
            n
            for n in booting_nodes
            if n.target_state == "Started"
            and n.state not in ["Ready", "Started"]
            and n.delayed_node_id.node_id
        ]

        by_id = partition_single(booting_nodes, lambda n: n.delayed_node_id.node_id)

        ret = []
        for node_id, hostname, create_time in self.node_history.find_booting(
            for_at_least=-1
        ):

            node = by_id.get(node_id)
            if not node:
                continue
            at_least_delta = at_least(node)
            omega = self.node_history.now() - at_least_delta
            if create_time < omega:
                ret.append(by_id[node_id])

        return ret

    @apitrace
    def delete(self, nodes: Optional[List[Node]] = None) -> DeleteResult:
        nodes = nodes if nodes is not None else self.get_demand().unmatched_nodes
        if not nodes:
            logging.info("No nodes to delete.")
            return DeleteResult("success", OperationId(""), None)

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

        by_hostname: Dict[str, Node] = partition_single(
            self.__scheduler_nodes_queue, lambda n: n.hostname_or_uuid  # type: ignore
        )

        by_node_id: Dict[str, Node] = partition_single(
            self.__scheduler_nodes_queue, lambda n: n.delayed_node_id.node_id or n.hostname_or_uuid  # type: ignore
        )

        for new_snode in scheduler_nodes:
            if new_snode.hostname not in by_hostname:
                by_hostname[new_snode.hostname] = new_snode
                self.__scheduler_nodes_queue.push(new_snode)
                self.node_mgr.add_unmanaged_nodes([new_snode])
                if new_snode.resources.get("ccnodeid"):
                    ccnodeid = new_snode.resources.get("ccnodeid")
                    if ccnodeid in by_node_id:
                        logging.warning(
                            "%s has a hostname that does not match what is in CycleCloud",
                            new_snode,
                        )
                    else:
                        logging.warning(
                            "%s has ccnodeid defined, but no longer exists in CycleCloud",
                            new_snode,
                        )
                else:
                    logging.debug(
                        "Found new node[hostname=%s] that does not exist in CycleCloud",
                        new_snode.hostname,
                    )

                # TODO inform bucket catalog?
            elif new_snode.metadata.get("override_resources", True):

                old_snode = by_hostname[new_snode.hostname_or_uuid]
                logging.fine(
                    "Found existing CycleCloud node[hostname=%s]", new_snode.hostname,
                )
                old_snode.update(new_snode)
            else:
                logging.fine(
                    "Found existing CycleCloud node[hostname=%s], but node.metadata.override_resources=false"
                    + " so ignoring the reported resources and only copying metadata",
                    new_snode.hostname,
                )
                old_snode = by_hostname[new_snode.hostname_or_uuid]
                old_snode.metadata.update(new_snode.metadata)

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

    def to_dict(self) -> Dict:
        ret = {}
        for attr_name in dir(self):
            if not (
                attr_name[0].isalpha() or attr_name.startswith("_DemandCalculator")
            ):
                continue

            attr = getattr(self, attr_name)
            if "__call__" not in dir(attr):
                attr_expr = attr_name.replace("_DemandCalculator", "")

                if hasattr(attr, "to_dict"):
                    attr_value = attr.to_dict()
                else:
                    attr_value = str(attr)

                ret[attr_expr] = attr_value
        return ret


@apitrace
def new_demand_calculator(
    config: Union[str, dict],
    existing_nodes: Optional[List[SchedulerNode]] = None,
    node_mgr: Optional[NodeManager] = None,
    node_history: Optional[NodeHistory] = None,
    disable_default_resources: bool = False,
    node_queue: Optional[NodeQueue] = None,
    singleton_lock: Optional[SingletonLock] = NullSingletonLock(),
) -> DemandCalculator:
    config_dict = load_config(config)

    existing_nodes = existing_nodes or []

    if node_mgr is None:
        node_mgr = new_node_manager(
            config_dict, disable_default_resources=disable_default_resources,
        )
    else:
        logging.initialize_logging(config_dict)

        if not disable_default_resources:
            node_mgr.set_system_default_resources()

    node_history = node_history or SQLiteNodeHistory()

    if singleton_lock is None:
        singleton_lock = new_singleton_lock(config_dict)

    dc = DemandCalculator(
        config_dict, node_mgr, node_history, node_queue, singleton_lock
    )

    dc.update_scheduler_nodes(existing_nodes)

    return dc
