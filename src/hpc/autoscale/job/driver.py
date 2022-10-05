import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.nodequeue import NodeQueue
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodehistory import NodeHistory, SQLiteNodeHistory
from hpc.autoscale.node.nodemanager import NodeManager
from hpc.autoscale.util import (
    NullSingletonLock,
    SingletonFileLock,
    SingletonLock,
    partition,
)


class SchedulerDriver(ABC):
    def __init__(self, name: str) -> None:
        self.name = name
        self.__jobs_cache: Optional[List[Job]] = None
        self.__scheduler_nodes_cache: Optional[List[SchedulerNode]] = None
        self.__node_history: Optional[NodeHistory] = None

    @property
    def autoscale_home(self) -> str:
        if os.getenv("AUTOSCALE_HOME"):
            return os.environ["AUTOSCALE_HOME"]
        return os.path.join("/opt", "azurehpc", self.name)

    @abstractmethod
    def initialize(self) -> None:
        """
        Placeholder for subclasses to customize initialization
        By default, we make sure that the ccnodeid exists
        """
        ...

    @abstractmethod
    def preprocess_config(self, config: Dict) -> Dict:
        """
        Placeholder for subclasses to customize config dynamically
        """
        ...

    def preprocess_node_mgr(self, config: Dict, node_mgr: NodeManager) -> None:
        add_ccnodeid_default_resource(node_mgr)
        add_default_placement_groups(config, node_mgr)

    def validate_nodes(
        self, scheduler_nodes: List[SchedulerNode], cc_nodes: List[Node]
    ) -> None:
        """Before any demand calculations are done, validate gives a chance
            to validate / preprocess nodes.
        """
        ...

    @abstractmethod
    def handle_failed_nodes(self, nodes: List[Node]) -> List[Node]:
        """
        These nodes are in a failed state and will not join the cluster.
        """
        ...

    @abstractmethod
    def add_nodes_to_cluster(
        self, nodes: List[Node], include_permanent: bool = False
    ) -> List[Node]:
        """
        These nodes are ready to join. They MAY already be a part of the cluster,
        so this should be monotonic - you can call it multiple times without issue.

        It is highly recommended you use a scheduler resource called ccnodeid to
        disambiguate onpremise to cloud nodes.
        for node in nodes:
            if not node.resources.get("ccnodeid"):
                logging.info(
                    "%s is not managed by CycleCloud, or at least 'ccnodeid' is not defined. Ignoring",
                    node,
                )
                continue

        """
        ...

    @abstractmethod
    def handle_post_join_cluster(self, nodes: List[Node]) -> List[Node]:
        ...

    @abstractmethod
    def handle_boot_timeout(self, nodes: List[Node]) -> List[Node]:
        """
        These nodes timed out on booting. It is likely they will require some sort of
        cleanup on the scheduler.
        """
        ...

    @abstractmethod
    def handle_draining(self, nodes: List[Node]) -> List[Node]:
        """
        Put nodes into a state where they can gracefully stop running jobs
        """
        ...

    @abstractmethod
    def handle_post_delete(self, nodes: List[Node]) -> List[Node]:
        """
        Cleanup after the autoscaler decided to delete nodes (i.e. the actual VM instances)
        """
        ...

    def new_node_queue(self, config: Dict) -> NodeQueue:
        """
        NodeQueue provides some scheduler/site specific optimizations for how nodes
        are ordered, especially during large pack operations.
        """
        return NodeQueue()

    def new_singleton_lock(self, config: Dict) -> SingletonLock:
        default_path = os.path.join(self.autoscale_home, "lock.file")

        lock_path = config.get("lock_file", default_path)

        if lock_path and not config.get("read_only", False):
            # TODO RDH check file perms
            return SingletonFileLock(lock_path)

        return NullSingletonLock()

    def new_node_history(self, config: Dict) -> NodeHistory:
        if self.__node_history:
            return self.__node_history
        db_path = config.get("nodehistorydb")
        if not db_path:
            db_path = os.path.join(self.autoscale_home, "nodehistory.db")

        read_only = config.get("read_only", False)
        self.__node_history = SQLiteNodeHistory(db_path, read_only)

        return self.__node_history

    def read_jobs_and_nodes(
        self, config: Dict, force: bool = False
    ) -> Tuple[List[Job], List[SchedulerNode]]:
        """
        Convert scheduler specific job information into generic hpc.autoscale.job.job.Job
        objects and convert scheduler specific node/host information into generic
        hpc.autoscale.job.schedulernode.SchedulerNode objects.

        The reason this is combined as one function rather than two is to encourage the author
        to consider how to get this information in a single invocation, so that the Jobs and Nodes
        agree - say you ask for the jobs when job X is idle but then ask for the nodes after job X
        has started running. When in doubt, choose the jobs second, as forcing a job to run on a node
        that does not seem to have the capacity is supported - i.e. if the job says it is running on
        node N but node N says it has no capacity, we will just trust the job and force the job
        to run on that node.
        """
        if self.__jobs_cache is None or self.__scheduler_nodes_cache is None or force:
            # do the the actual read
            self.__jobs_cache, self.__scheduler_nodes_cache = self._read_jobs_and_nodes(
                config
            )

        return self.__jobs_cache, self.__scheduler_nodes_cache

    @abstractmethod
    def _read_jobs_and_nodes(
        self, config: Dict
    ) -> Tuple[List[Job], List[SchedulerNode]]:
        ...


def add_default_placement_groups(config: Dict, node_mgr: NodeManager) -> None:
    nas = config.get("nodearrays", {})
    for name, child in nas.items():
        if child.get("placement_groups"):
            return

    by_pg = partition(
        node_mgr.get_buckets(), lambda b: (b.nodearray, b.placement_group)
    )
    by_na_vm = partition(node_mgr.get_buckets(), lambda b: (b.nodearray, b.vm_size))

    for key, buckets in by_na_vm.items():
        nodearray, vm_size = key
        non_pg_buckets = [b for b in buckets if not b.placement_group]
        if not non_pg_buckets:
            # hardcoded PlacementGroupId
            logging.debug(
                "Nodearray %s defines PlacementGroupId, so no additional "
                + "placement groups will be created automatically.",
                nodearray,
            )
            continue
        bucket = non_pg_buckets[0]
        if not bucket.supports_colocation:
            continue

        buf_size = int(
            nas.get(nodearray, {}).get("generated_placement_group_buffer", 2)
        )
        buf_remaining = buf_size
        pgi = 0
        while buf_remaining > 0:
            pg_name = ht.PlacementGroup("{}_pg{}".format(vm_size, pgi))
            pg_key = (nodearray, pg_name)
            if pg_key not in by_pg:
                logging.fine("Adding placement group %s", pg_name)
                node_mgr.add_placement_group(pg_name, bucket)
                buf_remaining -= 1
            pgi += 1


def add_ccnodeid_default_resource(node_mgr: NodeManager) -> None:
    """
    In order for this to function properly, ccnodeid must be defined
    """

    def get_node_id(n: Node) -> Optional[str]:
        return n.delayed_node_id.node_id

    node_mgr.add_default_resource({}, "ccnodeid", get_node_id)
