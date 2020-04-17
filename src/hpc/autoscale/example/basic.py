import os
import sys
from typing import List

import hpc.autoscale.hpclogging as logging
from hpc.autoscale.job import demandprinter
from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.demandcalculator import new_demand_calculator
from hpc.autoscale.job.job import Job
from hpc.autoscale.node.node import Node

logging.basicConfig(
    format="%(asctime)-15s: %(levelname)s %(message)s",
    stream=sys.stderr,
    level=logging.DEBUG,
)
_exit_code = 0


class BasicDriver:
    def __init__(self, jobs: List[Job], scheduler_nodes: List[SchedulerNode],) -> None:
        self.jobs = jobs
        self.scheduler_nodes = scheduler_nodes

    def handle_join_cluster(self, matched_nodes: List[Node]) -> None:
        pass

    def handle_draining(self, unmatched_nodes: List[Node]) -> List[Node]:
        return unmatched_nodes

    def handle_post_delete(self, deleted_nodes: List[Node]) -> None:
        pass

    def __str__(self) -> str:
        return "BasicDriver(jobs={}, scheduler_nodes={})".format(
            self.jobs[:100], self.scheduler_nodes[:100]
        )

    def __repr__(self) -> str:
        return str(self)


def autoscale_driver(config_path: str, driver: BasicDriver) -> DemandResult:
    global _exit_code

    # it has two member variables - jobs
    # ge_driver.jobs - autoscale Jobs
    # ge_driver.compute_nodes - autoscale SchedulerNodes

    demand_calculator = new_demand_calculator(
        config_path, existing_nodes=driver.scheduler_nodes
    )

    for htc_job in driver.jobs:
        demand_calculator.add_job(htc_job)

    demand_result = demand_calculator.finish()

    # by default we will just bootup whatever is required. We can start deallocated/off matching nodes
    # or create new ones. The user can also filter them out and pass in a list of filtered nodes as an argument
    # demand_calculator.bootup(my_filtered_nodes)
    demand_calculator.bootup()

    # details here are that we pass in nodes that matter (matched) and the driver figures out
    # which ones are new and need to be added via qconf
    driver.handle_join_cluster(demand_result.matched_nodes)

    # we also tell the driver about nodes that are unmatched. It filters them out
    # and returns a list of ones we can delete.

    unmatched_for_5_mins = demand_calculator.find_unmatched_for(at_least=3)

    nodes_to_delete = driver.handle_draining(unmatched_for_5_mins)

    if nodes_to_delete:
        try:
            demand_calculator.delete(nodes_to_delete)

            # in case it has anything to do after a node is deleted (usually just remove it from the cluster)
            driver.handle_post_delete(nodes_to_delete)
        except Exception as e:
            _exit_code = 1
            logging.warning("Deletion failed, will retry on next iteration: %s", e)
            logging.exception(str(e))

    # and let's use the demand printer to print the demand_result.
    output_columns = [
        "name",
        "hostname",
        "job_ids",
        "required",
        "slots",
        "*slots",
        "vm_size",
        "memory",
        "vcpu_count",
        "state",
        "placement_group_id",
    ]
    demandprinter.print_demand(output_columns, demand_result)
    return demand_result


def example(config_path: str) -> DemandResult:
    jobs = [
        Job("1", {"slots": 3}),
        Job("2", node_count=4),
    ]
    scheduler_nodes = [SchedulerNode("ip-01234567", {"slots": 4})]
    return autoscale_driver(config_path, BasicDriver(jobs, scheduler_nodes))


if __name__ == "__main__":
    example(os.path.expanduser(sys.argv[1]))
    sys.exit(_exit_code)
