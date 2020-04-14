import logging
import sys
import typing
from uuid import uuid4

from hpc.autoscale.example.readmeutil import clone_dcalc, example, withcontext
from hpc.autoscale.hpctypes import Memory
from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.job.demandcalculator import DemandCalculator, new_demand_calculator
from hpc.autoscale.job.demandprinter import print_demand
from hpc.autoscale.job.job import Job
from hpc.autoscale.node.constraints import BaseNodeConstraint
from hpc.autoscale.node.node import Node, UnmanagedNode
from hpc.autoscale.node.nodemanager import new_node_manager
from hpc.autoscale.results import DefaultContextHandler, SatisfiedResult

logging.basicConfig(
    format="%(asctime)-15s: %(levelname)s %(message)s",
    stream=sys.stderr,
    level=logging.DEBUG,
)


CONFIG = {
    "cluster_name": "example",
    "url": "https://localhost:7443",
    "username": "USERNAME",
    "password": "PASSWORD",
}


DRY_RUN = True


@withcontext
def target_counts_demand() -> None:
    """
    TODO
    """
    dcalc = new_demand_calculator(CONFIG)

    # # 100 cores
    dcalc.add_job(Job("tc-100", {"node.nodearray": "htc", "ncpus": 1}, iterations=10))

    # 10 nodes
    dcalc.add_job(Job("tn-10", {"node.nodearray": "htc", "ncpus": 1}, node_count=10))

    # 2 x 5 nodes
    dcalc.add_job(
        Job("tn-2x5", {"node.nodearray": "htc", "ncpus": 1}, iterations=2, node_count=5)
    )

    demand_result = dcalc.finish()

    if not DRY_RUN:
        dcalc.bootup()

    print_demand(["name", "job_ids", "nodearray", "*ncpus"], demand_result)

    assert len(demand_result.new_nodes) > 20, "expected until we can do scatter"


@withcontext
def default_resources() -> None:
    """
        have printer print out every resource for ever bucket. (and get_columns)
        add gpus by default (node.gpu_count, node.gpu_sku, node.gpu_vendor)
    """
    node_mgr = new_node_manager(CONFIG)
    node_mgr.add_default_resource({}, "ncpus", lambda node: node.resources["gpus"] * 2)
    node_mgr.add_default_resource(
        {"node.vm_family": "standard_Ffamily"}, "ncpus", "node.vcpu_count"
    )
    node_mgr.add_default_resource({"node.vm_size": "N24"}, "gpus", "4")


@withcontext
def target_counts_node_mgr() -> None:
    """
        break allocate to add_nodes / scale_to

    """
    node_mgr = new_node_manager(CONFIG)
    node_mgr.add_default_resource({}, "ncpus", "node.vcpu_count")

    result = node_mgr.allocate({"node.nodearray": "htc"}, node_count=2)

    if result:
        print("Allocated {} nodes".format(len(result.nodes)))
    else:
        print("Failed! {}".format(result))

    result = node_mgr.allocate({"node.nodearray": "htc", "ncpus": 1}, slot_count=10)

    if result:
        print("Allocated {} nodes".format(len(result.nodes)))
    else:
        print("Failed! {}".format(result))

    result = node_mgr.allocate({"node.nodearray": "htc"}, core_count=8)

    if result:
        print("Allocated {} nodes".format(len(result.nodes)))
    else:
        print("Failed! {}".format(result))

    # you can also do Memory.value_of("100g")
    # or even (Memory.value_of("1g") * 100), as the memory object is supposed
    # to be used as a number

    print("Allocated {} nodes in total".format(len(node_mgr.new_nodes)))

    if not DRY_RUN:
        node_mgr.bootup()


@withcontext
def onprem_burst_demand() -> None:
    onprem001 = SchedulerNode(
        "onprem001", resources={"onprem": True, "nodetype": "A", "ncpus": 16}
    )
    onprem002 = SchedulerNode(
        "onprem002", resources={"onprem": True, "nodetype": "A", "ncpus": 32}
    )

    # onprem002 already has 10 cores occupied
    onprem002.available["ncpus"] -= 10

    dcalc = new_demand_calculator(CONFIG, existing_nodes=[onprem001, onprem002])

    # we want 50 ncpus, but there are only 38 onpremise, so we need to burst
    # 12 more cores.
    dcalc.add_job(Job("tc-100", {"nodetype": "A", "ncpus": 1}, iterations=50))

    demand_result = dcalc.finish()

    if not DRY_RUN:
        dcalc.bootup()

    # also note we can add defaults to the column by adding a :, like
    # onprem:False, as this is only defined on the onprem nodes and not
    # on the Azure nodes.
    print_demand(
        ["name", "job_ids", "nodetype", "onprem:False", "*ncpus"], demand_result
    )


@withcontext
def onprem_burst_node_mgr() -> None:
    # Unlike the SchedulerNode above, here we can define the vcpu_count and memory
    # for the onprem nodes.
    onprem_res = {"onprem": True, "nodetype": "A"}
    onprem001 = UnmanagedNode(
        "onprem001", vcpu_count=16, memory=Memory(128, "g"), resources=onprem_res
    )
    onprem002 = UnmanagedNode(
        "onprem002", vcpu_count=32, memory=Memory(256, "g"), resources=onprem_res
    )

    node_mgr = new_node_manager(CONFIG, existing_nodes=[onprem001, onprem002])

    result = node_mgr.allocate({"nodetype": "A"}, count=5)

    if result:
        print(
            "Allocated {} nodes, {} are new".format(
                len(result.nodes), len(node_mgr.new_nodes)
            )
        )
    else:
        print("Failed! {}".format(result))

    if not DRY_RUN:
        node_mgr.bootup()


@withcontext
def shutdown_nodes_node_mgr() -> None:
    node_names = ["htc-1"]
    node_mgr = new_node_manager(CONFIG)
    to_shutdown = [x for x in node_mgr.get_nodes() if x.name in node_names]
    node_mgr.delete(to_shutdown)


@example
def use_result_handler() -> None:
    # use request_id in context
    request_id = str(uuid4())
    handler = DefaultContextHandler("")
    handler.set_context("[{}]".format(request_id))
    # ...


@example
def scaling_down_demand() -> None:
    columns = ["name", "job_ids", "required", "*ncpus"]

    @withcontext
    def scale_up() -> DemandCalculator:
        dcalc = new_demand_calculator(CONFIG)

        dcalc.add_job(
            Job("tc-100", {"node.nodearray": "htc", "ncpus": 1}, iterations=50)
        )

        demand_result = dcalc.finish()

        if not DRY_RUN:
            dcalc.bootup()

        print_demand(columns, demand_result)

        return dcalc

    @withcontext
    def scale_down(dcalc: typing.Optional[DemandCalculator]) -> None:
        dcalc = dcalc or new_demand_calculator(CONFIG)
        dcalc.add_job(
            Job("tc-50", {"node.nodearray": "htc", "ncpus": 1}, iterations=25)
        )

        demand_result = dcalc.finish()

        if not DRY_RUN:
            dcalc.bootup()

        print_demand(columns, demand_result)
        print(
            "The following nodes can be shutdown: {}".format(
                ",".join([n.name for n in demand_result.unmatched_nodes])
            )
        )

    scaleup_dcalc = scale_up()

    if DRY_RUN:
        scale_down(clone_dcalc(scaleup_dcalc))
    else:
        scale_down()


class MyCustomMemoryRatioConstraint(BaseNodeConstraint):
    def satisfied_by_node(self, node: Node) -> SatisfiedResult:

        if node.memory / node.vcpu_count > 8:
            return SatisfiedResult("success", self, node)

        return SatisfiedResult(
            "failed", self, node, reasons=["Not enough memory per core"],
        )


if __name__ == "__main__":
    if len(sys.argv) == 1 or sys.argv[1] not in globals():

        for name, value in list(globals().items()):
            if hasattr(value, "is_example"):
                print(name)
        sys.exit(1)
    globals()[sys.argv[1]]()
