import os
import sys
import typing

from hpc.autoscale.example.readmeutil import clone_dcalc, example, withcontext
from hpc.autoscale.hpctypes import Memory
from hpc.autoscale.job.demandcalculator import DemandCalculator, new_demand_calculator
from hpc.autoscale.job.demandprinter import print_demand
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.constraints import BaseNodeConstraint
from hpc.autoscale.node.node import Node, UnmanagedNode
from hpc.autoscale.node.nodemanager import new_node_manager
from hpc.autoscale.results import SatisfiedResult

CONFIG: typing.Dict[str, typing.Any] = {
    "cluster_name": "sge",
    "url": "http://localhost:8080",
    "username": "ryhamel",
    "password": ")ya58uRU0#m%",
    "logging": {
        "config_file": os.path.abspath(os.path.join(__file__, "..", "logging.conf"))
    },
}

DRY_RUN = True

if DRY_RUN:
    SchedulerNode.ignore_hostnames = True


@withcontext
def target_counts_demand() -> None:
    """
    Handle a mixture of 'target count' style allocation of ncpus and nodes via the
    DemandCalculator.
    """
    dcalc = new_demand_calculator(CONFIG)

    # job requires 10 cores (ncpus)
    dcalc.add_job(
        Job(
            name="tc-10",
            constraints={"node.nodearray": "htc", "ncpus": 1, "exclusive": False},
            iterations=10,
        )
    )

    # job requires 10 nodes with 4 cores (ncpus)
    dcalc.add_job(
        Job(
            name="tn-10",
            constraints={"node.nodearray": "htc", "ncpus": 4, "exclusive": True},
            node_count=10,
        )
    )

    # 2 x 5 nodes
    dcalc.add_job(
        Job(
            name="tn-2x5",
            constraints={"node.nodearray": "htc", "ncpus": 2, "exclusive": True},
            node_count=5,
        ),
    )

    demand_result = dcalc.finish()

    if not DRY_RUN:
        dcalc.bootup()

    # note that /ncpus will display available/total. ncpus will display the total, and
    # *ncpus will display available.
    print_demand(["name", "job_ids", "nodearray", "/ncpus"], demand_result)


@withcontext
def default_resources() -> None:
    """
    An example of how to programmatically create default resources.
    """
    # now we will disable the default resources, ncpus/pcpus/gpus etc
    # and define them ourselves.
    CONFIG["disable_default_resources"] = True
    node_mgr = new_node_manager(CONFIG)

    # let's define gpus for every node
    # then, for nodes that actually have a gpu, let's set the pcpus
    # to equal the number of gpus * 2

    # define ngpus
    node_mgr.add_default_resource({}, "ngpus", "node.gpu_count")
    # also could have just passed in a lambda/function
    # node_mgr.add_default_resource({}, "gpus", lambda node: node.gpu_count)

    # now that ngpus is defined, we can use ngpus: 1 here to filter out nodes that
    # have at least one ngpu. Let's set pcpus to 2 * ngpus
    node_mgr.add_default_resource(
        selection={"ngpus": 1},
        resource_name="pcpus",
        default_value=lambda node: node.resources["ngpus"] * 2,
    )
    # and for nodes without GPUs, let's just use the actual pcpu_count
    node_mgr.add_default_resource(
        selection={"ngpus": 0},
        resource_name="pcpus",
        default_value=lambda node: node.pcpu_count,
    )
    # note that a blank selection would have accomplished the same thing, as default resources
    # are processed in order.
    # node_mgr.add_default_resource({}, "pcpus", lambda node: node.pcpu_count)

    has_gpu = node_mgr.example_node("southcentralus", "Standard_NV24")
    no_gpu = node_mgr.example_node("southcentralus", "Standard_F16s")

    print(
        has_gpu.vm_size,
        "(pcpu_count=%s)" % has_gpu.pcpu_count,
        "-> %(ngpus)s ngpus %(pcpus)s pcpus" % has_gpu.resources,
    )
    print(
        no_gpu.vm_size,
        "(pcpu_count=%s)" % has_gpu.pcpu_count,
        "-> %(ngpus)s ngpus %(pcpus)s pcpus" % no_gpu.resources,
    )


@withcontext
def target_counts_node_mgr() -> None:
    """
    Handle a mixture of 'target count' style allocation of ncpus and nodes via the
    NodeMgr.
    """
    node_mgr = new_node_manager(CONFIG)

    result = node_mgr.allocate({"node.nodearray": "htc"}, node_count=2)

    if result:
        print("Allocated {} nodes.".format(len(result.nodes)))
    else:
        print("Failed! {}".format(result))

    result = node_mgr.allocate({"node.nodearray": "htc", "memgb": 1}, slot_count=128)

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
    dcalc.node_mgr.add_default_resource(
        {"node.nodearray": ["htc", "htcspot"]}, "nodetype", "A"
    )
    assert [b for b in dcalc.node_mgr.get_buckets() if b.nodearray == "htc"][
        0
    ].resources["nodetype"] == "A"
    dcalc.node_mgr.add_default_resource({}, "nodetype", "B")

    assert [b for b in dcalc.node_mgr.get_buckets() if b.nodearray == "htc"][
        0
    ].resources["nodetype"] == "A"
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
        ["name", "job_ids", "nodetype", "onprem:False", "/ncpus"], demand_result
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
    node_mgr.add_default_resource({"node.nodearray": "htc"}, "nodetype", "A")

    result = node_mgr.allocate({"nodetype": "A"}, node_count=5)
    assert result
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
    if to_shutdown:
        node_mgr.delete(to_shutdown)


@example
def scaling_down_demand() -> None:
    """
    Scale-up with 50 ncpus, then scale-down to 25 ncpu
    """
    columns = ["name", "job_ids", "required", "/ncpus"]

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

        dcalc.node_history.conn.close()

        return dcalc

    @withcontext
    def scale_down(dcalc: typing.Optional[DemandCalculator] = None) -> None:
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


@withcontext
def manual_node_mgmt() -> None:
    node_mgr = new_node_manager(CONFIG)

    assert node_mgr.allocate({}, node_count=2)
    if node_mgr.new_nodes:
        node_mgr.bootup()

    node1, node2 = node_mgr.get_nodes()

    assert node1 in node_mgr.get_nodes()
    res = node_mgr.delete([node1])
    print(res)
    assert node1 not in node_mgr.get_nodes(), node_mgr.get_nodes()


class MyCustomMemoryRatioConstraint(BaseNodeConstraint):
    def satisfied_by_node(self, node: Node) -> SatisfiedResult:

        if node.memory / node.vcpu_count > 8:
            return SatisfiedResult("success", self, node)

        return SatisfiedResult(
            "failed",
            self,
            node,
            reasons=["Not enough memory per core"],
        )


if __name__ == "__main__":
    if len(sys.argv) == 1 or sys.argv[1] not in globals():

        for name, value in list(globals().items()):
            if hasattr(value, "is_example"):
                print(name)
        sys.exit(1)
    globals()[sys.argv[1]]()
