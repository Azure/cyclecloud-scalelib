import inspect
from typing import Any, Callable

from hpc.autoscale.job.demandcalculator import DemandCalculator, new_demand_calculator
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.results import (
    DefaultContextHandler,
    register_result_handler,
    unregister_result_handler,
)


def example(func: Callable) -> Callable:
    setattr(func, "is_example", True)
    return func


def clone_dcalc(dcalc: DemandCalculator) -> DemandCalculator:
    scheduler_nodes = [
        SchedulerNode(str(n.name), dict(n.resources)) for n in dcalc.get_compute_nodes()
    ]
    if hasattr(dcalc.node_history, "conn"):
        conn = getattr(dcalc.node_history, "conn")
        conn.close()
    return new_demand_calculator(
        {}, node_mgr=dcalc.node_mgr, existing_nodes=scheduler_nodes
    )


def withcontext(func: Callable) -> Callable:
    @example
    def invoke(*args: Any, **kwargs: Any) -> Any:
        handler = register_result_handler(
            DefaultContextHandler("[{}]".format(func.__name__))
        )
        if "handler" in inspect.signature(func).parameters:
            kwargs["handler"] = handler

        ret = func(*args, **kwargs)

        unregister_result_handler(handler)
        return ret

    return invoke
