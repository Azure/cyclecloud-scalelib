import io
import json

from hpc.autoscale.hpctypes import Memory
from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.demandprinter import OutputFormat, print_demand


def _print_demand(output_format: OutputFormat) -> str:
    stream = io.StringIO()

    node = SchedulerNode("tux", {"ncpus": 2, "mem": Memory.value_of("1.0g")})
    node.available["ncpus"] = 1
    node.assign("11")
    node.assign("12")

    result = DemandResult([], [node], [], [])
    print_demand(
        ["hostname", "job_ids", "ncpus", "*ncpus", "mem"],
        result,
        stream=stream,
        output_format=output_format,
    )
    return stream.getvalue()


def test_print_demand_header() -> None:
    lines = _print_demand("table").splitlines()
    assert len(lines) == 2
    assert lines[0].split() == ["HOSTNAME", "JOB_IDS", "NCPUS", "*NCPUS", "MEM"]
    vals = lines[1].split()
    vals[1] = set(vals[1].split(","))  # type: ignore
    assert vals == ["tux", set("11,12".split(",")), "2", "1", "1.00g"]


def test_print_demand_headerless() -> None:
    lines = _print_demand("table_headerless").splitlines()
    assert len(lines) == 1
    vals = lines[0].split()
    vals[1] = set(vals[1].split(","))  # type: ignore
    assert vals == ["tux", set("11,12".split(",")), "2", "1", "1.00g"]


def test_print_demand_json() -> None:
    d = json.loads(_print_demand("json"))

    d[0]["job_ids"] = sorted(d[0]["job_ids"])
    assert d == [
        {
            "hostname": "tux",
            "job_ids": ["11", "12"],
            "ncpus": 2,
            "*ncpus": 1,
            "mem": "memory::1.00g",
        }
    ]
