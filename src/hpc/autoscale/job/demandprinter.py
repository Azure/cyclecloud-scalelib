import inspect
import io
import json
import logging as logginglib
import sys
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, TextIO, Tuple

import frozendict

from hpc.autoscale import hpclogging as logging
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.hpctypes import Hostname
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.node.node import Node
from typing_extensions import Literal

OutputFormat = Literal["json", "table", "table_headerless"]


@hpcwrapclass
class DemandPrinter:
    def __init__(
        self,
        column_names: Optional[List[str]] = None,
        stream: Optional[TextIO] = None,
        output_format: OutputFormat = "table",
    ) -> None:
        column_names_list: List[str] = []

        if column_names:
            column_names_list = column_names

        self.__defaults = {}

        for n in range(len(column_names_list)):
            expr = column_names_list[n]
            if ":" in expr:
                column, default_value = expr.split(":", 1)
                column_names_list[n] = column
                self.__defaults[column] = default_value

        self.column_names = [x.lower() for x in column_names_list]

        self.stream = stream or sys.stdout
        self.output_format = output_format

    def _calc_width(self, columns: List[str], rows: List[List[str]]) -> Tuple[int, ...]:
        maxes = [len(c) for c in columns]
        for row in rows:
            for n in range(len(row)):
                maxes[n] = max(len(row[n]), maxes[n])
        return tuple(maxes)

    def _get_all_columns(self, compute_nodes: List[Node]) -> List[str]:

        columns = []
        for attr_name in dir(Node):
            if not attr_name[0].isalpha():
                continue
            attr = getattr(Node, attr_name)
            if hasattr(attr, "__call__"):
                continue
            columns.append(attr_name)

        if compute_nodes:
            all_available: Set[str] = set()
            for n in compute_nodes:
                all_available.update(n.available.keys())

            columns += list(all_available)
        assert None not in columns
        columns = sorted(columns)
        return columns

    def print_columns(self, demand_result: DemandResult = None) -> None:
        columns = self.column_names
        if not columns:
            columns = self._get_all_columns(
                demand_result.compute_nodes if demand_result else []
            )

        columns = [c for c in columns if c != "hostname_required"]

        widths = self._calc_width(columns, [])
        formats = " ".join(["{:%d}" % x for x in widths])
        assert len(widths) == len(columns), "{} != {}".format(len(widths), len(columns))
        print(formats.format(*columns), file=self.stream)
        self.stream.flush()

    def print_demand(self, demand_result: DemandResult) -> None:
        rows = []
        columns = self.column_names
        if not columns:
            columns = self._get_all_columns(demand_result.compute_nodes)

        if self.output_format == "json":
            columns = [c for c in columns if c not in ["hostname_required"]]
        else:
            columns = [
                c
                for c in columns
                if c not in ["available", "node", "hostname_required"]
            ]

        columns = ["job_ids" if c == "assigned_job_ids" else c for c in columns]
        if "name" in columns:
            columns.remove("name")
            columns.insert(0, "name")

        # sort by private ip or the node name
        ordered_nodes = sorted(
            demand_result.compute_nodes,
            key=lambda n: tuple(map(int, n.private_ip.split(".")))
            if n.private_ip
            else tuple([2 ** 31] + [ord(l) for l in n.name]),  # noqa: E741
        )

        for node in ordered_nodes:
            row: List[str] = []
            rows.append(row)
            for column in columns:
                # TODO justify - this is a printing function, so this value could be lots of things etc.

                value: Any = None
                is_from_available = column.startswith("*")
                if is_from_available:
                    column = column[1:]

                if column == "hostname":
                    hostname = node.hostname

                    if not node.exists or not hostname:
                        if node.private_ip:
                            hostname = Hostname(str(node.private_ip))
                        else:
                            hostname = Hostname("tbd")
                    value = hostname
                elif column == "hostname_required":
                    continue
                elif column == "job_ids":
                    value = node.assignments
                elif hasattr(node, column):
                    value = getattr(node, column)
                else:
                    if is_from_available:
                        value = node.available.get(column)
                    else:
                        value = node.resources.get(column)

                if value is None:
                    value = self.__defaults.get(column)

                # convert sets to lists, as sets are not json serializable
                if isinstance(value, set):
                    value = list(value)
                elif isinstance(value, datetime):
                    value = value.isoformat()

                # for json, we support lists, null, numbers etc.
                # for table* we will output a string for every value.
                if self.output_format != "json":
                    if isinstance(value, list):
                        value = ",".join(value)
                    elif isinstance(value, set):
                        value = ",".join(value)
                    elif value is None:
                        value = ""
                    elif isinstance(value, float):
                        value = "{:.1f}".format(value)
                    elif not isinstance(value, str):
                        value = str(value)

                else:
                    if hasattr(value, "to_json"):
                        value = value.to_json()
                    elif isinstance(value, frozendict.frozendict):
                        value = dict(value)

                row.append(value)

        if self.output_format == "json":
            json.dump(
                [dict(zip(columns, row)) for row in rows], self.stream, indent=2,
            )
        else:
            widths = self._calc_width(columns, rows)
            formats = " ".join(["{:%d}" % x for x in widths])
            if self.output_format == "table":
                print(formats.format(*[c.upper() for c in columns]), file=self.stream)

            for row in rows:
                print(formats.format(*[str(r) for r in row]), file=self.stream)
        self.stream.flush()

    def __str__(self) -> str:
        return "DemandPrinter(columns={}, output_format={}, stream={})".format(
            str(self.column_names), self.output_format, self.stream
        )

    def __repr__(self) -> str:
        return str(self)


def print_columns(
    demand_result: DemandResult,
    stream: Optional[TextIO] = None,
    output_format: OutputFormat = "table",
) -> None:
    printer = DemandPrinter(None, stream=stream, output_format=output_format)
    printer.print_columns(demand_result)


def print_demand(
    columns: List[str],
    demand_result: DemandResult,
    stream: Optional[TextIO] = None,
    output_format: OutputFormat = "table",
    log: bool = False,
) -> None:
    if log:
        stream = logging_stream(stream or sys.stdout)
    printer = DemandPrinter(columns, stream=stream, output_format=output_format)
    printer.print_demand(demand_result)


def wrap_text_io(clz: Any) -> Callable[[TextIO], TextIO]:
    members: Dict[str, Any] = {}
    for attr in dir(TextIO):
        if not attr[0].islower() and attr not in [
            "__enter__",
            "__exit__",
            "__iter__",
            "__next__",
        ]:
            continue

        if attr in dir(clz):
            continue

        def make_member(mem_name: str) -> Any:
            is_function = inspect.isfunction(getattr(TextIO, mem_name))
            if is_function:
                return lambda *args: getattr(args[0].wrapped, mem_name)(*args[1:])
            else:
                return property(lambda *args: getattr(args[0].wrapped, mem_name))

        members[attr] = make_member(attr)
    return type("LoggingStream", (clz,), members)


class _LoggingStream:
    def __init__(self, wrapped: TextIO) -> None:
        self.line_buffer = io.StringIO()
        self.wrapped = wrapped

    def write(self, s: str) -> int:
        self.line_buffer.write(s)
        return self.wrapped.write(s)

    def flush(self) -> None:
        buf = self.line_buffer.getvalue()
        if not buf:
            return
        fact = logginglib.getLogRecordFactory()
        root = logging.getLogger()

        if not root.filters:
            root.addFilter(ExcludeDemandPrinterFilter("root"))

        for line in buf.splitlines(keepends=False):
            record = fact(
                name="demandprinter",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg=line,
                args=(),
                exc_info=None,
            )
            root.handle(record)

        self.line_buffer = io.StringIO()

    def close(self) -> None:
        self.flush()
        self.wrapped.close()


LoggingStream = wrap_text_io(_LoggingStream)


def logging_stream(wrapped: TextIO) -> TextIO:
    return LoggingStream(wrapped)


class ExcludeDemandPrinterFilter(logginglib.Filter):
    def __init__(self, name: str = "") -> None:
        super().__init__(name)

    def filter(self, record: logginglib.LogRecord) -> bool:
        return record.name != "demandprinter"
