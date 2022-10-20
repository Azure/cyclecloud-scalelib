import inspect
import io
import json
import logging as logginglib
import sys
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, TextIO, Tuple

from typing_extensions import Literal

from hpc.autoscale import hpclogging as logging
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.hpctypes import Hostname
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.node.node import Node

OutputFormat = Literal["json", "table", "table_headerless"]


@hpcwrapclass
class DemandPrinter:
    def __init__(
        self,
        column_names: Optional[List[str]] = None,
        stream: Optional[TextIO] = None,
        output_format: OutputFormat = "table",
        long: bool = False,
    ) -> None:
        column_names_list: List[str] = []

        if column_names:
            column_names_list = column_names

        self.__defaults = {}

        for n in range(len(column_names_list)):
            expr = column_names_list[n]

            if ":" in expr and "[" not in expr:
                column, default_value = expr.split(":", 1)
                column_names_list[n] = column
                self.__defaults[column] = default_value

        self.column_names = [x.lower() for x in column_names_list]

        self.stream = stream or sys.stdout
        self.output_format = output_format
        self.long = long

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

        short_columns = [c.split("@")[0] for c in columns]
        long_columns = [c.split("@")[-1] for c in columns]

        # sort by private ip or the node name

        def sort_by_ip_or_name(node: Node) -> Any:
            if node.private_ip:
                return tuple(map(int, node.private_ip.split(".")))

            name_toks = node.name.split("-")
            if name_toks[-1].isdigit():
                node_index = int(name_toks[-1])
                nodearray_ord = [ord(x) for x in node.nodearray]
                # 2**31 to make these come after private ips
                # then nodearray name, then index
                return tuple([2 ** 31] + nodearray_ord + [node_index])
            return tuple([-1] + name_toks)

        ordered_nodes = sorted(demand_result.compute_nodes, key=sort_by_ip_or_name)

        for node in ordered_nodes:
            row: List[str] = []
            rows.append(row)
            for column in long_columns:
                # TODO justify - this is a printing function, so this value could be lots of things etc.

                value: Any = None
                is_from_available = column.startswith("*")
                is_ratio = column.startswith("/")
                is_slice = "[" in column

                if is_from_available or is_ratio:
                    column = column[1:]

                def _slice(v: str) -> str:
                    return v

                slice = _slice

                if is_slice:
                    slice_expr = column[column.index("[") :]
                    column = column.split("[")[0]
                    # TODO maybe parse this instead of eval-ing a lambda
                    if self.long:
                        slice = lambda v: v  # noqa: E731
                    else:
                        slice = eval(
                            "lambda v: v%s if v is not None else v" % slice_expr
                        )

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
                    elif is_ratio:
                        value = "{}/{}".format(
                            node.available.get(column), node.resources.get(column)
                        )
                    elif column in node.resources:
                        value = node.resources.get(column)
                    else:
                        value = node.metadata.get(column)

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
                        value = ",".join(sorted(value))
                    elif isinstance(value, set):
                        value = ",".join(sorted(list(value)))
                    elif value is None:
                        value = ""
                    elif isinstance(value, float):
                        value = "{:.1f}".format(value)
                    elif not isinstance(value, str):
                        value = str(value)

                else:
                    if hasattr(value, "to_json"):
                        value = value.to_json()
                    elif hasattr(value, "keys"):
                        value = dict(value)

                row.append(slice(value))

        # remove / and slice expressions

        stripped_short_names = [c.lstrip("/").split("[")[0] for c in short_columns]
        if self.output_format != "json":
            stripped_short_names = [x.upper() for x in stripped_short_names]
        print_rows(stripped_short_names, rows, self.stream, self.output_format)

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
    long: bool = False,
) -> None:
    printer = DemandPrinter(None, stream=stream, output_format=output_format, long=long)
    printer.print_columns(demand_result)


def print_demand(
    columns: List[str],
    demand_result: DemandResult,
    stream: Optional[TextIO] = None,
    output_format: OutputFormat = "table",
    log: bool = False,
    long: bool = False,
) -> None:
    if log:
        stream = logging_stream(stream or sys.stdout)
    printer = DemandPrinter(
        columns, stream=stream, output_format=output_format, long=long
    )
    printer.print_demand(demand_result)


def wrap_text_io(clz: Any) -> Callable[[TextIO, Optional[str]], TextIO]:
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
    def __init__(self, wrapped: TextIO, logger_name: Optional[str] = None) -> None:
        self.line_buffer = io.StringIO()
        self.wrapped = wrapped
        self.logger_name = logger_name

    def write(self, s: str) -> int:
        self.line_buffer.write(s)
        return self.wrapped.write(s)

    def flush(self) -> None:
        buf = self.line_buffer.getvalue()
        if not buf:
            return
        fact = logginglib.getLogRecordFactory()
        logger = logging.getLogger(self.logger_name)
        created = None
        for line in buf.splitlines(keepends=False):
            record = fact(
                name="demandprinter",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg=line,
                args=(),
                exc_info=None,
                created=created,
            )
            created = created or record.created
            logger.handle(record)

        self.line_buffer = io.StringIO()

    def close(self) -> None:
        self.flush()
        self.wrapped.close()


LoggingStream = wrap_text_io(_LoggingStream)


def logging_stream(wrapped: TextIO, logger_name: Optional[str] = None) -> TextIO:
    logger_name = logger_name or "demand"
    return LoggingStream(wrapped, logger_name)


class ExcludeDemandPrinterFilter(logginglib.Filter):
    def __init__(self, name: str = "") -> None:
        super().__init__(name)

    def filter(self, record: logginglib.LogRecord) -> bool:
        return record.name != "demandprinter"


def calculate_column_widths(
    columns: List[str], rows: List[List[str]]
) -> Tuple[int, ...]:
    maxes = [len(c.split("@")[0]) for c in columns]
    for row in rows:
        for n in range(len(row)):
            maxes[n] = max(len(row[n]), maxes[n])
    return tuple(maxes)


def print_rows(
    columns: List[str],
    rows: List[List[str]],
    stream: Optional[TextIO] = None,
    output_format: str = "table",
) -> None:
    output_format = output_format or "table"

    stream = stream or sys.stdout

    short_names = [c.split("@")[0] for c in columns]

    if output_format.lower() == "json":
        json.dump(
            [dict(zip(short_names, row)) for row in rows],
            stream,
            indent=2,
        )
    else:
        widths = calculate_column_widths(short_names, rows)
        formats = " ".join(["{:%d}" % x for x in widths])
        if output_format == "table":
            print(formats.format(*short_names), file=stream)

        for row in rows:
            print(formats.format(*[str(r) for r in row]), file=stream)
    stream.flush()
