import argparse
import code
import io
import json
import os
import re
import sys
import traceback
import typing
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from fnmatch import fnmatch
from subprocess import check_output
from typing import Any, Callable, Dict, Iterable, List, Optional, TextIO, Tuple, Union

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale import util as hpcutil
from hpc.autoscale.job import demandprinter
from hpc.autoscale.job.demand import DemandResult
from hpc.autoscale.job.demandcalculator import DemandCalculator, new_demand_calculator
from hpc.autoscale.job.demandprinter import OutputFormat
from hpc.autoscale.job.driver import SchedulerDriver
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node import node as nodelib
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.node.bucket import NodeBucket
from hpc.autoscale.node.constraints import NodeConstraint, get_constraints
from hpc.autoscale.node.node import Node
from hpc.autoscale.node.nodehistory import NodeHistory
from hpc.autoscale.node.nodemanager import NodeManager, new_node_manager
from hpc.autoscale.results import (
    DefaultContextHandler,
    EarlyBailoutResult,
    MatchResult,
    register_result_handler,
)
from hpc.autoscale.util import (
    json_dump,
    load_config,
    parse_boot_timeout,
    parse_idle_timeout,
    partition_single,
)


def _print(*msg: Any) -> None:
    # if os.getenv("SCALELIB_AUTOCOMPLETE_LOG"):
    log_file = os.getenv("SCALELIB_AUTOCOMPLETE_LOG", "/tmp/scalelib_autocomplete.log")
    assert log_file
    with open(log_file, "a") as fw:
        print(*msg, file=fw)


def error(msg: Any, *args: Any) -> None:
    print(str(msg) % args, file=sys.stderr)
    raise RuntimeError(str(msg) % args)
    sys.exit(1)


def warn(msg: Any, *args: Any) -> None:
    print(str(msg) % args, file=sys.stderr)


def str_list(c: str) -> List[str]:
    return c.rstrip(",").split(",")


def json_type(c: str) -> Dict:
    try:
        if os.path.exists(c):
            with open(c) as fr:
                return json.load(fr)
        return json.loads(c)
    except Exception as e:
        error("Given json file/literal '{}': {}".format(c, e))
        sys.exit(1)


def constraint_type(c: Union[str, List[str]]) -> str:
    assert isinstance(c, str)

    try:
        return json.dumps(json.loads(c))
    except Exception:
        ...
    try:
        key, value_expr = c.split("=", 1)
        values = [x.strip() for x in value_expr.split(",")]
        values_converted = []
        for v in values:
            converted: Union[int, float, bool, str, None] = v
            if v.startswith('"') and v.endswith('"'):
                converted = v.lstrip('"').rstrip('"')
            elif v.lower() in ["true", "false"]:
                converted = v.lower() == "true"
            else:
                try:
                    converted = int(v)
                except Exception:
                    try:
                        converted = float(v)
                    except Exception:
                        ...

            if v == "null":
                converted = None

            values_converted.append(converted)

        if len(values_converted) == 1:
            return json.dumps({key: values_converted[0]})

        return json.dumps({key: values_converted})
    except Exception as e:
        _print(str(e))
        raise


def parse_format(c: str) -> str:
    c = c.lower()
    if c in ["json", "table", "table_headerless"]:
        return c
    print("Expected json, table or table_headerless - got", c, file=sys.stderr)
    sys.exit(1)


class ReraiseAssertionInterpreter(code.InteractiveConsole):
    def __init__(
        self,
        locals: Optional[Dict] = None,
        filename: str = "<console>",
        reraise: bool = True,
    ) -> None:
        code.InteractiveConsole.__init__(self, locals=locals, filename=filename)
        self.reraise = reraise
        hist_file = os.path.expanduser("~/.azurehpchistory")

        if os.path.exists(hist_file):
            with open(hist_file) as fr:
                self.history_lines = fr.readlines()
        else:
            self.history_lines = []
        self.history_fw = open(hist_file, "a")

    def raw_input(self, prompt: str = "") -> str:
        line = super().raw_input(prompt)
        if line.strip():
            self.history_fw.write(line)
            self.history_fw.write("\n")
            self.history_fw.flush()
        return line

    def showtraceback(self) -> None:
        if self.reraise:
            _, value, _ = sys.exc_info()
            if isinstance(value, AssertionError) or isinstance(value, SyntaxError):
                raise value

        return code.InteractiveConsole.showtraceback(self)


class ShellDict(dict):
    def __init__(self, wrapped: Dict[str, Any], prefix: str = "") -> None:
        super().__init__(wrapped)
        for key, value in wrapped.items():
            # let's replace invalid letters with _
            # e.g. ip-012345678 -> ip_012345678

            attr_safe_key = re.sub("[^a-zA-Z0-9_]", "_", prefix + key)
            if not attr_safe_key[0].isalpha():
                # default prefix if the user has something like job ids, which
                # are integers and can't be used as attributes
                attr_safe_key = "k_" + attr_safe_key
            setattr(self, attr_safe_key, value)


def shell(
    config: Dict,
    shell_locals: Dict[str, Any],
    script: Optional[str],
) -> None:
    """
    Provides read only interactive shell. type gehelp()
    in the shell for more information
    """
    banner = "\nScaleLib Shell"
    interpreter = ReraiseAssertionInterpreter(locals=shell_locals)
    try:
        __import__("readline")
        # some magic - create a completer that is bound to the locals in this interpreter and not
        # the __main__ interpreter.
        interpreter.push("import readline, rlcompleter")
        interpreter.push('readline.parse_and_bind("tab: complete")')
        interpreter.push("_completer = rlcompleter.Completer(locals())")
        interpreter.push("def _complete_helper(text, state):")
        interpreter.push("    ret = _completer.complete(text, state)")
        interpreter.push('    ret = ret + ")" if ret[-1] == "(" else ret')
        interpreter.push("    return ret")
        interpreter.push("")
        interpreter.push("readline.set_completer(_complete_helper)")
        for item in interpreter.history_lines:
            try:
                if '"""' in item:
                    interpreter.push(
                        "readline.add_history('''%s''')" % item.rstrip("\n")
                    )
                else:
                    interpreter.push(
                        'readline.add_history("""%s""")' % item.rstrip("\n")
                    )
            except Exception:
                pass

        interpreter.push("from hpc.autoscale.job.job import Job\n")
        interpreter.push("from hpc.autoscale import hpclogging as logging\n")

    except ImportError:
        banner += (
            "\nWARNING: `readline` is not installed, so autocomplete will not work."
        )

    if script:

        with open(script) as fr:
            source = fr.read()
            # important - if you pass in a separate globals dict then
            # any locally defined functions will be stored incorrectly
            # and you will get unknown func errors
            exec(source, shell_locals, shell_locals)
    else:
        interpreter.interact(banner=banner)


def disablecommand(f: Callable) -> Callable:
    setattr(f, "disabled", True)
    return f


class CommonCLI(ABC):
    def __init__(self, project_name: str) -> None:
        self.project_name = project_name
        self.example_nodes: List[Node] = []
        self.node_names: List[str] = []
        self.hostnames: List[str] = []
        self.__node_mgr: Optional[NodeManager] = None

    def connect(self, config: Dict) -> None:
        """Tests connection to CycleCloud"""
        self._node_mgr(config)

    @abstractmethod
    def _setup_shell_locals(self, config: Dict) -> Dict:
        ...

    def shell_parser(self, parser: ArgumentParser) -> None:
        parser.set_defaults(read_only=False)
        parser.add_argument("--script", "-s", required=False)

    def shell(self, config: Dict, script: Optional[str] = None) -> None:
        """
        Interactive python shell with relevant objects in local scope.
        Use --script to run python scripts
        """
        shell_locals = self._setup_shell_locals(config)

        for t in [Job, ht.Memory, ht.Size, Node, SchedulerNode, NodeBucket]:
            simple_name = t.__name__.split(".")[-1]
            if simple_name not in shell_locals:
                shell_locals[simple_name] = t

        if script and not os.path.exists(script):
            error("Script does not exist: %s", script)
        shell(config, shell_locals, script)

    @abstractmethod
    def _driver(self, config: Dict) -> SchedulerDriver:
        ...

    def _make_example_nodes(self, config: Dict, node_mgr: NodeManager) -> List[Node]:
        buckets = node_mgr.get_buckets()
        return [b.example_node for b in buckets]

    def _get_example_nodes(
        self, config: Union[List[Dict], Dict], force: bool = False
    ) -> List[Node]:
        if isinstance(config, str):
            with open(config) as fr:
                config = json.load(fr)
        if self.example_nodes:
            return self.example_nodes

        if isinstance(config, list):
            config = load_config(*config)

        driver = self._driver(config)
        cache_file = os.path.join(driver.autoscale_home, ".example_node_cache.json")

        if os.path.exists(cache_file) and not force:
            with open(cache_file) as fr:
                cache = json.load(fr)
            self.example_nodes = [Node.from_dict(x) for x in cache["example-nodes"]]
            self.node_names = cache["node-names"]
            self.hostnames = cache["hostnames"]
            self._read_completion_data(cache)
        else:
            node_mgr = self._node_mgr(config, driver)
            self.example_nodes = self._make_example_nodes(config, node_mgr)
            _, scheduler_nodes = driver._read_jobs_and_nodes(config)
            self.example_nodes.extend(scheduler_nodes)

            self.node_names = [n.name for n in node_mgr.get_nodes()]

            self.hostnames = [
                n.hostname for n in node_mgr.get_nodes() if n.hostname
            ] + [x.hostname for x in scheduler_nodes]

            with open(cache_file, "w") as fw:
                to_dump = {
                    "example-nodes": self.example_nodes,
                    "node-names": self.node_names,
                    "hostnames": self.hostnames,
                }
                self._add_completion_data(to_dump)

                json_dump(to_dump, fw)

        return self.example_nodes

    def _add_completion_data(self, completion_json: Dict) -> None:
        pass

    def _read_completion_data(self, completion_json: Dict) -> None:
        pass

    def _node_mgr(
        self,
        config: Dict,
        driver: Optional[SchedulerDriver] = None,
        force: bool = False,
    ) -> NodeManager:
        if force:
            self.__node_mgr = None

        if self.__node_mgr is not None:
            return self.__node_mgr
        driver = driver or self._driver(config)
        config = driver.preprocess_config(config)
        jobs, nodes = driver.read_jobs_and_nodes(config)
        node_mgr = new_node_manager(config, existing_nodes=nodes)
        driver.preprocess_node_mgr(config, node_mgr)
        self.__node_mgr = node_mgr
        return self.__node_mgr

    def _node_history(self, config: Dict) -> NodeHistory:
        return self._driver(config).new_node_history(config)

    def _demand_calc(
        self,
        config: Dict,
        driver: SchedulerDriver,
        node_mgr: Optional[NodeManager] = None,
    ) -> Tuple[DemandCalculator, List[Job]]:
        node_mgr = node_mgr or self._node_mgr(config, driver)
        node_history = self._node_history(config)
        driver = self._driver(config)

        jobs, scheduler_nodes = driver.read_jobs_and_nodes(config)

        dcalc = new_demand_calculator(
            config,
            node_mgr=node_mgr,
            node_history=node_history,
            node_queue=driver.new_node_queue(config),
            singleton_lock=driver.new_singleton_lock(config),
            existing_nodes=scheduler_nodes,
        )

        return dcalc, jobs

    def _demand(
        self,
        config: Dict,
        driver: Optional[SchedulerDriver] = None,
        ctx_handler: Optional[DefaultContextHandler] = None,
        node_mgr: Optional[NodeManager] = None,
    ) -> DemandCalculator:
        driver = driver or self._driver(config)
        if not ctx_handler:
            ctx_handler = self._ctx_handler(config)
            register_result_handler(ctx_handler)

        dcalc, jobs = self._demand_calc(config, driver, node_mgr)
        logging.info(
            "Calculating demand for %s jobs: %s", len(jobs), [j.name for j in jobs]
        )

        for job in jobs:
            ctx_handler.set_context("[Job {}]".format(job.name))
            logging.info("Adding %s", job)
            dcalc.add_job(job)

        demand = dcalc.get_demand()
        logging.info(
            "Done calculating demand. %s new nodes, %s unmatched nodes, %s nodes total",
            len(demand.new_nodes),
            len(demand.unmatched_nodes),
            len(demand.compute_nodes),
        )

        return dcalc

    def _ctx_handler(self, config: Dict) -> DefaultContextHandler:
        return DefaultContextHandler("[{}]".format(self.project_name))

    def autoscale_parser(self, parser: ArgumentParser) -> None:
        parser.set_defaults(read_only=False)
        self._add_output_format(parser)
        self._add_output_columns(parser)

    def autoscale(
        self,
        config: Dict,
        output_columns: Optional[List[str]],
        output_format: OutputFormat,
        dry_run: bool = False,
        long: bool = False,
    ) -> None:
        """End-to-end autoscale process, including creation, deletion and joining of nodes."""
        output_columns = output_columns or self._get_default_output_columns(config)

        if dry_run:
            logging.warning("Running gridengine autoscaler in dry run mode")
            # allow multiple instances
            config["lock_file"] = None
            # put in read only mode
            config["read_only"] = True

        ctx_handler = self._ctx_handler(config)

        register_result_handler(ctx_handler)

        driver = self._driver(config)
        driver.initialize()

        config = driver.preprocess_config(config)

        logging.debug("Driver = %s", driver)

        invalid_nodes: List[Node] = []

        jobs, scheduler_nodes = driver.read_jobs_and_nodes(config)

        for snode in scheduler_nodes:
            if snode.marked_for_deletion:
                invalid_nodes.append(snode)

        # nodes in error state must also be deleted

        nodes_to_delete = driver.handle_failed_nodes(invalid_nodes)

        node_mgr = self._node_mgr(config, driver)

        driver.validate_nodes(scheduler_nodes, node_mgr.get_nodes())

        demand_calculator = self._demand(config, driver, ctx_handler, node_mgr)

        failed_nodes = demand_calculator.node_mgr.get_failed_nodes()
        failed_nodes_to_delete = driver.handle_failed_nodes(failed_nodes)
        nodes_to_delete.extend(failed_nodes_to_delete)

        demand_result = demand_calculator.finish()

        if dry_run:
            demandprinter.print_demand(
                output_columns,
                demand_result,
                output_format=output_format,
                log=not dry_run,
                long=long,
            )
            return
        ctx_handler.set_context("[joining]")

        # details here are that we pass in nodes that matter (matched) and the driver figures out
        # which ones are new and need to be added via qconf
        joined = driver.add_nodes_to_cluster(
            [x for x in demand_result.compute_nodes if x.exists]
        )

        driver.handle_post_join_cluster(joined)

        ctx_handler.set_context("[scaling]")

        # bootup all nodes. Optionally pass in a filtered list
        if demand_result.new_nodes:
            # if not dry_run:
            result = demand_calculator.bootup()
            logging.info(result)

        # if not dry_run:
        demand_calculator.update_history()

        # we also tell the driver about nodes that are unmatched. It filters them out
        # and returns a list of ones we can delete.

        def idle_at_least(node: Node) -> float:
            return parse_idle_timeout(config, node)

        def booting_at_least(node: Node) -> float:
            return parse_boot_timeout(config, node)

        unmatched_for_5_mins = demand_calculator.find_unmatched_for(
            at_least=idle_at_least
        )
        timed_out_booting = demand_calculator.find_booting(at_least=booting_at_least)

        # I don't care about nodes that have keep_alive=true
        timed_out_booting = [n for n in timed_out_booting if not n.keep_alive]

        timed_out_to_deleted: List[Node] = []
        unmatched_nodes_to_delete: List[Node] = []

        if timed_out_booting:
            logging.info(
                "BootTimeout reached: %s",
                timed_out_booting,
            )
            timed_out_to_deleted = driver.handle_boot_timeout(timed_out_booting) or []

        if unmatched_for_5_mins:
            node_expr = ", ".join([str(x) for x in unmatched_for_5_mins])
            logging.info("IdleTimeout reached: %s", node_expr)
            unmatched_nodes_to_delete = (
                driver.handle_draining(unmatched_for_5_mins) or []
            )
        nodes_to_delete.extend(timed_out_to_deleted + unmatched_nodes_to_delete)
        can_not_delete_bc_assigned = [n for n in nodes_to_delete if n.assignments]
        nodes_to_delete = [n for n in nodes_to_delete if not n.assignments]
        for node in can_not_delete_bc_assigned:
            if node.assignments:
                logging.warning(
                    "%s has jobs assigned to it so we will take no action.", node
                )
                continue
            nodes_to_delete.append(node)

        if nodes_to_delete:
            try:
                logging.info("Deleting %s", [str(n) for n in nodes_to_delete])
                delete_result = demand_calculator.delete(nodes_to_delete)

                if delete_result:
                    # in case it has anything to do after a node is deleted (usually just remove it from the cluster)
                    driver.handle_post_delete(delete_result.nodes)
            except Exception as e:
                logging.warning("Deletion failed, will retry on next iteration: %s", e)
                logging.exception(str(e))

        demandprinter.print_demand(
            output_columns,
            demand_result,
            output_format=output_format,
            log=not dry_run,
            long=long,
        )

        try:
            self.refresh_autocomplete(config)
        except Exception as e:
            logging.error(
                "Ignoring error that occurred while updating autocomplete refresh: %s",
                e,
            )

        return demand_result

    def demand_parser(self, parser: ArgumentParser) -> None:
        self._add_output_format(parser)
        self._add_output_columns(parser)

    def demand(
        self,
        config: Dict,
        output_columns: Optional[List[str]],
        output_format: OutputFormat,
        long: bool = False,
    ) -> None:
        """Dry-run version of autoscale."""
        output_columns = output_columns or self._get_default_output_columns(config)
        self.autoscale(config, output_columns, output_format, dry_run=True, long=long)

    def jobs(self, config: Dict) -> None:
        """
        Writes out autoscale jobs as json. Note: Running jobs are excluded.
        """
        jobs, _ = self._driver(config).read_jobs_and_nodes(config)
        json.dump(
            jobs,
            sys.stdout,
            indent=2,
            default=lambda x: x.to_dict() if hasattr(x, "to_dict") else str(x),
        )

    def create_nodes_parser(self, parser: ArgumentParser) -> None:
        parser.set_defaults(read_only=False)
        parser.add_argument("-k", "--keep-alive", action="store_true", default=False)
        parser.add_argument("-n", "--nodes", type=int, default=-1)
        parser.add_argument("-s", "--slots", type=int, default=-1, required=False)

        parser.add_argument(  # type: ignore
            "-a", "--nodearray", type=str, required=False
        ).completer = self._nodearray_completer  # type: ignore

        parser.add_argument(  # type: ignore
            "-v", "--vm-size", type=str, required=False
        ).completer = self._vm_size_completer  # type: ignore

        parser.add_argument("-p", "--placement-group", type=str, required=False)
        parser.add_argument("--name-format", type=str, required=False)
        parser.add_argument(
            "-S", "--software-configuration", type=json_type, required=False
        )
        parser.add_argument(
            "-O", "--node-attribute-overrides", type=json_type, required=False
        )
        self._add_constraint_expr(parser)

        parser.add_argument(
            "-d", "--dry-run", action="store_true", default=False, required=False
        )
        parser.add_argument(
            "-x", "--exclusive", action="store_true", default=False, required=False
        )
        parser.add_argument(
            "-X", "--exclusive-task", action="store_true", default=False, required=False
        )
        self._add_output_format(parser)
        self._add_output_columns(parser)
        parser.add_argument("--strategy", "-t", type=str, choices=["pack", "scatter"])

    def create_nodes(
        self,
        config: Dict,
        nodes: int,
        slots: int,
        constraint_expr: List[str],
        nodearray: Optional[str],
        vm_size: Optional[str],
        placement_group: Optional[str],
        strategy: Optional[str],
        exclusive: bool,
        exclusive_task: bool,
        name_format: Optional[str],
        software_configuration: Optional[Dict],
        node_attribute_overrides: Optional[Dict],
        output_columns: Optional[List[str]],
        output_format: OutputFormat,
        long: bool = False,
        keep_alive: bool = False,
        dry_run: bool = False,
    ) -> None:
        """
        Create a set of nodes given various constraints. A CLI version of the nodemanager interface.
        """

        if nodes < 0 and slots < 0:
            nodes = 1

        if nodes > 0 and slots > 0:
            error("Please pick -n/--nodes or -s/--slots, but not both.")

        if not strategy:
            strategy = "pack" if slots > 0 else "scatter"

        if dry_run:
            config["lock_file"] = None
            config["read_only"] = True

        if placement_group:
            config["nodearrays"] = nodearrays = config.get("nodearrays", {})
            nodearrays[nodearray] = na = nodearrays.get(nodearray, {})
            na["placement_groups"] = pgs = na.get("placement_groups", [])
            if placement_group not in pgs:
                pgs.append(placement_group)

        cons_dict: Dict[str, Any] = {"node.exists": False}

        if exclusive_task:
            cons_dict["exclusive-task"] = True
        elif exclusive:
            cons_dict["exclusive"] = True

        if nodearray:
            cons_dict["node.nodearray"] = nodearray
        if vm_size:
            cons_dict["node.vm_size"] = vm_size

        # none is also a valid placement group
        cons_dict["node.placement_group"] = placement_group

        writer = io.StringIO()
        self.validate_constraint(config, constraint_expr, writer, quiet=True)
        validated_cons = json.loads(writer.getvalue())

        if not isinstance(validated_cons, list):
            validated_cons = [validated_cons]

        unparsed_cons = validated_cons + [cons_dict]

        parsed_cons = get_constraints(unparsed_cons)

        node_mgr = self._node_mgr(config)

        result = node_mgr.allocate(
            parsed_cons,
            node_count=nodes,
            slot_count=slots,
            allow_existing=False,
            assignment_id="create_nodes()",
        )

        if result:

            for node in result.nodes:
                node.name_format = name_format
                node.keep_alive = keep_alive

                if software_configuration:
                    node.node_attribute_overrides[
                        "Configuration"
                    ] = node.node_attribute_overrides.get("Configuration", {})
                    node.node_attribute_overrides["Configuration"].update(
                        software_configuration
                    )

                if node_attribute_overrides:
                    node.node_attribute_overrides.update(node_attribute_overrides)

            bootup_result = node_mgr.bootup()
            if bootup_result:
                # assert bootup_result.nodes

                demandprinter.print_demand(
                    columns=output_columns or self._get_default_output_columns(config),
                    demand_result=DemandResult(
                        bootup_result.nodes, bootup_result.nodes, [], []
                    ),
                    output_format=output_format,
                    long=long,
                )
                return
            else:
                error(str(bootup_result))
        else:
            error(str(result))

    def _node_name_completer(
        self,
        prefix: str,
        action: argparse.Action,
        parser: ArgumentParser,
        parsed_args: argparse.Namespace,
    ) -> List[str]:
        self._get_example_nodes(parsed_args.config)
        output_prefix = ""
        if prefix.endswith(","):
            output_prefix = prefix
        return [output_prefix + x + "," for x in self.node_names]

    def _hostname_completer(
        self,
        prefix: str,
        action: argparse.Action,
        parser: ArgumentParser,
        parsed_args: argparse.Namespace,
    ) -> List[str]:
        self._get_example_nodes(parsed_args.config)
        output_prefix = ""
        if "," in prefix:
            left, _right = prefix.rsplit(",", 1)
            output_prefix = left + ","
        return [output_prefix + x + "," for x in self.hostnames]

    def _nodearray_completer(
        self,
        prefix: str,
        action: argparse.Action,
        parser: ArgumentParser,
        parsed_args: argparse.Namespace,
    ) -> List[str]:
        self._get_example_nodes(parsed_args.config)
        return list(set([x.nodearray for x in self.example_nodes]))

    def _output_columns_completer(
        self,
        prefix: str,
        action: argparse.Action,
        parser: ArgumentParser,
        parsed_args: argparse.Namespace,
    ) -> List[str]:
        try:
            config = parsed_args.config
            if isinstance(config, list):
                config = load_config(*config)

            self._get_example_nodes(config)
            _print(action)
            _print(dir(action))
            _print(parser)
            _print(dir(parser))
            _print(parsed_args)
            _print(dir(parsed_args))
            cmd = None
            if hasattr(parsed_args, "cmd"):
                cmd = getattr(parsed_args, "cmd")

            default_output_columns = self._get_default_output_columns(config, cmd) + []
            for node in self.example_nodes:
                for res_name in node.resources:
                    if res_name not in default_output_columns:
                        default_output_columns.append(res_name)
                for meta_name in node.metadata:
                    if meta_name not in default_output_columns:
                        default_output_columns.append(meta_name)
                for prop in nodelib.QUERYABLE_PROPERTIES:
                    if prop not in default_output_columns:
                        default_output_columns.append(prop)

            output_prefix = ""

            if "," in prefix:
                rest_of_list = prefix[: prefix.rindex(",")]
                output_prefix = "{},".format(rest_of_list)

            return ["{}{},".format(output_prefix, x) for x in default_output_columns]
            # return ["{},".format(x) for x in default_output_columns]
        except Exception:
            traceback.print_exc(file=sys.__stderr__)
            raise

    def _vm_size_completer(
        self,
        prefix: str,
        action: argparse.Action,
        parser: ArgumentParser,
        parsed_args: argparse.Namespace,
    ) -> List[str]:

        try:
            self._get_example_nodes(parsed_args.config)
            filtered_nodes = self.example_nodes
            if hasattr(parsed_args, "nodearray") and parsed_args.nodearray:
                filtered_nodes = [
                    n
                    for n in self.example_nodes
                    if n.nodearray == parsed_args.nodearray
                ]
            return list(set([x.vm_size for x in filtered_nodes]))
        except Exception:
            import traceback

            _print(traceback.format_exc())
            raise

    def _all_vm_size_completer(
        self,
        prefix: str,
        action: argparse.Action,
        parser: ArgumentParser,
        parsed_args: argparse.Namespace,
    ) -> List[str]:

        try:
            from hpc.autoscale.node import vm_sizes as vmlib

            output_prefix = ""

            if "," in prefix:
                rest_of_list = prefix[: prefix.rindex(",")]
                output_prefix = "{},".format(rest_of_list)
            return [
                "{}{},".format(output_prefix, x) for x in vmlib.all_possible_vm_sizes()
            ]
        except Exception:
            import traceback

            _print(traceback.format_exc())
            raise

    def join_nodes_parser(self, parser: ArgumentParser) -> None:
        parser.set_defaults(read_only=False)
        parser.add_argument("--include-permanent", action="store_true", default=False)
        self._add_hostnames(parser)
        self._add_nodenames(parser)

    def join_nodes(
        self,
        config: Dict,
        hostnames: List[str],
        node_names: List[str],
        include_permanent: bool = False,
    ) -> None:
        """Adds selected nodes to the scheduler"""
        driver, demand_calc, nodes = self._find_nodes(config, hostnames, node_names)
        if include_permanent:
            self._node_history(config).unmark_ignored(nodes)
        joined_nodes = driver.add_nodes_to_cluster(nodes)
        print("Joined the following nodes:")
        for n in joined_nodes or []:
            print("   ", n)

    def retry_failed_nodes_parser(self, parser: ArgumentParser) -> None:
        parser.set_defaults(read_only=False)

    def retry_failed_nodes(self, config: Dict) -> None:
        """Retries all nodes in a failed state."""
        node_mgr = self._node_mgr(config)
        node_mgr.cluster_bindings.retry_failed_nodes()

    def remove_nodes_parser(self, parser: ArgumentParser) -> None:
        self._add_hostnames(parser)
        self._add_nodenames(parser)
        parser.add_argument("--force", action="store_true", default=False)
        parser.add_argument("--permanent", action="store_true", default=False)
        parser.set_defaults(read_only=False)

    def remove_nodes(
        self,
        config: Dict,
        hostnames: List[str],
        node_names: List[str],
        force: bool = False,
        permanent: bool = False,
    ) -> None:
        """Removes the node from the scheduler without terminating the actual instance."""
        self.delete_nodes(
            config,
            hostnames,
            node_names,
            do_delete=False,
            force=force,
            permanent=permanent,
        )

    def delete_nodes_parser(self, parser: ArgumentParser) -> None:
        self._add_hostnames(parser)
        self._add_nodenames(parser)
        parser.add_argument("--force", action="store_true", default=False)
        parser.set_defaults(read_only=False)

    def delete_nodes(
        self,
        config: Dict,
        hostnames: List[str],
        node_names: List[str],
        do_delete: bool = True,
        force: bool = False,
        permanent: bool = False,
    ) -> None:
        driver, demand_calc, nodes = self._find_nodes(config, hostnames, node_names)

        if not force:
            for node in nodes:
                if node.assignments:
                    error(
                        "%s is currently matched to one or more jobs (%s)."
                        + " Please specify --force to continue.",
                        node,
                        node.assignments,
                    )

                if node.keep_alive and do_delete:
                    error(
                        "%s is marked as KeepAlive=true. Please exclude this.",
                        node,
                    )

                if node.required:
                    error(
                        "%s is unmatched but is flagged as required."
                        + " Please specify --force to continue.",
                        node,
                    )

        drained_nodes = driver.handle_draining(nodes) or []
        print("Drained the following nodes that have joined {}:".format(driver.name))
        for n in drained_nodes:
            print("   ", n)

        if do_delete:
            result = demand_calc.delete(nodes)
            print("Deleting the following nodes:")
            for n in result.nodes or []:
                print("   ", n)

        removed_nodes = driver.handle_post_delete(nodes) or []
        print("Removed the following nodes from {}:".format(driver.name))
        for n in removed_nodes:
            print("   ", n)

        # This should only happen when do_delete=false, but as an extra asssertion
        if permanent and not do_delete:
            print(
                "Ignoring the removed nodes until join_nodes --include-permanent is called."
            )
            self._node_history(self.config).mark_ignored(removed_nodes)

    def default_output_columns_parser(self, parser: ArgumentParser) -> None:
        cmds = [
            x
            for x in dir(self)
            if x[0].isalpha() and hasattr(getattr(self, x), "__call__")
        ]
        parser.add_argument("-d", "--command", choices=cmds)

    def default_output_columns(
        self, config: Dict, command: Optional[str] = None
    ) -> None:
        """
        Output what are the default output columns for an optional command.
        """
        self._get_default_output_columns(config)
        def_cols = self._default_output_columns(config, command)
        sys.stdout.write("# cli option\n")
        sys.stdout.write("--output-columns {}\n".format(",".join(def_cols)))
        sys.stdout.write("# json snippet for autoscale.json\n")
        sys.stdout.write('"default-output-columns": ')
        arg_parser = create_arg_parser(self.project_name, self)

        output_columns = {} if command else {"default": def_cols}
        assert arg_parser._subparsers
        assert arg_parser._subparsers._actions

        for action in arg_parser._subparsers._actions:
            if not action.choices:
                continue

            choices: Dict[str, Any] = action.choices  # type: ignore
            for cmd_name, choice in choices.items():
                # if they specified a specific command, filter for it
                if command and cmd_name != command:
                    continue
                for action in choice._actions:
                    if "--output-columns" in action.option_strings:
                        output_columns[cmd_name] = self._default_output_columns(
                            config, cmd_name
                        )

        json.dump({"output-columns": output_columns}, sys.stdout, indent=2)

    @abstractmethod
    def _default_output_columns(
        self, config: Dict, cmd: Optional[str] = None
    ) -> List[str]:
        ...

    def _get_default_output_columns(
        self, config: Dict, cmd_name: Optional[str] = None
    ) -> List[str]:
        cmd_name = cmd_name or traceback.extract_stack()[-2].name

        cmd_specified = config.get("output-columns", {}).get(cmd_name)
        if cmd_specified:
            return cmd_specified

        default_specified = config.get("output-columns", {}).get("default")
        if default_specified:
            return default_specified

        default_cmd = self._default_output_columns(config, cmd_name)
        if default_cmd:
            return default_cmd

        return self._default_output_columns(config)

    def nodes_parser(self, parser: ArgumentParser) -> None:
        self._add_output_columns(parser)
        self._add_output_format(parser)
        self._add_constraint_expr(parser)

    def nodes(
        self,
        config: Dict,
        constraint_expr: List[str],
        output_columns: List[str],
        output_format: OutputFormat,
        long: bool = False,
    ) -> None:
        """Query nodes"""
        writer = io.StringIO()
        self.validate_constraint(
            config, constraint_expr, writer=io.StringIO(), quiet=True
        )
        validated_constraints = writer.getvalue()

        driver = self._driver(config)
        output_columns = output_columns or self._get_default_output_columns(config)
        demand_calc, _ = self._demand_calc(config, driver)

        filtered = _query_with_constraints(
            config, validated_constraints, demand_calc.node_mgr.get_nodes()
        )

        demand_result = DemandResult([], filtered, [], [])
        demandprinter.print_demand(
            output_columns,
            demand_result,
            output_format=output_format,
            long=long,
        )

    def buckets_parser(self, parser: ArgumentParser) -> None:
        self._add_output_columns(parser)
        self._add_output_format(parser)
        self._add_constraint_expr(parser)

    def buckets(
        self,
        config: Dict,
        constraint_expr: List[str],
        output_format: OutputFormat,
        long: bool = False,
        output_columns: Optional[List[str]] = None,
    ) -> None:
        """Prints out autoscale bucket information, like limits etc"""
        writer = io.StringIO()
        self.validate_constraint(config, constraint_expr, writer=writer, quiet=True)

        node_mgr = self._node_mgr(config)
        specified_output_columns = output_columns
        output_format = output_format or "table"

        output_columns = output_columns or [
            "nodearray",
            "placement_group",
            "vm_size",
            "vcpu_count",
            "pcpu_count",
            "memory",
            "available_count",
        ]

        if specified_output_columns is None:
            # fill in other columns
            for bucket in node_mgr.get_buckets():
                for resource_name in bucket.resources:
                    if resource_name not in output_columns:
                        if (
                            resource_name not in ["memkb", "memmb", "memtb", "memb"]
                            and resource_name not in output_columns
                        ):
                            output_columns.append(resource_name)

        for bucket in node_mgr.get_buckets():
            for attr in dir(bucket.limits):

                if attr[0].isalpha() and "count" in attr:
                    value = getattr(bucket.limits, attr)
                    if isinstance(value, int):
                        bucket.resources[attr] = value
                        bucket.example_node._resources[attr] = value

        filtered = _query_with_constraints(
            config, writer.getvalue(), node_mgr.get_buckets()
        )

        demand_result = DemandResult([], [f.example_node for f in filtered], [], [])

        config["output_columns"] = output_columns

        demandprinter.print_demand(
            output_columns,
            demand_result,
            output_format=output_format,
            long=long,
        )

    def limits_parser(self, parser: ArgumentParser) -> None:
        self._add_output_format(parser, default="json")

    def limits(
        self,
        config: Dict,
        output_format: OutputFormat,
        long: bool = False,
    ) -> None:
        f"""
        Writes a detailed set of limits for each "bucket". Defaults to json due to number of fields.
        """
        node_mgr = self._node_mgr(config)
        output_format = output_format or "json"
        
        output_columns = [
            "nodearray",
            "placement_group",
            "vm_size",
            "vm_family",
            "vcpu_count",
            "available_count",
        ]
        
        for bucket in node_mgr.get_buckets():

            for attr in dir(bucket.limits):

                if attr[0].isalpha() and "count" in attr:
                    value = getattr(bucket.limits, attr)
                    if isinstance(value, int):
                        bucket.resources[attr] = value
                        bucket.example_node._resources[attr] = value

            for resource_name in bucket.resources:
                if resource_name not in output_columns:
                    output_columns.append(resource_name)

        demand_result = DemandResult(
            [], [f.example_node for f in node_mgr.get_buckets()], [], []
        )

        demandprinter.print_demand(
            output_columns,
            demand_result,
            output_format=output_format,
            long=long,
        )

    def config_parser(self, parser: ArgumentParser) -> None:
        parser.set_defaults(read_only=False)

    def config(self, config: Dict, writer: TextIO = sys.stdout) -> None:
        """Writes the effective autoscale config, after any preprocessing, to stdout"""
        driver = self._driver(config)
        driver.preprocess_config(config)
        json.dump(config, writer, indent=2)

    def validate_constraint_parser(self, parser: ArgumentParser) -> None:
        self._add_constraint_expr(parser)

    def validate_constraint(
        self,
        config: Dict,
        constraint_expr: List[str],
        writer: TextIO = sys.stdout,
        quiet: bool = False,
    ) -> Union[List, Dict]:
        """
        Validates then outputs as json one or more constraints.
        """
        ret: List = []
        for expr in constraint_expr:

            value = json.loads(expr)

            if len(ret) == 0 or not isinstance(value, dict):
                ret.append(value)
            else:
                last = ret[-1]
                overlapped = set(value.keys()).intersection(set(last.keys()))

                if overlapped:
                    # there are conflicts, so just add a new dictionary
                    ret.append(value)
                else:
                    # no conflicts, just update the last dictionary
                    # and they will be anded together
                    last.update(value)

        as_cons = get_constraints(ret)

        if not quiet:
            if len(as_cons) == 1:
                # simple case - just a single dictionary
                json_dump(as_cons[0], writer)
            else:
                json_dump(as_cons, writer)

            writer.write("\n")

            for cons in as_cons:
                sys.stderr.write(str(cons))

        return ret

    def refresh_autocomplete(self, config: Dict) -> None:
        """Refreshes local autocomplete information for cluster specific resources and nodes."""
        self._get_example_nodes(config, force=True)

    def _find_nodes(
        self, config: Dict, hostnames: List[str], node_names: List[str]
    ) -> Tuple[SchedulerDriver, DemandCalculator, List[Node]]:
        hostnames = hostnames or []
        node_names = node_names or []

        driver = self._driver(config)

        demand_calc = self._demand(config, driver)
        demand_result = demand_calc.finish()

        if hostnames == ["*"] or node_names == ["*"]:
            return driver, demand_calc, demand_result.compute_nodes

        by_hostname = partition_single(
            demand_result.compute_nodes, lambda n: n.hostname_or_uuid.lower()
        )
        by_node_name = partition_single(
            demand_result.compute_nodes, lambda n: n.name.lower()
        )
        found_nodes = []
        for hostname in hostnames:
            if not hostname:
                error("Please specify a hostname")

            if hostname.lower() not in by_hostname:
                # it doesn't exist in CC, but we still want to delete it
                # from the cluster
                by_hostname[hostname.lower()] = SchedulerNode(hostname, {})

            found_nodes.append(by_hostname[hostname.lower()])

        for node_name in node_names:
            if not node_name:
                error("Please specify a node_name")

            if node_name.lower() not in by_node_name:
                error(
                    "Could not find a CycleCloud node that has node_name %s."
                    + " Run 'nodes' to see available nodes.",
                    node_name,
                )
            found_nodes.append(by_node_name[node_name.lower()])

        return driver, demand_calc, found_nodes

    @property
    def autoscale_home(self) -> str:
        if os.getenv("AUTOSCALE_HOME"):
            return os.environ["AUTOSCALE_HOME"]
        return os.path.join("/opt", "azurehpc", self.project_name)

    def initconfig_parser(self, parser: ArgumentParser) -> None:
        parser.add_argument("--cluster-name", required=True)
        parser.add_argument("--username", required=True)
        parser.add_argument("--password")
        parser.add_argument("--url", required=True)
        default_home = self.autoscale_home

        parser.add_argument(
            "--log-config",
            default=os.path.join(default_home, "logging.conf"),
            dest="logging__config_file",
        ).completer = default_completer  # type:ignore

        parser.add_argument(
            "--lock-file", default=os.path.join(default_home, "scalelib.lock")
        ).completer = default_completer  # type:ignore

        parser.add_argument(
            "--default-resource",
            type=json.loads,
            action="append",
            default=[],
            dest="default_resources",
        )

        parser.add_argument(
            "--idle-timeout", default=300, type=int, dest="idle_timeout"
        )
        parser.add_argument(
            "--boot-timeout", default=1800, type=int, dest="boot_timeout"
        )

        parser.add_argument(
            "--disable-default-resources",
            required=False,
            action="store_true",
            default=False,
            # help="Disables generation of default resources for ncpus,pcpus,ngpus,mem*b",
        )

        self._initconfig_parser(parser)

    @abstractmethod
    def _initconfig_parser(self, parser: ArgumentParser) -> None:
        ...

    def initconfig(self, writer: TextIO = sys.stdout, **config: Dict) -> None:
        """Creates an initial autoscale config. Writes to stdout"""
        self._initconfig(config)

        for key in list(config.keys()):

            if "__" in key:
                parent, child = key.split("__")
                if parent not in config:
                    config[parent] = {}
                config[parent][child] = config.pop(key)
        json.dump(config, writer, indent=2)

    @abstractmethod
    def _initconfig(self, config: Dict) -> None:
        ...

    def analyze_parser(self, parser: ArgumentParser) -> None:

        parser.add_argument("--job-id", "-j", required=True)
        parser.add_argument("--long", "-l", action="store_true", default=False)

    def analyze(
        self,
        config: Dict,
        job_id: str,
        long: bool = False,
    ) -> None:
        """
        Prints out relevant reasons that a job was not matched to any nodes.
        """
        if not long:
            try:
                _, columns_str = os.popen("stty size", "r").read().split()
            except Exception:
                columns_str = "120"
            columns = int(columns_str)
        else:
            columns = 2**31

        ctx_handler = DefaultContextHandler("[demand-cli]")

        register_result_handler(ctx_handler)
        dcalc = self._demand(config, ctx_handler=ctx_handler)

        found_nodes = []
        for node in dcalc.get_demand().compute_nodes:
            if job_id in node.assignments:
                found_nodes.append(node)

        if found_nodes:
            print("Job {} is assigned to the following nodes:".format(job_id))
            for node in found_nodes:
                print("   ", node)
            return

        if long:
            jobs, _ = self._driver(config).read_jobs_and_nodes(config)
            jobs = [x for x in jobs if x.name == job_id]
            if jobs:
                sys.stdout.write("Job {}:\n".format(jobs[0].name))
                json_dump(jobs[0].to_dict(), sys.stdout)
                sys.stdout.write("\n")

        key = "[Job {}]".format(job_id)
        if key not in ctx_handler.by_context:
            print("Unknown job id {}".format(job_id), file=sys.stderr)
            sys.exit(1)

        results = ctx_handler.by_context[key]
        for result in results:
            if isinstance(result, (EarlyBailoutResult, MatchResult)) and result:
                continue

            if not long and result.status == "CompoundFailure":
                continue

            if not result:

                whitespace = " " * max(1, 24 - len(result.status))
                message_lines = result.message.splitlines()
                if len(message_lines) > 1:
                    print()
                prefix = result.status + whitespace + ":"
                line_columns = max(20, columns - len(prefix) - 1)

                print(prefix, message_lines[0][:line_columns], end="")
                print()

                for line in message_lines[1:]:
                    print(" " * len(prefix), line[:line_columns], end="")
                    print()

    def _add_constraint_expr(self, parser: ArgumentParser) -> None:
        parser.add_argument(  # type: ignore
            "--constraint-expr",
            "-C",
            default=[],
            type=constraint_type,
            action="append",
        ).completer = self._constraint_completer  # type:ignore

    def _add_hostnames(self, parser: ArgumentParser) -> None:
        parser.add_argument(  # type: ignore
            "-H", "--hostnames", type=str_list, default=[]
        ).completer = self._hostname_completer  # type: ignore

    def _add_nodenames(self, parser: ArgumentParser) -> None:
        parser.add_argument(  # type: ignore
            "-N", "--node-names", type=str_list, default=[]
        ).completer = self._node_name_completer  # type: ignore

    def _add_output_columns(self, parser: ArgumentParser) -> None:
        parser.add_argument(  # type: ignore
            "--output-columns", "-o", type=str_list
        ).completer = self._output_columns_completer  # type: ignore
        parser.add_argument("--long", "-l", action="store_true", default=False)

    def _add_output_format(
        self, parser: ArgumentParser, default: OutputFormat = "table"
    ) -> None:
        parser.add_argument(
            "--output-format",
            "-F",
            default=default,
            type=parse_format,
            choices=["json", "table", "table_headerless"],
        )

    def _constraint_completer(
        self,
        prefix: str,
        action: argparse.Action,
        parser: ArgumentParser,
        parsed_args: argparse.Namespace,
    ) -> List[str]:
        try:
            return self.__constraint_completer(prefix, action, parser, parsed_args)
        except Exception as e:
            _print(str(e))

            traceback.print_exc(file=sys.__stderr__)
            raise

    def __constraint_completer(
        self,
        prefix: str,
        action: argparse.Action,
        parser: ArgumentParser,
        parsed_args: argparse.Namespace,
    ) -> List[str]:
        example_nodes = self._get_example_nodes(parsed_args.config)

        _print("prefix is", prefix)

        if "=" in prefix:
            default_values: List[str] = []

            def _convert_default_value(default_value: Any) -> List[str]:
                if isinstance(default_value, ht.Size):

                    if default_value.magnitude.lower()[0] in "bkm":
                        default_value = default_value.convert_to("g")

                    if isinstance(default_value, ht.Memory):
                        default_value = "memory::%s" % str(default_value)
                    else:
                        default_value = "size::%s" % str(default_value)

                elif isinstance(default_value, bool):
                    # to encourage use of json style bools and not python
                    # as you would do when defining things in autoscale.json
                    default_value = str(default_value).lower() == "true"
                    return [
                        str(default_value).lower(),
                        str(not default_value).lower(),
                    ]
                else:
                    _print("not size or bool", default_value)
                return [str(default_value)]

            if prefix.startswith("node."):
                attr = prefix[len("node.") :].split("=")[0]
                output_prefix = "node.{}=".format(attr)
                _print("output_prefix", output_prefix)
                if "," in prefix:
                    _print("output_prefix2", output_prefix)
                    rest_of_list = prefix[prefix.index("=") + 1 : prefix.rindex(",")]
                    output_prefix = "node.{}={},".format(attr, rest_of_list)

                if attr == "vm_size":
                    poss_vm_sizes = []

                    # let's use the example nodes' vm_sizes unless they are all unknown
                    if example_nodes:
                        known = [
                            x.vm_size for x in example_nodes if x.vm_size != "unknown"
                        ]
                        poss_vm_sizes = list(set(known))

                    if not poss_vm_sizes:
                        poss_vm_sizes = [
                            x
                            for x in vm_sizes.all_possible_vm_sizes()
                            if not x.startswith("Basic_")
                        ]

                    ret = [
                        output_prefix + x
                        for x in poss_vm_sizes
                        if x and not x.startswith("Basic")
                    ]

                    return ret

                if attr == "vm_family":
                    poss_vm_families = []

                    # same idea as vm_size
                    if example_nodes:
                        known = [
                            x.vm_family
                            for x in example_nodes
                            if x.vm_family != "unknown"
                        ]
                        poss_vm_families = list(set(known))

                    if not poss_vm_families:
                        poss_vm_families = [
                            x
                            for x in vm_sizes.all_possible_vm_families()
                            if x and not x.startswith("basic_")
                        ]
                    return [output_prefix + x for x in poss_vm_families]

                if attr == "location":
                    poss_locations = []

                    # same idea as vm_size
                    if example_nodes:
                        known = [
                            x.location for x in example_nodes if x.location != "unknown"
                        ]
                        poss_locations = list(set(known))

                    if not poss_locations:
                        poss_locations = [x for x in vm_sizes.all_possible_locations()]

                    return [output_prefix + x for x in poss_locations if x]

                _print("attr is", attr)

                for example_node in example_nodes:
                    if hasattr(example_node, attr):
                        _print("hasattr ", attr, str(getattr(example_node, attr)))
                        default_value = getattr(example_node, attr)
                        default_values.extend(_convert_default_value(default_value))
            else:
                res_name = prefix.split("=")[0]

                output_prefix = "{}=".format(res_name)

                if "," in prefix:
                    rest_of_list = prefix[prefix.index("=") + 1 : prefix.rindex(",")]
                    output_prefix = "{}={},".format(res_name, rest_of_list)

                for example_node in example_nodes:
                    if res_name in example_node.resources:
                        default_values.extend(
                            _convert_default_value(example_node.resources[res_name])
                        )

            _print("Returning", [output_prefix + x for x in default_values])
            return [output_prefix + x for x in default_values]

        from hpc.autoscale.node.node import QUERYABLE_PROPERTIES

        node_attrs = ["node.%s=" % x for x in QUERYABLE_PROPERTIES]

        resources = []
        for example_node in example_nodes:
            for expr in ["%s=" % x for x in example_node.resources.keys()]:
                if expr not in resources:
                    resources.append(expr)

        return sorted(node_attrs + resources)

    def _invoke_autocomplete(self, parser: ArgumentParser) -> None:
        try:
            import argcomplete

            argcomplete.autocomplete(
                parser, validator=fnmatch_validator, always_complete_options="long"
            )
        except ImportError:
            pass


NodeLike = typing.TypeVar("NodeLike", Node, NodeBucket)


def _query_with_constraints(
    config: Dict, constraint_expr: str, targets: List[NodeLike]
) -> List[NodeLike]:
    constraints = _parse_constraint(constraint_expr)

    filtered: List[NodeLike] = []
    append: NodeLike

    for targ in targets:
        satisfied = True
        for c in constraints:
            if isinstance(targ, Node):
                result = c.satisfied_by_node(targ)
            elif isinstance(targ, NodeBucket):
                result = c.satisfied_by_node(targ.example_node)
            else:
                raise TypeError(
                    "Expected Node or NodeBucket, got {}".format(type(targ))
                )
            if not result:
                satisfied = False
                logging.warning(result)
                break

        if satisfied:
            assert isinstance(targ, (Node, NodeBucket))
            filtered.append(targ)
    return filtered


def _parse_constraint(constraint_expr: str) -> List[NodeConstraint]:
    try:
        if constraint_expr:
            constraint_parsed = json.loads(constraint_expr)
        else:
            constraint_parsed = []
    except Exception as e:
        print(
            "Could not parse constraint as json '{}' - {}".format(constraint_expr, e),
            file=sys.stderr,
        )
        sys.exit(1)

    if not isinstance(constraint_parsed, list):
        constraint_parsed = [constraint_parsed]

    return get_constraints(constraint_parsed)


def default_completer(
    prefix: str,
    action: argparse.Action,
    parser: ArgumentParser,
    parsed_args: argparse.Namespace,
) -> List[str]:
    if isinstance(action.default, list):
        return action.default
    return [action.default]


def create_arg_parser(
    project_name: str, module: Any, default_config: Optional[str] = None
) -> ArgumentParser:
    parser = ArgumentParser()
    sub_parsers = parser.add_subparsers()

    def csv_list(x: str) -> List[str]:
        return [x.strip() for x in x.split(",")]

    help_msg = io.StringIO()

    default_install_dir = os.path.join("/", "opt", "azurehpc", project_name)
    if hasattr(module, "autoscale_dir"):
        default_install_dir = getattr(module, "autoscale_dir")

    def add_parser(
        name: str,
        func: Callable,
        read_only: bool = True,
        skip_config: bool = False,
        default_config: Optional[str] = None,
    ) -> ArgumentParser:
        doc_str = (func.__doc__ or "").strip()
        doc_str = " ".join([x.strip() for x in doc_str.splitlines()])
        help_msg.write("\n    {:20} - {}".format(name, doc_str))
        default_config = default_config or os.path.join(
            default_install_dir, "autoscale.json"
        )

        if not os.path.exists(default_config):
            default_config = None

        new_parser = sub_parsers.add_parser(name)
        new_parser.set_defaults(func=func, cmd=name, read_only=read_only)

        if skip_config:
            return new_parser

        new_parser.add_argument(
            "--config",
            "-c",
            default=default_config,
            required=not bool(default_config),
        ).completer = default_completer  # type: ignore

        # note this is true if you set the env variable
        # AZURE_HPC_DEV=1
        if hpcutil.AZURE_HPC_DEV:
            new_parser.add_argument(
                "--cluster-response",
                required=False,
                help="Path to a json file containing the response from a CycleCloud clusters/{cluster_name}/status call for debugging / reproduction.",
            )
            new_parser.add_argument(
                "--nodes-response",
                required=False,
                help="Path to a json file containing the response from a CycleCloud clusters/{cluster_name}/nodes call for debugging / reproduction." + 
                " Note that this is optional if --cluster-reponse is specified, but you must specify --cluster-response if you specify this",
            )
        return new_parser

    configure_parser_functions = {}
    for attr_name in dir(module):
        if attr_name[0].isalpha() and attr_name[0].lower():
            if attr_name.endswith("_parser"):
                cli_name = attr_name[: -len("_parser")]
                configure_parser_functions[cli_name] = getattr(module, attr_name)
            else:
                attr = getattr(module, attr_name)
                if hasattr(attr, "__call__"):

                    if hasattr(attr, "disabled") and getattr(attr, "disabled"):
                        continue

                    configure_parser_functions[attr_name] = lambda parser: 0

    for cli_name, ap_func in configure_parser_functions.items():
        func = getattr(module, cli_name)
        if hasattr(func, "disabled") and getattr(func, "disabled"):
            continue
        child_parser = add_parser(
            cli_name,
            func,
            skip_config=cli_name == "initconfig",
            default_config=default_config,
        )
        ap_func(child_parser)

    parser.usage = help_msg.getvalue()

    if hasattr(module, "_invoke_autocomplete"):
        module._invoke_autocomplete(parser)

    return parser


def main(
    argv: Iterable[str],
    project_name: str,
    module: Any,
    default_config: Optional[str] = None,
) -> None:
    parser = create_arg_parser(project_name, module, default_config)
    args = parser.parse_args(list(argv))

    if not hasattr(args, "func") or not hasattr(args, "cmd"):
        parser.print_help()
        sys.exit(1)

    # parse list of config paths to a single config
    if hasattr(args, "config"):
        args.config = load_config(args.config)

        # special handling for reproducing issues with reponses provided externally.
        if hasattr(args, "cluster_response"):
            if args.cluster_response:
                args.config["_mock_bindings"] = {
                    "name": "reproduce",
                    "cluster_response": args.cluster_response
                }
            if args.nodes_response:
                args.config["_mock_bindings"]["nodes_response"] = args.nodes_response
        logging.initialize_logging(args.config)

        # if applicable, set read_only/lock_file
        if args.read_only:
            args.config["read_only"] = True
            args.config["lock_file"] = None

    kwargs = {}
    for k in dir(args):
        if k[0].islower() and k not in ["read_only", "func", "cmd", "cluster_response", "nodes_response"]:
            kwargs[k] = getattr(args, k)

    if hasattr(module, "_initialize") and hasattr(args, "config"):
        getattr(module, "_initialize")(args.cmd, args.config)
    try:
        args.func(**kwargs)
    except AssertionError:
        raise
    except Exception as e:
        print("Error '%s': See the rest in the log file" % str(e), file=sys.stderr)
        if hasattr(e, "message"):
            print(getattr(e, "message"), file=sys.stderr)

        logging.debug("Full stacktrace", exc_info=sys.exc_info())
        sys.exit(1)


def fnmatch_validator(keyword_to_check_against: str, current_input: str) -> bool:
    if "*" not in current_input:
        current_input = current_input + "*"

    return fnmatch(keyword_to_check_against, current_input)
