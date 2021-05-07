import os
import sys
from argparse import ArgumentParser
from typing import Any, Dict, Iterable, List, Optional, Tuple

from hpc.autoscale import clilib
from hpc.autoscale.clilib import CommonCLI, ShellDict, disablecommand
from hpc.autoscale.job.demandprinter import OutputFormat
from hpc.autoscale.job.driver import SchedulerDriver
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.node import Node
from hpc.autoscale.results import DefaultContextHandler
from hpc.autoscale.util import partition, partition_single


class GenericDriver(SchedulerDriver):
    def initialize(self) -> None:
        pass

    def preprocess_config(self, config: Dict) -> Dict:
        return config

    def add_nodes_to_cluster(self, nodes: List[Node]) -> List[Node]:
        return nodes

    def handle_boot_timeout(self, nodes: List[Node]) -> List[Node]:
        return nodes

    def handle_draining(self, nodes: List[Node]) -> List[Node]:
        return nodes

    def handle_failed_nodes(self, nodes: List[Node]) -> List[Node]:
        return nodes

    def handle_post_delete(self, nodes: List[Node]) -> List[Node]:
        return nodes

    def handle_post_join_cluster(self, nodes: List[Node]) -> List[Node]:
        return nodes

    def _read_jobs_and_nodes(
        self, config: Dict
    ) -> Tuple[List[Job], List[SchedulerNode]]:
        return ([], [])


class ScaleLibCLI(CommonCLI):
    @disablecommand
    def jobs(self, config: Dict) -> None:
        return super().jobs(config)

    @disablecommand
    def autoscale(
        self,
        config: Dict,
        output_columns: Optional[List[str]],
        output_format: OutputFormat,
    ) -> None:
        return super().autoscale(config, output_columns, output_format)

    @disablecommand
    def demand(
        self,
        config: Dict,
        output_columns: Optional[List[str]],
        output_format: OutputFormat,
    ) -> None:
        return super().demand(config, output_columns, output_format)

    @disablecommand
    def join_nodes(
        self, config: Dict, hostnames: List[str], node_names: List[str]
    ) -> None:
        return super().join_nodes(config, hostnames, node_names)

    @disablecommand
    def remove_nodes(
        self, config: Dict, hostnames: List[str], node_names: List[str], force: bool
    ) -> None:
        return super().remove_nodes(config, hostnames, node_names, force=force)

    def _initconfig_parser(self, parser: ArgumentParser) -> None:
        pass

    def _initconfig(self, config: Dict) -> None:
        pass

    def _default_output_columns(
        self, config: Dict, cmd: Optional[str] = None
    ) -> List[str]:
        return [
            "name",
            "hostname",
            "instance_id",
            "ccnodeid",
            "required",
            "job_ids",
            "state",
            "ctr@create_time_remaining",
            "itr@idle_time_remaining",
        ]

    def _driver(self, config: Dict) -> SchedulerDriver:
        return GenericDriver(self.project_name)

    def _setup_shell_locals(self, config: Dict) -> Dict:
        ctx = DefaultContextHandler("[interactive-readonly]")

        driver = self._driver(config)
        dcalc, jobs_list = self._demand_calc(config, driver)
        nodes_list = dcalc.node_mgr.get_nodes()
        for node in nodes_list:
            node.shellify()
        nodes = partition_single(nodes_list, lambda n: n.name)
        nodes.update(
            partition_single(
                [x for x in nodes_list if x.hostname], lambda n: n.hostname
            )
        )
        jobs: Dict[str, Any]
        try:
            jobs = partition_single(jobs_list, lambda j: j.name)
        except Exception:
            jobs = partition(jobs_list, lambda j: j.name)

        return {
            "config": config,
            "cli": self,
            "ctx": ctx,
            "demand_calc": dcalc,
            "node_mgr": dcalc.node_mgr,
            "jobs": ShellDict(jobs),
            "nodes": ShellDict(nodes),
        }


def main(argv: Iterable[str], default_config: Optional[str] = None) -> None:
    clilib.main(
        argv, "scalelib", ScaleLibCLI("scalelib"), default_config=default_config
    )


if __name__ == "__main__":
    main(sys.argv[1 if os.path.isfile(sys.argv[0]) else 0 :])
