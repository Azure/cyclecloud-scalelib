import json
import logging
import os
import sys
import socket

from hpc.autoscale.job import demandcalculator
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.node.nodehistory import SQLiteNodeHistory
from hpc.autoscale.node.nodemanager import new_node_manager
from hpc.autoscale.job.demandprinter import print_demand

import pdb

class celery_driver():
    def __init__(self):
        self.jobs = []
        self.scheduler_nodes = []

def c_strip(_host):
    return _host.split("@")[1]

def celery_status():
    from celery import Celery
    app = Celery()
    appc = app.control.inspect()
    celery_d = celery_driver()
    celery_d.jobs = []
    master_name = socket.gethostname()
    nodes = set()
    i = 0
    for arr in [appc.active(), appc.reserved()]:
        i += 1
        for k, v in arr.items():
            on_master = False
            if c_strip(k) == master_name:
                on_master = True
            nodes.add(c_strip(k))
            for _job in v:
                print(_job)
                if i == 1 and not on_master:
                    job = Job(name=_job['id'], constraints={"ncpus":1},executing_hostnames=[c_strip(_job['hostname'])])
                else:
                    job = Job(name=_job['id'], constraints={"ncpus":1})
                celery_d.jobs.append(job)

    celery_d.scheduler_nodes = [SchedulerNode(hostname=x) for x in list(nodes)]
    return celery_d

def auto():

    logging.basicConfig(
        format="%(asctime)-15s: %(levelname)s %(message)s",
        stream=sys.stderr,
        level=logging.DEBUG,
    )

    CONFIG = {
        "cluster_name": "celery2",
        "url": "https://127.0.0.1:37140",
        "username": "mirequa",
        "password": "cycl3R0cks!",
    }

    celery_d = celery_status()

    node_mgr = new_node_manager(CONFIG)
    demand_calculator = demandcalculator.new_demand_calculator(CONFIG, 
                    existing_nodes=celery_d.scheduler_nodes, 
                    node_history=SQLiteNodeHistory())
    demand_calculator.add_jobs(celery_d.jobs)

    # Define consumable resource "ncpus" initial value is vcpu_count
    #node_mgr.add_default_resource({}, "ncpus", "node.vcpu_count")

    #  [bucket.vcpu_count for bucket in node_mgr.get_buckets()]
    #demand_calculator.add_job(Job("x1", {"ncpus": 1}, iterations=10))
    # result = node_mgr.allocate({"node.nodearray": "worker", "ncpus": 1}, slot_count=2)
    demand_result = demand_calculator.finish()
    output_columns = [
    "name",
    "hostname",
    "job_ids",
    "required",
    "slots",
    "vm_size",
    "vcpu_count",
    "state"
    ]
    print("jobs=%s" % celery_d.jobs)
    
    print_demand(output_columns, demand_result)
    demand_calculator.bootup()
    delete_result = demand_calculator.find_unmatched_for(at_least=180)
    if delete_result:
        try:
            demand_calculator.delete(delete_result)
        except Exception as e:
            _exit_code = 1
            logging.warning("Deletion failed, will retry on next iteration: %s", e)
            logging.exception(str(e))


if __name__ == "__main__":
    auto()