import json
import logging
import os
import sys
import socket
import pdb

from hpc.autoscale.job import demandcalculator
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.schedulernode import SchedulerNode
from hpc.autoscale.node.nodehistory import SQLiteNodeHistory
from hpc.autoscale.node.nodemanager import new_node_manager
from hpc.autoscale.job.demandprinter import print_demand
from hpc.autoscale.util import json_load

import pdb

class celery_driver():
    def __init__(self):
        self.jobs = []
        self.scheduler_nodes = []

def c_strip(_host):
    return _host.split("@")[1]

def job_buffer(n=1):
    jobs = []
    for i in range(n):
        jobs.append(Job(name="pad-%s" % i, constraints={"ncpus":1}))
    return jobs

def celery_status():
    from celery import Celery
    app = Celery()
    appc = app.control.inspect()
    celery_d = celery_driver()
    celery_d.jobs = []
    master_name = socket.gethostname()
    nodes = set()
    i = 0
    for arr in (arr for arr in [appc.active(), appc.reserved()] if arr != None):
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
    CONFIG = json_load("/opt/cycle/scalelib/autoscale.json")

    MIN_CORE_COUNT = 4
    WARM_BUFFER = 2

    # Get hosts / tasks
    celery_d = celery_status()

    dcalc = demandcalculator.new_demand_calculator(CONFIG,
                    existing_nodes=celery_d.scheduler_nodes,
                    node_history=SQLiteNodeHistory())

    dcalc.add_jobs(celery_d.jobs)
    n_jobs = len(celery_d.jobs)
    n_add_jobs = max(n_jobs + WARM_BUFFER, max(n_jobs, MIN_CORE_COUNT))
    if n_add_jobs > 0:
        # RIGHT-SIZE based on Min Count and Buffer
        # It's possible that the padded jobs will float around extending the timer
        # but it seems like they're placed in some kind of normal order that's
        # preserved across autoscale runs
        print("add padding of %d jobs, to existing %d" % (n_add_jobs, n_jobs))
        dcalc.add_jobs(job_buffer(n_add_jobs))

    demand_result = dcalc.finish()
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

    print_demand(output_columns, demand_result)
    dcalc.bootup()
    dcalc.update_history()
    delete_result = dcalc.find_unmatched_for(at_least=180)
    if delete_result:
        try:
            dcalc.delete(delete_result)
        except Exception as e:
            _exit_code = 1
            logging.warning("Deletion failed, will retry on next iteration: %s", e)
            logging.exception(str(e))


if __name__ == "__main__":
    auto()
