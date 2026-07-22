# Integration with Celery

_Celery_ is a python-based distributed task queues

## An Autoscale Routine

A very simple autoscaler that integrates _Celery_ with
_scalelib_ can be written in [single file](specs/broker/cluster-init/files/autoscale.py).
It's the intention that this can be extended to
many other HPC schedulers for use with CycleCloud.
We refer to code which integrating with the scheduler
as the scheduler driver.

The autoscale routine is meant to be (nearly) stateless
and run on a regular scheduler. Each iteration would:

1. [driver] Collect host and job information
1. Add jobs to the Demand Calculator, calculate demand
1. [driver] Add new hosts to the cluster
1. Launch new hosts as recommended by the Demand Calculator
1. [driver] Disable or delete hosts with no existing demand

_Scalelib_ maintains a list of cluster nodes managed by
CycleCloud which can act as the ground-truth for the
scheduler driver to add or remove nodes as
part of their normal autoscale lifecycle.

### Jobs and the Demand Calculator

In the example autoscale script, see the code that collects the jobs
from _Celery_ and supplies them to the demand calculator.

```python
celery_d = celery_status()
dcalc = demandcalculator.new_demand_calculator(CONFIG,
                existing_nodes=celery_d.scheduler_nodes,
                node_history=SQLiteNodeHistory())
dcalc.add_jobs(celery_d.jobs)
```

The _Job_ object specifies the constraints. In this simple example we
set a single constraint, `ncpus=`. This simple constraint says that
the job needs only one resource, a single cpu.

```python
job = Job(name=_job['id'], constraints={"ncpus":1})
celery_d.jobs.append(job)
```

Other resources are available by default for job constraints

| Default Constraint | Description  |
|---|---|
| ncpus | virtual CPUs  |
| pcpus  | physical (non-hyperthreaded) CPUs  |
| ngpus  | physical GPUs  |
| memmb | Memory in MB |
| memgb | Memory in GB |
| memtb | Memory in TB |

And all these constraints can be used simultaneously e.g.
 `constraints={"ncpus":1, "memgb":200, "ngpus":1}`. Scalelib will
 consider all these constraints in combination

#### Adding a new default resource

TODO:
How to expose node capabilities as a resource to constrain.

### Additional Job Requirements

| Name | Type | Description  |
|---|---|---|
| node_count  | Int | Reserve the constraint envelope accros multiple nodes |
| colocated  | Boolean  | If colocated, use a single placement group for the nodes which host the job.  |
| exclusive | Boolean  | If true, reserve the entire node even if there are unused resources on the node.

Then the constrain can use any and all of these in any combination. So that the job `Job("my-job", constraints={"ncpus":1, "memgb":200, "ngpus":1}, node_count=2, colacated=True, exclusive=True)` will exclusively reserve 2 nodes which meet the minimum constraints and will create them
within a single placement group.

### Demand Result

_scalelib_ will calculate the appropriate nodes to run the supplied jobs.
The demand result can be printed into a human-readable log:

```txt
print_demand(output_columns, demand_result)
NAME        HOSTNAME    JOB_IDS     REQUIRED SLOTS VM_SIZE         VCPU_COUNT STATE
worker-1    ip-0A050005 pad-1,pad-0 True           Standard_F2s_v2 2          Ready
worker-2    ip-0A050008 pad-3,pad-2 True           Standard_F2s_v2 2          Ready
ip-0A050010 ip-0A050010             False          unknown         1          running
```

### Scale-down behavior

The only stateful aspect of the _scalelib_ is the built-in tracker of idle nodes.
Jobs can be assigned one or more execution hosts.
When the demand calculator processes jobs that include a `executing_hostnames` attribute
the node will be marked as active and the idle timer will be reset.

The demand calculator allows filtering of the nodes
by how long they've been idle, making a useful function
to determine which nodes to delete.

## Prepare the cluster

This section contains a demo of an autoscaling _Celery_
cluster. The rest of the instructions describe setting up and use.

### Staging the libraries

Build the distribution of this library, and copy it to
_Celery_ cluster
by running these commands in the top-level directory.

```bash
python3 setup.py sdist
cp dist/* example-celery/blobs
cd example-celery
cyclecloud project upload
```
The distributable will be staged into the blobs directory
as well as the _CycleCloud_ locker.

### Launch the Cluster

The cluster template for the project is [here](templates/celery.txt). Follow the documentation for [importing a cluster](https://docs.microsoft.com/azure/cyclecloud/how-to/create-cluster?view=cyclecloud-7#importing-a-cluster-template).

Once running, the _/root_ directory has all the scripts to operate
the cluster and activate autoscaling.  Before running the scripts however, you must start a celery worker to add tasks to.

```bash
source /opt/cycle/scalelib/venv/celery/bin/activate
celery -A tasks worker -D
```
Once celery is running, the scripts below can be run in any order to add work,
scale to meet demand, and check on the status of submitted tasks.

```bash
python add_task.py
python autoscale.py
python check_tasks.py
```

### Testing
Scalelib supports integration testing through standard pytests.  Example tests can be found in _/root_ and all tests (on any role) can be run using `jetpack test`.
