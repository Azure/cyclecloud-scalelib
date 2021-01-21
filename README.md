# Azure  CycleCloud Autoscaling Library

The cyclecloud-scalelib project provides Python helpers to simplify autoscaler development for any scheduler in Azure using [Azure CycleCloud](https://docs.microsoft.com/en-us/azure/cyclecloud/overview?view=cyclecloud-8) and the [Azure CycleCloud REST API](https://docs.microsoft.com/en-us/azure/cyclecloud/api?view=cyclecloud-8) to orchestrate resource creation in Microsoft Azure.


## Autoscale Example

The primary use-case of this library is to facilitate and standardize scheduler autoscale integrations.
An example of such an integration with [_Celery_](https://github.com/celery/celery) is included [in this project](example-celery/README.md).

## Building the project

The cyclecloud-scalelib project is generally used in a Python 3 virtualenv and has several standard python dependencies, but it also depends on the [Azure CycleCloud Python Client Library](https://docs.microsoft.com/en-us/azure/cyclecloud/python-api?view=cyclecloud-8).

## Pre-requisites
The instructions below assume that:

* you have python 3 available on your system
* you have access to an Azure CycleCloud installation

Before attempting to build the project, obtain a copy of the Azure CycleCloud Python Client library.   You can get the wheel distribution from the `/opt/cycle_server/tools/` directory in your Azure CycleCloud installation or you can download the wheel from the CycleCloud UI following the instructions [here](https://docs.microsoft.com/en-us/azure/cyclecloud/python-api?view=cyclecloud-8).  

The instructions below assume that you have copied the cyclecloud-api.tar.gz to your working directory.

## Creating the virtualenv

```bash
    # If Cyclecloud is installed on the current machine:
    # cp /opt/cycle_server/tools/cyclecloud_api*.whl .

    python3 -m venv ~/.virtualenvs/autoscale/
    . ~/.virtualenvs/autoscale/bin/activate
    pip install -r ./dev-requirements.txt
    pip install ./cyclecloud_api*.whl
    python setup.py build
    pip install -e .
```

## Testing the project:

The project includes several helpers for contributors to validate, test and format changes to the code.

```bash
    # OPTIONAL: use the following to type check / reformat code
    python setup.py types
    python setup.py format
    python setup.py test
```


# Resources
# Node Properties

| Property | Type | Description |
| :---     | :--- | :---        |
| node.bucket_id | BucketId | UUID for this combination of NodeArray, VM Size and Placement Group |
| node.colocated | bool | Will this node be put into a VMSS with a placement group, allowing infiniband |
| node.cores_per_socket | int | CPU cores per CPU socket |
| node.create_time | datetime | When was this node created, as a datetime |
| node.create_time_remaining | float | How much time is remaining for this node to reach the `Ready` state |
| node.create_time_unix | float | When was this node created, in unix time |
| node.delete_time | datetime | When was this node deleted, as datetime |
| node.delete_time_unix | float | When was this node deleted, in unix time |
| node.exists | bool | Has this node actually been created in CycleCloud yet |
| node.gpu_count | int | GPU Count |
| node.hostname | Optional[Hostname] | Hostname for this node. May be `None` if the node has not been given one yet |
| node.hostname_or_uuid | Optional[Hostname] | Hostname or a UUID. Useful when partitioning a mixture of real and potential nodes by hostname |
| node.infiniband | bool | Does the VM Size of this node support infiniband |
| node.instance_id | Optional[InstanceId] | Azure VM Instance Id, if the node has a backing VM. |
| node.keep_alive | bool | Is this node protected by CycleCloud to prevent it from being terminated. |
| node.last_match_time | datetime | The last time this node was matched with a job, as datetime. |
| node.last_match_time_unix | float | The last time this node was matched with a job, in unix time. |
| node.location | Location | Azure location for this VM, e.g. `westus2` |
| node.memory | Memory | Amount of memory, as reported by the Azure API. OS reported memory will differ. |
| node.name | NodeName | Name of the node in CycleCloud, e.g. `execute-1` |
| node.nodearray | NodeArrayName | NodeArray name associated with this node, e.g. `execute` |
| node.pcpu_count | int | Physical CPU count |
| node.placement_group | Optional[PlacementGroup] | If set, this node is put into a VMSS where all nodes with the same placement group are tightly coupled |
| node.private_ip | Optional[IpAddress] | Private IP address of the node, if it has one. |
| node.spot | bool | If true, this node is taking advantage of unused capacity for a cheaper rate |
| node.state | NodeStatus | State of the node, as reported by CycleCloud. |
| node.vcpu_count | int | Virtual CPU Count |
| node.version | str | Internal version property to handle upgrades |
| node.vm_family | VMFamily | Azure VM Family of this node, e.g. `standardFFamily` |
| node.vm_size | VMSize | Azure VM Size of this node, e.g. `Standard_F2` |


# Constraints



## And


The logical 'and' operator ensures that all of its child constraints are met. 
```json
{"and": [{"ncpus": 1}, {"mem": "memory::4g"}]}
```
Note that and is implied when combining multiple resource definitions in the same
dictionary. e.g. the following have identical semantic meaning, the latter being
shorthand for the former.

```json
{"and": [{"ncpus": 1}, {"mem": "memory::4g"}]}
```
```json
{"ncpus": 1, "mem": "memory::4g"}
```
    


## ExclusiveNode


Defines whether, when allocating, if a node will exclusively run this job.
```json
{"exclusive": true}
```
-> One and only one iteration of the job can run on this node.
    
```json
{"exclusive-task": true}
```
-> One or more iterations of the same job can run on this node.
    


## InAPlacementGroup


Ensures that all nodes allocated will be in any placement group or not. Typically
this is most useful to prevent a job from being allocated to a node in a placement group.

```json
{"in-a-placement-group": true}
```
```json
{"in-a-placement-group": false}
```
    


## MinResourcePerNode


Filters for nodes that have at least a certain amount of a resource left to
allocate.

```json
{"ncpus": 1}
```
```json
{"mem": "memory::4g"}
```
```json
{"ngpus": 4}
```
Or, shorthand for combining the above into one expression
```json
{"ncpus": 1, "mem": "memory::4g", "ngpus": 4}
```
    


## Never


Rejects every node. Most useful when generating a complex node constraint
that cannot be determined to be satisfiable until it is generated.
For example, say a scheduler supports an 'excluded_users' list for scheduler specific
"projects". When constructing a set of constraints you may realize that this user will
never be able to run a job on a node with that project.
```json
{"or":
    [{"project": "open"},
     {"project": "restricted",
      "never": "User is denied access to this project"}
    ]
}
```
    


## NodePropertyConstraint


Similar to NodeResourceConstraint, but these are constraints based purely
on the read only node properties, i.e. those starting with 'node.'
```json
{"node.vm_size": ["Standard_F16", "Standard_E32"]}
```
```json
{"node.location": "westus2"}
```
```json
{"node.pcpu_count": 44}
```
Note that the last example does not allocate 44 node.pcpu_count, but simply
matches nodes that have a pcpu_count of exactly 44.
    


## NodeResourceConstraint


These are constraints that filter out which node is matched based on
read-only resources.

```json
{"custom_string1": "custom_value"}
```
```json
{"custom_string2": ["custom_value1", "custom_value2"]}
```
    
For read-only integers you can programmatically call NodeResourceConstraint("custom_int", 16)
or use a list of integers
    
```json
{"custom_integer": [16, 32]}
```

For shorthand, you can combine the above expressions
    
```json
{
    "custom_string1": "custom_value",
    "custom_string2": ["custom_value1", "custom_value2"],
    "custom_integer": [16, 32]
}
```
    


## Not


Logical 'not' operator negates the single child constraint. 

Only allocate machines with GPUs
```json
{"not": {"node.gpu_count": 0}}
```
Only allocate machines with no GPUs available
```json
{"not": {"ngpus": 1}}
```
    


## Or


Logical 'or' for matching a set of child constraints. Given a list of child constraints,
the first constraint that matches is the one used to decrement the node. No
further constraints are considered after the first child constraint has been satisfied.
For example, say we want to use a GPU instance if we can get a spot instance, otherwise
we want to use a non-spot CPU instance.
```json
{"or": [{"node.vm_size": "Standard_NC6", "node.spot": true},
        {"node.vm_size": "Standard_F16", "node.spot": false}]
}
```
    


## SharedConsumableConstraint


Represent a shared consumable resource, for example a queue
quota or number of licenses. Please use the SharedConsumableResource
object to represent this resource.

While there is a json representation of this object, it is up to the
author to create the SharedConsumableResources programmatically so
programmatic creation of this constraint is recommended.
```python
# global value
SHARED_RESOURCES = [SharedConsumableConstraint(resource_name="licenses",
                                               source="/path/to/license_limits",
                                               initial_value=1000,
                                               current_value=1000)]
    
def make_constraint(value: int) -> SharedConsumableConstraint:
    return SharedConsumableConstraint(SHARED_RESOURCES, value)
```
    


## SharedNonConsumableConstraint


Similar to a SharedConsumableConstraint, except that the resource that is shared
is not consumable (like a string etc). Please use the SharedNonConsumableResource
object to represent this resource.

While there is a json representation of this object, it is up to the
author to create the SharedNonConsumableResource programmatically so
programmatic creation of this constraint is recommended.
```python
# global value
SHARED_RESOURCES = [SharedNonConsumableResource(resource_name="prodversion",
                                                source="/path/to/prod_version",
                                                current_value="1.2.3")]
    
def make_constraint(value: str) -> SharedConstraint:
    return SharedConstraint(SHARED_RESOURCES, value)
```
    


## XOr


Similar to the or operation, however one and only one of the child constraints may satisfy
the node. Here is a trivial example where we have a failover for allocating to a second
region but we ensure that only one of them is valid at a time.
```json
{"xor": [{"node.location": "westus2"},
         {"node.location": "eastus"}]
}
```
    

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
