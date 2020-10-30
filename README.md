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
