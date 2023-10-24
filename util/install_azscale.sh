#!/usr/bin/env bash

source /opt/azure/scalelib/venv/bin/activate
python3 -m hpc.autoscale.cliinstall --cli azscale --cls hpc.autoscale.cli
