#!/usr/bin/env bash
export CLUSTER_SERVICE_DIR=~/code/ClusterService
. ./init.sh
source ~/.virtualenvs/mockcs/bin/activate
python generate_python.py
PY3=1 python generate_python.py
