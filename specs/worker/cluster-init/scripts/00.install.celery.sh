#!/bin/bash
set -e
apt-get install -y python3-venv
python3 -m venv /root/.venv/celery
source /root/.venv/celery/bin/activate
pip install celery