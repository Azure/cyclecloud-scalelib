#!/bin/bash
yum install -y python3
python3 -m venv /root/.venv/celery
source /root/.venv/celery/bin/activate
pip install celery