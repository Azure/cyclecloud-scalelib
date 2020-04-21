#!/bin/bash

yum install -y rabbitmq-server
service rabbitmq-server start

source /root/.venv/celery/bin/activate

cs_hostname=$(jetpack config cyclecloud.config.web_server)
FILE=cyclecloud-api.tar.gz
if ! [ -f "$FILE" ]; then
    wget --no-check-certificate ${cs_hostname}/download/tools/cyclecloud-api.tar.gz
    pip install cyclecloud-api.tar.gz
fi

pip install $CYCLECLOUD_SPEC_PATH/files/autoscale.tgz
