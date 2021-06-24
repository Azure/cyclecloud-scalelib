#!/bin/bash
set -e
apt-get install -y rabbitmq-server
service rabbitmq-server start

source /root/.venv/celery/bin/activate

cs_hostname=$(jetpack config cyclecloud.config.web_server)
CC_VERSION="8.0.2"
FILE=cyclecloud-api.tar.gz
if ! [ -f "$FILE" ]; then
    wget --no-check-certificate ${cs_hostname}/static/tools/cyclecloud_api-${CC_VERSION}-py2.py3-none-any.whl
    pip install cyclecloud_api-${CC_VERSION}-py2.py3-none-any.whl
fi

jetpack download cyclecloud-scalelib-0.1.5.tar.gz $CYCLECLOUD_SPEC_PATH/files/
pip install $CYCLECLOUD_SPEC_PATH/files/cyclecloud-scalelib-0.1.5.tar.gz

cp $CYCLECLOUD_SPEC_PATH/files/add_task.py /root/
cp $CYCLECLOUD_SPEC_PATH/files/autoscale.py /root/
cp $CYCLECLOUD_SPEC_PATH/files/check_tasks.py /root/

jetpack config cyclecloud.config --json > /root/cyclecloud.config.json
jetpack config cyclecloud.cluster.name --json > /root/cyclecloud.cluster.name.json
