#!/bin/bash
set -e
apt-get install -y rabbitmq-server
service rabbitmq-server start

INSTALLDIR=/opt/cycle/scalelib
source $INSTALLDIR/venv/bin/activate

cs_hostname=$(jetpack config cyclecloud.config.web_server)
CC_VERSION=$(jetpack config cyclecloud.cookbooks.version)

wget --no-check-certificate ${cs_hostname}/static/tools/cyclecloud_api-${CC_VERSION}-py2.py3-none-any.whl
pip install cyclecloud_api-${CC_VERSION}-py2.py3-none-any.whl

jetpack download --project celery-autoscale-demo cyclecloud-scalelib-1.0.3.tar.gz $CYCLECLOUD_SPEC_PATH/files/
pip install $CYCLECLOUD_SPEC_PATH/files/cyclecloud-scalelib-1.0.3.tar.gz

LOGGING_CONF=$(python -c 'import hpc.autoscale, os; print(os.path.abspath(os.path.join(hpc.autoscale.__file__, "..", "logging.conf")))')
cp $LOGGING_CONF $INSTALLDIR
cp $CYCLECLOUD_SPEC_PATH/files/add_task.py /root/
cp $CYCLECLOUD_SPEC_PATH/files/autoscale.py /root/
cp $CYCLECLOUD_SPEC_PATH/files/check_tasks.py /root/

python -m hpc.autoscale.cli initconfig \
                  --cluster-name $(jetpack config cyclecloud.cluster.name) \
                  --username     $(jetpack config cyclecloud.config.username) \
                  --password     $(jetpack config cyclecloud.config.password) \
                  --url          $(jetpack config cyclecloud.config.web_server) \
                  --lock-file    $INSTALLDIR/scalelib.lock \
                  --log-config   $INSTALLDIR/logging.conf \
                  --default-resource '{"select": {}, "name": "ncpus", "value": "node.vcpu_count"}' \
                  --idle-timeout 300 \
                  --boot-timeout 1800 > \
                  $INSTALLDIR/autoscale.json 
