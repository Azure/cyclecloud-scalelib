#!/bin/bash
INSTALLDIR=/opt/cycle/scalelib
BROKER_IP=$(jetpack config celery.broker.privateip localhost)
sed  "s/__localhost__/$BROKER_IP/g" $CYCLECLOUD_SPEC_PATH/files/app.py > /root/tasks.py  
source $INSTALLDIR/venv/bin/activate

pushd /root
celery -A tasks -O fair worker --loglevel=info 2>&1 > /var/log/celery.worker.log &
popd