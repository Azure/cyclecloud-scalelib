#!/bin/bash
set -e
set -x

apt-get install -y python3
apt-get install -y python3-pip
apt-get install -y python3-venv

INSTALLDIR=/opt/cycle/scalelib

if [ -e $INSTALLDIR ]; then
  mkdir -p $INSTALLDIR 
fi

/usr/bin/python3 -m venv $INSTALLDIR/venv/
source $INSTALLDIR/venv/bin/activate
pip3 install celery