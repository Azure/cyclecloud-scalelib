#!/usr/bin/env bash
./build.sh

cp generated/cluster_service_plugin.py $CS_HOME/plugins/
cat >$CS_HOME/plugins/cluster_service_plugin.cfg<<EOF
WebContent=dynamic
UriPatterns="/clusterservice/{function}"
EOF

cp generated/cluster_service_client.py ../src/hpc/autoscale/ccbindings/cluster_service_client.py

