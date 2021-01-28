#!/usr/bin/env bash


venv_bin=$(dirname $0)
default_config=${1:-$venv_bin/../../autoscale.json}

embedded_log_conf=$($venv_bin/python -c "import os, hpc.autoscale; print(os.path.join(os.path.dirname(hpc.autoscale.__file__), 'logging.conf'))")

log_conf=$(dirname $venv_bin)/logging.conf

if [ ! -e $log_conf ]; then
    cp $embedded_log_conf $log_conf
fi

cat > $venv_bin/azscale <<EOF
#!$venv_bin/python
import sys
from hpc.autoscale import cli

cli.main(sys.argv[1:], default_config="""${default_config}""")
EOF

cat > /etc/profile.d/azscale_autocomplete.sh <<EOF
  #!/usr/bin/env bash
  eval "\$( ${venv_bin}/register-python-argcomplete azscale)" || echo "Warning: Autocomplete is disabled for azscale" 1>&2
EOF
chmod +x $venv_bin/azscale