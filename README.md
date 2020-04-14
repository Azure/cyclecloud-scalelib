Building:
    virtualenv ~/.virtualenvs/autoscale
    source ~/.virtualenvs/autoscale/bin/activate
    pip install dev-requirements.txt
    pip install path/to/cyclecloud-api.tar.gz
    python setup.py build
    # use the following to type check / reformat code
    python setup.py types / python setup.py format
    python setup.py test
    