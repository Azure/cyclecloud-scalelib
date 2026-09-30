#!/usr/bin/env bash
python3 -m venv ~/.virtualenvs/mockcs
source ~/.virtualenvs/mockcs/bin/activate
pip install -r dev-requirements.txt
mkdir -p generated
