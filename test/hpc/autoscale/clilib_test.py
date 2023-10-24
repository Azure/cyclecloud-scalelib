import io
import json
import os
import shutil
import sys
import tempfile
from typing import Dict, List

import pytest

from hpc.autoscale import cli
from hpc.autoscale import hpclogging as logging


class TempDir:
    def __enter__(self) -> str:
        self.path = tempfile.mkdtemp()
        return self.path

    def __exit__(self, *exc_info) -> None:
        if hasattr(self, "path") and os.path.exists(self.path):
            shutil.rmtree(self.path)


@pytest.mark.skip
def test_add_default_resource() -> None:
    with TempDir() as tempdir:
        config_path = os.path.join(tempdir, "config.json")
        with open(config_path, "w") as f:
            json.dump({}, f)

        def _run_test(name: str, value: str, *additional_args: str) -> List[Dict]:
            args = [
                "add_default_resource",
                "--name",
                name,
                "--value",
                value,
                "--config",
                config_path,
            ] + list(additional_args)

            cli.main(args)
            with open(config_path) as fr:
                return json.load(fr)["default_resources"]

        assert _run_test("foo", "bar") == [
            {"name": "foo", "value": "bar", "select": {}}
        ]

        assert _run_test(
            "foo", "bar2", "--constraint-expr", "node.nodearray=n2", "--prepend"
        ) == [
            {"name": "foo", "value": "bar2", "select": {"node.nodearray": ["n2"]}},
            {"name": "foo", "value": "bar", "select": {}},
        ]

        assert _run_test("fooi", "7") == [
            {"name": "foo", "value": "bar2", "select": {"node.nodearray": ["n2"]}},
            {"name": "foo", "value": "bar", "select": {}},
            {"name": "fooi", "value": 7, "select": {}},
        ]

        with open(config_path, "w") as f:
            json.dump({}, f)

        assert _run_test("foob", "true") == [
            {"name": "foob", "value": True, "select": {}}
        ]

        with open(config_path, "w") as f:
            json.dump({}, f)

        assert _run_test("foof", "4.2") == [
            {"name": "foof", "value": 4.2, "select": {}}
        ]
