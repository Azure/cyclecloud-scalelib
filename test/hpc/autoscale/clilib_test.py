import io
import json
from pathlib import Path
from typing import Dict, Optional

import pytest

from hpc.autoscale import clilib
from hpc.autoscale.cli import ScaleLibCLI


PASSWORDS = [None, "", " synthetic 'quoted' \" $HOME `id`; \\ * ? [abc]\t\n\n"]
INIT_ARGS = [
    "initconfig",
    "--cluster-name",
    "synthetic",
    "--username",
    "synthetic",
    "--url",
    "https://invalid.example",
]


@pytest.mark.parametrize("environment", PASSWORDS)
@pytest.mark.parametrize("explicit", PASSWORDS)
def test_initconfig_password(
    monkeypatch: pytest.MonkeyPatch, environment: Optional[str], explicit: Optional[str]
) -> None:
    monkeypatch.delenv("SCALELIB_PASSWORD", raising=False)
    monkeypatch.setenv("AZGE_PASSWORD", "ignored-legacy-variable")
    if environment is not None:
        monkeypatch.setenv("SCALELIB_PASSWORD", environment)
    owner = ScaleLibCLI("scalelib")
    output = io.StringIO()
    serializer = owner.initconfig
    monkeypatch.setattr(
        owner, "initconfig", lambda **config: serializer(writer=output, **config)
    )
    argv = INIT_ARGS + ([] if explicit is None else ["--password", explicit])
    clilib.main(argv, "scalelib", owner)
    config = json.loads(output.getvalue())
    assert config["password"] == (environment if explicit is None else explicit)
    assert config["cluster_name"] == "synthetic"
    assert config["logging"]["config_file"].endswith("/logging.conf")


@pytest.mark.parametrize("config", [{}, {"password": None}, {"password": ""}])
def test_direct_serializer_ignores_environment(
    monkeypatch: pytest.MonkeyPatch, config: Dict
) -> None:
    monkeypatch.setenv("SCALELIB_PASSWORD", "must-not-inherit")
    output = io.StringIO()
    ScaleLibCLI("scalelib").initconfig(writer=output, **config)
    assert json.loads(output.getvalue()) == config


def test_other_command_ignores_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SCALELIB_PASSWORD", "must-not-inherit")
    config_path = tmp_path / "autoscale.json"
    config_path.write_text(json.dumps({"password": "configured"}))
    owner = ScaleLibCLI("scalelib")
    received = {}
    monkeypatch.setattr(owner, "nodes", lambda **kwargs: received.update(kwargs))
    monkeypatch.setattr(clilib.logging, "initialize_logging", lambda config: None)
    clilib.main(["nodes", "--config", str(config_path)], "scalelib", owner)
    assert "password" not in received
    assert received["config"]["password"] == "configured"


def test_password_requires_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCALELIB_PASSWORD", "not-a-missing-value-default")
    with pytest.raises(SystemExit) as error:
        clilib.main(INIT_ARGS + ["--password"], "scalelib", ScaleLibCLI("scalelib"))
    assert error.value.code == 2
