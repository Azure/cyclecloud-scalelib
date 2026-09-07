import importlib
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest

from hpc.autoscale.ccbindings import new_cluster_bindings
from hpc.autoscale.util import ConfigurationException


def _new_bindings(config: Dict) -> None:
    client = MagicMock()
    setattr(client.clusters.get.return_value, "_client", MagicMock())
    client_module = importlib.import_module("cyclecloud.client")
    with patch.object(client_module, "Client", return_value=client):
        new_cluster_bindings(config)


def test_new_cluster_bindings_looks_up_missing_credentials() -> None:
    config = {"cluster_name": "cluster", "url": "https://cyclecloud"}

    with patch(
        "hpc.autoscale.ccbindings.auth.get_cyclecloud_access_credentials",
        return_value=("jetpack-user", "jetpack-password"),
    ) as get_credentials:
        _new_bindings(config)

    get_credentials.assert_called_once_with()
    assert config["username"] == "jetpack-user"
    assert config["password"] == "jetpack-password"


def test_new_cluster_bindings_replaces_incomplete_configured_username() -> None:
    config = {
        "cluster_name": "cluster",
        "url": "https://cyclecloud",
        "username": "configured-user",
    }

    with patch(
        "hpc.autoscale.ccbindings.auth.get_cyclecloud_access_credentials",
        return_value=("jetpack-user", "jetpack-password"),
    ):
        _new_bindings(config)

    assert config["username"] == "jetpack-user"
    assert config["password"] == "jetpack-password"


def test_new_cluster_bindings_replaces_incomplete_configured_password() -> None:
    config = {
        "cluster_name": "cluster",
        "url": "https://cyclecloud",
        "password": "configured-password",
    }

    with patch(
        "hpc.autoscale.ccbindings.auth.get_cyclecloud_access_credentials",
        return_value=("jetpack-user", "jetpack-password"),
    ):
        _new_bindings(config)

    assert config["username"] == "jetpack-user"
    assert config["password"] == "jetpack-password"


def test_new_cluster_bindings_reports_missing_valid_credentials() -> None:
    config = {"cluster_name": "cluster", "url": "https://cyclecloud"}

    with patch(
        "hpc.autoscale.ccbindings.auth.get_cyclecloud_access_credentials",
        side_effect=RuntimeError("Jetpack lookup failed"),
    ):
        with pytest.raises(ConfigurationException) as exc_info:
            new_cluster_bindings(config)

    assert str(exc_info.value) == "No valid CycleCloud credentials were found."
    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_new_cluster_bindings_does_not_look_up_configured_credentials() -> None:
    config = {
        "cluster_name": "cluster",
        "url": "https://cyclecloud",
        "username": "configured-user",
        "password": "configured-password",
    }

    with patch(
        "hpc.autoscale.ccbindings.auth.get_cyclecloud_access_credentials"
    ) as get_credentials:
        _new_bindings(config)

    get_credentials.assert_not_called()
