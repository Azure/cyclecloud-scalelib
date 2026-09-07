import os
from typing import Optional
from unittest.mock import call, patch

import pytest

from hpc.autoscale.ccbindings.auth import (
    JetpackNotFoundError,
    _find_jetpack,
    get_cyclecloud_access_credentials,
)


def test_get_cyclecloud_access_credentials() -> None:
    with patch("hpc.autoscale.ccbindings.auth.shutil.which") as which:
        which.return_value = "/usr/local/bin/jetpack"
        with patch(
            "hpc.autoscale.ccbindings.auth.check_output",
            side_effect=["cyclecloud-user\n", "cyclecloud-password\n"],
        ) as check_output:
            credentials = get_cyclecloud_access_credentials()

    assert credentials == ("cyclecloud-user", "cyclecloud-password")
    which.assert_called_once_with("jetpack")
    assert check_output.call_args_list == [
        call(
            ["/usr/local/bin/jetpack", "config", "cyclecloud.config.username"],
            universal_newlines=True,
        ),
        call(
            ["/usr/local/bin/jetpack", "config", "cyclecloud.config.password"],
            universal_newlines=True,
        ),
    ]


def test_get_cyclecloud_access_credentials_preserves_password_whitespace() -> None:
    with patch(
        "hpc.autoscale.ccbindings.auth._find_jetpack",
        return_value="/usr/local/bin/jetpack",
    ):
        with patch(
            "hpc.autoscale.ccbindings.auth.check_output",
            side_effect=["cyclecloud-user\n", " cyclecloud-password \n"],
        ):
            credentials = get_cyclecloud_access_credentials()

    assert credentials == ("cyclecloud-user", " cyclecloud-password ")


@pytest.mark.parametrize(
    "outputs",
    [("\n", "cyclecloud-password\n"), ("cyclecloud-user\n", "\n")],
)
def test_get_cyclecloud_access_credentials_rejects_empty_values(outputs: tuple) -> None:
    with patch(
        "hpc.autoscale.ccbindings.auth._find_jetpack",
        return_value="/usr/local/bin/jetpack",
    ):
        with patch(
            "hpc.autoscale.ccbindings.auth.check_output", side_effect=outputs
        ):
            with pytest.raises(ValueError):
                get_cyclecloud_access_credentials()


def test_find_jetpack_cmd_on_path() -> None:
    with patch(
        "hpc.autoscale.ccbindings.auth.shutil.which",
        side_effect=[None, "C:\\cycle\\jetpack.cmd"],
    ) as which:
        executable = _find_jetpack()

    assert executable == "C:\\cycle\\jetpack.cmd"
    assert which.call_args_list == [call("jetpack"), call("jetpack.cmd")]


@pytest.mark.parametrize("filename", ["jetpack", "jetpack.cmd"])
def test_find_jetpack_in_cyclecloud_home(filename: str) -> None:
    cyclecloud_home = "/opt/cycle/jetpack"
    expected = os.path.join(cyclecloud_home, "bin", filename)

    def find_executable(candidate: str) -> Optional[str]:
        return expected if candidate == expected else None

    with patch.dict(os.environ, {"CYCLECLOUD_HOME": cyclecloud_home}, clear=True):
        with patch(
            "hpc.autoscale.ccbindings.auth.shutil.which",
            side_effect=find_executable,
        ):
            assert _find_jetpack() == expected


def test_get_cyclecloud_access_credentials_raises_when_jetpack_is_missing() -> None:
    with patch.dict(os.environ, {}, clear=True):
        with patch("hpc.autoscale.ccbindings.auth.shutil.which", return_value=None):
            with patch("hpc.autoscale.ccbindings.auth.logging.error") as error:
                with pytest.raises(JetpackNotFoundError) as exc_info:
                    get_cyclecloud_access_credentials()

    message = str(exc_info.value)
    assert "Could not find the CycleCloud Jetpack executable" in message
    assert "jetpack" in message
    assert "jetpack.cmd" in message
    assert "CYCLECLOUD_HOME" in message
    error.assert_called_once_with(message)
