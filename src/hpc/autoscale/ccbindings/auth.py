import os
import shutil
from subprocess import check_output
from typing import List, Tuple

from hpc.autoscale import hpclogging as logging


class JetpackNotFoundError(RuntimeError):
    pass


def _find_jetpack() -> str:
    searched: List[str] = []
    for command in ("jetpack", "jetpack.cmd"):
        searched.append(command)
        executable = shutil.which(command)
        if executable:
            return executable

    cyclecloud_home = os.getenv("CYCLECLOUD_HOME")
    if cyclecloud_home:
        for filename in ("jetpack", "jetpack.cmd"):
            candidate = os.path.join(cyclecloud_home, "bin", filename)
            searched.append(candidate)
            executable = shutil.which(candidate)
            if executable:
                return executable

    searched_locations = "\n  - ".join(searched)
    message = (
        "Could not find the CycleCloud Jetpack executable. Searched:\n"
        "  - {}\n"
        "Add jetpack to PATH or set CYCLECLOUD_HOME to the CycleCloud installation "
        "directory.".format(searched_locations)
    )
    logging.error(message)
    raise JetpackNotFoundError(message)


def get_cyclecloud_access_credentials() -> Tuple[str, str]:
    jetpack = _find_jetpack()
    username = check_output(
        [jetpack, "config", "cyclecloud.config.username"], universal_newlines=True
    ).rstrip("\r\n")
    password = check_output(
        [jetpack, "config", "cyclecloud.config.password"], universal_newlines=True
    ).rstrip("\r\n")
    if not username or not password:
        raise ValueError("Jetpack returned empty CycleCloud credentials.")
    return username, password
