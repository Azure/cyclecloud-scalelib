import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

import yaml


BUILD_STEPS = ("Check release version", "Download CycleCloud API wheel", "Build package")


def build_steps(workflow):
    steps = workflow["jobs"]["release"]["steps"]
    selected = [step for step in steps if step.get("name") in BUILD_STEPS]
    if [step["name"] for step in selected] != list(BUILD_STEPS):
        raise ValueError("Expected release preparation and build steps in order")
    for step in selected:
        if set(step) - {"name", "shell", "run", "env"}:
            raise ValueError("Unsupported build step fields: " + step["name"])
        if step.get("shell") != "bash":
            raise ValueError("Expected bash step: " + step["name"])
        command = step.get("run")
        if not isinstance(command, str) or not command.strip() or "${{" in command:
            raise ValueError("Expected a plain shell command: " + step["name"])
        for value in step.get("env", {}).values():
            if not isinstance(value, str) or "${{" in value:
                raise ValueError("Expected a literal environment: " + step["name"])
    return selected


def build(source, output):
    sys.path.insert(0, str(source))
    from package import CYCLECLOUD_SCALELIB_VERSION

    with (source / ".github/workflows/release.yml").open() as stream:
        steps = build_steps(yaml.safe_load(stream))
    environment_file = source / "build/local-release.env"
    environment_file.parent.mkdir(exist_ok=True)
    environment_file.write_text("")
    environment = dict(
        os.environ, GITHUB_WORKSPACE=str(source),
        GITHUB_REF_NAME=CYCLECLOUD_SCALELIB_VERSION,
        GITHUB_ENV=str(environment_file),
    )
    for step in steps:
        print("Running workflow step: " + step["name"], flush=True)
        subprocess.run(
            ["/bin/bash", "-e", "-o", "pipefail", "-c", step["run"]],
            cwd=source, env=dict(environment, **step.get("env", {})), check=True,
        )
        for line in environment_file.read_text().splitlines():
            name, value = line.split("=", 1)
            environment[name] = value

    archive_path = source / "dist" / ("cyclecloud-scalelib-pkg-" + CYCLECLOUD_SCALELIB_VERSION + ".tar.gz")
    with tarfile.open(archive_path, "r:gz") as archive:
        if not archive.getmembers():
            raise ValueError("Empty release package")
    shutil.copyfile(archive_path, output / archive_path.name)
    shutil.copyfile(source / "libs" / environment["API_WHEEL"], output / environment["API_WHEEL"])


if __name__ == "__main__":
    build(Path.cwd(), Path("/output"))