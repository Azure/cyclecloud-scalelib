import argparse
import configparser
import glob
import io
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
from subprocess import check_call as _check_call
from typing import Any, Dict, List, Optional


class UserError(RuntimeError):
    pass


def check_call(cmd: List[str], **kwargs: Dict[str, Any]) -> None:
    print(" ".join(cmd))
    _check_call(cmd, **kwargs)


class ProjectCreator:
    def __init__(
        self, project_name: str, cli_name: str, parent_dir: str, version: str
    ) -> None:
        self.project_name = project_name
        self.cli_name = cli_name
        self.parent_dir = parent_dir
        self.scalelib_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.project_dir = os.path.join(self.parent_dir, self.project_name)
        self.version = version

    def create_project(self) -> None:
        if os.path.exists(self.project_dir) and os.listdir(self.project_dir):
            raise UserError(
                f"Project dir {self.project_dir} already exists and is not empty"
            )
        os.makedirs(
            os.path.join(self.project_dir, "src", self.project_name), exist_ok=True
        )
        with open(
            os.path.join(self.project_dir, "src", self.project_name, "__init__.py"), "w"
        ):
            pass
        os.makedirs(os.path.join(self.project_dir, "test", self.project_name + "test"))

        scalelib_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(f"{scalelib_root}/setup.cfg") as fr:
            with open(f"{self.project_dir}/setup.cfg", "w") as fw:
                self.create_setup_cfg(fr, fw)

        self.create_simple_cli()

        self.create_build_settings()
        self.create_setup_py()
        self.create_init_env_sh()
        self.create_package_sh()
        self.create_build_sh()
        self.create_install_sh()
        self.create_readme_md()

    def create_build_settings(self) -> None:
        json_path = os.path.join(self.project_dir, "build-settings.json")
        with open(json_path, "w") as fw:
            json.dump(
                {
                    "cyclecloud-scalelib-root": self.scalelib_dir,
                    "docker-runtime": "docker",
                    "docker-image-alma": "mcr.microsoft.com/mirror/docker/library/almalinux:8.7",
                    "docker-image-ubuntu": "mcr.microsoft.com/mirror/docker/library/ubuntu:20.04",
                    "docker-image": "mcr.microsoft.com/mirror/docker/library/ubuntu:20.04",
                },
                fw,
                indent=2,
            )

    def create_setup_cfg(
        self, original: io.TextIOWrapper, writer: io.TextIOWrapper
    ) -> None:
        parser = configparser.ConfigParser()
        parser.read_file(original)

        if parser.has_section("metadata"):
            parser.remove_section("metadata")
        parser.add_section("metadata")
        parser.set("metadata", "name", self.project_name)
        parser.set("metadata", "version", self.version)
        parser.set("metadata", "author", "TODO")
        parser.set("metadata", "author_email", "TODO")
        parser.set("metadata", "description", "TODO")
        parser.set("metadata", "url", "TODO")
        parser.set("metadata", "license", "MIT")

        if parser.has_section("options"):
            parser.remove_section("options")
        parser.add_section("options")
        parser.set("options", "packages", "find:")
        parser.set("options", "package_dir", " =src")
        parser.set("options", "install_requires", "cyclecloud-scalelib==1.0.2")
        parser.set("options", "python_requires", " >=3.7")

        if parser.has_section("options.packages.find"):
            parser.remove_section("options.packages.find")
        parser.add_section("options.packages.find")
        parser.set("options.packages.find", "where", "src")

        parser.write(writer)

    def create_setup_py(self) -> None:
        with open(os.path.join(self.project_dir, "setup.py"), "w") as fw:
            fw.write(
                """import setuptools
setuptools.setup()"""
            )

    def create_init_env_sh(self) -> None:
        init_env_sh = os.path.join(self.project_dir, "init-env.sh")
        with open(init_env_sh, "w") as fw:
            fw.write(
                """#!/bin/bash
set -e
proj_dir=$(dirname $0)
cd $proj_dir
python3 -m venv venv 
source venv/bin/activate
pip install --upgrade pip
scalelib_root=$(python3 -c "import json; print(json.load(open('build-settings.json'))['cyclecloud-scalelib-root'])")
pip install $scalelib_root
pip install build
python setup.py install"""
            )
        os.chmod(init_env_sh, 0o500)

    def create_build_sh(self) -> None:
        build_sh = os.path.join(self.project_dir, "build.sh")
        with open(build_sh, "w") as fw:
            fw.write(
                """#!/bin/bash
set -e
cd $(dirname $0)
source venv/bin/activate
black src test
isort -y
flake8 src test *.py
pytest
python setup.py sdist"""
            )
        os.chmod(build_sh, 0o500)

    def create_install_sh(self) -> None:
        install_sh = os.path.join(self.project_dir, "install.sh")
        with open(install_sh, "w") as fw:
            fw.write(
                f"""#!/bin/bash
set -e

SCHEDULER={self.project_name}
INSTALL_PYTHON3=0
USE_JETPACK_PYTHON3=0
INSTALL_VIRTUALENV=0
PROJ_DIR=/opt/azure/$SCHEDULER
VENV=$PROJ_DIR/venv


while (( "$#" )); do
    case "$1" in
        --install-python3)
            INSTALL_PYTHON3=1
            INSTALL_VIRTUALENV=1
            shift
            ;;
        --install-venv)
            INSTALL_VIRTUALENV=1
            shift
            ;;
        --venv)
            VENV=$2
            shift 2
            ;;

        -*|--*=)
            echo "Unknown option $1" >&2
            exit 1
            ;;
        *)
            echo "Unknown option  $1" >&2
            exit 1
            ;;
    esac
done

echo INSTALL_PYTHON3=$INSTALL_PYTHON3
echo INSTALL_VIRTUALENV=$INSTALL_VIRTUALENV
echo VENV=$VENV

which python3 > /dev/null;
if [ $? != 0 ]; then
    if [ $INSTALL_PYTHON3 == 1 ]; then
        yum install -y python3 || exit 1
    else
        echo Please install python3 >&2;
        exit 1
    fi
fi

export PATH=$(python3 -c '
import os
paths = os.environ["PATH"].split(os.pathsep)
cc_home = os.getenv("CYCLECLOUD_HOME", "/opt/cycle/jetpack")
print(os.pathsep.join(
    [p for p in paths if cc_home not in p]))')

if [ $INSTALL_VIRTUALENV == 1 ]; then
    python3 -m pip install venv
fi

python3 -m venv -h 2>&1 > /dev/null
if [ $? != 0 ]; then
    if [ $INSTALL_VIRTUALENV ]; then
        python3 -m pip install venv || exit 1
    else
        echo Please install venv for python3 >&2
        exit 1
    fi
fi


python3 -m venv $VENV
source $VENV/bin/activate
# not sure why but pip gets confused installing frozendict locally
# if you don't install it first. It has no dependencies so this is safe.
pip install --no-binary {self.project_name},cyclecloud-scalelib packages/*

python3 -m hpc.autoscale.cliinstall install --cli {self.cli_name} --module {self.project_name}.cli

source /etc/profile.d/{self.cli_name}_autocomplete.sh

{self.cli_name} -h 2>&1 > /dev/null || exit 1

{self.cli_name} initconfig > $PROJ_DIR/autoscale.json
"""
            )
        os.chmod(install_sh, 0o500)

        uninstall_sh = os.path.join(self.project_dir, "uninstall.sh")
        with open(uninstall_sh, "w") as fw:
            fw.write(
                f"""#!/bin/bash
set -e
echo TODO: implement uninstall"""
            )
        os.chmod(uninstall_sh, 0o500)

    def create_package_sh(self) -> None:
        package_sh = os.path.join(self.project_dir, "package.sh")
        with open(package_sh, "w") as fw:
            fw.write(
                f"""#!/bin/bash
set -e
cd $(dirname $0)
source venv/bin/activate
scalelib_root=$(python3 -c "import json; print(json.load(open('build-settings.json'))['cyclecloud-scalelib-root'])")
python $scalelib_root/util/package.py \\
    --version {self.version} \\
    --project-name {self.project_name} \\
    --cyclecloud-scalelib {self.scalelib_dir} \\
    --cyclecloud-api $1
                     """
            )
        os.chmod(package_sh, 0o500)

    def create_simple_cli(self) -> None:
        cli_py = os.path.join(self.project_dir, f"src/{self.project_name}/cli.py")
        with open(cli_py, "w") as fw:
            fw.write(
                """from typing import Dict, Iterable, List, Optional, Tuple
import argparse
import sys

from hpc.autoscale.clilib import CommonCLI, main as clilibmain
from hpc.autoscale.job.driver import SchedulerDriver, SimpleSchedulerDriver
from hpc.autoscale.job.job import Job
from hpc.autoscale.job.schedulernode import SchedulerNode


class %(cli_name)sDriver(SimpleSchedulerDriver):
    def __init__(self) -> None:
        super().__init__("htc")

    def _read_jobs_and_nodes(
        self, config: Dict
    ) -> Tuple[List[Job], List[SchedulerNode]]:
        jobs = [
            Job(name="job1", iterations=10, constraints={"ncpus": 1}),
            Job(name="job1", iterations=2, constraints={"ncpus": 1}, executing_hostnames=["node1"]),
        ]
        scheduler_nodes = [
            SchedulerNode(hostname="node1", constraints={"ncpus": 4}),
        ]
        return jobs, scheduler_nodes


class %(cli_name)sCLI(CommonCLI):
    def __init__(self):
        super().__init__("htc")

    def _initconfig_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--foo", default="bar")

    def _initconfig(self, config: Dict) -> None:
        # note this comes from the parser.add_argument call above
        print(f"The value of foo is {config['foo']}")
        # config["default_resources"].append({"name": "arch", "select": {}, "value": "x86_64"})
    
    def _default_output_columns(self, config: Dict) -> List[str]:
        
        columns = ["name", "hostname", "jobid", "resources"]

        for def_res in config.get("default_resources", []):
            if def_res["name"] not in columns:
                columns.append(def_res["name"])
    
    def _setup_shell_locals(self, config: Dict, shell_locals: Dict) -> None:
        shell_locals["foo"] = config["foo"]
        # when you run {cli_name} shell, a local variable called 'foo' will be
        # defined. It will have the value of config["foo"]

    def _driver(self, config: Dict) -> SchedulerDriver:
        return %(cli_name)sDriver()


def main(argv: Optional[Iterable[str]] = None) -> None:
    clilibmain(argv or sys.argv[1:], "%(project_name)s", %(cli_name)sCLI())


if __name__ == "__main__":
    main()
"""
                % {"cli_name": self.cli_name, "project_name": self.project_name}
            )

    def create_readme_md(self) -> None:
        readme_md = os.path.join(self.project_dir, "README.md")
        with open(readme_md, "w") as fw:
            fw.write(f"""# {self.project_name}""")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--cli-name", required=True)
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--version", default="0.0.1")
    args = parser.parse_args()
    ProjectCreator(
        args.project_name, args.cli_name, args.project_dir, args.version
    ).create_project()


if __name__ == "__main__":
    main()
