import argparse
import glob
import os
import re
import shutil
import sys
import tarfile
import tempfile
from subprocess import check_call as _check_call
from typing import Any, Dict, List, Optional


def check_call(cmd: List[str], **kwargs: Dict[str, Any]) -> None:
    print(" ".join(cmd))
    _check_call(cmd, **kwargs)


class Packager:
    def __init__(self) -> None:
        self.scalelib = None
        self.cyclecloud_api_version = None
        self.libs_dir = os.path.abspath("libs")
        self.scripts = [("install.sh", "install.sh"), ("uninstall.sh", "uninstall.sh")]

    def build_scalelib(self) -> None:
        assert self.scalelib
        if os.path.isdir(self.scalelib):
            self._run_sdist(self.scalelib)
        elif os.path.exists(self.scalelib):
            dest = os.path.join(self.libs_dir, os.path.basename(self.scalelib))
            if os.path.abspath(self.scalelib) != dest:
                shutil.copyfile(self.scalelib, dest)
        elif re.match("[0-9]+\\.[0-9]+\\.[0-9]+", self.scalelib):
            self.scalelib_sdist = os.path.abspath(
                f"{self.libs_dir}/cyclecloud-scalelib-{self.scalelib}.tar.gz"
            )
            cmd = [
                "wget",
                "-k",
                "-O",
                self.scalelib_sdist,
                f"https://github.com/Azure/cyclecloud-scalelib/archive/refs/tags/{self.scalelib}.tar.gz",
            ]
            check_call(cmd)
        else:
            raise RuntimeError(
                f"Could not find scalelib - either a local project directory, a .tar.gz or a version (e.g. 1.0.1) {self.scalelib}"
            )

    def build_self(self) -> None:
        self._run_sdist()

    def build(self) -> None:
        if not os.path.exists("libs"):
            os.makedirs("libs")
        self.build_scalelib()
        self.build_self()

        source = os.path.abspath(self.cyclecloud_api_version)
        dest = os.path.join(self.libs_dir, os.path.basename(source))
        if dest != source:
            shutil.copyfile(source, dest)

    def _run_sdist(self, directory=None) -> None:
        pwd = os.getcwd()
        if directory:
            os.chdir(directory)
        if os.path.exists("dist"):
            shutil.rmtree("dist")
        cmd = [sys.executable, "setup.py", "sdist"]
        check_call(cmd)
        sdists = glob.glob("dist/*.tar.gz")
        for sdist in sdists:
            shutil.move(sdist, os.path.join(self.libs_dir, os.path.basename(sdist)))

        if directory:
            os.chdir(pwd)


def execute(packager: Packager) -> None:
    argument_parser = argparse.ArgumentParser(
        "Builds a Scalelib based project with all dependencies."
    )
    argument_parser.add_argument("--cyclecloud-api", required=True)
    argument_parser.add_argument("--cyclecloud-scalelib", required=True)
    argument_parser.add_argument("--version", required=True)
    argument_parser.add_argument("--project-name", required=True)
    args = argument_parser.parse_args()

    packager.scalelib = args.cyclecloud_scalelib
    packager.cyclecloud_api_version = args.cyclecloud_api

    packager.build()

    if not os.path.exists("dist"):
        os.makedirs("dist")

    tf = tarfile.TarFile.gzopen(
        f"dist/{args.project_name}-pkg-{args.version}.tar.gz", "w"
    )

    build_dir = tempfile.mkdtemp(args.project_name)

    def _add(name: str, path: Optional[str] = None, mode: Optional[int] = None) -> None:
        path = path or name
        tarinfo = tarfile.TarInfo(f"{args.project_name}/{name}")
        tarinfo.size = os.path.getsize(path)
        tarinfo.mtime = int(os.path.getmtime(path))
        if mode:
            tarinfo.mode = mode

        with open(path, "rb") as fr:
            tf.addfile(tarinfo, fr)

    packages = []
    for dep in os.listdir(packager.libs_dir):
        dep_path = os.path.abspath(os.path.join(packager.libs_dir, dep))
        _add("packages/" + dep, dep_path)
        packages.append(dep_path)

    if not packages:
        raise RuntimeError(f"No packages found in libs dir - {packager.libs_dir}")
    check_call(["pip3", "download"] + packages, cwd=build_dir)

    print("Using build dir", build_dir)
    by_package: Dict[str, List[str]] = {}
    for fil in os.listdir(build_dir):
        toks = fil.split("-", 1)
        package = toks[0]
        if package == "cyclecloud":
            package = "{}-{}".format(toks[0], toks[1])
        if package not in by_package:
            by_package[package] = []
        by_package[package].append(fil)

    for package, fils in by_package.items():
        assert len(fils) == 1, "Duplicate package found!: %s=%s" % (package, fils)

    for fil in os.listdir(build_dir):
        if "PyYAML" in fil:
            print("Skipping addition of PyYAML")
            continue
        path = os.path.join(build_dir, fil)
        _add("packages/" + fil, path)

    for script_name, script_path in packager.scripts:
        _add(
            script_name,
            script_path,
            mode=os.stat(script_path).st_mode & 0o777,
        )


if __name__ == "__main__":
    execute(Packager())
