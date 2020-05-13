# test: ignore
import os
import shutil
import sys
from subprocess import check_call
from typing import Any, List

from setuptools import find_packages, setup
from setuptools.command.test import Command
from setuptools.command.test import test as TestCommand  # noqa: N812


__version__ = "0.1.0"


class PyTest(TestCommand):
    skip_hypothesis = True

    def finalize_options(self) -> None:
        TestCommand.finalize_options(self)
        import os

        xml_out = os.path.join(".", "build", "test-results", "pytest.xml")
        if not os.path.exists(os.path.dirname(xml_out)):
            os.makedirs(os.path.dirname(xml_out))
        # -s is needed so py.test doesn't mess with stdin/stdout
        self.test_args = ["-s", "test", "--junitxml=%s" % xml_out]
        # needed for older setuptools to actually run this as a test
        self.test_suite = True

    def run_tests(self) -> None:
        # import here, cause outside the eggs aren't loaded
        import sys
        import pytest

        # run the tests, then the format checks.
        os.environ["HPC_RUNTIME_CHECKS"] = "true"
        errno = pytest.main(self.test_args + ["-k", "not hypothesis"])
        if errno != 0:
            sys.exit(errno)

        if not PyTest.skip_hypothesis:
            os.environ["HPC_RUNTIME_CHECKS"] = "false"
            errno = pytest.main(self.test_args + ["-k", "hypothesis"])
            if errno != 0:
                sys.exit(errno)

        check_call(
            ["black", "--check", "src", "test", "util"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        check_call(
            ["isort", "-c"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"),
        )
        check_call(
            ["isort", "-c"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "test"),
        )
        check_call(
            ["isort", "-c"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "util"),
        )

        run_type_checking()

        sys.exit(errno)


class Formatter(Command):
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        check_call(
            ["black", "src", "test", "util"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        check_call(
            ["isort", "-y"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"),
        )
        check_call(
            ["isort", "-y"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "test"),
        )
        check_call(
            ["isort", "-y"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "util"),
        )
        run_type_checking()


class InitCommitHook(Command):
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        activate_script = os.path.join(os.path.dirname(sys.executable), "activate")
        if not os.path.exists(activate_script):
            print("Run this command after activating your venv!")
            sys.exit(1)

        pre_commit = os.path.join(
            os.path.dirname(__file__), ".git", "hooks", "pre-commit"
        )
        if os.path.exists(pre_commit):
            print(pre_commit, "exists already. Please remove it and run it again.")
            sys.exit(1)

        with open(".git/hooks/pre-commit", "w") as fw:
            fw.write("#!/bin/sh\n")
            fw.write("source {}\n".format(activate_script))
            fw.write("python setup.py commithook")
        check_call(["chmod", "+x", pre_commit])


class PreCommitHook(PyTest):
    def __init__(self, dist: Any, **kw: Any) -> None:
        super().__init__(dist, **kw)
        PyTest.skip_hypothesis = True


class ResourceFiles(Command):
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        check_call([sys.executable, "util/create_vm_sizes.py"])
        shutil.move("new_vm_sizes.json", "src/hpc/autoscale/node/vm_sizes.json")


def run_type_checking() -> None:
    check_call(
        ["mypy", os.path.join(os.path.dirname(os.path.abspath(__file__)), "test")]
    )
    check_call(
        ["mypy", os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")]
    )
    check_call(
        ["mypy", os.path.join(os.path.dirname(os.path.abspath(__file__)), "util")]
    )

    check_call(["flake8", "--ignore=F405,E501,W503", "src", "test", "setup.py"])


class TypeChecking(Command):
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        run_type_checking()


setup(
    name="autoscale",
    version=__version__,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    package_data={
        "hpc": [
            "BUILD_NUMBER",
            "private-requirements.json",
            "../NOTICE",
            "../notices",
            "vm_sizes.json",
        ],
        "": ["vm_sizes.json", "../notices"],
    },
    include_package_data=True,
    install_requires=[
        "requests == 2.21.0",
        "typing_extensions",
        "frozendict==1.2.0",
        "jsonpickle==1.4.1",
    ]
    + ["urllib3==1.24.1"],  # noqa: W503
    tests_require=["pytest==3.2.3"],
    cmdclass={
        "test": PyTest,
        "format": Formatter,
        "types": TypeChecking,
        "commithook": PreCommitHook,
        "initcommithook": InitCommitHook,
        "resourcefiles": ResourceFiles,
    },
    url="http://www.microsoft.com",
    maintainer="Azure CycleCloud",
)
