# test: ignore
import os
import shutil
import sys
from subprocess import check_call
from typing import Any, List

from setuptools import find_packages, setup
from setuptools.command.test import Command
from setuptools.command.test import test as TestCommand  # noqa: N812

import inspect

__version__ = "0.2.6"


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


class AutoDoc(Command):
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        from hpc.autoscale.node import constraints
        from hpc.autoscale.node.constraints import NodeConstraint  # noqa: N812

        with open("README.md", "w") as fw:
            with open("README.md.in") as fr:
                print(fr.read(), file=fw)
            print("# Node Properties", file=fw)
            print(file=fw)
            from hpc.autoscale.node import node as nodelib

            print("| Property | Type | Description |", file=fw)
            print("| :---     | :--- | :---        |", file=fw)
            for prop_name in sorted(nodelib.QUERYABLE_PROPERTIES):
                prop = getattr(nodelib.Node, prop_name)
                sig = inspect.signature(prop.fget)
                ra = sig.return_annotation
                return_type: str
                if hasattr(ra, "__name__"):
                    return_type = ra.__name__
                elif hasattr(ra, "_name"):
                    optional = False
                    inner_types = [x.__name__ for x in ra.__args__]
                    if "NoneType" in inner_types:
                        optional = True
                        inner_types = [x for x in inner_types if x != "NoneType"]

                    return_type = "\\|".join(inner_types)
                    if optional:
                        return_type = "Optional[{}]".format(return_type)
                else:
                    return_type = str(ra)
                print(
                    "| node.{} | {} | {} |".format(
                        prop_name, return_type, prop.__doc__
                    ),
                    file=fw,
                )

            print(file=fw)
            print(file=fw)
            print("# Constraints", file=fw)
            print(file=fw)
            for attr in dir(constraints):
                if not attr[0].isupper():
                    continue
                value = getattr(constraints, attr)
                if not isinstance(value, type):
                    continue
                if inspect.isabstract(value):
                    continue
                if not issubclass(value, NodeConstraint):
                    continue

                if not value.__doc__:
                    continue

                print(file=fw)
                print(file=fw)
                print("##", value.__name__, file=fw)
                print(file=fw)
                for line in (value.__doc__ or "").splitlines(keepends=False):
                    if len(line) > 4 and line[:4] == "    ":
                        line = line[4:]
                    print(line, file=fw)

            print(file=fw)
            print("# Contributing", file=fw)
            print(file=fw)
            print(
                """This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.""",
                file=fw,
            )


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

    check_call(["flake8", "--ignore=F405,E501,W503,E203", "src", "test", "setup.py"])


class TypeChecking(Command):
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        run_type_checking()


setup(
    name="cyclecloud-scalelib",
    version=__version__,
    packages=find_packages(where="src"),
    package_dir={"": "src", "conf": "conf"},
    include_package_data=True,
    install_requires=[
        "requests >= 2.24.0",
        "typing_extensions",
        "immutabledict==1.0.0",
        "jsonpickle==1.4.1",
        "argcomplete==1.12.2",
        "certifi==2020.12.5",
        "importlib-metadata<4",
    ]
    + ["urllib3==1.25.11"],  # noqa: W503
    tests_require=["pytest==3.2.3"],
    cmdclass={
        "test": PyTest,
        "docs": AutoDoc,
        "format": Formatter,
        "types": TypeChecking,
        "commithook": PreCommitHook,
        "initcommithook": InitCommitHook,
        "resourcefiles": ResourceFiles,
    },
    scripts=["util/install_azscale.sh"],
    url="http://www.microsoft.com",
    maintainer="Azure CycleCloud",
)
