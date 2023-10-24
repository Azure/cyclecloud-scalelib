import argparse
import os
import sys


def install(cli: str, module: str) -> None:
    project_dir = os.path.abspath(os.path.join(sys.executable, "..", "..", ".."))

    print(f"Installing {cli} with {module}")
    if not os.path.exists(project_dir):
        raise RuntimeError(f"Project dir {project_dir} does not exist")

    venv_dir = os.path.join(project_dir, "venv")
    if not os.path.exists(venv_dir):
        raise RuntimeError(
            f"Project dir {project_dir} does not have a venv in it. Is this the correct directory?"
        )

    bin_dir = os.path.join(project_dir, "venv", "bin")
    if not os.path.exists(bin_dir):
        os.makedirs(bin_dir)

    default_config = os.path.join(project_dir, "autoscale.json")

    cli_path = os.path.join(bin_dir, cli)

    with open(cli_path, "w") as fw:
        fw.write(
            f"""#!{venv_dir}/bin/python
import sys
import {module} as cli

cli.main(sys.argv[1:])
"""
        )

    os.chmod(cli_path, 0o500)

    with open(f"/etc/profile.d/{cli}_autocomplete.sh", "w") as fw:
        fw.write(
            f"""#!/usr/bin/env bash
export PATH=$PATH:{bin_dir}  
eval "$( {bin_dir}/register-python-argcomplete {cli})" || echo "Warning: Autocomplete is disabled for {cli}" 1>&2
"""
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Install a Scalelib-based CLI")
    sub_parsers = parser.add_subparsers(dest="cmd")
    install_parser = sub_parsers.add_parser("install")
    install_parser.add_argument("--cli", required=True)
    install_parser.add_argument("--module", required=True)

    args = parser.parse_args()
    if args.cmd == "install":
        install(args.cli, args.module)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
