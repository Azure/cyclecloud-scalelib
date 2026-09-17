import copy
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

import yaml

from util.local_release import BUILD_STEPS, build_steps


class LocalReleaseTest(unittest.TestCase):
    def setUp(self):
        self.source = Path(__file__).resolve().parents[1]
        self.workflow = yaml.safe_load(
            (self.source / ".github/workflows/release.yml").read_text()
        )

    def test_only_build_steps_are_selected(self):
        steps = build_steps(self.workflow)
        self.assertEqual(tuple(step["name"] for step in steps), BUILD_STEPS)
        for step in steps:
            self.assertNotIn("gh release", step["run"])
            self.assertNotIn("./build.sh", step["run"])
            subprocess.run(["bash", "-n"], input=step["run"], text=True, check=True)

    def test_rejects_missing_duplicate_or_unsupported_steps(self):
        for mutation in ("missing", "duplicate", "expression", "condition"):
            with self.subTest(mutation=mutation):
                workflow = copy.deepcopy(self.workflow)
                steps = workflow["jobs"]["release"]["steps"]
                if mutation == "missing":
                    del steps[1]
                elif mutation == "duplicate":
                    steps.append(copy.deepcopy(steps[1]))
                elif mutation == "expression":
                    steps[1]["run"] = "echo ${{ secrets.TOKEN }}"
                else:
                    steps[1]["if"] = "false"
                with self.assertRaises(ValueError):
                    build_steps(workflow)

    def test_override_metadata_is_validated(self):
        step = build_steps(self.workflow)[1]
        for version, expected_code in (("8.9.3", 0), ("8.0.1", 1)):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                wheel = Path(directory) / "input.whl"
                with zipfile.ZipFile(wheel, "w") as archive:
                    archive.writestr(
                        "cyclecloud_api-" + version + ".dist-info/METADATA",
                        "Name: cyclecloud-api\nVersion: " + version + "\n",
                    )
                environment = dict(
                    os.environ, CYCLECLOUD_API=str(wheel), API_VERSION="8.9.3",
                    API_WHEEL="cyclecloud_api-8.9.3-py2.py3-none-any.whl",
                )
                result = subprocess.run(
                    ["bash", "-e", "-o", "pipefail", "-c", step["run"]],
                    cwd=directory, env=environment, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, expected_code, result.stderr)
                if expected_code:
                    self.assertIn("metadata does not match", result.stderr)


if __name__ == "__main__":
    unittest.main()