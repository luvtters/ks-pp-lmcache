# SPDX-License-Identifier: Apache-2.0
"""Check that original model arguments survive the eager lookup configuration."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from configure_model import configured_script


class ConfigureModelTest(unittest.TestCase):
    """Exercise the script conversion without launching a service."""

    def sample(self, eager: bool | None = None) -> str:
        extra = {"lmcache.mp.autostart": False,
                 "lmcache.mp.server_urls": ["tcp://192.0.2.1:5565", "tcp://192.0.2.2:5565"]}
        if eager is not None:
            extra["lmcache.mp.eager_prefetch"] = eager
        config = {"kv_connector": "MultiConnector", "kv_connector_extra_config": {
            "connectors": [{"kv_connector": "MooncakeConnectorV1", "kv_role": "kv_producer"},
                           {"kv_connector": "LMCacheMPConnector", "kv_connector_extra_config": extra}]}}
        return ("#!/bin/bash\nexport MODEL_PATH=/model\n"
                "vllm serve $MODEL_PATH --max-model-len 990000 --tensor-parallel-size 16 "
                "--pipeline-parallel-size 2 --kv-transfer-config '" + json.dumps(config, indent=2) + "'\n")

    def test_adds_only_eager_and_enable_preserves_original_arguments(self) -> None:
        before = self.sample()
        result = configured_script(before)
        self.assertIn("customer_pp2/enable.sh", result)
        original_json = json.loads(re.search(r"--kv-transfer-config '([^']*)'", before, re.S)[1])
        actual_json = json.loads(re.search(r"--kv-transfer-config '([^']*)'", result, re.S)[1])
        original_json["kv_connector_extra_config"]["connectors"][1]["kv_connector_extra_config"]["lmcache.mp.eager_prefetch"] = True
        self.assertEqual(actual_json, original_json)
        self.assertIn("vllm serve $MODEL_PATH --max-model-len 990000 --tensor-parallel-size 16 --pipeline-parallel-size 2", result)
        self.assertEqual(configured_script(result), result)

    def test_false_becomes_true(self) -> None:
        result = configured_script(self.sample(False))
        self.assertIn('"lmcache.mp.eager_prefetch": true', result)

    def test_ambiguous_input_is_rejected(self) -> None:
        before = self.sample()
        with self.assertRaises(ValueError):
            configured_script(before + before)

    def test_cli_backs_up_original_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            script = Path(temporary) / "original.sh"
            original = self.sample().encode("utf-8")
            script.write_bytes(original)
            env = dict(os.environ)
            git_bash = Path("D:/work_tool/Git/bin")
            if git_bash.exists():
                env["PATH"] = str(git_bash) + os.pathsep + env["PATH"]
            command = [sys.executable, "-B", str(Path(__file__).with_name("configure_model.py")), str(script)]
            first = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertTrue(first.stdout.lstrip().startswith("{"), repr(first))
            row = json.loads(first.stdout)
            self.assertEqual(Path(row["backup"]).read_bytes(), original)
            updated = script.read_bytes()
            second = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertEqual(json.loads(second.stdout)["state"], "ALREADY_CONFIGURED")
            self.assertEqual(script.read_bytes(), updated)


if __name__ == "__main__":
    unittest.main(verbosity=2)
