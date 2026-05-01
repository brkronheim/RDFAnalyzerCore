#!/usr/bin/env python3

import os
import tempfile
import unittest

import yaml

from core.python import validate_datacard_generator as validator


class ValidateDatacardGeneratorTest(unittest.TestCase):
    def test_validate_config_schema_reports_missing_fields(self):
        config = {
            "output_dir": "out",
            "input_files": {"data": {"type": "data"}},
            "processes": {"signal": {}},
            "control_regions": {"SR": {"observable": "mT"}},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as handle:
            yaml.safe_dump(config, handle)
            config_file = handle.name

        try:
            self.assertFalse(validator.validate_config_schema(config_file))
        finally:
            os.unlink(config_file)

    def test_yaml_config_generation_round_trip(self):
        self.assertTrue(validator.test_yaml_config_generation())

    def test_validate_example_config_and_docs(self):
        self.assertTrue(validator.validate_example_config())
        self.assertTrue(validator.check_documentation())


if __name__ == "__main__":
    unittest.main()