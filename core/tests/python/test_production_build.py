import json
from pathlib import Path
import shutil
import tempfile
import unittest

from production_build import build_verified, inspect_build, verify_build_receipt
from run_bundle import file_digest


@unittest.skipUnless(
    all(shutil.which(tool) for tool in ("cmake", "ctest", "c++", "readelf", "ldd")),
    "native build checks require CMake, CTest, ELF tools and a C++ compiler",
)
class TestProductionBuild(unittest.TestCase):
    def setUp(self):
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        self.base = Path(work.name)
        self.source = self.base / "src"
        self.source.mkdir()
        (self.source / "CMakeLists.txt").write_text(
            """cmake_minimum_required(VERSION 3.19)
project(fixture LANGUAGES CXX)
enable_testing()
add_executable(analysis analysis.cc)
add_executable(check_executable EXCLUDE_FROM_ALL analysis.cc)
add_test(NAME analysis_smoke COMMAND check_executable)
"""
        )
        self.code = self.source / "analysis.cc"
        self.code.write_text("int main() { return 0; }\n")
        self.executable = self.base / "build" / "analysis"
        self.raw = {
            "source_directory": "src",
            "build_directory": "build",
            "target": "analysis",
            "configure_args": ["-DCMAKE_BUILD_TYPE=Release"],
            "inputs": {"analysis": "src/analysis.cc"},
            "test_targets": ["check_executable"],
            "ctest_regex": "^analysis_smoke$",
        }

    def inspect(self, executable=None, **overrides):
        return inspect_build(
            dict(self.raw, **overrides),
            base=self.base,
            executable=self.executable if executable is None else executable,
        )

    def test_fresh_configure_build_and_verify(self):
        plan = self.inspect()
        self.assertFalse(self.executable.parent.exists())
        self.assertEqual(plan["input_hashes"]["analysis"], file_digest(self.code))
        receipt = build_verified(plan, self.executable, self.base / "logs")
        self.assertEqual(receipt["executable_sha256"], file_digest(self.executable))
        test_log = Path(receipt["validation"]["ctest_log"]) / "stdout.log"
        self.assertIn("analysis_smoke", test_log.read_text())
        self.assertTrue(Path(receipt["validation"]["link"]).is_file())
        receipt_path = self.base / "receipt.json"
        receipt_path.write_text(json.dumps(receipt))
        self.assertEqual(verify_build_receipt(receipt_path, plan, self.executable), receipt)
        # An operator can reuse an existing CMake tree using only its directory.
        self.raw.pop("source_directory")
        self.assertEqual(self.inspect(), plan)

    def test_rejects_existing_binary_from_a_different_target(self):
        unrelated = Path(shutil.which("ctest"))
        with self.assertRaisesRegex(ValueError, "not the executable artifact of analysis"):
            build_verified(self.inspect(unrelated), unrelated, self.base / "logs")
        self.assertFalse(self.executable.exists())

    def test_empty_ctest_selection_fails(self):
        plan = self.inspect(test_targets=[], ctest_regex="^missing_test$")
        with self.assertRaisesRegex(ValueError, "matched no tests"):
            build_verified(plan, self.executable, self.base / "logs")
        self.assertTrue(self.executable.is_file())

    def test_declared_source_changes_and_product_changes_are_rejected(self):
        plan = self.inspect(test_targets=[], ctest_regex=None)
        receipt = build_verified(plan, self.executable, self.base / "logs")
        source = self.code.read_text()
        self.code.write_text("int main() { return 1; }\n")
        with self.assertRaisesRegex(ValueError, "declared build input changed since inspection"):
            verify_build_receipt(receipt, plan, self.executable)
        self.code.write_text(source)
        with self.executable.open("ab") as stream:
            stream.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "native build product is missing or has changed"):
            verify_build_receipt(receipt, plan, self.executable)

    def test_detects_cmake_target_source_edits_without_an_explicit_input(self):
        plan = self.inspect(inputs={}, test_targets=[], ctest_regex=None)
        receipt = build_verified(plan, self.executable, self.base / "logs")
        self.assertEqual(receipt["target_source_hashes"][str(self.code)], file_digest(self.code))
        self.code.write_text("int main() { return 1; }\n")
        with self.assertRaisesRegex(ValueError, "CMake-listed target source changed"):
            verify_build_receipt(receipt, plan, self.executable)
