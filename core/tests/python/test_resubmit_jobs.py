import os
import sys
import tempfile
import unittest
from unittest.mock import patch

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CORE_PYTHON = os.path.join(_REPO_ROOT, "core", "python")
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)

import resubmit_jobs


class TestResubmitJobs(unittest.TestCase):
    def test_parse_job_list_handles_ranges_and_duplicates(self):
        self.assertEqual(resubmit_jobs.parse_job_list("0,2-4,2"), [0, 2, 3, 4])
        self.assertEqual(resubmit_jobs.parse_job_list(" 5 , 7-8 "), [5, 7, 8])

    def test_make_per_job_submit_doubles_maxruntime(self):
        template = "executable=/bin/true\njob_$(Process)/output.root\n+MaxRuntime=100\nqueue 10\n"
        result = resubmit_jobs.make_per_job_submit(template, job_index=3, double_maxruntime=True)
        self.assertIn("job_3/output.root", result)
        self.assertIn("+MaxRuntime=200", result)
        self.assertIn("queue 1", result)

    def test_main_dry_run_skips_missing_jobs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            submit_path = os.path.join(tmpdir, "condor_submit.sub")
            with open(submit_path, "w") as fh:
                fh.write("executable=/bin/true\n+MaxRuntime=100\nqueue 10\n")
            os.makedirs(os.path.join(tmpdir, "job_1"), exist_ok=True)
            with open(os.path.join(tmpdir, "job_1", "submit_config.txt"), "w") as fh:
                fh.write("dummy")

            rc = resubmit_jobs.main([tmpdir, "--jobs", "0,1", "--dry-run", "--yes"])
            self.assertEqual(rc, 0)
