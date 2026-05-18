#!/usr/bin/env python3
"""Tests for the XRDFS file-discovery law module."""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch


_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "law")
_CORE_PYTHON = os.path.join(_REPO_ROOT, "core", "python")
for _path in (_LAW_DIR, _CORE_PYTHON):
    if _path not in sys.path:
        sys.path.insert(0, _path)


try:
    import luigi  # noqa: F401
    import law  # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False


_SKIP_MSG = "law and luigi packages not available"


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestXRDFSHelpers(unittest.TestCase):
    def _import(self):
        import xrdfs_tasks

        return xrdfs_tasks

    def test_should_ignore_eos_sys_entries(self):
        mod = self._import()
        self.assertTrue(mod._should_ignore_xrdfs_entry("/eos/user/a/.sys.v#.nano_1.root"))
        self.assertFalse(mod._should_ignore_xrdfs_entry("/eos/user/a/nano_1.root"))

    def test_xrdfs_list_files_skips_eos_sys_entries(self):
        mod = self._import()

        class _Completed:
            def __init__(self, stdout: str):
                self.stdout = stdout
                self.stderr = ""
                self.returncode = 0

        listing = "\n".join(
            [
                "drwxr-x--- user group 0 2026-04-08 00:00:00 /eos/user/a/sample/.sys.v#.nano_1.root",
                "-rw-r--r-- user group 10 2026-04-08 00:00:00 /eos/user/a/sample/nano_1.root",
                "-rw-r--r-- user group 10 2026-04-08 00:00:00 /eos/user/a/sample/nano_2.root",
            ]
        )

        with patch("xrdfs_tasks.subprocess.run", return_value=_Completed(listing)):
            files = mod.xrdfs_list_files(
                server="root://eosuser.cern.ch/",
                remote_path="/eos/user/a/sample",
                recursive=False,
            )

        self.assertEqual(
            files,
            [
                "root://eosuser.cern.ch/eos/user/a/sample/nano_1.root",
                "root://eosuser.cern.ch/eos/user/a/sample/nano_2.root",
            ],
        )