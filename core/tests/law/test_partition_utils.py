from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "core", "python", "law")
if _LAW_DIR not in sys.path:
    sys.path.insert(0, _LAW_DIR)

import partition_utils


class TestPartitionUtils(unittest.TestCase):
    def test_file_mode_deduplicates_and_sorts(self):
        parts = partition_utils._make_partitions(
            ["root://x//b.root", "root://x//a.root", "root://x//a.root"],
            mode="file",
            files_per_job=2,
            entries_per_job=100,
        )
        self.assertEqual([p["files"] for p in parts], ["root://x//a.root", "root://x//b.root"])
        self.assertTrue(all(p["first_entry"] == 0 and p["last_entry"] == 0 for p in parts))

    def test_entry_range_empty_file_falls_back_to_full_file_partition(self):
        with patch.object(partition_utils, "_query_tree_entries", return_value=0):
            parts = partition_utils._make_partitions(
                ["root://x//empty.root"],
                mode="entry_range",
                files_per_job=1,
                entries_per_job=100,
            )

        self.assertEqual(parts, [{"files": "root://x//empty.root", "first_entry": 0, "last_entry": 0}])

    def test_unknown_mode_mentions_valid_choices(self):
        with self.assertRaises(ValueError) as ctx:
            partition_utils._make_partitions([], mode="bad", files_per_job=1, entries_per_job=1)
        self.assertIn("file_group", str(ctx.exception))
        self.assertIn("entry_range", str(ctx.exception))
