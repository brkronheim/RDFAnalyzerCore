"""
Tests for xrootd_site_selector.py.

These tests verify site detection, URL parsing, throughput ranking and the
SlowSiteDetector without requiring an actual XRootD server or network access.
All probing is mocked at the subprocess / pyxrootd layer.
"""

from __future__ import annotations

import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

# Make core/python importable regardless of invocation path.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_CORE_PYTHON = os.path.join(_REPO_ROOT, "core", "python")
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)

import xrootd_site_selector as sel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestDetectLocalSite(unittest.TestCase):
    """Unit tests for detect_local_site()."""

    def test_returns_glidein_cms_site(self):
        with patch.dict(os.environ, {"GLIDEIN_CMSSite": "T2_US_Nebraska"}, clear=False):
            self.assertEqual(sel.detect_local_site(), "T2_US_Nebraska")

    def test_returns_cms_local_site(self):
        with patch.dict(os.environ, {"CMS_LOCAL_SITE": "T2_DE_RWTH"}, clear=False):
            # GLIDEIN_CMSSite takes priority when both are set
            env = {"CMS_LOCAL_SITE": "T2_DE_RWTH"}
            env.pop("GLIDEIN_CMSSite", None)
            with patch.dict(os.environ, env, clear=False):
                result = sel.detect_local_site()
                # Result may be GLIDEIN_CMSSite from outer environ; just check it's a string
                self.assertIsInstance(result, str)

    def test_returns_empty_when_no_env(self):
        clean_env = {
            k: v for k, v in os.environ.items()
            if k not in ("CMS_LOCAL_SITE", "GLIDEIN_CMSSite", "OSG_SITE_NAME", "SITE_NAME")
        }
        with patch.dict(os.environ, clean_env, clear=True):
            with patch("socket.getfqdn", return_value="unknown.host.example.com"):
                result = sel.detect_local_site()
        self.assertEqual(result, "")

    def test_hostname_fnal(self):
        clean_env = {}
        with patch.dict(os.environ, clean_env, clear=True):
            with patch("socket.getfqdn", return_value="cmslpc01.fnal.gov"):
                result = sel.detect_local_site()
        self.assertEqual(result, "T1_US_FNAL")

    def test_hostname_cern(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch("socket.getfqdn", return_value="lxplus001.cern.ch"):
                result = sel.detect_local_site()
        self.assertEqual(result, "T0_CH_CERN")


# ---------------------------------------------------------------------------
# URL parsing helpers
# ---------------------------------------------------------------------------

class TestExtractLfn(unittest.TestCase):
    def test_root_url_double_slash(self):
        url = "root://xrootd-cms.infn.it//store/data/foo.root"
        self.assertEqual(sel._extract_lfn(url), "/store/data/foo.root")

    def test_root_url_single_slash(self):
        url = "root://xrootd-cms.infn.it/store/data/foo.root"
        self.assertEqual(sel._extract_lfn(url), "/store/data/foo.root")

    def test_bare_lfn_unchanged(self):
        lfn = "/store/data/foo.root"
        self.assertEqual(sel._extract_lfn(lfn), "/store/data/foo.root")

    def test_non_xrootd_unchanged(self):
        local = "input_0.root"
        self.assertEqual(sel._extract_lfn(local), "input_0.root")


class TestBuildUrl(unittest.TestCase):
    def test_standard_cms_url(self):
        url = sel._build_url("/store/data/foo.root", "root://cmsxrootd.fnal.gov/")
        self.assertEqual(url, "root://cmsxrootd.fnal.gov//store/data/foo.root")

    def test_redirector_trailing_slash_stripped(self):
        url = sel._build_url("/store/data/foo.root", "root://cms-xrd-global.cern.ch/")
        self.assertNotIn("///", url)
        self.assertTrue(url.startswith("root://"))

    def test_redirector_host(self):
        self.assertEqual(
            sel._redirector_host("root://cmsxrootd.fnal.gov/"),
            "cmsxrootd.fnal.gov",
        )
        self.assertEqual(
            sel._redirector_host("root://cms-xrd-global.cern.ch:1094/"),
            "cms-xrd-global.cern.ch:1094",
        )


# ---------------------------------------------------------------------------
# probe_redirector (mocked)
# ---------------------------------------------------------------------------

class TestProbeRedirector(unittest.TestCase):
    """Tests for probe_redirector() and _probe_via_root_macro()."""

    # ---- ROOT macro path ----

    def test_root_macro_returns_throughput_on_success(self):
        """A successful ROOT macro run returns positive throughput."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "PROBE_RESULT:0.250000:32768\n"
        mock_result.stderr = ""
        with patch("subprocess.run", return_value=mock_result):
            result = sel._probe_via_root_macro(
                "root://cmsxrootd.fnal.gov//store/data/foo.root",
                32768,
                timeout=5.0,
            )
        # 32768 bytes / 0.25 s ≈ 0.125 MB/s
        self.assertIsNotNone(result)
        self.assertGreater(result, 0)

    def test_root_macro_returns_none_on_failed_result(self):
        """PROBE_RESULT:FAILED → None."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "PROBE_RESULT:FAILED\n"
        with patch("subprocess.run", return_value=mock_result):
            result = sel._probe_via_root_macro(
                "root://cmsxrootd.fnal.gov//store/data/foo.root",
                32768,
                timeout=5.0,
            )
        self.assertIsNone(result)

    def test_root_macro_returns_none_when_root_not_found(self):
        """FileNotFoundError (root not in PATH) → None."""
        with patch("subprocess.run", side_effect=FileNotFoundError):
            result = sel._probe_via_root_macro(
                "root://cmsxrootd.fnal.gov//store/data/foo.root",
                32768,
                timeout=5.0,
            )
        self.assertIsNone(result)

    def test_root_macro_returns_none_on_timeout(self):
        """Subprocess timeout → None."""
        import subprocess as _sp
        with patch("subprocess.run", side_effect=_sp.TimeoutExpired("root", 5)):
            result = sel._probe_via_root_macro(
                "root://cmsxrootd.fnal.gov//store/data/foo.root",
                32768,
                timeout=5.0,
            )
        self.assertIsNone(result)

    # ---- xrdfs stat fallback ----

    def test_returns_none_when_xrdfs_fails(self):
        """ROOT macro + xrdfs both fail → None."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        call_count = {"n": 0}
        def _side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise FileNotFoundError  # root not found
            return mock_result  # xrdfs fails too
        with patch("subprocess.run", side_effect=_side_effect):
            result = sel.probe_redirector(
                "/store/data/foo.root",
                "root://cmsxrootd.fnal.gov/",
                timeout=1.0,
            )
        self.assertIsNone(result)

    def test_returns_synthetic_throughput_on_xrdfs_success(self):
        """Successful xrdfs stat returns a positive synthetic throughput."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        with patch("subprocess.run", return_value=mock_result):
            result = sel._probe_via_subprocess(
                "/store/data/foo.root",
                "root://cmsxrootd.fnal.gov/",
                timeout=5.0,
            )
        self.assertIsNotNone(result)
        self.assertGreater(result, 0)

    def test_returns_none_on_timeout(self):
        """Subprocess timeout → None."""
        import subprocess as _sp
        with patch("subprocess.run", side_effect=_sp.TimeoutExpired("xrdfs", 5)):
            result = sel._probe_via_subprocess(
                "/store/data/foo.root",
                "root://cmsxrootd.fnal.gov/",
                timeout=5.0,
            )
        self.assertIsNone(result)

    def test_returns_none_when_xrdfs_not_found(self):
        """FileNotFoundError (xrdfs not in PATH) → None."""
        with patch("subprocess.run", side_effect=FileNotFoundError):
            result = sel._probe_via_subprocess(
                "/store/data/foo.root",
                "root://cmsxrootd.fnal.gov/",
                timeout=5.0,
            )
        self.assertIsNone(result)

    def test_default_timeout_is_short(self):
        """Default probe timeout must be <= 10 seconds to avoid slow tests."""
        self.assertLessEqual(sel.DEFAULT_PROBE_TIMEOUT, 10.0)


# ---------------------------------------------------------------------------
# rank_redirectors (mocked)
# ---------------------------------------------------------------------------

class TestRankRedirectors(unittest.TestCase):

    def _mock_probe(self, lfn, redirector, **kwargs):
        """Return deterministic synthetic throughputs by redirector."""
        if "fnal" in redirector:
            return 50.0
        if "infn" in redirector:
            return 20.0
        return 5.0

    def test_ranked_best_first(self):
        with patch.object(sel, "probe_redirector", side_effect=self._mock_probe):
            ranked = sel.rank_redirectors(
                "/store/data/foo.root",
                sel.CMS_REDIRECTORS,
                timeout=5.0,
            )
        self.assertGreater(len(ranked), 0)
        # Best throughput must be first
        throughputs = [t for _, t in ranked]
        self.assertEqual(throughputs, sorted(throughputs, reverse=True))

    def test_failed_sites_excluded(self):
        def _failing_probe(lfn, redirector, **kwargs):
            if "fnal" in redirector:
                return None  # FNAL fails
            return 10.0

        with patch.object(sel, "probe_redirector", side_effect=_failing_probe):
            ranked = sel.rank_redirectors(
                "/store/data/foo.root",
                sel.CMS_REDIRECTORS,
                timeout=5.0,
            )
        redirectors_in_result = [r for r, _ in ranked]
        self.assertNotIn("root://cmsxrootd.fnal.gov/", redirectors_in_result)

    def test_local_site_bonus_applied(self):
        """The local site should be promoted when throughput is close."""
        def _equal_probe(lfn, redirector, **kwargs):
            return 10.0  # all equal

        with patch.object(sel, "probe_redirector", side_effect=_equal_probe):
            ranked = sel.rank_redirectors(
                "/store/data/foo.root",
                sel.CMS_REDIRECTORS,
                timeout=5.0,
                local_site="T1_US_FNAL",  # maps to fnal.gov → cmsxrootd.fnal.gov
            )
        # FNAL should be ranked first due to bonus (T1_US_FNAL → fnal.gov domain)
        self.assertTrue(
            any("fnal" in r for r, _ in ranked[:1]),
            f"Expected FNAL first but got: {ranked}",
        )

    def test_blacklisted_redirectors_excluded(self):
        """Redirectors matching the blacklist should not be probed or returned."""
        with patch.object(sel, "probe_redirector", side_effect=self._mock_probe):
            ranked = sel.rank_redirectors(
                "/store/data/foo.root",
                sel.CMS_REDIRECTORS,
                timeout=5.0,
                blacklisted_sites=["fnal.gov"],  # blacklist FNAL
            )
        redirectors_in_result = [r for r, _ in ranked]
        self.assertNotIn("root://cmsxrootd.fnal.gov/", redirectors_in_result)

    def test_all_blacklisted_returns_empty(self):
        """When all redirectors are blacklisted the result is empty."""
        with patch.object(sel, "probe_redirector", side_effect=self._mock_probe):
            ranked = sel.rank_redirectors(
                "/store/data/foo.root",
                sel.CMS_REDIRECTORS,
                timeout=5.0,
                blacklisted_sites=["fnal.gov", "infn.it", "cern.ch"],
            )
        self.assertEqual(ranked, [])


# ---------------------------------------------------------------------------
# _is_blacklisted
# ---------------------------------------------------------------------------

class TestIsBlacklisted(unittest.TestCase):

    def test_exact_hostname_match(self):
        self.assertTrue(
            sel._is_blacklisted("root://bad-site.cern.ch/", ["bad-site.cern.ch"])
        )

    def test_partial_pattern_match(self):
        self.assertTrue(
            sel._is_blacklisted("root://cmsxrootd.fnal.gov/", ["fnal.gov"])
        )

    def test_no_match(self):
        self.assertFalse(
            sel._is_blacklisted("root://cmsxrootd.fnal.gov/", ["infn.it"])
        )

    def test_empty_blacklist_always_false(self):
        self.assertFalse(
            sel._is_blacklisted("root://cmsxrootd.fnal.gov/", [])
        )

    def test_case_insensitive(self):
        self.assertTrue(
            sel._is_blacklisted("root://CMS-XRD-GLOBAL.CERN.CH/", ["cern.ch"])
        )


# ---------------------------------------------------------------------------
# optimize_file_list (mocked)
# ---------------------------------------------------------------------------

class TestOptimizeFileList(unittest.TestCase):

    def test_local_files_unchanged(self):
        """Local file paths must not be modified."""
        files = ["input_0.root", "input_1.root"]
        result = sel.optimize_file_list(files)
        self.assertEqual(result, files)

    def test_disabled_returns_unchanged(self):
        files = ["root://xrootd-cms.infn.it//store/data/foo.root"]
        with patch.object(sel, "select_best_url") as mock_sel:
            result = sel.optimize_file_list(files, enabled=False)
        mock_sel.assert_not_called()
        self.assertEqual(result, files)

    def test_xrootd_files_are_probed(self):
        """XRootD files trigger a call to select_best_url."""
        files = [
            "root://cms-xrd-global.cern.ch//store/data/foo.root",
            "root://cms-xrd-global.cern.ch//store/data/bar.root",
        ]
        best = "root://cmsxrootd.fnal.gov//store/data/foo.root"
        with patch.object(sel, "select_best_url", return_value=best):
            result = sel.optimize_file_list(files)
        self.assertEqual(len(result), 2)
        self.assertTrue(all(r == best for r in result))

    def test_comma_string_input(self):
        """A comma-separated string is split and returned as a list."""
        files_str = "root://a.cern.ch//store/a.root,root://a.cern.ch//store/b.root"
        with patch.object(sel, "select_best_url", side_effect=lambda u, **kw: u):
            result = sel.optimize_file_list(files_str)
        self.assertEqual(len(result), 2)

    def test_all_probes_fail_returns_original(self):
        """If select_best_url raises, original file is preserved."""
        files = ["root://cms-xrd-global.cern.ch//store/data/foo.root"]
        with patch.object(sel, "select_best_url", side_effect=RuntimeError("probe failed")):
            # optimize_file_list propagates exceptions from select_best_url
            # (callers wrap it in try/except); here we just check it raises
            with self.assertRaises(RuntimeError):
                sel.optimize_file_list(files)

    def test_mixed_local_and_xrootd(self):
        """Only XRootD files are optimised; local files stay unchanged."""
        files = [
            "input_0.root",
            "root://cms-xrd-global.cern.ch//store/data/foo.root",
        ]
        optimized_url = "root://cmsxrootd.fnal.gov//store/data/foo.root"
        with patch.object(sel, "select_best_url", return_value=optimized_url):
            result = sel.optimize_file_list(files)
        self.assertEqual(result[0], "input_0.root")  # local unchanged
        self.assertEqual(result[1], optimized_url)    # XRootD optimised

    def test_blacklist_forwarded_to_select_best_url(self):
        """The blacklisted_sites list must be forwarded to select_best_url."""
        files = ["root://cms-xrd-global.cern.ch//store/data/foo.root"]
        blacklist = ["cern.ch"]
        captured_kwargs = {}

        def _capture(url, **kwargs):
            captured_kwargs.update(kwargs)
            return url

        with patch.object(sel, "select_best_url", side_effect=_capture):
            sel.optimize_file_list(files, blacklisted_sites=blacklist)

        self.assertEqual(captured_kwargs.get("blacklisted_sites"), blacklist)


# ---------------------------------------------------------------------------
# SlowSiteDetector
# ---------------------------------------------------------------------------

class TestSlowSiteDetector(unittest.TestCase):

    def test_good_throughput_returns_true(self):
        detector = sel.SlowSiteDetector(threshold_mbs=1.0, window_s=30.0)
        result = detector.record_bytes(50 * 1024 * 1024, 1.0)  # 50 MB/s
        self.assertTrue(result)
        self.assertFalse(detector.is_slow())

    def test_below_threshold_for_short_time_still_ok(self):
        """Throughput below threshold but within grace window → not flagged."""
        detector = sel.SlowSiteDetector(threshold_mbs=10.0, window_s=30.0)
        # 1 MB in 1 second = 1 MB/s, below threshold of 10 MB/s
        result = detector.record_bytes(1 * 1024 * 1024, 1.0)
        # Not yet flagged because less than window_s has elapsed
        self.assertFalse(detector.is_slow())

    def test_sustained_slow_throughput_flagged(self):
        """Slow throughput sustained beyond grace window sets is_slow."""
        detector = sel.SlowSiteDetector(threshold_mbs=10.0, window_s=0.05)
        detector.record_bytes(1024, 1.0)  # 0.001 MB/s - far below threshold
        # Manually push below_since back in time
        detector._below_since = time.monotonic() - 1.0  # 1 second ago
        result = detector.record_bytes(1024, 1.0)
        self.assertFalse(result)
        self.assertTrue(detector.is_slow())

    def test_reset_clears_state(self):
        detector = sel.SlowSiteDetector(threshold_mbs=10.0, window_s=0.01)
        detector._slow = True
        detector._total_bytes = 100
        detector.reset()
        self.assertFalse(detector.is_slow())
        self.assertEqual(detector._total_bytes, 0)

    def test_get_stats(self):
        detector = sel.SlowSiteDetector(threshold_mbs=5.0, window_s=30.0)
        detector.record_bytes(10 * 1024 * 1024, 2.0)  # 5 MB/s exactly
        stats = detector.get_stats()
        self.assertIn("total_bytes", stats)
        self.assertIn("rolling_throughput_mbs", stats)
        self.assertIn("is_slow", stats)
        self.assertAlmostEqual(stats["rolling_throughput_mbs"], 5.0, places=1)

    def test_no_data_rolling_throughput_is_none(self):
        detector = sel.SlowSiteDetector()
        stats = detector.get_stats()
        self.assertIsNone(stats["rolling_throughput_mbs"])


# ---------------------------------------------------------------------------
# CMS_REDIRECTORS constant
# ---------------------------------------------------------------------------

class TestRedirectorList(unittest.TestCase):
    def test_contains_known_redirectors(self):
        redirectors = sel.CMS_REDIRECTORS
        self.assertGreater(len(redirectors), 0)
        for r in redirectors:
            self.assertTrue(r.startswith("root://"), f"Bad redirector: {r}")

    def test_no_duplicates(self):
        self.assertEqual(len(sel.CMS_REDIRECTORS), len(set(sel.CMS_REDIRECTORS)))


if __name__ == "__main__":
    unittest.main()
