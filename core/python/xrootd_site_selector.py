"""
XRootD site selection and performance monitoring for RDFAnalyzerCore.

When stage-in is not applied, this module ranks available XRootD sites by
reading a small amount of data from each and selects the fastest redirector
for each file.  A local-site preference is applied when the job is running
close to a storage element that performs well.  Known-problematic sites can
be excluded via the *blacklisted_sites* parameter.

For single-threaded reads a :class:`SlowSiteDetector` can monitor ongoing
throughput and transparently flag slow sites mid-run so the caller can retry
with a better source.

Typical usage::

    from xrootd_site_selector import optimize_file_list

    original_files = cfg.get("fileList", "").split(",")
    optimized_files = optimize_file_list(
        original_files,
        blacklisted_sites=["T2_Bad_Site"],  # optional
    )
    cfg["fileList"] = ",".join(optimized_files)

Probing uses a ROOT macro (``root -b -q -l macro.C``) to open the file and
read a small amount of data, which measures real read throughput in the CMS
grid environment where ROOT is always available.  When ROOT is not in the
PATH the module falls back to ``xrdfs stat`` latency probing.
"""

from __future__ import annotations

import logging
import os
import re
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known CMS XRootD redirectors (tried in order when no site override is given)
# ---------------------------------------------------------------------------

#: Ordered list of CMS XRootD redirectors to probe.  Tier-1/global entries
#: come first so they are tried before purely regional fallbacks.
CMS_REDIRECTORS: list[str] = [
    "root://cmsxrootd.fnal.gov/",       # FNAL (US Tier-1)
    "root://xrootd-cms.infn.it/",       # INFN (European Tier-1)
    "root://cms-xrd-global.cern.ch/",   # Global CMS redirector (fallback)
]

# Bytes to read when probing a site (32 KiB gives a decent bandwidth sample
# without unduly stressing the server or wasting time on large files).
DEFAULT_PROBE_BYTES: int = 32 * 1024

# Per-site probe timeout in seconds.  Kept short so a slow/unreachable site
# does not significantly delay the site-ranking step before the job starts.
DEFAULT_PROBE_TIMEOUT: float = 5.0

# Maximum wall-clock time for the entire site-ranking step.
DEFAULT_RANK_TIMEOUT: float = 60.0

# Throughput bonus (multiplicative) applied to the local site's measured
# value before ranking so it is preferred when approximately equivalent.
LOCAL_SITE_BONUS: float = 1.25

# Minimum acceptable throughput (MB/s) below which a site is considered slow
# during the :class:`SlowSiteDetector` monitoring window.
DEFAULT_SLOW_THRESHOLD_MBS: float = 1.0

# Scaling constant for the latency-to-synthetic-throughput conversion used in
# ``_probe_via_subprocess``.  A site that responds to xrdfs stat in 1 second
# receives a synthetic value of LATENCY_SYNTHETIC_SCALE MB/s; faster sites
# receive proportionally higher values.  The absolute magnitude is less
# important than the relative ordering it produces.
_LATENCY_SYNTHETIC_SCALE: float = 10.0

# Environment variable names that may contain the local CMS site name.
_SITE_ENV_VARS: tuple[str, ...] = (
    "CMS_LOCAL_SITE",    # set by some grid job environments
    "GLIDEIN_CMSSite",   # HTCondor pilot job attributes
    "OSG_SITE_NAME",     # OSG glidein environments
    "SITE_NAME",         # generic site name variable
)

# Mapping from CMS site name fragments to the redirector host domain that
# serves that region.  Used when applying the local-site bonus during ranking.
_SITE_TO_REDIRECTOR_DOMAIN: dict[str, str] = {
    "T1_US_FNAL":   "fnal.gov",
    "T0_CH_CERN":   "cern.ch",
    "T2_US_":       "fnal.gov",   # US Tier-2 sites → prefer FNAL redirector
    "T2_DE_":       "infn.it",    # German sites → prefer European redirector
    "T2_IT_":       "infn.it",
    "T2_FR_":       "infn.it",
    "T2_UK_":       "infn.it",
    "T2_ES_":       "infn.it",
    "T2_CH_":       "cern.ch",
    "T3_US_":       "fnal.gov",
}


def _local_site_matches_redirector(local_site: str, redirector: str) -> bool:
    """Return True when *redirector* is the preferred gateway for *local_site*.

    Checks the :data:`_SITE_TO_REDIRECTOR_DOMAIN` mapping first, then falls
    back to a simple substring check on the redirector URL.
    """
    if not local_site:
        return False
    redir_lower = redirector.lower()
    # Exact prefix mapping.
    for site_prefix, domain in _SITE_TO_REDIRECTOR_DOMAIN.items():
        if local_site.startswith(site_prefix) and domain in redir_lower:
            return True
    # Fallback: direct substring match (covers site-specific test redirectors
    # that embed the site name in the URL, e.g. /store/test/xrootd/<site>/).
    return local_site in redirector


# ---------------------------------------------------------------------------
# Site detection
# ---------------------------------------------------------------------------

def detect_local_site() -> str:
    """Return the local CMS site name (e.g. ``T2_US_Nebraska``), or ``""``.

    Checks well-known environment variables in order, then falls back to a
    simple hostname heuristic for CERN and FNAL.

    Returns
    -------
    str
        Local CMS site name, or empty string when not determinable.
    """
    for var in _SITE_ENV_VARS:
        value = os.environ.get(var, "").strip()
        if value:
            return value

    # Hostname-based heuristic as last resort.
    try:
        hostname = socket.getfqdn().lower()
        if hostname.endswith(".fnal.gov") or hostname == "fnal.gov":
            return "T1_US_FNAL"
        if hostname.endswith(".cern.ch") or hostname == "cern.ch":
            return "T0_CH_CERN"
    except Exception:
        pass

    return ""


# ---------------------------------------------------------------------------
# URL parsing helpers
# ---------------------------------------------------------------------------

def _extract_lfn(url: str) -> str:
    """Extract the logical file name (LFN) from an XRootD URL or bare path.

    Parameters
    ----------
    url:
        An XRootD URL like ``root://host//store/data/foo.root`` or a bare
        path like ``/store/data/foo.root``.

    Returns
    -------
    str
        The LFN beginning with ``/``, e.g. ``/store/data/foo.root``.
        Returns the original *url* unchanged if it does not look like an
        XRootD URL or absolute path.

    Examples
    --------
    >>> _extract_lfn("root://xrootd-cms.infn.it//store/data/foo.root")
    '/store/data/foo.root'
    >>> _extract_lfn("/store/data/foo.root")
    '/store/data/foo.root'
    """
    if url.startswith("root://"):
        m = re.match(r"root://[^/]+/(.*)", url)
        if m:
            path = m.group(1).lstrip("/")
            return "/" + path
    return url


def _build_url(lfn: str, redirector: str) -> str:
    """Construct a full XRootD URL from *lfn* and *redirector*.

    Ensures the resulting URL has exactly two slashes after the host (the
    XRootD convention required by most CMS storage systems).

    Parameters
    ----------
    lfn:
        Logical file name (must start with ``/``).
    redirector:
        XRootD redirector, e.g. ``root://cmsxrootd.fnal.gov/``.

    Returns
    -------
    str
        Full URL like ``root://cmsxrootd.fnal.gov//store/data/foo.root``.
    """
    base = redirector.rstrip("/")
    path = "/" + lfn.lstrip("/")
    return f"{base}/{path}"


def _redirector_host(redirector: str) -> str:
    """Extract the host (and optional port) from an XRootD redirector URL.

    >>> _redirector_host("root://cmsxrootd.fnal.gov/")
    'cmsxrootd.fnal.gov'
    >>> _redirector_host("root://cms-xrd-global.cern.ch:1094/")
    'cms-xrd-global.cern.ch:1094'
    """
    m = re.match(r"root://([^/]+)", redirector)
    return m.group(1) if m else redirector


def _is_blacklisted(redirector: str, blacklisted_sites: list[str]) -> bool:
    """Return ``True`` when *redirector* matches any entry in *blacklisted_sites*.

    Each entry in *blacklisted_sites* is tested as a case-insensitive
    substring of the redirector URL, so it can be a CMS site name fragment
    (e.g. ``"T2_IT_"``) or a hostname pattern (e.g. ``"bad-site.cern.ch"``).

    Parameters
    ----------
    redirector:
        XRootD redirector URL to test.
    blacklisted_sites:
        List of patterns to match against.  An empty list always returns
        ``False``.

    Returns
    -------
    bool
        ``True`` when the redirector should be excluded from probing.
    """
    redir_lower = redirector.lower()
    for pattern in blacklisted_sites:
        if pattern.lower() in redir_lower:
            return True
    return False



# ---------------------------------------------------------------------------
# Probing
# ---------------------------------------------------------------------------

# ROOT macro template used by _probe_via_root_macro.  The macro opens the
# file via TFile::Open (which exercises the XRootD transport), reads a small
# number of bytes to measure actual transfer throughput, and prints a single
# structured result line to stdout.
#
# NOTE: TFile::ReadBuffer uses the ROOT/POSIX errno convention where
# kFALSE (0) means SUCCESS and kTRUE (1) means FAILURE.  The check below
# uses ``if (ok)`` (i.e. if the call returned non-zero / kTRUE) to detect
# errors.
#
# NOTE: If you update this template also update ROOT_MACRO_TMPL in
# submission_backend.py::xrootd_optimize_block() which embeds a copy for
# execution on Condor worker nodes.
_ROOT_PROBE_MACRO_TMPL: str = """\
void probe_xrootd() {{
  const char* url = "{url}";
  Long64_t nbytes = {nbytes}LL;
  TStopwatch sw;
  sw.Start();
  TFile* f = TFile::Open(url, "READ");
  if (!f || f->IsZombie()) {{
    printf("PROBE_RESULT:FAILED\\n");
    if (f) {{ f->Close(); delete f; }}
    return;
  }}
  Long64_t sz = f->GetSize();
  Long64_t to_read = (sz > 0 && sz < nbytes) ? sz : nbytes;
  Bool_t ok = kFALSE;
  if (to_read > 0) {{
    char* buf = new char[to_read];
    ok = f->ReadBuffer(buf, 0LL, (Int_t)to_read);
    delete[] buf;
  }}
  sw.Stop();
  f->Close();
  delete f;
  if (ok) {{ printf("PROBE_RESULT:FAILED\\n"); return; }}
  double elapsed = sw.RealTime();
  if (elapsed > 0 && to_read > 0)
    printf("PROBE_RESULT:%.6f:%.0f\\n", elapsed, (double)to_read);
  else
    printf("PROBE_RESULT:FAILED\\n");
}}
"""


def _probe_via_root_macro(url: str, read_bytes: int, timeout: float) -> Optional[float]:
    """Probe *url* by running a temporary ROOT macro and return MB/s or ``None``.

    Generates a small ROOT C++ macro that opens the file via ``TFile::Open``
    and reads *read_bytes* bytes with ``TFile::ReadBuffer``, measures the
    elapsed wall-clock time, and prints a structured result line.  The macro
    is executed as a subprocess via ``root -b -q -l``.

    This is the preferred probing method in CMS grid environments where ROOT
    (and therefore XRootD) is always available.

    Parameters
    ----------
    url:
        Full XRootD URL (e.g. ``root://host//store/data/foo.root``).
    read_bytes:
        Number of bytes to read from the beginning of the file.
    timeout:
        Maximum seconds to allow for the entire ``root`` subprocess.

    Returns
    -------
    float or None
        Measured throughput in MB/s, or ``None`` when ROOT is unavailable,
        the file cannot be opened, or the probe times out.
    """
    import tempfile

    macro = _ROOT_PROBE_MACRO_TMPL.format(url=url, nbytes=read_bytes)
    macro_path: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".C", delete=False, prefix="xrd_probe_"
        ) as tmp:
            tmp.write(macro)
            macro_path = tmp.name

        result = subprocess.run(
            ["root", "-b", "-q", "-l", macro_path],
            capture_output=True,
            timeout=timeout,
            text=True,
        )

        for line in result.stdout.splitlines():
            if line.startswith("PROBE_RESULT:"):
                parts = line.split(":")
                if len(parts) >= 2 and parts[1] == "FAILED":
                    return None
                if len(parts) >= 3:
                    try:
                        elapsed = float(parts[1])
                        to_read = float(parts[2])
                        if elapsed > 0 and to_read > 0:
                            return to_read / (1024.0 * 1024.0) / elapsed
                    except ValueError:
                        pass
        return None

    except FileNotFoundError:
        logger.debug("root not found in PATH; cannot probe via ROOT macro")
        return None
    except subprocess.TimeoutExpired:
        logger.debug("ROOT macro probe timed out for %s", url)
        return None
    except Exception as exc:
        logger.debug("ROOT macro probe error for %s: %s", url, exc)
        return None
    finally:
        if macro_path is not None:
            try:
                os.unlink(macro_path)
            except OSError:
                pass


def _probe_via_subprocess(lfn: str, redirector: str, timeout: float) -> Optional[float]:
    """Probe *redirector* for *lfn* using ``xrdfs stat`` (latency-only proxy).

    When the XRootD Python bindings are not available this falls back to
    ``xrdfs stat`` which only measures metadata latency.  The result is a
    synthetic MB/s value that is useful for ranking but not directly
    comparable to actual read throughput.

    Returns ``None`` when the file is unreachable or ``xrdfs`` is not found.
    """
    host = _redirector_host(redirector)
    path = "/" + lfn.lstrip("/")

    t0 = time.perf_counter()
    try:
        result = subprocess.run(
            ["xrdfs", host, "stat", path],
            capture_output=True,
            timeout=timeout,
            text=True,
        )
        elapsed = time.perf_counter() - t0
        if result.returncode != 0:
            logger.debug("xrdfs stat failed for %s %s: %s", host, path,
                         result.stderr.strip())
            return None
        # Invert latency so that lower latency → higher synthetic throughput.
        synthetic = max(0.01, _LATENCY_SYNTHETIC_SCALE / elapsed)
        logger.debug("xrdfs stat %s %s -> latency=%.3fs synthetic=%.3f MB/s",
                     host, path, elapsed, synthetic)
        return synthetic
    except FileNotFoundError:
        logger.debug("xrdfs not found in PATH; cannot probe %s", redirector)
        return None
    except subprocess.TimeoutExpired:
        logger.debug("xrdfs stat timed out for %s %s", host, path)
        return None
    except Exception as exc:
        logger.debug("xrdfs stat error for %s %s: %s", host, path, exc)
        return None


def probe_redirector(
    lfn: str,
    redirector: str,
    read_bytes: int = DEFAULT_PROBE_BYTES,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
) -> Optional[float]:
    """Probe *redirector* for *lfn* and return throughput in MB/s or ``None``.

    Tries a ROOT macro first (``root -b -q -l``) to measure actual read
    throughput via ``TFile::Open`` and ``TFile::ReadBuffer``.  Falls back to
    ``xrdfs stat`` latency probing when ROOT is not available in the PATH.
    The per-probe *timeout* is applied to each attempt independently.

    Parameters
    ----------
    lfn:
        Logical file name starting with ``/`` (e.g. ``/store/data/foo.root``).
    redirector:
        XRootD redirector URL (e.g. ``root://cmsxrootd.fnal.gov/``).
    read_bytes:
        Number of bytes to read when using the ROOT macro path.
    timeout:
        Maximum seconds to wait for a single probe attempt.

    Returns
    -------
    float or None
        Measured (or synthetic) throughput in MB/s, or ``None`` when the
        site is unreachable or all probe attempts fail.
    """
    url = _build_url(lfn, redirector)

    # Prefer actual read measurement via ROOT macro (TFile).
    result = _probe_via_root_macro(url, read_bytes, timeout)
    if result is not None:
        return result

    # Fall back to latency-based probing via xrdfs subprocess.
    return _probe_via_subprocess(lfn, redirector, timeout)


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def rank_redirectors(
    lfn: str,
    redirectors: list[str],
    probe_bytes: int = DEFAULT_PROBE_BYTES,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
    local_site: str = "",
    max_workers: int = 6,
    blacklisted_sites: Optional[list[str]] = None,
) -> list[tuple[str, float]]:
    """Probe all *redirectors* in parallel and return them ranked by throughput.

    The local site's measured throughput is multiplied by :data:`LOCAL_SITE_BONUS`
    so it is preferred when approximately equal to other sites.  Redirectors
    that match any entry in *blacklisted_sites* are skipped before probing.

    Parameters
    ----------
    lfn:
        Logical file name to probe (``/store/...``).
    redirectors:
        List of XRootD redirector URLs to probe.
    probe_bytes:
        Bytes to read per probe (ROOT macro path only).
    timeout:
        Per-probe timeout in seconds.
    local_site:
        Local CMS site identifier (from :func:`detect_local_site`).  Used
        to apply a bonus to redirectors that serve the local site.
    max_workers:
        Maximum concurrent probe threads.
    blacklisted_sites:
        Patterns to skip (e.g. ``["T2_Bad_Site", "bad-host.cern.ch"]``).
        A redirector is excluded when any pattern is a case-insensitive
        substring of its URL.

    Returns
    -------
    list of (redirector, throughput_mbs)
        Pairs sorted best-first; sites that failed or were blacklisted are
        omitted.
    """
    results: list[tuple[str, float]] = []
    _blacklist = blacklisted_sites or []

    # Filter out blacklisted redirectors before any probing.
    active_redirectors = [
        r for r in redirectors if not _is_blacklisted(r, _blacklist)
    ]
    if not active_redirectors:
        logger.debug("All redirectors are blacklisted; returning empty ranking")
        return results

    def _probe_one(redir: str) -> tuple[str, Optional[float]]:
        tput = probe_redirector(lfn, redir, read_bytes=probe_bytes, timeout=timeout)
        return redir, tput

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_probe_one, r): r for r in active_redirectors}
        for future in as_completed(futures, timeout=timeout + 5):
            try:
                redir, tput = future.result()
            except Exception as exc:
                logger.debug("probe future error for %s: %s", futures[future], exc)
                continue
            if tput is None:
                continue
            # Apply local-site bonus using the site-to-redirector mapping.
            if local_site and _local_site_matches_redirector(local_site, redir):
                tput = tput * LOCAL_SITE_BONUS
            results.append((redir, tput))

    results.sort(key=lambda x: x[1], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Per-file selection
# ---------------------------------------------------------------------------

def select_best_url(
    url: str,
    redirectors: Optional[list[str]] = None,
    probe_bytes: int = DEFAULT_PROBE_BYTES,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
    local_site: str = "",
    blacklisted_sites: Optional[list[str]] = None,
) -> str:
    """Return the fastest XRootD URL for *url* among *redirectors*.

    If none of the redirectors can serve the file (all probes fail), the
    original *url* is returned unchanged so the analysis can still attempt
    to open it via the default redirector.

    Parameters
    ----------
    url:
        Original XRootD URL or bare LFN for the file.
    redirectors:
        Redirectors to probe.  Defaults to :data:`CMS_REDIRECTORS`.
    probe_bytes:
        Bytes to read per probe.
    timeout:
        Per-site probe timeout in seconds.
    local_site:
        Local CMS site name for bonus application.
    blacklisted_sites:
        Patterns for redirectors to skip entirely.

    Returns
    -------
    str
        XRootD URL for the best-performing site, or *url* on failure.
    """
    if redirectors is None:
        redirectors = CMS_REDIRECTORS

    lfn = _extract_lfn(url)

    # If the URL is not an XRootD path or bare /store/... path, leave it alone.
    if not (lfn.startswith("/store/") or lfn.startswith("/eos/")):
        return url

    ranked = rank_redirectors(
        lfn,
        redirectors,
        probe_bytes=probe_bytes,
        timeout=timeout,
        local_site=local_site,
        blacklisted_sites=blacklisted_sites,
    )

    if not ranked:
        logger.warning("All site probes failed for %s; using original URL", url)
        return url

    best_redirector, best_tput = ranked[0]
    optimized = _build_url(lfn, best_redirector)
    logger.info("Selected %s (%.2f MB/s) for %s", best_redirector, best_tput, lfn)
    return optimized


# ---------------------------------------------------------------------------
# Batch optimisation
# ---------------------------------------------------------------------------

def optimize_file_list(
    file_list: "list[str] | str",
    redirectors: Optional[list[str]] = None,
    probe_bytes: int = DEFAULT_PROBE_BYTES,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
    local_site: Optional[str] = None,
    enabled: bool = True,
    blacklisted_sites: Optional[list[str]] = None,
) -> list[str]:
    """Optimize a list of file URLs by selecting the best XRootD redirector.

    For each file that looks like an XRootD-accessible CMS path (starts with
    ``root://`` or ``/store/``), all *redirectors* are probed in parallel and
    the fastest one is substituted into the URL.  Files that are local paths
    or non-XRootD URLs are returned unchanged.  Redirectors that match any
    entry in *blacklisted_sites* are excluded from probing entirely.

    Parameters
    ----------
    file_list:
        A list of file URLs, or a comma-separated string of file URLs.
    redirectors:
        XRootD redirectors to probe.  Defaults to :data:`CMS_REDIRECTORS`.
    probe_bytes:
        Bytes to read per probe.
    timeout:
        Per-site probe timeout in seconds.
    local_site:
        Local CMS site name.  ``None`` triggers auto-detection via
        :func:`detect_local_site`.
    enabled:
        When ``False`` the function returns *file_list* unchanged, which
        allows disabling site selection without changing call sites.
    blacklisted_sites:
        Patterns for redirectors/sites that should never be used.
        Each entry is matched as a case-insensitive substring of the
        redirector URL (e.g. ``["bad-site.cern.ch", "T2_IT_BadSite"]``).

    Returns
    -------
    list of str
        Optimized file URLs (same length as input, same order).
    """
    if isinstance(file_list, str):
        files = [f.strip() for f in file_list.split(",") if f.strip()]
    else:
        files = [f.strip() for f in file_list if f and f.strip()]

    if not enabled or not files:
        return files

    if redirectors is None:
        redirectors = CMS_REDIRECTORS

    if local_site is None:
        local_site = detect_local_site()
    if local_site:
        logger.info("Local site detected: %s", local_site)

    # Separate XRootD-eligible files from local/non-XRootD files.
    xrootd_indices = [
        i for i, f in enumerate(files)
        if f.startswith("root://") or f.startswith("/store/") or f.startswith("/eos/")
    ]

    if not xrootd_indices:
        return files

    logger.info(
        "Probing %d redirector(s) for %d file(s)…",
        len(redirectors), len(xrootd_indices),
    )

    optimized = list(files)
    for i in xrootd_indices:
        optimized[i] = select_best_url(
            files[i],
            redirectors=redirectors,
            probe_bytes=probe_bytes,
            timeout=timeout,
            local_site=local_site,
            blacklisted_sites=blacklisted_sites,
        )

    return optimized


# ---------------------------------------------------------------------------
# Runtime slow-site detection
# ---------------------------------------------------------------------------

class SlowSiteDetector:
    """Monitor XRootD read throughput and detect slow sources at runtime.

    Designed for single-threaded reads where bytes-read and elapsed time can
    be reported incrementally.  When the rolling average throughput falls
    below *threshold_mbs* for at least *window_s* seconds the detector marks
    the current source as slow.

    This class is **not** tied to a specific network library; the caller is
    responsible for calling :meth:`record_bytes` after each read.

    Parameters
    ----------
    threshold_mbs:
        Throughput (MB/s) below which the current source is considered slow.
    window_s:
        Minimum continuous duration (seconds) below the threshold before
        the site is flagged as slow.
    """

    def __init__(
        self,
        threshold_mbs: float = DEFAULT_SLOW_THRESHOLD_MBS,
        window_s: float = 30.0,
    ) -> None:
        self.threshold_mbs = threshold_mbs
        self.window_s = window_s

        self._total_bytes: int = 0
        self._total_elapsed: float = 0.0
        self._below_since: Optional[float] = None  # wall-clock time
        self._slow: bool = False

    # ------------------------------------------------------------------ public

    def record_bytes(self, bytes_read: int, elapsed_s: float) -> bool:
        """Record a completed read and return ``True`` if throughput is OK.

        Parameters
        ----------
        bytes_read:
            Number of bytes successfully read in this call.
        elapsed_s:
            Wall-clock seconds the read took.

        Returns
        -------
        bool
            ``True`` when the current rolling throughput is above the
            threshold, ``False`` when a slow-site condition has been
            sustained for *window_s* seconds.
        """
        if bytes_read > 0 and elapsed_s > 0:
            self._total_bytes += bytes_read
            self._total_elapsed += elapsed_s

        current_tput = self._rolling_throughput_mbs()

        if current_tput is None or current_tput >= self.threshold_mbs:
            self._below_since = None
            return True

        now = time.monotonic()
        if self._below_since is None:
            self._below_since = now

        if (now - self._below_since) >= self.window_s:
            self._slow = True
            logger.warning(
                "Slow XRootD site detected: throughput %.3f MB/s < threshold %.3f MB/s "
                "for %.1f s",
                current_tput, self.threshold_mbs, now - self._below_since,
            )
            return False

        return True

    def is_slow(self) -> bool:
        """Return ``True`` when a slow-site condition has been flagged."""
        return self._slow

    def reset(self) -> None:
        """Reset all counters (e.g. after switching to a new source)."""
        self._total_bytes = 0
        self._total_elapsed = 0.0
        self._below_since = None
        self._slow = False

    def get_stats(self) -> dict:
        """Return a dictionary of current monitoring statistics."""
        tput = self._rolling_throughput_mbs()
        return {
            "total_bytes": self._total_bytes,
            "total_elapsed_s": round(self._total_elapsed, 3),
            "rolling_throughput_mbs": round(tput, 3) if tput is not None else None,
            "threshold_mbs": self.threshold_mbs,
            "is_slow": self._slow,
        }

    # ------------------------------------------------------------------ internals

    def _rolling_throughput_mbs(self) -> Optional[float]:
        if self._total_elapsed <= 0 or self._total_bytes <= 0:
            return None
        return (self._total_bytes / (1024.0 * 1024.0)) / self._total_elapsed
