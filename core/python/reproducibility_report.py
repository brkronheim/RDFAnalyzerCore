"""
Provenance-driven reproducibility report for RDFAnalyzerCore.

This module consolidates all structured provenance metadata—gathered from the
framework build, the analysis repository, runtime environment, configuration,
plugins, services, task metadata, and dataset manifests—into a single
human- and machine-readable reproducibility artifact.

Provenance data is collected at C++ analysis time by
:class:`ProvenanceService` and written as TNamed objects into a ``provenance``
TDirectory inside the meta ROOT output file.  This module provides:

* :class:`ReproducibilityReport` – structured report object with
  ``to_dict()`` / ``to_yaml()`` / ``to_json()`` / ``to_text()`` output and
  symmetric ``from_dict()`` / ``load_yaml()`` / ``load_json()`` deserialisation.

* :func:`load_provenance_from_root` – reads the flat provenance key–value map
  from a ROOT meta file using **uproot** (no compiled ROOT required).

* :func:`build_report_from_provenance` – converts a flat ``{str: str}``
  provenance map into a :class:`ReproducibilityReport`.

* A CLI entrypoint (``main()``) for command-line report generation.

Usage
-----
Read provenance from a ROOT meta file and print a human-readable report::

    from reproducibility_report import load_provenance_from_root, build_report_from_provenance

    prov = load_provenance_from_root("output_meta.root")
    report = build_report_from_provenance(prov)
    print(report.to_text())
    report.save_yaml("reproducibility_report.yaml")

Build a report programmatically from a plain dict::

    from reproducibility_report import ReproducibilityReport

    report = ReproducibilityReport(
        provenance={
            "framework.git_hash": "abc123",
            "framework.git_dirty": "false",
            "config.hash": "deadbeef",
            "plugin.histManager.version": "2",
        }
    )
    report.save_json("repro.json")
    print(report.to_text())
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
from typing import Any, Dict, List, Optional

import yaml


# ---------------------------------------------------------------------------
# Schema version
# ---------------------------------------------------------------------------

#: Schema version for :class:`ReproducibilityReport` serialised output.
REPRODUCIBILITY_REPORT_VERSION: int = 1


# ---------------------------------------------------------------------------
# Internal namespace parsing helpers
# ---------------------------------------------------------------------------

# Known top-level namespaces and their display names (order matters for text output)
_FRAMEWORK_PREFIX = "framework."
_ROOT_PREFIX = "root."
_ANALYSIS_PREFIX = "analysis."
_ENV_PREFIX = "env."
_EXECUTOR_PREFIX = "executor."
_CONFIG_PREFIX = "config."
_FILELIST_PREFIX = "filelist."
_FILE_HASH_PREFIX = "file.hash."
_DATASET_MANIFEST_PREFIX = "dataset_manifest."
_PLUGIN_PREFIX = "plugin."
_TASK_PREFIX = "task."


def _strip_prefix(key: str, prefix: str) -> str:
    """Return *key* with *prefix* removed (assumes key starts with prefix)."""
    return key[len(prefix):]


def _entries_with_prefix(
    provenance: Dict[str, str], prefix: str
) -> Dict[str, str]:
    """Return sub-dict of entries whose keys start with *prefix*, stripped."""
    return {
        _strip_prefix(k, prefix): v
        for k, v in sorted(provenance.items())
        if k.startswith(prefix)
    }


# ---------------------------------------------------------------------------
# ReproducibilityReport
# ---------------------------------------------------------------------------


class ReproducibilityReport:
    """Structured provenance-driven reproducibility report.

    A :class:`ReproducibilityReport` holds all provenance metadata collected
    during a single analysis run and can be serialised to both machine-readable
    (JSON / YAML) and human-readable (plain text) formats.

    The report organises the flat ``{key: value}`` provenance map—as written
    by :class:`ProvenanceService`—into logical sections:

    * **Framework** – build-time information (git hash, dirty flag, compiler,
      timestamp) and the ROOT version.
    * **Analysis** – runtime git info of the analysis repository.
    * **Environment** – container tag and thread-pool size.
    * **Configuration** – deterministic hash of the configuration map and of
      the file-list file.
    * **File hashes** – MD5 digests of auxiliary input files referenced by
      configuration values (``file.hash.*`` entries).
    * **Dataset manifest** – identity of the dataset manifest that was used
      (file hash, query parameters, resolved dataset names).
    * **Plugins** – per-plugin provenance entries contributed via
      ``collectProvenanceEntries()``, including the auto-computed
      ``config_hash`` per plugin role.
    * **Task metadata** – arbitrary key–value pairs injected via
      ``Analyzer::setTaskMetadata()``.
    * **Other** – any entry that does not match a known namespace prefix.

    Parameters
    ----------
    provenance : dict, optional
        Flat ``{key: value}`` provenance map.  When omitted an empty map is
        used.
    timestamp : str, optional
        UTC creation timestamp (ISO 8601).  Defaults to the current time.

    Examples
    --------
    Build from a plain dict and render as text::

        report = ReproducibilityReport({"framework.git_hash": "abc123"})
        print(report.to_text())

    Round-trip through YAML::

        yaml_str = report.to_yaml()
        report2 = ReproducibilityReport.from_dict(yaml.safe_load(yaml_str))
    """

    def __init__(
        self,
        provenance: Optional[Dict[str, str]] = None,
        timestamp: Optional[str] = None,
    ) -> None:
        self.provenance: Dict[str, str] = dict(provenance or {})
        self.timestamp: str = (
            timestamp
            or datetime.datetime.now(tz=datetime.timezone.utc).isoformat(
                timespec="seconds"
            )
        )
        self.report_version: int = REPRODUCIBILITY_REPORT_VERSION

    # ------------------------------------------------------------------
    # Structured section accessors
    # ------------------------------------------------------------------

    @property
    def framework(self) -> Dict[str, str]:
        """Entries under the ``framework.*`` and ``root.*`` namespaces."""
        d = _entries_with_prefix(self.provenance, _FRAMEWORK_PREFIX)
        d.update(_entries_with_prefix(self.provenance, _ROOT_PREFIX))
        return d

    @property
    def analysis(self) -> Dict[str, str]:
        """Entries under the ``analysis.*`` namespace."""
        return _entries_with_prefix(self.provenance, _ANALYSIS_PREFIX)

    @property
    def environment(self) -> Dict[str, str]:
        """Entries under the ``env.*`` and ``executor.*`` namespaces."""
        d = _entries_with_prefix(self.provenance, _ENV_PREFIX)
        d.update(_entries_with_prefix(self.provenance, _EXECUTOR_PREFIX))
        return d

    @property
    def configuration(self) -> Dict[str, str]:
        """Entries under the ``config.*`` and ``filelist.*`` namespaces.

        ``config.*`` sub-keys are stored as-is (e.g. ``config.hash`` →
        ``hash``).  ``filelist.*`` sub-keys are prefixed with ``filelist_``
        (e.g. ``filelist.hash`` → ``filelist_hash``) to avoid collisions
        with same-named ``config.*`` entries.
        """
        d = _entries_with_prefix(self.provenance, _CONFIG_PREFIX)
        for k, v in _entries_with_prefix(self.provenance, _FILELIST_PREFIX).items():
            d["filelist_" + k] = v
        return d

    @property
    def file_hashes(self) -> Dict[str, str]:
        """Entries under the ``file.hash.*`` namespace."""
        return _entries_with_prefix(self.provenance, _FILE_HASH_PREFIX)

    @property
    def dataset_manifest(self) -> Dict[str, str]:
        """Entries under the ``dataset_manifest.*`` namespace."""
        return _entries_with_prefix(self.provenance, _DATASET_MANIFEST_PREFIX)

    @property
    def plugins(self) -> Dict[str, Dict[str, str]]:
        """Per-role plugin provenance, keyed by role name.

        Returns a dict of the form ``{role: {sub_key: value}}``, where each
        inner dict holds all ``plugin.<role>.<sub_key>`` entries.
        """
        result: Dict[str, Dict[str, str]] = {}
        for key, value in sorted(self.provenance.items()):
            if not key.startswith(_PLUGIN_PREFIX):
                continue
            remainder = _strip_prefix(key, _PLUGIN_PREFIX)
            # remainder is "<role>.<sub_key>" or just "<role>"
            dot = remainder.find(".")
            if dot == -1:
                role, sub_key = remainder, ""
            else:
                role, sub_key = remainder[:dot], remainder[dot + 1:]
            if role not in result:
                result[role] = {}
            if sub_key:
                result[role][sub_key] = value
        return result

    @property
    def task_metadata(self) -> Dict[str, str]:
        """Entries under the ``task.*`` namespace."""
        return _entries_with_prefix(self.provenance, _TASK_PREFIX)

    @property
    def other(self) -> Dict[str, str]:
        """Entries that do not match any known namespace prefix."""
        known_prefixes = (
            _FRAMEWORK_PREFIX,
            _ROOT_PREFIX,
            _ANALYSIS_PREFIX,
            _ENV_PREFIX,
            _EXECUTOR_PREFIX,
            _CONFIG_PREFIX,
            _FILELIST_PREFIX,
            _FILE_HASH_PREFIX,
            _DATASET_MANIFEST_PREFIX,
            _PLUGIN_PREFIX,
            _TASK_PREFIX,
        )
        return {
            k: v
            for k, v in sorted(self.provenance.items())
            if not any(k.startswith(p) for p in known_prefixes)
        }

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON/YAML-serialisable dict representation.

        The returned dict includes both the flat ``provenance`` map and the
        structured sections derived from it, making it convenient for both
        programmatic access and human inspection.

        Returns
        -------
        dict
            Full report as a nested dictionary.
        """
        return {
            "report_version": self.report_version,
            "timestamp": self.timestamp,
            "summary": {
                "n_framework_entries": len(self.framework),
                "n_analysis_entries": len(self.analysis),
                "n_environment_entries": len(self.environment),
                "n_configuration_entries": len(self.configuration),
                "n_file_hash_entries": len(self.file_hashes),
                "n_dataset_manifest_entries": len(self.dataset_manifest),
                "n_plugin_roles": len(self.plugins),
                "n_task_metadata_entries": len(self.task_metadata),
                "n_other_entries": len(self.other),
                "n_total_entries": len(self.provenance),
            },
            "framework": dict(self.framework),
            "analysis": dict(self.analysis),
            "environment": dict(self.environment),
            "configuration": dict(self.configuration),
            "file_hashes": dict(self.file_hashes),
            "dataset_manifest": dict(self.dataset_manifest),
            "plugins": {role: dict(entries) for role, entries in self.plugins.items()},
            "task_metadata": dict(self.task_metadata),
            "other": dict(self.other),
            "provenance": {k: v for k, v in sorted(self.provenance.items())},
        }

    def to_yaml(self) -> str:
        """Return the report serialised as a YAML string.

        Returns
        -------
        str
            YAML representation of the full report.
        """
        return yaml.dump(
            self.to_dict(),
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    def to_json(self, indent: int = 2) -> str:
        """Return the report serialised as a JSON string.

        Parameters
        ----------
        indent : int
            Number of spaces used for indentation.  Defaults to ``2``.

        Returns
        -------
        str
            JSON representation of the full report.
        """
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def to_text(self) -> str:
        """Return a human-readable plain-text representation.

        The text output uses ASCII formatting with section headers and
        two-column key/value tables.  It is intended for terminal display
        or inclusion in log files.

        Returns
        -------
        str
            Multi-line human-readable report string.
        """
        lines: List[str] = []

        def _header(title: str) -> None:
            lines.append("")
            lines.append("=" * 60)
            lines.append(f"  {title}")
            lines.append("=" * 60)

        def _kv(d: Dict[str, str], indent: int = 2) -> None:
            if not d:
                lines.append(" " * indent + "(none)")
                return
            key_w = max(len(k) for k in d) + 2
            for k, v in sorted(d.items()):
                lines.append(f"{'  ' * (indent // 2)}{k:<{key_w}}: {v}")

        # Title block
        lines.append("=" * 60)
        lines.append("  REPRODUCIBILITY REPORT")
        lines.append(f"  Timestamp : {self.timestamp}")
        lines.append(
            f"  Entries   : {len(self.provenance)} total"
        )
        lines.append("=" * 60)

        # Framework provenance
        _header("FRAMEWORK PROVENANCE")
        _kv(self.framework)

        # Analysis provenance
        _header("ANALYSIS PROVENANCE")
        _kv(self.analysis)

        # Environment
        _header("ENVIRONMENT")
        _kv(self.environment)

        # Configuration
        _header("CONFIGURATION")
        _kv(self.configuration)

        # File hashes
        if self.file_hashes:
            _header("FILE HASHES")
            _kv(self.file_hashes)

        # Dataset manifest
        if self.dataset_manifest:
            _header("DATASET MANIFEST")
            _kv(self.dataset_manifest)

        # Plugin provenance
        if self.plugins:
            _header("PLUGIN PROVENANCE")
            for role, entries in sorted(self.plugins.items()):
                lines.append(f"  [{role}]")
                if entries:
                    key_w = max(len(k) for k in entries) + 2
                    for k, v in sorted(entries.items()):
                        lines.append(f"    {k:<{key_w}}: {v}")
                else:
                    lines.append("    (type name recorded, no custom entries)")

        # Task metadata
        if self.task_metadata:
            _header("TASK METADATA")
            _kv(self.task_metadata)

        # Other entries
        if self.other:
            _header("OTHER ENTRIES")
            _kv(self.other)

        lines.append("")
        lines.append("=" * 60)
        lines.append("  END OF REPORT")
        lines.append("=" * 60)

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def save_yaml(self, path: str) -> None:
        """Save the machine-readable report to a YAML file.

        Parameters
        ----------
        path : str
            Destination file path (created or overwritten).
        """
        _ensure_dir(path)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_yaml())

    def save_json(self, path: str) -> None:
        """Save the machine-readable report to a JSON file.

        Parameters
        ----------
        path : str
            Destination file path (created or overwritten).
        """
        _ensure_dir(path)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_json())

    def save_text(self, path: str) -> None:
        """Save the human-readable report to a plain-text file.

        Parameters
        ----------
        path : str
            Destination file path (created or overwritten).
        """
        _ensure_dir(path)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_text())

    # ------------------------------------------------------------------
    # Deserialisation
    # ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReproducibilityReport":
        """Deserialise a :class:`ReproducibilityReport` from a plain dict.

        Accepts dicts produced by :meth:`to_dict` or any dict that contains a
        ``"provenance"`` key.  As a convenience, bare flat provenance maps
        (i.e. dicts without a ``"provenance"`` sub-key) are also accepted and
        treated as the provenance map directly.

        Parameters
        ----------
        data : dict
            Dictionary as produced by :meth:`to_dict`, or a bare flat
            ``{key: value}`` provenance map.

        Returns
        -------
        ReproducibilityReport
        """
        if "provenance" in data:
            raw_prov = data["provenance"]
        else:
            # Treat the entire dict as a flat provenance map (convenience path)
            raw_prov = {
                k: v
                for k, v in data.items()
                if k
                not in (
                    "report_version",
                    "timestamp",
                    "summary",
                    "framework",
                    "analysis",
                    "environment",
                    "configuration",
                    "file_hashes",
                    "dataset_manifest",
                    "plugins",
                    "task_metadata",
                    "other",
                )
            }
        provenance = {str(k): str(v) for k, v in raw_prov.items() if v is not None}
        report = cls(
            provenance=provenance,
            timestamp=data.get("timestamp"),
        )
        report.report_version = data.get("report_version", REPRODUCIBILITY_REPORT_VERSION)
        return report

    @classmethod
    def load_yaml(cls, path: str) -> "ReproducibilityReport":
        """Load a :class:`ReproducibilityReport` from a YAML file.

        Parameters
        ----------
        path : str
            Path to the YAML report file produced by :meth:`save_yaml`.

        Returns
        -------
        ReproducibilityReport
        """
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        if not isinstance(raw, dict):
            raise ValueError(
                f"Reproducibility report file '{path}' must be a YAML mapping."
            )
        return cls.from_dict(raw)

    @classmethod
    def load_json(cls, path: str) -> "ReproducibilityReport":
        """Load a :class:`ReproducibilityReport` from a JSON file.

        Parameters
        ----------
        path : str
            Path to the JSON report file produced by :meth:`save_json`.

        Returns
        -------
        ReproducibilityReport
        """
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
        if not isinstance(raw, dict):
            raise ValueError(
                f"Reproducibility report file '{path}' must be a JSON object."
            )
        return cls.from_dict(raw)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"ReproducibilityReport("
            f"n_entries={len(self.provenance)}, "
            f"timestamp={self.timestamp!r})"
        )


# ---------------------------------------------------------------------------
# ROOT reader
# ---------------------------------------------------------------------------


def load_provenance_from_root(meta_root_path: str) -> Dict[str, str]:
    """Read the provenance key–value map from a ROOT meta output file.

    Uses **uproot** to read all :class:`TNamed` objects stored in the
    ``"provenance"`` TDirectory of the given ROOT file.  The TNamed name is
    used as the key and the TNamed title as the value.

    Parameters
    ----------
    meta_root_path : str
        Path to the ROOT meta file written by the framework.

    Returns
    -------
    dict
        Flat ``{key: value}`` provenance map.  Returns an empty dict when the
        ``"provenance"`` directory is absent or uproot is not available.

    Raises
    ------
    FileNotFoundError
        If *meta_root_path* does not exist on disk.
    ImportError
        If uproot is not installed.
    """
    if not os.path.exists(meta_root_path):
        raise FileNotFoundError(
            f"ROOT meta file not found: {meta_root_path!r}"
        )

    try:
        import uproot  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "uproot is required to read ROOT meta files.  "
            "Install it with: pip install uproot"
        ) from exc

    provenance: Dict[str, str] = {}
    with uproot.open(meta_root_path) as root_file:
        # The provenance directory may or may not exist
        if "provenance" not in root_file:
            return provenance
        prov_dir = root_file["provenance"]
        for key in prov_dir.keys():
            # Strip cycle numbers (";1") that uproot appends
            clean_key = re.sub(r";[0-9]+$", "", key)
            obj = prov_dir[key]
            # TNamed objects have a .title attribute
            if hasattr(obj, "title"):
                provenance[clean_key] = str(obj.title)
            elif hasattr(obj, "member"):
                try:
                    provenance[clean_key] = str(obj.member("fTitle"))
                except Exception:  # noqa: BLE001
                    provenance[clean_key] = ""
    return provenance


# ---------------------------------------------------------------------------
# Convenience builder
# ---------------------------------------------------------------------------


def build_report_from_provenance(
    provenance: Dict[str, str],
    timestamp: Optional[str] = None,
) -> ReproducibilityReport:
    """Build a :class:`ReproducibilityReport` from a flat provenance map.

    This is a thin wrapper around the :class:`ReproducibilityReport`
    constructor provided for symmetry with other ``build_report_from_*``
    helpers in the analysis framework.

    Parameters
    ----------
    provenance : dict
        Flat ``{key: value}`` provenance map as returned by
        :func:`load_provenance_from_root` or collected directly from
        ``ProvenanceService.getProvenance()``.
    timestamp : str, optional
        UTC creation timestamp (ISO 8601).  Defaults to the current time.

    Returns
    -------
    ReproducibilityReport
    """
    return ReproducibilityReport(provenance=provenance, timestamp=timestamp)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ensure_dir(path: str) -> None:
    """Create parent directories for *path* if they do not exist."""
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entrypoint for generating a reproducibility report.

    Usage
    -----
    .. code-block:: console

        # From a ROOT meta file:
        python reproducibility_report.py output_meta.root

        # Save all formats:
        python reproducibility_report.py output_meta.root \\
            --yaml repro.yaml --json repro.json --text repro.txt

        # From a previously saved YAML report:
        python reproducibility_report.py --load-yaml repro.yaml

    Parameters
    ----------
    argv : list[str], optional
        Command-line arguments (defaults to ``sys.argv[1:]``).

    Returns
    -------
    int
        Exit code: 0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(
        description="Generate a provenance-driven reproducibility report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "root_file",
        nargs="?",
        metavar="META_ROOT_FILE",
        help="Path to the ROOT meta output file containing the provenance directory.",
    )
    parser.add_argument(
        "--load-yaml",
        metavar="FILE",
        help="Load an existing report from a YAML file instead of a ROOT file.",
    )
    parser.add_argument(
        "--load-json",
        metavar="FILE",
        help="Load an existing report from a JSON file instead of a ROOT file.",
    )
    parser.add_argument(
        "--yaml",
        metavar="FILE",
        dest="out_yaml",
        help="Write the machine-readable report to a YAML file.",
    )
    parser.add_argument(
        "--json",
        metavar="FILE",
        dest="out_json",
        help="Write the machine-readable report to a JSON file.",
    )
    parser.add_argument(
        "--text",
        metavar="FILE",
        dest="out_text",
        help="Write the human-readable report to a plain-text file.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print the report to stdout.",
    )

    args = parser.parse_args(argv)

    # Load the report
    try:
        if args.load_yaml:
            report = ReproducibilityReport.load_yaml(args.load_yaml)
        elif args.load_json:
            report = ReproducibilityReport.load_json(args.load_json)
        elif args.root_file:
            prov = load_provenance_from_root(args.root_file)
            report = build_report_from_provenance(prov)
        else:
            parser.print_help()
            return 1
    except (FileNotFoundError, ImportError, ValueError) as exc:
        print(f"Error: {exc}")
        return 1

    # Output
    if args.out_yaml:
        report.save_yaml(args.out_yaml)
        print(f"Wrote YAML report to {args.out_yaml}")
    if args.out_json:
        report.save_json(args.out_json)
        print(f"Wrote JSON report to {args.out_json}")
    if args.out_text:
        report.save_text(args.out_text)
        print(f"Wrote text report to {args.out_text}")
    if not args.quiet:
        print(report.to_text())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
