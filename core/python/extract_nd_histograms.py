#!/usr/bin/env python3
"""Reconstruct the legacy 1D histogram layout from merged THnSparse output.

The input is expected to contain ``nd_histograms_v1/<key>`` THnSparse objects
and a ``__rdf_histogram_schema_v1`` TObjString written by NDHistogramManager.
The output includes a copy of the input objects plus reconstructed TH1F
histograms under the channel/control and sample directories used by older
RDFAnalyzerCore outputs.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import tempfile
from array import array
from typing import Any, Optional


SCHEMA_KEY = "__rdf_histogram_schema_v1"
SCHEMA_VERSION = 1
HISTOGRAM_DIRECTORY = "nd_histograms_v1"


def read_embedded_histogram_schema(path: str) -> Optional[dict[str, Any]]:
    """Read the merge-safe histogram schema, including from an XRootD URL."""
    uproot_error: Optional[Exception] = None
    try:
        import uproot  # type: ignore
        with uproot.open(path) as root_file:
            try:
                schema_obj = root_file[SCHEMA_KEY]
            except KeyError:
                return None
            schema = json.loads(str(schema_obj))
    except Exception as exc:
        # Uproot's XRootD source depends on optional XRootD Python bindings.
        # ROOT itself commonly has XRootD support even when those bindings are
        # absent, so use PyROOT as a fallback for remote files (and unusual
        # local ROOT files that uproot cannot read).
        uproot_error = exc
        try:
            import ROOT  # type: ignore
        except ImportError as root_import_error:
            raise RuntimeError(
                "Could not inspect histogram metadata with uproot, and PyROOT "
                f"is unavailable for fallback: {uproot_error}"
            ) from root_import_error

        root_file = ROOT.TFile.Open(path, "READ")
        if not root_file or root_file.IsZombie() or not root_file.IsOpen():
            raise RuntimeError(
                f"Could not read histogram schema from {path!r}: uproot failed "
                f"({uproot_error}) and ROOT could not open the file"
            ) from uproot_error
        try:
            schema_obj = root_file.Get(SCHEMA_KEY)
            if not schema_obj:
                return None
            if not schema_obj.InheritsFrom("TObjString"):
                raise ValueError(
                    f"{SCHEMA_KEY} in {path!r} is not a TObjString"
                )
            schema_text = schema_obj.GetString()
            if hasattr(schema_text, "Data"):
                schema_text = schema_text.Data()
            schema = json.loads(str(schema_text))
        except Exception as exc:
            raise RuntimeError(
                f"Could not read histogram schema from {path!r}: {exc}"
            ) from exc
        finally:
            root_file.Close()

    _validate_schema(schema, path)
    return schema


def histogram_schema_signature(schema: dict[str, Any]) -> str:
    """Return a canonical signature used to reject incompatible hadd inputs."""
    _validate_schema(schema, "<schema>")
    canonical = dict(schema)
    canonical["histograms"] = sorted(
        schema["histograms"], key=lambda entry: str(entry["key"])
    )
    return json.dumps(canonical, sort_keys=True, separators=(",", ":"))


def validate_compatible_histogram_schemas(
    schemas: list[tuple[str, dict[str, Any]]],
) -> Optional[dict[str, Any]]:
    """Require every file in a merge group to have the same sparse schema.

    Legacy files without embedded metadata are represented by ``None`` by the
    caller. Mixing legacy and sparse-first files is rejected because their
    histogram keys and merge semantics differ.
    """
    if not schemas:
        return None

    reference_path, reference_schema = schemas[0]
    reference_signature = histogram_schema_signature(reference_schema)
    for path, schema in schemas[1:]:
        if histogram_schema_signature(schema) != reference_signature:
            raise ValueError(
                "Incompatible N-dimensional histogram schemas in merge group: "
                f"{reference_path!r} and {path!r}. Histogram keys, axis roles, "
                "binning, and category labels must match before hadd."
            )
    return reference_schema


def _validate_schema(schema: Any, source: str) -> None:
    if not isinstance(schema, dict):
        raise ValueError(f"Invalid histogram schema in {source}: expected an object")
    if schema.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported histogram schema version in {source}: "
            f"{schema.get('schema_version')!r}"
        )
    if schema.get("directory") != HISTOGRAM_DIRECTORY:
        raise ValueError(
            f"Invalid histogram directory in {source}: {schema.get('directory')!r}"
        )
    histograms = schema.get("histograms")
    if not isinstance(histograms, list):
        raise ValueError(f"Invalid histogram schema in {source}: histograms must be a list")

    keys: set[str] = set()
    for index, histogram in enumerate(histograms):
        if not isinstance(histogram, dict):
            raise ValueError(f"Invalid histogram entry {index} in {source}")
        key = histogram.get("key")
        name = histogram.get("name")
        axes = histogram.get("axes")
        if not isinstance(key, str) or not key or not isinstance(name, str) or not name:
            raise ValueError(f"Histogram entry {index} in {source} has no key or name")
        if key in keys:
            raise ValueError(f"Duplicate sparse histogram key {key!r} in {source}")
        keys.add(key)
        if not isinstance(axes, list) or not axes:
            raise ValueError(f"Histogram {key!r} in {source} has no axis metadata")
        roles: set[str] = set()
        for axis_index, axis in enumerate(axes):
            if not isinstance(axis, dict):
                raise ValueError(f"Invalid axis {axis_index} for {key!r} in {source}")
            role = axis.get("role")
            bins = axis.get("bins")
            labels = axis.get("labels")
            edges = axis.get("edges")
            if not isinstance(role, str) or not role or role in roles:
                raise ValueError(f"Invalid or duplicate axis role for {key!r} in {source}")
            roles.add(role)
            if not isinstance(bins, int) or bins <= 0:
                raise ValueError(f"Invalid bin count for {key!r} axis {role!r} in {source}")
            if not isinstance(labels, list) or not all(isinstance(v, str) for v in labels):
                raise ValueError(f"Invalid labels for {key!r} axis {role!r} in {source}")
            if not isinstance(edges, list) or len(edges) != bins + 1:
                raise ValueError(f"Invalid edges for {key!r} axis {role!r} in {source}")
            if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in edges):
                raise ValueError(f"Non-finite edges for {key!r} axis {role!r} in {source}")
            if any(right <= left for left, right in zip(edges, edges[1:])):
                raise ValueError(f"Unordered edges for {key!r} axis {role!r} in {source}")

        required_roles = {"channel", "control", "sample", "systematic", "observable"}
        if not required_roles.issubset(roles):
            missing = sorted(required_roles - roles)
            raise ValueError(f"Histogram {key!r} in {source} is missing axes: {missing}")


def _axis_map(histogram_schema: dict[str, Any]) -> dict[str, int]:
    return {
        axis["role"]: index
        for index, axis in enumerate(histogram_schema["axes"])
    }


def _histogram_axis_edges(axis: Any, metadata: dict[str, Any]) -> list[float]:
    bins = int(metadata["bins"])
    if axis.GetNbins() != bins:
        raise ValueError(
            f"Sparse axis {metadata['role']!r} has {axis.GetNbins()} bins, "
            f"metadata declares {bins}"
        )
    return [float(edge) for edge in metadata["edges"]]


def _directory_for_path(root_file: Any, path: str) -> Any:
    current = root_file
    for component in path.split("/"):
        child = current.GetDirectory(component)
        if not child:
            child = current.mkdir(component)
        if not child:
            raise RuntimeError(f"Could not create ROOT directory {path!r}")
        current = child
    return current


def _sparse_bins(histogram: Any):
    """Yield (coordinate bins, content, sumw2) from an allocated THnSparse bin."""
    dimensions = int(histogram.GetNdimensions())
    coordinates = array("i", [0] * dimensions)
    for sparse_bin in range(int(histogram.GetNbins())):
        content = float(histogram.GetBinContent(sparse_bin, coordinates))
        sumw2 = float(histogram.GetBinError2(sparse_bin))
        yield tuple(coordinates), content, sumw2


def _open_root_file(ROOT: Any, path: str, mode: str) -> Any:
    root_file = ROOT.TFile.Open(path, mode)
    if not root_file or root_file.IsZombie() or not root_file.IsOpen():
        raise RuntimeError(f"Could not open ROOT file {path!r} in {mode} mode")
    return root_file


def extract_nd_histograms(
    input_path: str,
    output_path: Optional[str] = None,
    *,
    required: bool = True,
) -> int:
    """Add reconstructed TH1F objects to a copy of a sparse ROOT file.

    If *output_path* is omitted, the input file is replaced atomically after
    extraction. Existing THnSparse objects and unrelated ROOT objects are
    retained. Returns the number of single histograms written. With
    ``required=False``, files without the sparse-first schema are left alone
    and the function returns zero.
    """
    input_path = os.path.abspath(input_path)
    output_path = os.path.abspath(output_path or input_path)
    if not os.path.isfile(input_path):
        raise FileNotFoundError(input_path)

    schema = read_embedded_histogram_schema(input_path)
    if schema is None:
        if required:
            raise ValueError(f"No {SCHEMA_KEY} schema found in {input_path!r}")
        return 0

    try:
        import ROOT  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "PyROOT is required to reconstruct TH1F histograms from THnSparse"
        ) from exc

    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)
    fd, working_path = tempfile.mkstemp(
        prefix=".ndhist_extract_", suffix=".root", dir=output_dir
    )
    os.close(fd)

    try:
        shutil.copy2(input_path, working_path)
        root_file = _open_root_file(ROOT, working_path, "UPDATE")
        try:
            histogram_records: list[tuple[dict[str, Any], Any, dict[str, int]]] = []
            populated_sample_bins: set[int] = set()

            for metadata in schema["histograms"]:
                sparse = root_file.Get(f"{schema['directory']}/{metadata['key']}")
                if not sparse or not sparse.InheritsFrom("THnSparse"):
                    raise ValueError(
                        f"Missing THnSparse object {schema['directory']}/"
                        f"{metadata['key']} in {input_path!r}"
                    )
                axes = metadata["axes"]
                if int(sparse.GetNdimensions()) != len(axes):
                    raise ValueError(
                        f"Dimension mismatch for sparse histogram {metadata['key']!r}"
                    )
                axis_indices = _axis_map(metadata)
                sample_axis = axis_indices["sample"]
                for axis_index, axis_metadata in enumerate(axes):
                    _histogram_axis_edges(sparse.GetAxis(axis_index), axis_metadata)
                for coordinates, content, sumw2 in _sparse_bins(sparse):
                    sample_bin = coordinates[sample_axis]
                    if (content != 0.0 or sumw2 != 0.0) and sample_bin > 0:
                        populated_sample_bins.add(sample_bin)
                histogram_records.append((metadata, sparse, axis_indices))

            written = 0
            for metadata, sparse, axis_indices in histogram_records:
                axes = metadata["axes"]
                channel_labels = axes[axis_indices["channel"]]["labels"]
                control_labels = axes[axis_indices["control"]]["labels"]
                sample_labels = axes[axis_indices["sample"]]["labels"]
                systematic_labels = axes[axis_indices["systematic"]]["labels"]
                observable_axis_index = axis_indices["observable"]
                observable_metadata = axes[observable_axis_index]
                observable_axis = sparse.GetAxis(observable_axis_index)
                edges = _histogram_axis_edges(observable_axis, observable_metadata)
                n_bins = int(observable_metadata["bins"])

                output_hists: dict[tuple[int, int, int, int], Any] = {}
                for channel_bin, channel in enumerate(channel_labels, start=1):
                    for control_bin, control in enumerate(control_labels, start=1):
                        for sample_bin in sorted(populated_sample_bins):
                            if sample_bin > len(sample_labels):
                                continue
                            sample = sample_labels[sample_bin - 1]
                            for systematic_bin, systematic in enumerate(
                                systematic_labels, start=1
                            ):
                                if systematic_bin > int(
                                    sparse.GetAxis(axis_indices["systematic"]).GetNbins()
                                ):
                                    continue
                                histogram_name = metadata["name"]
                                if systematic != "Nominal":
                                    histogram_name += "_" + systematic
                                output_key = (
                                    channel_bin,
                                    control_bin,
                                    sample_bin,
                                    systematic_bin,
                                )
                                title = (
                                    f"{metadata['name']};{metadata['name']};Counts"
                                )
                                output_hist = ROOT.TH1F(
                                    histogram_name,
                                    title,
                                    n_bins,
                                    array("d", edges),
                                )
                                output_hist.Sumw2()
                                output_hist.SetDirectory(0)
                                output_hists[output_key] = output_hist
                                written += 1

                for coordinates, content, sumw2 in _sparse_bins(sparse):
                    channel_bin = coordinates[axis_indices["channel"]]
                    control_bin = coordinates[axis_indices["control"]]
                    sample_bin = coordinates[axis_indices["sample"]]
                    systematic_bin = coordinates[axis_indices["systematic"]]
                    observable_bin = coordinates[observable_axis_index]
                    if min(channel_bin, control_bin, sample_bin, systematic_bin) <= 0:
                        continue
                    if channel_bin > len(channel_labels) or control_bin > len(control_labels):
                        continue
                    if sample_bin > len(sample_labels) or systematic_bin > len(systematic_labels):
                        continue
                    if content == 0.0 and sumw2 == 0.0:
                        continue
                    output_hist = output_hists.get(
                        (channel_bin, control_bin, sample_bin, systematic_bin)
                    )
                    if output_hist is None:
                        continue
                    output_hist.SetBinContent(observable_bin, content)
                    output_hist.SetBinError(observable_bin, math.sqrt(max(0.0, sumw2)))

                # Write after filling; pre-created objects preserve empty
                # category and systematic combinations in the output.
                for channel_bin, channel in enumerate(channel_labels, start=1):
                    for control_bin, control in enumerate(control_labels, start=1):
                        for sample_bin in sorted(populated_sample_bins):
                            if sample_bin > len(sample_labels):
                                continue
                            sample = sample_labels[sample_bin - 1]
                            directory_path = f"{channel}_{control}/{sample}"
                            destination = _directory_for_path(root_file, directory_path)
                            destination.cd()
                            for systematic_bin, systematic in enumerate(
                                systematic_labels, start=1
                            ):
                                key = (
                                    channel_bin,
                                    control_bin,
                                    sample_bin,
                                    systematic_bin,
                                )
                                output_hist = output_hists.get(key)
                                if output_hist is None:
                                    continue
                                histogram_name = metadata["name"]
                                if systematic != "Nominal":
                                    histogram_name += "_" + systematic
                                output_hist.Write(
                                    histogram_name, ROOT.TObject.kOverwrite
                                )
                                output_hist.SetDirectory(0)
                            root_file.cd()

        finally:
            root_file.Close()

        os.replace(working_path, output_path)
        return written
    except Exception:
        try:
            os.unlink(working_path)
        except OSError:
            pass
        raise


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="Merged ROOT file with nd_histograms_v1 objects")
    parser.add_argument(
        "--output",
        help="Output ROOT file (defaults to atomically updating the input file)",
    )
    args = parser.parse_args(argv)
    count = extract_nd_histograms(args.input, args.output)
    print(f"Wrote {count} reconstructed 1D histogram(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
