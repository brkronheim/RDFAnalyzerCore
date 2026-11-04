"""Shared low-level readers for RDFAnalyzerCore histogram inputs.

Plotting and analysis adapters may apply different physics selections, but
they should agree on how an allocated THnSparse bin, its coordinates, and its
sum-of-weights-squared value are read.
"""
from __future__ import annotations

from array import array
from functools import lru_cache
import importlib
import sys
from pathlib import Path
from typing import Any, Iterator

import numpy as np


def iter_sparse_bins(histogram: Any) -> Iterator[tuple[tuple[int, ...], float, float]]:
    """Yield ``(coordinate bins, content, sumw2)`` for allocated sparse bins."""
    dimensions = int(histogram.GetNdimensions())
    coordinates = array("i", [0] * dimensions)
    for sparse_bin in range(int(histogram.GetNbins())):
        content = float(histogram.GetBinContent(sparse_bin, coordinates))
        sumw2 = float(histogram.GetBinError2(sparse_bin))
        yield tuple(int(value) for value in coordinates), content, sumw2


@lru_cache(maxsize=1)
def _compiled_reader() -> Any | None:
    """Load the optional RDFAnalyzerCore extension containing the C++ reader."""
    try:
        module = importlib.import_module("rdfanalyzer")
    except (ImportError, OSError):
        core_dir = Path(__file__).resolve().parents[1]
        build_module_dir = core_dir.parent / "build" / "python"
        if build_module_dir.is_dir() and str(build_module_dir) not in sys.path:
            sys.path.insert(0, str(build_module_dir))
        importlib.invalidate_caches()
        try:
            module = importlib.import_module("rdfanalyzer")
        except (ImportError, OSError):
            return None
    return module if hasattr(module, "read_sparse_bins") else None


def sparse_bin_arrays(file_name: str | Path, histogram_path: str,
                      histogram: Any | None = None):
    """Return allocated sparse-bin coordinates, content, and sumw2 arrays.

    The compiled binding reads and copies bins in one C++ loop. If the optional
    extension is unavailable, a PyROOT fallback preserves the same result.
    ``histogram`` can be supplied to avoid reopening a file in the fallback.
    """
    reader = _compiled_reader()
    if reader is not None:
        return reader.read_sparse_bins(str(file_name), histogram_path)
    if histogram is None:
        import ROOT
        root_file = ROOT.TFile.Open(str(file_name), "READ")
        if not root_file or root_file.IsZombie():
            raise OSError(f"cannot open sparse histogram file: {file_name}")
        try:
            histogram = root_file.Get(histogram_path)
            if not histogram:
                raise KeyError(f"missing THnSparse {histogram_path} in {file_name}")
            return _arrays_from_histogram(histogram)
        finally:
            root_file.Close()
    return _arrays_from_histogram(histogram)


def project_sparse_bins(file_name: str | Path, histogram_path: str,
                        output_axes: list[int], axis_maps: list[list[int]],
                        output_shape: list[int], histogram: Any | None = None,
                        include_sumw2: bool = True):
    """Aggregate sparse bins directly into a dense output shape.

    The C++ implementation performs filtering and accumulation in one pass.
    When it is unavailable, NumPy provides a compatible fallback.
    """
    reader = _compiled_reader()
    if reader is not None and hasattr(reader, "project_sparse_bins"):
        compiled_maps = [np.ascontiguousarray(axis_map, dtype=np.int64)
                         for axis_map in axis_maps]
        contents, sumw2 = reader.project_sparse_bins(
            str(file_name), histogram_path, output_axes, compiled_maps, output_shape,
            include_sumw2,
        )
        return contents, sumw2

    coordinates, values, variances = sparse_bin_arrays(
        file_name, histogram_path, histogram
    )
    if len(output_axes) != coordinates.shape[1] or len(axis_maps) != coordinates.shape[1]:
        raise ValueError("sparse projection mapping has wrong dimensions")
    contents = np.zeros(output_shape, dtype=np.float64)
    sumw2 = np.zeros(output_shape, dtype=np.float64)
    strides = np.asarray(contents.strides, dtype=np.int64) // contents.itemsize
    flat = np.zeros(len(coordinates), dtype=np.int64)
    selected = np.ones(len(coordinates), dtype=bool)
    for source_axis, target_axis in enumerate(output_axes):
        if not 0 <= target_axis < len(output_shape):
            raise ValueError("sparse projection output axis is invalid")
        source_bins = coordinates[:, source_axis]
        destination_map = np.asarray(axis_maps[source_axis], dtype=np.int64)
        in_range = (source_bins >= 1) & (source_bins <= len(destination_map))
        selected &= in_range
        destination = np.zeros(len(coordinates), dtype=np.int64)
        destination[in_range] = destination_map[source_bins[in_range] - 1]
        selected &= destination >= 0
        selected &= destination < output_shape[target_axis]
        flat += destination * strides[target_axis]
    contents_flat = contents.reshape(-1)
    sumw2_flat = sumw2.reshape(-1)
    np.add.at(contents_flat, flat[selected], values[selected])
    if include_sumw2:
        selected_variances = variances[selected]
        selected_variances = np.where(
            np.isfinite(selected_variances) & (selected_variances < 0),
            0.0,
            selected_variances,
        )
        np.add.at(sumw2_flat, flat[selected], selected_variances)
    return contents, sumw2


def project_sparse_bins_batch(file_name: str | Path,
                              requests: list[dict[str, Any]]):
    """Project multiple sparse histograms from one file.

    Each request contains ``histogram_path``, ``output_axes``, ``axis_maps``,
    ``output_shape``, and optional ``include_sumw2`` fields. The compiled path
    keeps one ROOT file open for the whole batch. The fallback preserves the
    same results while using the single-histogram reader.
    """
    reader = _compiled_reader()
    compiled_requests = []
    for request in requests:
        maps = [np.ascontiguousarray(axis_map, dtype=np.int64)
                for axis_map in request["axis_maps"]]
        compiled_requests.append((
            str(request["histogram_path"]),
            list(request["output_axes"]),
            maps,
            list(request["output_shape"]),
            bool(request.get("include_sumw2", True)),
        ))

    if reader is not None and hasattr(reader, "project_sparse_bins_batch"):
        return reader.project_sparse_bins_batch(str(file_name), compiled_requests)

    return [
        project_sparse_bins(
            file_name,
            request["histogram_path"],
            request["output_axes"],
            request["axis_maps"],
            request["output_shape"],
            include_sumw2=request.get("include_sumw2", True),
        )
        for request in requests
    ]


def supports_parallel_sparse_projection() -> bool:
    """Return whether the native reader can safely overlap independent files."""
    reader = _compiled_reader()
    return reader is not None and hasattr(reader, "project_sparse_bins_batch")


class SystematicAccumulator:
    """Accumulate correlated variation shifts, using the C++ binding when present.

    ``add`` accepts values shaped ``[variation, channel, bin]``. An empty
    ``(nuisance, direction)`` pair identifies the nominal slice. Other pairs
    are correlated across calls by their exact nuisance and direction labels.
    """

    def __init__(self, channels: int, bins: int):
        if channels <= 0 or bins <= 0:
            raise ValueError("Systematic accumulator dimensions must be positive")
        reader = _compiled_reader()
        native = getattr(reader, "SystematicAccumulator", None) if reader else None
        self._native = native(channels, bins) if native is not None else None
        self._channels = channels
        self._bins = bins
        self._shifts: dict[tuple[str, str], np.ndarray] = {}

    def add(self, values: np.ndarray,
            variation_keys: list[tuple[str, str]], scale: float) -> None:
        values = np.ascontiguousarray(values, dtype=np.float64)
        if (values.ndim != 3 or values.shape[1] != self._channels or
                values.shape[2] != self._bins or
                len(variation_keys) != values.shape[0]):
            raise ValueError(
                "Systematic values must have shape [variations, channels, bins]"
            )
        if not np.isfinite(scale):
            raise ValueError("Systematic scale must be finite")
        keys = [(str(nuisance), str(direction))
                for nuisance, direction in variation_keys]
        if self._native is not None:
            self._native.add(values, keys, float(scale))
            return

        try:
            nominal_index = keys.index(("", ""))
        except ValueError as exc:
            raise ValueError("Systematic values have no nominal slice") from exc
        nominal = values[nominal_index]
        for index, key in enumerate(keys):
            if not key[0]:
                continue
            if key[1] not in {"up", "down"}:
                raise ValueError("Systematic direction must be 'up' or 'down'")
            self._shifts.setdefault(key, np.zeros((self._channels, self._bins)))
            self._shifts[key] += (values[index] - nominal) * scale

    def shifts(self) -> dict[tuple[str, str], np.ndarray]:
        if self._native is not None:
            return self._native.shifts()
        return self._shifts


def _arrays_from_histogram(histogram: Any):
    dimensions = int(histogram.GetNdimensions())
    count = int(histogram.GetNbins())
    coordinates = np.empty((count, dimensions), dtype=np.int32)
    contents = np.empty(count, dtype=np.float64)
    sumw2 = np.empty(count, dtype=np.float64)
    for index, (bins, value, variance) in enumerate(iter_sparse_bins(histogram)):
        coordinates[index, :] = bins
        contents[index] = value
        sumw2[index] = variance
    return coordinates, contents, sumw2
