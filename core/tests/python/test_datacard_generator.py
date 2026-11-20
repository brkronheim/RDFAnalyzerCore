#!/usr/bin/env python3
"""
Originally a PyROOT-based datacard generator test; now rewritten to use
uproot so that no ROOT Python bindings are required.

The logic mirrors test_uproot_datacard.py but remains self-contained so the
existing CTest rule can continue to execute this file.
"""

import os
import sys
import tempfile
import pytest

# Add parent directory to path to import the library under test
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# dependencies
import yaml

try:
    import uproot
except ImportError:
    uproot = None

import numpy as np


def create_mock_root_file(filename, histograms):
    """Create a mock ROOT file with simple histograms using uproot.

    `histograms` should be a dict mapping hist_name -> (values_array, edges_array)
    """
    with uproot.recreate(filename) as f:
        for name, (values, edges) in histograms.items():
            f[name] = (values, edges)

def create_test_config(test_dir, input_dir):
    """Create a concise YAML configuration for the datacard generator."""
    config = {
        'output_dir': os.path.join(test_dir, 'output'),
        'input_files': {
            'data_obs': {'path': os.path.join(input_dir, 'data.root'), 'type': 'data'},
            'signal': {'path': os.path.join(input_dir, 'signal.root'), 'type': 'signal'},
            'ttbar': {'path': os.path.join(input_dir, 'ttbar.root'), 'type': 'background'},
        },
        'processes': {
            'signal': {'samples': ['signal'], 'description': 'Test signal'},
            'ttbar': {'samples': ['ttbar'], 'description': 'Top pair'},
        },
        'control_regions': {
            'signal_region': {
                'observable': 'mT',
                'processes': ['signal', 'ttbar'],
                'signal_processes': ['signal'],
                'data_process': 'data_obs',
                'description': 'Test signal region'
            }
        },
        'systematics': {
            'lumi': {
                'type': 'rate',
                'distribution': 'lnN',
                'value': 1.025,
                'applies_to': {'signal': True, 'ttbar': True},
                'regions': ['signal_region'],
                'description': 'Luminosity'
            },
            'JES': {
                'type': 'shape',
                'distribution': 'shape',
                'variation': 0.03,
                'applies_to': {'signal': True, 'ttbar': True},
                'regions': ['signal_region'],
                'description': 'Jet energy scale'
            }
        }
    }
    config_file = os.path.join(test_dir, 'test_config.yaml')
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    print(f"Created test configuration: {config_file}")
    return config_file


def _run_datacard_generator():
    """Main test function using uproot-based histogram files."""
    print("=" * 80)
    print("Testing Datacard Generator (pure uproot)")
    print("=" * 80)

    # Create temporary directory for test files
    test_dir = tempfile.mkdtemp(prefix='datacard_test_')
    input_dir = os.path.join(test_dir, 'input')
    os.makedirs(input_dir, exist_ok=True)

    try:
        print("\nCreating mock ROOT files...")
        # uniform binning for simplicity
        edges = np.linspace(0, 500, 51)

        create_mock_root_file(
            os.path.join(input_dir, 'data.root'),
            {'mT': (np.random.poisson(100, 50), edges)}
        )
        create_mock_root_file(
            os.path.join(input_dir, 'signal.root'),
            {
                'mT': (np.random.poisson(10, 50), edges),
                'mT_JESUp': (np.random.poisson(10, 50) * 1.03, edges),
                'mT_JESDown': (np.random.poisson(10, 50) * 0.97, edges),
            }
        )
        create_mock_root_file(
            os.path.join(input_dir, 'ttbar.root'),
            {
                'mT': (np.random.poisson(50, 50), edges),
                'mT_JESUp': (np.random.poisson(50, 50) * 1.03, edges),
                'mT_JESDown': (np.random.poisson(50, 50) * 0.97, edges),
            }
        )

        # Create test configuration
        print("\nCreating test configuration...")
        config_file = create_test_config(test_dir, input_dir)

        # Import and run the datacard generator
        print("\nRunning datacard generator...")
        from create_datacards import DatacardGenerator
        generator = DatacardGenerator(config_file)
        generator.run()

        # Verify outputs
        print("\nVerifying outputs...")
        output_dir = os.path.join(test_dir, 'output')
        expected_files = [
            'datacard_signal_region.txt',
            'shapes_signal_region.root'
        ]
        all_found = True
        for expected_file in expected_files:
            file_path = os.path.join(output_dir, expected_file)
            if os.path.exists(file_path):
                print(f"  ✓ Found: {expected_file}")
                size = os.path.getsize(file_path)
                print(f"    Size: {size} bytes")
                
                # For datacard, print first few lines
                if expected_file.endswith('.txt'):
                    print(f"    Content preview:")
                    with open(file_path, 'r') as f:
                        for i, line in enumerate(f):
                            if i < 10:
                                print(f"      {line.rstrip()}")
                
                # For ROOT file, check histograms with uproot
                if expected_file.endswith('.root'):
                    with uproot.open(file_path) as rf:
                        print(f"    Histograms: {rf.keys()}")
            else:
                print(f"  ✗ Missing: {expected_file}")
                all_found = False
        
        print("\n" + "=" * 80)
        if all_found:
            print("TEST PASSED: All expected files were created")
        else:
            print("TEST FAILED: Some expected files are missing")
        print("=" * 80)
        
        # Keep test directory for inspection
        print(f"\nTest files are in: {test_dir}")
        print("(Directory will not be automatically cleaned up)")
        
        return all_found
        
    except Exception as e:
        print(f"\nTEST FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    if uproot is None:
        print("SKIP: uproot not available")
        sys.exit(77)
    success = _run_datacard_generator()
    sys.exit(0 if success else 1)


def test_datacard_generator():
    """Expose the legacy script check as a failing/skip-aware pytest test."""
    if uproot is None:
        pytest.skip("uproot is not available")
    assert _run_datacard_generator(), "datacard generator output checks failed"


def _make_generator(tmp_path, input_file, systematics=None):
    from create_datacards import DatacardGenerator

    config = {
        "output_dir": str(tmp_path / "output"),
        "input_files": {"sig": {"path": str(input_file)}},
        "processes": {"signal": {"samples": ["sig"]}},
        "control_regions": {
            "sr": {
                "observable": "mT",
                "processes": ["signal"],
                "signal_processes": ["signal"],
            }
        },
        "systematics": systematics or {},
    }
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.safe_dump(config))
    return DatacardGenerator(str(config_file))


def _write_weighted_root_histogram(filename, name, values, edges, variances):
    identify = uproot.writing.identify
    values = np.asarray(values, dtype=np.float64)
    edges = np.asarray(edges, dtype=np.float64)
    variances = np.asarray(variances, dtype=np.float64)
    axis = identify.to_TAxis(
        fName="xaxis",
        fTitle="",
        fNbins=len(values),
        fXmin=float(edges[0]),
        fXmax=float(edges[-1]),
        fXbins=edges,
    )
    root_hist = identify.to_TH1x(
        fName=name,
        fTitle=name,
        data=np.concatenate(([0.0], values, [0.0])),
        fEntries=float(np.sum(np.abs(values))),
        fTsumw=float(np.sum(values)),
        fTsumw2=float(np.sum(variances)),
        fTsumwx=float(np.dot(values, (edges[:-1] + edges[1:]) / 2.0)),
        fTsumwx2=float(np.dot(values, ((edges[:-1] + edges[1:]) / 2.0) ** 2)),
        fSumw2=np.concatenate(([0.0], variances, [0.0])),
        fXaxis=axis,
    )
    with uproot.recreate(filename) as root_file:
        root_file[name] = root_hist


def test_datacard_round_trip_preserves_variances_and_requests_combine_stats(tmp_path):
    if uproot is None:
        pytest.skip("uproot is not available")

    input_file = tmp_path / "weighted.root"
    values = np.array([4.0, 9.0, 16.0])
    edges = np.array([0.0, 1.0, 3.0, 6.0])
    variances = np.array([1.5, 7.25, 12.0])
    _write_weighted_root_histogram(input_file, "mT", values, edges, variances)

    generator = _make_generator(tmp_path, input_file)
    generator.read_histograms()
    generator.generate_datacard("sr")

    with uproot.open(tmp_path / "output" / "shapes_sr.root") as shapes:
        np.testing.assert_array_equal(shapes["signal"].values(), values)
        np.testing.assert_array_equal(shapes["signal"].axis().edges(), edges)
        np.testing.assert_allclose(shapes["signal"].variances(), variances)

    card = (tmp_path / "output" / "datacard_sr.txt").read_text()
    assert "* autoMCStats 0 1" in card


def test_variable_rebinning_sums_values_and_variances_only_at_source_edges(tmp_path):
    from create_datacards import Histogram1D

    generator = _make_generator(tmp_path, tmp_path / "unused.root")
    hist = Histogram1D(
        values=np.array([2.0, 3.0, 5.0, 7.0]),
        edges=np.array([0.0, 1.0, 2.0, 3.0, 4.0]),
        variances=np.array([0.5, 1.25, 2.5, 3.5]),
    )

    rebinned = generator._rebin_histogram(hist, [0.0, 2.0, 4.0])
    np.testing.assert_array_equal(rebinned.values, [5.0, 12.0])
    np.testing.assert_array_equal(rebinned.variances, [1.75, 6.0])

    with pytest.raises(ValueError, match="not an existing source bin edge"):
        generator._rebin_histogram(hist, [0.0, 1.5, 4.0])


def test_histogram_arithmetic_combines_and_scales_variances():
    from create_datacards import Histogram1D

    combined = Histogram1D([2.0, 3.0], [0.0, 1.0, 2.0], variances=[0.5, 1.0])
    combined.add(Histogram1D([4.0, 5.0], [0.0, 1.0, 2.0], variances=[1.5, 2.0]))
    combined.scale(2.0)
    rebinned = combined.rebin(2)

    np.testing.assert_array_equal(combined.values, [12.0, 16.0])
    np.testing.assert_array_equal(combined.variances, [8.0, 12.0])
    np.testing.assert_array_equal(rebinned.values, [28.0])
    np.testing.assert_array_equal(rebinned.variances, [20.0])


@pytest.mark.parametrize(
    ("available_directions", "missing_directions"),
    [
        ({"Down"}, "Up"),
        ({"Up"}, "Down"),
        (set(), "Up, Down"),
    ],
)
def test_missing_shape_direction_fails_with_process_and_systematic_context(
    tmp_path, available_directions, missing_directions
):
    from create_datacards import Histogram1D

    generator = _make_generator(
        tmp_path,
        tmp_path / "unused.root",
        systematics={"JES": {"type": "shape", "variation": 0.1}},
    )
    generator.processes = {"signal": {"samples": ["sig"]}}
    generator.histograms = {
        "sig": {
            "mT": Histogram1D([4.0, 6.0], [0.0, 1.0, 2.0]),
            **{
                f"mT_JES{direction}": Histogram1D([5.0, 5.0], [0.0, 1.0, 2.0])
                for direction in available_directions
            },
        }
    }
    stale_shapes = tmp_path / "output" / "shapes_sr.root"
    stale_card = tmp_path / "output" / "datacard_sr.txt"
    stale_shapes.write_bytes(b"stale shapes from a previous successful generation")
    stale_card.write_text("stale card from a previous successful generation\n")

    with pytest.raises(ValueError) as exc_info:
        generator.generate_datacard("sr")

    message = str(exc_info.value)
    assert "process 'signal'" in message
    assert "systematic 'JES'" in message
    assert f"direction '{missing_directions.split(',')[0]}'" in message
    assert "sample 'sig'" in message
    assert not stale_shapes.exists()
    assert not stale_card.exists()


def test_aggregate_shape_requires_each_component_sample_direction(tmp_path):
    from create_datacards import Histogram1D

    generator = _make_generator(
        tmp_path,
        tmp_path / "unused.root",
        systematics={"JES": {"type": "shape"}},
    )
    generator.processes = {"signal": {"samples": ["sigA", "sigB"]}}
    generator.histograms = {
        "sigA": {
            "mT": Histogram1D([4.0, 6.0], [0.0, 1.0, 2.0]),
            "mT_JESUp": Histogram1D([5.0, 7.0], [0.0, 1.0, 2.0]),
            "mT_JESDown": Histogram1D([3.0, 5.0], [0.0, 1.0, 2.0]),
        },
        "sigB": {
            "mT": Histogram1D([5.0, 7.0], [0.0, 1.0, 2.0]),
            "mT_JESDown": Histogram1D([4.0, 6.0], [0.0, 1.0, 2.0]),
        },
    }

    with pytest.raises(ValueError) as exc_info:
        generator.generate_datacard("sr")

    message = str(exc_info.value)
    assert "process 'signal'" in message
    assert "systematic 'JES'" in message
    assert "direction 'Up'" in message
    assert "sample 'sigB'" in message


def test_zero_and_negative_shape_templates_are_kept_without_fabrication(tmp_path):
    from create_datacards import Histogram1D

    generator = _make_generator(
        tmp_path,
        tmp_path / "unused.root",
        systematics={"JES": {"type": "shape", "variation": 0.1}},
    )
    generator.processes = {"signal": {"samples": ["sig"]}}
    generator.histograms = {
        "sig": {
            "mT": Histogram1D([4.0, 6.0], [0.0, 1.0, 2.0]),
            "mT_JESUp": Histogram1D(
                [0.0, 0.0], [0.0, 1.0, 2.0], variances=[0.0, 0.0]
            ),
            "mT_JESDown": Histogram1D(
                [-2.0, 1.0], [0.0, 1.0, 2.0], variances=[0.25, 0.75]
            ),
        }
    }

    variations = generator._get_systematic_histograms(
        "signal", generator.histograms["sig"]["mT"], "sr"
    )
    np.testing.assert_array_equal(variations["JESUp"].values, [0.0, 0.0])
    np.testing.assert_array_equal(variations["JESDown"].values, [-2.0, 1.0])
    np.testing.assert_array_equal(variations["JESDown"].variances, [0.25, 0.75])

    generator.generate_datacard("sr")
    with uproot.open(tmp_path / "output" / "shapes_sr.root") as shapes:
        np.testing.assert_array_equal(shapes["signal_JESUp"].values(), [0.0, 0.0])
        np.testing.assert_array_equal(shapes["signal_JESDown"].values(), [-2.0, 1.0])
        np.testing.assert_array_equal(shapes["signal_JESDown"].variances(), [0.25, 0.75])
