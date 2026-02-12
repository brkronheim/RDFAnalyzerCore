#!/usr/bin/env python3
"""
Integration test for RDFAnalyzerCore Python bindings.

This test validates that:
1. The module imports correctly.
2. DefineFromPointer works with numba-compiled functions.
3. C++-style naming aliases (Define/Filter) work in Python.
4. The output file contains the expected filtered entries and values.

Exit codes:
  0: success
  1: failure
 77: skipped due to missing runtime prerequisites (ROOT/numba/module)
"""

import ctypes
import math
import sys
import tempfile
from array import array
from pathlib import Path


SKIP_EXIT_CODE = 77


def _add_module_path() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "build" / "python"
    sys.path.insert(0, str(module_path))


def _require_imports():
    _add_module_path()

    try:
        import rdfanalyzer  # type: ignore
    except Exception as exc:
        print(f"SKIP: unable to import rdfanalyzer: {exc}")
        sys.exit(SKIP_EXIT_CODE)

    try:
        import ROOT  # type: ignore
    except Exception as exc:
        print(f"SKIP: unable to import ROOT: {exc}")
        sys.exit(SKIP_EXIT_CODE)

    try:
        import numba  # type: ignore
    except Exception as exc:
        print(f"SKIP: unable to import numba: {exc}")
        sys.exit(SKIP_EXIT_CODE)

    return rdfanalyzer, ROOT, numba


def _write_input_root(root_module, input_file: Path) -> None:
    root_file = root_module.TFile(str(input_file), "RECREATE")
    tree = root_module.TTree("Events", "Events")

    pt = array("d", [0.0])
    tree.Branch("pt", pt, "pt/D")

    for value in (10.0, 30.0, 50.0):
        pt[0] = value
        tree.Fill()

    tree.Write()
    root_file.Close()


def _write_save_config(save_config: Path) -> None:
    save_config.write_text("pt\npt_scaled\npass_high_pt\n")


def _write_minimal_plugin_configs(workdir: Path) -> dict[str, Path]:
    files = {
        "onnxConfig": workdir / "onnx.cfg",
        "bdtConfig": workdir / "bdt.cfg",
        "triggerConfig": workdir / "trigger.cfg",
        "sofieConfig": workdir / "sofie.cfg",
        "pairConfig": workdir / "pairs.cfg",
        "vectorConfig": workdir / "vector.cfg",
        "multikeyConfig": workdir / "multi.cfg",
    }
    files["onnxConfig"].write_text("")
    files["bdtConfig"].write_text("")
    files["triggerConfig"].write_text("")
    files["sofieConfig"].write_text("")
    files["pairConfig"].write_text("alpha=1\nbeta=two\n")
    files["vectorConfig"].write_text("x\ny\nz\n")
    files["multikeyConfig"].write_text(
        "name=groupA sample=123 triggers=HLT_A,HLT_B\n"
    )
    return files


def _write_analysis_config(config_path: Path, input_file: Path, output_file: Path, save_config: Path) -> None:
    content = "\n".join(
        [
            f"fileList={input_file}",
            f"saveFile={output_file}",
            "saveTree=Events",
            "threads=1",
            f"saveConfig={save_config}",
        ]
    )
    config_path.write_text(content + "\n")


def _assert_output(root_module, output_file: Path) -> None:
    root_file = root_module.TFile.Open(str(output_file), "READ")
    if not root_file or root_file.IsZombie():
        raise RuntimeError(f"Failed to open output file: {output_file}")

    tree = root_file.Get("Events")
    if tree is None:
        raise RuntimeError("Output file does not contain tree 'Events'")

    entries = tree.GetEntries()
    if entries != 2:
        raise AssertionError(f"Expected 2 entries after filtering, got {entries}")

    scaled_values = []
    pass_flags = []
    for idx in range(entries):
        tree.GetEntry(idx)
        scaled_values.append(float(tree.pt_scaled))
        pass_flags.append(bool(tree.pass_high_pt))

    expected_scaled = [60.0, 100.0]
    for actual, expected in zip(scaled_values, expected_scaled):
        if not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12):
            raise AssertionError(
                f"Unexpected pt_scaled value. Expected {expected}, got {actual}"
            )

    if pass_flags != [True, True]:
        raise AssertionError(f"Expected pass_high_pt flags [True, True], got {pass_flags}")

    root_file.Close()


def run_test() -> int:
    rdfanalyzer, root_module, numba = _require_imports()

    expected_methods = [
        "Define", "Filter", "DefineVector", "DefineFromPointer", "DefineFromVector",
        "AddPlugin", "AddDefaultPlugins", "SetupPlugin",
        "setConfig", "getConfigMap", "getConfigList",
        "registerSystematic", "getSystematics", "makeSystList",
        "applyAllOnnxModels", "applyAllBDTs", "applyCorrection",
        "applyAllTriggers", "applyAllSofieModels",
        "bookNDHistograms", "saveNDHistograms", "clearNDHistograms",
    ]
    for method in expected_methods:
        if not hasattr(rdfanalyzer.Analyzer, method):
            raise AssertionError(f"Analyzer is missing expected method: {method}")

    if not hasattr(rdfanalyzer, "HistInfo"):
        raise AssertionError("Module is missing expected class: HistInfo")
    if not hasattr(rdfanalyzer, "SelectionInfo"):
        raise AssertionError("Module is missing expected class: SelectionInfo")

    with tempfile.TemporaryDirectory(prefix="rdf_pybind_test_") as tmpdir:
        workdir = Path(tmpdir)
        input_file = workdir / "input.root"
        output_file = workdir / "output.root"
        save_config = workdir / "save_columns.txt"
        config_file = workdir / "config.txt"

        _write_input_root(root_module, input_file)
        _write_save_config(save_config)
        _write_analysis_config(config_file, input_file, output_file, save_config)
        plugin_cfgs = _write_minimal_plugin_configs(workdir)

        @numba.cfunc("float64(float64)")
        def scale_pt(value):
            return value * 2.0

        @numba.cfunc("bool(float64)")
        def is_high_pt(value):
            return value > 50.0

        scale_ptr = ctypes.cast(scale_pt.address, ctypes.c_void_p).value
        high_pt_ptr = ctypes.cast(is_high_pt.address, ctypes.c_void_p).value

        analyzer = rdfanalyzer.Analyzer(str(config_file))

        analyzer.setConfig("onnxConfig", str(plugin_cfgs["onnxConfig"]))
        analyzer.setConfig("bdtConfig", str(plugin_cfgs["bdtConfig"]))
        analyzer.setConfig("triggerConfig", str(plugin_cfgs["triggerConfig"]))
        analyzer.setConfig("sofieConfig", str(plugin_cfgs["sofieConfig"]))
        analyzer.setConfig("type", "123")

        cfg_map = analyzer.getConfigMap()
        if cfg_map.get("type") != "123":
            raise AssertionError("setConfig/getConfigMap failed for key 'type'")

        analyzer.setConfig("csvValues", "a,b,c")
        if analyzer.getConfigList("csvValues") != ["a", "b", "c"]:
            raise AssertionError("getConfigList did not split comma-delimited values as expected")

        parsed_pairs = analyzer.parsePairBasedConfig(str(plugin_cfgs["pairConfig"]))
        if parsed_pairs.get("alpha") != "1":
            raise AssertionError("parsePairBasedConfig failed")

        parsed_vector = analyzer.parseVectorConfig(str(plugin_cfgs["vectorConfig"]))
        if parsed_vector != ["x", "y", "z"]:
            raise AssertionError("parseVectorConfig failed")

        parsed_multi = analyzer.parseMultiKeyConfig(
            str(plugin_cfgs["multikeyConfig"]), ["name", "sample", "triggers"]
        )
        if not parsed_multi or parsed_multi[0].get("name") != "groupA":
            raise AssertionError("parseMultiKeyConfig failed")

        analyzer.registerSystematic("jes", ["dummyVar"])
        if "jes" not in analyzer.getSystematics():
            raise AssertionError("registerSystematic/getSystematics failed")

        analyzer.registerExistingSystematics(["jer"], ["dummyExisting_jerUp"])
        if "jer" not in analyzer.getSystematicsForVariable("dummyExisting"):
            raise AssertionError("registerExistingSystematics/getSystematicsForVariable failed")

        analyzer.AddPlugin("onnx", "OnnxManager")
        analyzer.AddPlugin("bdt", "BDTManager")
        analyzer.AddPlugin("trigger", "TriggerManager")
        analyzer.AddPlugin("sofie", "SofieManager")
        analyzer.AddPlugin("hist", "NDHistogramManager")

        analyzer.applyAllOnnxModels("onnx")
        analyzer.applyAllBDTs("bdt")
        analyzer.applyAllSofieModels("sofie")

        if analyzer.getOnnxModelNames("onnx") != []:
            raise AssertionError("Expected no ONNX models with empty config")
        if analyzer.getBDTNames("bdt") != []:
            raise AssertionError("Expected no BDT models with empty config")
        if analyzer.getTriggerGroups("trigger") != []:
            raise AssertionError("Expected no trigger groups with empty config")
        analyzer.DefineFromPointer("pt_scaled", scale_ptr, "double(double)", ["pt"])
        analyzer.DefineFromPointer("pass_high_pt", high_pt_ptr, "bool(double)", ["pt_scaled"])
        analyzer.Define("pass_high_pt_copy", "pass_high_pt", ["pass_high_pt"])

        hist_info = rdfanalyzer.HistInfo(
            "h_pt_scaled",
            "pt_scaled",
            "pt scaled",
            "pt_scaled",
            10,
            0.0,
            200.0,
        )
        if hist_info.name != "h_pt_scaled":
            raise AssertionError("HistInfo binding getter failed")

        selection_info = rdfanalyzer.SelectionInfo(
            "pass_high_pt_copy", 2, 0.0, 2.0, ["all"]
        )
        if selection_info.variable != "pass_high_pt_copy":
            raise AssertionError("SelectionInfo binding getter failed")

        region_names = analyzer.bookNDHistograms(
            "hist", [hist_info], [selection_info], "unit_test"
        )
        if len(region_names) == 0:
            raise AssertionError("bookNDHistograms returned no region metadata")

        analyzer.clearNDHistograms("hist")

        analyzer.Filter("selected_high_pt", "pass_high_pt_copy", ["pass_high_pt_copy"])
        _ = analyzer.makeSystList("pt_scaled")
        analyzer.save()

        _assert_output(root_module, output_file)

    print("PASS: Python bindings numba DefineFromPointer + Define/Filter alias integration")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_test())
    except SystemExit:
        raise
    except Exception as exc:
        print(f"FAIL: {exc}")
        sys.exit(1)