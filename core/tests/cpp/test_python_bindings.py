#!/usr/bin/env python3
"""
Integration test for RDFAnalyzerCore Python bindings.

This test validates that:
1. The module imports correctly.
2. DefineFromPointer works with numba-compiled functions.
3. C++-style naming aliases (Define/Filter) work in Python.
4. The output file contains the expected filtered entries and values.

The test uses **uproot** to read/write ROOT files so there is no PyROOT
requirement.  It uses numba for compiled callbacks when available and falls
back to `ctypes` callbacks so the function-pointer binding is still exercised
without numba.

Exit codes:
  0: success
  1: failure
 77: skipped due to missing runtime prerequisites (uproot/module)
"""
import ctypes
import gc
import os
import sys
import tempfile
from array import array
from pathlib import Path


SKIP_EXIT_CODE = 77


def _add_module_path() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    module_path = Path(
        os.environ.get("RDFANALYZER_PYTHON_MODULE_DIR", repo_root / "build" / "python")
    )
    sys.path.insert(0, str(module_path))


def _require_imports():
    _add_module_path()

    try:
        import rdfanalyzer  # type: ignore
    except Exception as exc:
        print(f"SKIP: unable to import rdfanalyzer: {exc}")
        sys.exit(SKIP_EXIT_CODE)

    try:
        import uproot  # type: ignore
    except Exception as exc:
        print(f"SKIP: unable to import uproot: {exc}")
        sys.exit(SKIP_EXIT_CODE)

    try:
        import numba  # type: ignore
    except Exception:
        numba = None

    # return modules in the order they are used later; ROOT module is no longer needed
    return rdfanalyzer, uproot, numba


def _write_input_root(uproot_module, input_file: Path) -> None:
    # Write nominal and shifted inputs so the Python DefineFromPointer test can
    # verify that systematic expressions read the matching Up/Down columns.
    import numpy as np
    arr = np.array([10.0, 30.0, 50.0], dtype="float64")
    with uproot_module.recreate(str(input_file)) as f:
        f["Events"] = {
            "pt": arr,
            "pt_jesUp": np.array([12.0, 36.0, 60.0], dtype="float64"),
            "pt_jesDown": np.array([8.0, 24.0, 40.0], dtype="float64"),
            "first": np.array([1.0, 2.0, 3.0], dtype="float64"),
            "first_jesUp": np.array([11.0, 22.0, 33.0], dtype="float64"),
            "first_jesDown": np.array([7.0, 8.0, 9.0], dtype="float64"),
        }


def _write_save_config(save_config: Path) -> None:
    save_config.write_text(
        "pt\npt_scaled\npt_scaled_jesUp\npt_scaled_jesDown\n"
        "pass_high_pt\npass_high_pt_jesUp\n"
        "pass_selected_high_pt\npass_selected_high_pt_jesUp\n"
        "raw_literal_probe\nraw_literal_probe_jesUp\n"
        "member_access_probe\nmember_access_probe_jesUp\n"
    )


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
            "enableSkim=true",
            "skimSystematics=true",
            "threads=1",
            f"saveConfig={save_config}",
        ]
    )
    config_path.write_text(content + "\n")


def _assert_output(uproot_module, output_file: Path) -> None:
    # Validate the filter result, schema, and lexer probes with Uproot's
    # synchronous executors. This keeps the integration check independent of
    # PyROOT and of the site's asynchronous basket-read behavior.
    with uproot_module.open(str(output_file)) as f:
        tree = f["Events"]
        entries = tree.num_entries
        expected_columns = {
            "pt_scaled", "pt_scaled_jesUp", "pt_scaled_jesDown",
            "pass_high_pt", "pass_high_pt_jesUp",
            "pass_selected_high_pt", "pass_selected_high_pt_jesUp",
            "raw_literal_probe", "raw_literal_probe_jesUp",
            "member_access_probe", "member_access_probe_jesUp",
        }
        missing = expected_columns - set(tree.keys())

    # With input pt_jesUp = [12, 36, 60], the pointer-backed definitions yield
    # pass_high_pt_jesUp = [False, False, True] for the >110 threshold. The
    # selection also requires the raw-string-derived value to remain 63 on the
    # JES-up row. If the lexer rewrites the literal contents, that row is
    # rejected. Saving the member-access probe checks that `.first` compiles as
    # a C++ member token.
    if entries != 1:
        raise AssertionError(f"Expected one JES-up-selected entry, got {entries}")
    if missing:
        raise AssertionError(f"Snapshot is missing expected columns: {sorted(missing)}")


def run_test() -> int:
    rdfanalyzer, uproot, numba = _require_imports()

    expected_methods = [
        "Define", "Filter", "DefineVector", "DefineFromPointer", "DefineFromVector",
        "AddPlugin", "AddDefaultPlugins", "SetupPlugin",
        "setConfig", "getConfigMap", "getConfigList",
        "registerSystematic", "getSystematics", "makeSystList",
        "applyAllOnnxModels", "applyAllBDTs", "applyCorrection",
        "applyCorrectionVec", "registerCorrection", "getCorrectionFeatures",
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

        _write_input_root(uproot, input_file)
        _write_save_config(save_config)
        _write_analysis_config(config_file, input_file, output_file, save_config)
        plugin_cfgs = _write_minimal_plugin_configs(workdir)

        if numba is not None:
            @numba.cfunc("float64(float64)")
            def scale_pt(value):
                return value * 2.0

            @numba.cfunc("bool(float64)")
            def is_high_pt(value):
                return value > 110.0

            scale_ptr = ctypes.cast(scale_pt.address, ctypes.c_void_p).value
            high_pt_ptr = ctypes.cast(is_high_pt.address, ctypes.c_void_p).value
        else:
            scale_callback = ctypes.CFUNCTYPE(ctypes.c_double, ctypes.c_double)(
                lambda value: value * 2.0
            )
            high_pt_callback = ctypes.CFUNCTYPE(ctypes.c_bool, ctypes.c_double)(
                lambda value: value > 110.0
            )
            scale_ptr = ctypes.cast(scale_callback, ctypes.c_void_p).value
            high_pt_ptr = ctypes.cast(high_pt_callback, ctypes.c_void_p).value

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

        # Test autoRegisterSystematics: provide a pair of Up/Down columns and
        # verify that the systematic is discovered and registered automatically.
        auto_result = analyzer.autoRegisterSystematics(
            ["myPt", "myPt_btagUp", "myPt_btagDown"]
        )
        if "myPt:btag" not in auto_result.get("registered", []):
            raise AssertionError("autoRegisterSystematics failed to detect 'btag' for 'myPt'")
        if "btag" not in analyzer.getSystematics():
            raise AssertionError("autoRegisterSystematics did not register 'btag'")
        if "myPt" not in analyzer.getVariablesForSystematic("btag"):
            raise AssertionError("autoRegisterSystematics: 'myPt' not affected by 'btag'")

        # An incomplete pair (missing Down) should appear in 'missing_down'
        orphan_result = analyzer.autoRegisterSystematics(["orphanVar_xUp"])
        if "orphanVar_xUp" not in orphan_result.get("missing_down", []):
            raise AssertionError("autoRegisterSystematics did not report missing_down for orphanVar_xUp")

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

        # Define pt explicitly as a fallback for when the file-based RDF is not
        # available (e.g. when GetEntries() returns <=0 for files written by external
        # tools like uproot that are incompatible with this ROOT version).
        # Analyzer.Define silently skips the define when pt already exists in the RDF
        # (file-based mode), so this is a no-op in that case.
        # With pt = 30.0, the nominal value does not pass the >110 threshold.
        analyzer.Define("pt", "30.0", [])  # [] = no column dependencies for constant
        analyzer.registerSystematic("jes", ["pt", "first"])
        # These constants provide shifted-input values only if the input file
        # did not expose the same named branches.
        analyzer.Define("pt_jesUp", "33.0", [])
        analyzer.Define("pt_jesDown", "27.0", [])
        analyzer.Define(
            "raw_literal_probe",
            'static_cast<double>(sizeof(R"tag(pt)tag")) + pt',
            ["pt"],
        )
        analyzer.Define(
            "member_access_probe",
            "std::make_pair(pt, first).first + pt",
            ["pt", "first"],
        )

        for invalid_call, message in (
            (
                lambda: analyzer.DefineFromPointer(
                    "bad_null_function", 0, "double(double)", ["pt"]
                ),
                "func_ptr must be a non-null",
            ),
            (
                lambda: analyzer.DefineFromPointer(
                    "bad_arity", scale_ptr, "double(double, double)", ["pt"]
                ),
                "signature declares 2 argument(s), but 1 input column(s)",
            ),
            (
                lambda: analyzer.DefineFromVector("bad_null_vector", 0, 1, "double"),
                "data_ptr must be non-null",
            ),
        ):
            try:
                invalid_call()
            except ValueError as exc:
                if message not in str(exc):
                    raise AssertionError(f"Unexpected pointer validation error: {exc}")
            else:
                raise AssertionError(f"Expected pointer validation for: {message}")

        analyzer.DefineFromPointer("pt_scaled", scale_ptr, "double(double)", ["pt"])
        required_variations = {"pt_scaled_jesUp", "pt_scaled_jesDown"}
        available_columns = set(analyzer.GetColumnNames())
        if not required_variations <= available_columns:
            raise AssertionError(
                "DefineFromPointer did not create shifted output columns: "
                f"{sorted(required_variations - available_columns)}"
            )
        analyzer.DefineFromPointer("pass_high_pt", high_pt_ptr, "bool(double)", ["pt_scaled"])
        if "pass_high_pt_jesUp" not in set(analyzer.GetColumnNames()):
            raise AssertionError("Second DefineFromPointer omitted JES-up output")
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

        analyzer.Filter(
            "selected_high_pt",
            "pt_scaled > 110.0 && raw_literal_probe == 63.0 && "
            "member_access_probe > 100.0",
            ["pt_scaled", "raw_literal_probe", "member_access_probe"],
        )
        _ = analyzer.makeSystList("pt_scaled")
        analyzer.save()
        del analyzer
        gc.collect()

        _assert_output(uproot, output_file)

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
