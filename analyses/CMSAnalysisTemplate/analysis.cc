/**
 * @file analysis.cc
 * @brief CMS analysis template — C++ binary entry point.
 *
 * This file is intentionally thin.  All analysis logic (object collections,
 * event cuts, region definitions, derived quantities, plugin setup) lives in
 * @ref analysis_setup.h.  That header is the **single source of truth** shared
 * by both this binary and the Python pybind11 module in @ref analysis_bindings.cc.
 *
 * ## Build and run (C++)
 *
 *   cmake -S . -B build && cmake --build build -j$(nproc)
 *   cd build/analyses/CMSAnalysisTemplate
 *   ./cms_analysis_template ../../../analyses/CMSAnalysisTemplate/cfg.yaml
 *
 * ## Python usage (no recompilation needed for Python-level extensions)
 *
 *   # After building, add build/python to PYTHONPATH:
 *   export PYTHONPATH=$PWD/build/python:$PYTHONPATH
 *
 *   from analyses.CMSAnalysisTemplate.analysis_wrapper import CMSAnalysisBase
 *   base = CMSAnalysisBase("analyses/CMSAnalysisTemplate/cfg.yaml")
 *   an   = base.analyzer   # fully set up by C++ (setupCMSAnalysis called)
 *   an.Define("myVar", "TransverseMass * 2.0f")  # Python extension
 *   base.run()
 *
 * @see analysis_setup.h    — all analysis logic (the single C++ implementation)
 * @see analysis_bindings.cc — Python binding entry point
 * @see analysis_wrapper.py  — Python base class (CMSAnalysisBase)
 * @see README.md            — Full documentation
 */

#include "analysis_setup.h"
#include <iostream>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file.yaml>\n";
        return 1;
    }

    Analyzer an(argv[1]);
    setupCMSAnalysis(an);
    an.run();
    return 0;
}
