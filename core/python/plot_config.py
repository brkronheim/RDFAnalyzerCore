"""Translate extended plotting options without numerical work in Python."""
from __future__ import annotations

import json


def apply_plot_options(request, config, rdfanalyzer):
    """Copy optional settings through the shared, versioned C++ parser.

    Legacy callers can still build basic requests without the extended API.
    The parser handles enums, styles, nuisances, and input arrays identically
    for the compiled CLI, Python, and LAW tasks.
    """
    if not hasattr(rdfanalyzer.PlottingUtility, "parseRequests"):
        return request
    base = {
        "metaFile": request.metaFile,
        "outputFile": request.outputFile,
        "title": request.title,
        "xAxisTitle": request.xAxisTitle,
        "yAxisTitle": request.yAxisTitle,
        "logY": request.logY,
        "drawRatio": request.drawRatio,
    }
    base.update(config)
    # The output path and region-expanded title were resolved by the workflow.
    base["outputFile"] = request.outputFile
    base["title"] = request.title
    return rdfanalyzer.PlottingUtility.parseRequests(json.dumps([base]))[0]
