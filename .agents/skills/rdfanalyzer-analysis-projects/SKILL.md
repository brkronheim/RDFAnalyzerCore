---
name: rdfanalyzer-analysis-projects
description: Locate checkout-specific RDFAnalyzerCore analyses and their executable, configuration, production, plotting, fitting, or reweighting entry points.
---

# Analysis project navigation

Use after [rdfanalyzer-analysis](../rdfanalyzer-analysis/SKILL.md) identifies the analysis. These references are source navigation maps, not installed-framework defaults. Read only the relevant one:

| Project or task | Reference |
|---|---|
| vjet-njetpt selections, weights, MET, response storage, configs | [vjet event workflow](references/vjet-events.md) |
| vjet saved fit, multi-era cards, polynomial plots, correction export | [vjet fit workflow](references/vjet-fits.md) |
| VH-RDF resolved/boosted selections, taggers, fits, models | [VH-RDF](references/vh-rdf.md) |
| nlo-reweight skims/training/export, testReweight comparisons, stitching | [Reweighting and stitching](references/reweighting.md) |
| ExampleAnalysis or CMS_Run2016_DoubleMuon | [Examples and open data](references/examples.md) |
| IRIS-HEP AGC v2 RDF/Coffea processing and BDT inference comparison | Separate AGC repository |

Before changes near a project, record `git -C analyses/<name> rev-parse --show-toplevel`, `rev-parse HEAD`, and `status --short`. Some directories inherit the framework Git root; others are independent, and a checkout can have no valid commit. Root `.gitignore` ignores `analyses/*`; a clean framework status says nothing about nested work. Keep the reusable skill files in this framework's tracked `.agents/skills`, not in an ignored analysis output directory.

Check each project's current `CMakeLists.txt` and entry point. A directory without one can be a Python comparison package rather than a CMake analysis. Follow generated configs and delegated scripts instead of inferring behavior from wrapper names. Preserve existing run directories, model payloads, and external analysis changes. Optional projects may be absent in another checkout; use the reference to identify what to inspect, then report absence rather than substituting another analysis.

Route reusable changes back to [development](../rdfanalyzer-development/SKILL.md). For [production](../rdfanalyzer-production/SKILL.md), [plotting](../rdfanalyzer-plotting/SKILL.md), or [statistics](../rdfanalyzer-statistics/SKILL.md), combine that domain skill with the relevant project reference; do not load all project references.

The optional [VHbb RDF tools](../../../analyses/VHbbSTXS/README.md) own the historical
preflight, process/group inference and migration-bin policy extracted from core.
This local tools repository is not a full VHqqRDF analysis checkout; do not
confuse it with the Coffea project under VH-RDF.
