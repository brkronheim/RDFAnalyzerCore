---
name: rdfanalyzer-cms
description: Locate the optional CMS extension's owned workflows for era resolution, corrections, MET, weights and discovery.
---

# CMS extension workflows

CMS implementation guidance ships in the independently versioned extension.
Read [CMS extension skill](../../../extensions/cms/.agents/skills/rdfanalyzer-cms-extension/SKILL.md),
which routes to its era, correction, cleaning, object, weight and production
workflows. Its source links are relative to that repository and work when it is
cloned elsewhere. If absent, follow [extension setup](../../../docs/CMS_EXTENSION.md)
or locate the checkout selected through `RDF_EXTENSION_PATHS`.

Keep generic STXS, correction evaluation, weight products and statistical
integration in core. Use [STXS](../rdfanalyzer-stxs/SKILL.md),
[object corrections](../rdfanalyzer-object-corrections/SKILL.md),
[weights](../rdfanalyzer-weights/SKILL.md), and
[production](../rdfanalyzer-production/SKILL.md) for those framework contracts.
Keep selections and analysis-specific policies in the owning analysis.

Legacy routes: [eras](references/eras.md), [corrections/MET](references/corrections-met.md),
and [cleaning](references/cleaning.md) forward to the extension guidance.
