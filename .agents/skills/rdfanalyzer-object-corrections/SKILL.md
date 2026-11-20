---
name: rdfanalyzer-object-corrections
description: Apply or modify correctionlib evaluation, jet and lepton energy scale/smearing managers, corrected physics-object collections, and original-index mapping in RDFAnalyzerCore.
---

# Object corrections and collections

Select the detail reference for the requested family:

- [Correctionlib and energy managers](references/energy-managers.md): CorrectionManager, JES/JER, electron/photon/tau scales, and muon Rochester corrections.
- [Physics-object collections](references/collections.md): indices, overlap removal, corrected collection wrappers, and workflow actions.

Start with the input column and the public manager method called by the analysis. Establish whether it schedules an action or defines the column immediately. Follow output pt **and mass**, collection indices, random/smearing inputs, and variation registrations through the downstream selection. Do not correct a nominal object twice or combine features indexed in different collection spaces.

Use [CMS](../rdfanalyzer-cms/SKILL.md) to resolve era payloads and MET propagation; energy corrections alone do not prove the MET vector or object-selection masks were updated. Use [systematics](../rdfanalyzer-systematics/SKILL.md) for derived-column/acceptance propagation and [tagging](../rdfanalyzer-tagging/SKILL.md) for discriminator weights.

Read [JET_ENERGY_CORRECTIONS](../../../docs/JET_ENERGY_CORRECTIONS.md) and [PHYSICS_OBJECTS](../../../docs/PHYSICS_OBJECTS.md) when their examples fit. Source and closest tests are identified in the family references. For a new correction, verify the payload's ordered input schema, scalar/vector types, output names, data/MC branch, and one concrete numerical case before using it in production.
