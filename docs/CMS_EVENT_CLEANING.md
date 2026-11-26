# Strict CMS event cleaning

`cms::applyCmsRun3MetFilters(analyzer)` requires the individual Run 3 flags:

- `Flag_goodVertices`
- `Flag_globalSuperTightHalo2016Filter`
- `Flag_EcalDeadCellTriggerPrimitiveFilter`
- `Flag_BadPFMuonFilter`
- `Flag_BadPFMuonDzFilter`
- `Flag_hfNoisyHitsFilter`
- `Flag_eeBadScFilter`
- `Flag_ecalBadCalibFilter`

All eight flags must exist and pass. Missing flags throw before the event loop.
The aggregate `Flag_METFilters`, HBHE noise/isolation flags and bad-charged-candidate
flag are not part of this Run 3 selection. `applyCmsMetFilters(analyzer, names)`
provides an explicit required flag list; the existing one-argument
`applyCmsMetFilters` retains its legacy list but now also requires every flag.

The stored ECAL flag is appropriate for the confirmed re-reconstructed 2022/2023
inputs and 2024 inputs used by `analyses/vjet-njetpt`. Affected 2022/2023 prompt-reco
data require the special ECAL crystal event selection instead; do not reuse this
stored-flag helper unchanged for those inputs. The under-review 2025/2026 ECAL
recommendation is not covered by this helper.

`JetVetoMapManager` requires exactly one period-specific map file containing one
era correction. Empty configuration, unreadable lists, empty or multi-file lists,
missing or malformed payloads, ambiguous corrections, missing columns and map
evaluation errors are fatal. No unconditional-pass fallback is generated.
A pre-existing `jet_veto_pass` column is rejected to avoid silently reusing an
unverified selection. Invalid array lengths, nonfinite jet inputs and nonfinite
map results also abort processing.

The event veto uses nominal corrected AK4 pT (`Jet_pt_corr_nominal` by default),
jet-axis eta/phi, both tight and lepton-veto ID bits (`(id & 6) == 6`), pT >15 GeV,
and charged+neutral EM fraction <0.9. It rejects the entire event for a nonzero
`jetvetomap` result and applies equally to data and corresponding MC.
`jetVetoMapJetPtColumn` and `jetVetoMapJetIdColumn` can name the appropriate nominal
corrected pT and repaired/computed ID columns. These must be available when
`applyJetVetoMap()` is called. The filter uses nominal kinematics directly rather
than registering JES/JER-dependent detector-map selections.

The map payload's outer correction has an era-specific name; `jetvetomap` is the
map-type input evaluated within that correction. The manager does not choose the
first of several corrections or silently ignore additional map files.

These changes require rebuilding executables. Existing histograms retain their
old cleaning and stored-pT map selection; regenerate event outputs to use the new
selection, then rerun merge/plot/fit stages.
