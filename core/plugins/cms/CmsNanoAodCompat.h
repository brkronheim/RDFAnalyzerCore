#ifndef CMS_NANO_AOD_COMPAT_H_INCLUDED
#define CMS_NANO_AOD_COMPAT_H_INCLUDED

//==============================================================================
// CmsNanoAodCompat — NanoAOD compatibility shims and column definitions
//
// Purpose:
//   Defines compatibility columns and type-consistent aliases for NanoAOD
//   inputs that differ between NanoAOD production versions (v9, v12, v15)
//   and data/MC formats.  All definitions here are analysis-agnostic and
//   safe for any CMS analysis to call at the top of their pipeline.
//
//   The following columns are defined (if not already present):
//     PFMET_pt/phi        — stored MET_pt/phi for schemas using the older PF name
//     PV_npvsGood_u32      — UInt_t copy of PV_npvsGood (UChar_t in v9)
//     Jet_genJetIdx_int    — Int_t copy of Jet_genJetIdx (Short_t in v9)
//     Jet_hadronFlavour_int — Int_t copy of Jet_hadronFlavour (UChar_t in v15)
//     Jet_jetId_i          — Int_t copy of Jet_jetId (UChar_t in v12 or missing)
//     Jet_puId_i           — Int_t copy of Jet_puId (UChar_t in v12 or missing)
//     Jet_pt_raw           — Raw pT before JEC (from Jet_pt × (1-rawFactor))
//     Jet_mass_raw         — Raw mass before JEC (from Jet_mass × (1-rawFactor))
//     Muon_genPartIdx_int  — Int_t copy of Muon_genPartIdx (Short_t in v15)
//
//   Each column is defined once; if the column already exists on the
//   dataframe, the definition is skipped.
//
// Usage:
//   defineCmsNanoAodCompatibilityColumns(analyzer);
//   // All compatibility columns are now available.
//==============================================================================

class Analyzer;

namespace cms {

/// Define all NanoAOD compatibility columns on the analyzer's dataframe.
/// Safe to call multiple times (idempotent, checks for existing columns).
void defineNanoAodCompatibilityColumns(Analyzer &an);

} // namespace cms

#endif // CMS_NANO_AOD_COMPAT_H_INCLUDED
