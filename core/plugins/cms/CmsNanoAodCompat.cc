//==============================================================================
// CmsNanoAodCompat — NanoAOD compatibility shims and column definitions
//
// Implements the compatibility columns defined in the header.
// Extracted from VHqqCorrections.cc (VHbb STXS analysis) and generalized.
//==============================================================================

#include "CmsNanoAodCompat.h"

#include "analyzer.h"

#include <ROOT/RVec.hxx>
#include <RtypesCore.h>

#include <algorithm>
#include <string>
#include <vector>

namespace cms {

namespace {

// Check if a column exists in the dataframe.
bool hasColumn(Analyzer &an, const std::string &name) {
  const auto columnNames = an.GetColumnNames();
  return std::find(columnNames.begin(), columnNames.end(), name) !=
         columnNames.end();
}

// Get the type of an existing column.
std::string columnType(Analyzer &an, const std::string &name) {
  try {
    return an.GetColumnType(name);
  } catch (...) {
    return "";
  }
}

} // namespace

void defineNanoAodCompatibilityColumns(Analyzer &an) {
  // v12 stores PF MET as MET; v15 names it PFMET. Keep the stored PF
  // observable distinct from PuppiMET and from every recalibrated MET column.
  for (const std::string component : {"pt", "phi"}) {
    const auto output = "PFMET_" + component;
    const auto input = "MET_" + component;
    if (!hasColumn(an, output) && hasColumn(an, input)) {
      an.DefineNominal(output, [](Float_t value) { return value; }, {input});
    }
  }
  // -----------------------------------------------------------------------
  // PV_npvsGood_u32 — PV_npvsGood is UChar_t in some NanoAOD versions (v9);
  // convert to UInt_t for uniform downstream handling.
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "PV_npvsGood_u32") && hasColumn(an, "PV_npvsGood")) {
    an.Define("PV_npvsGood_u32",
              [](UChar_t npv) { return static_cast<UInt_t>(npv); },
              {"PV_npvsGood"});
  }

  // -----------------------------------------------------------------------
  // Jet_genJetIdx_int — Jet_genJetIdx is Short_t in some NanoAOD versions (v9);
  // convert to Int_t for uniform handling.
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "Jet_genJetIdx_int") && hasColumn(an, "Jet_genJetIdx")) {
    an.Define("Jet_genJetIdx_int",
              [](const ROOT::VecOps::RVec<Short_t> &indices) {
                std::vector<int> converted(indices.size(), -1);
                for (size_t i = 0; i < indices.size(); ++i) {
                  converted[i] = static_cast<int>(indices[i]);
                }
                return converted;
              },
              {"Jet_genJetIdx"});
  }

  // -----------------------------------------------------------------------
  // Jet_hadronFlavour_int — Jet_hadronFlavour is UChar_t in NanoAODv15;
  // convert it to Int_t for correctionlib and b-tag helper inputs.
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "Jet_hadronFlavour_int") &&
      hasColumn(an, "Jet_hadronFlavour")) {
    if (columnType(an, "Jet_hadronFlavour").find("UChar") != std::string::npos) {
      an.Define("Jet_hadronFlavour_int",
                [](const ROOT::VecOps::RVec<UChar_t> &source) {
                  ROOT::VecOps::RVec<int> converted(source.size(), 0);
                  for (size_t i = 0; i < source.size(); ++i) {
                    converted[i] = static_cast<int>(source[i]);
                  }
                  return converted;
                },
                {"Jet_hadronFlavour"});
    } else {
      an.Define("Jet_hadronFlavour_int",
                [](const ROOT::VecOps::RVec<int> &source) { return source; },
                {"Jet_hadronFlavour"});
    }
  }

  // -----------------------------------------------------------------------
  // NanoAODv15 names this index FatJet_genJetAK8Idx. Normalize the newer
  // name (and the older FatJet_genJetIdx spelling) for AK8 JER matching.
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "FatJet_genJetAK8Idx_int")) {
    const std::string source = hasColumn(an, "FatJet_genJetAK8Idx")
                                   ? "FatJet_genJetAK8Idx"
                                   : (hasColumn(an, "FatJet_genJetIdx")
                                          ? "FatJet_genJetIdx"
                                          : "");
    if (!source.empty()) {
      const auto sourceType = columnType(an, source);
      if (sourceType.find("Short") != std::string::npos ||
          sourceType.find("short") != std::string::npos) {
        an.Define("FatJet_genJetAK8Idx_int",
                  [](const ROOT::VecOps::RVec<Short_t> &indices) {
                    ROOT::VecOps::RVec<int> converted(indices.size(), -1);
                    for (size_t i = 0; i < indices.size(); ++i) {
                      converted[i] = static_cast<int>(indices[i]);
                    }
                    return converted;
                  },
                  {source});
      } else {
        an.Define("FatJet_genJetAK8Idx_int",
                  [](const ROOT::VecOps::RVec<Int_t> &indices) {
                    return ROOT::VecOps::RVec<int>(indices.begin(), indices.end());
                  },
                  {source});
      }
    }
  }
  if (!hasColumn(an, "FatJet_genJetIdx_int") &&
      hasColumn(an, "FatJet_genJetAK8Idx_int")) {
    an.Define("FatJet_genJetIdx_int",
              [](const ROOT::VecOps::RVec<int> &indices) { return indices; },
              {"FatJet_genJetAK8Idx_int"});
  }

  // -----------------------------------------------------------------------
  // Jet_jetId_i — Jet_jetId may be missing (old NanoAOD) or stored as
  // UChar_t (Run 3 NanoAODv12).  Define a re-typed copy Jet_jetId_i
  // (RVec<int>) so downstream code expects a uniform type.
  // -----------------------------------------------------------------------
  if (hasColumn(an, "Jet_jetId_i")) {
    // Already defined, skip.
  } else if (!hasColumn(an, "Jet_jetId")) {
    // Missing entirely: define as a constant (bitmask 6 = tight + tightLepVeto).
    an.Define("Jet_jetId_i",
              [](const ROOT::VecOps::RVec<float> &pt) {
                (void)pt;
                return ROOT::VecOps::RVec<int>(pt.size(), 6);
              },
              {"Jet_pt"});
  } else if (columnType(an, "Jet_jetId").find("UChar") != std::string::npos) {
    an.Define("Jet_jetId_i",
              [](const ROOT::VecOps::RVec<UChar_t> &src) {
                ROOT::VecOps::RVec<int> out(src.size());
                for (size_t i = 0; i < src.size(); ++i) out[i] = static_cast<int>(src[i]);
                return out;
              },
              {"Jet_jetId"});
  } else {
    an.Define("Jet_jetId_i",
              [](const ROOT::VecOps::RVec<int> &x) { return x; },
              {"Jet_jetId"});
  }

  // -----------------------------------------------------------------------
  // Jet_puId_i — Jet_puId may be UChar_t in Run 3 NanoAODv12, or missing
  // entirely.  Define a re-typed copy Jet_puId_i (RVec<int>).
  // -----------------------------------------------------------------------
  if (hasColumn(an, "Jet_puId_i")) {
    // Already defined.
  } else if (hasColumn(an, "Jet_puId") &&
             columnType(an, "Jet_puId").find("UChar") != std::string::npos) {
    an.Define("Jet_puId_i",
              [](const ROOT::VecOps::RVec<UChar_t> &src) {
                ROOT::VecOps::RVec<int> out(src.size());
                for (size_t i = 0; i < src.size(); ++i) out[i] = static_cast<int>(src[i]);
                return out;
              },
              {"Jet_puId"});
  } else if (hasColumn(an, "Jet_puId")) {
    an.Define("Jet_puId_i",
              [](const ROOT::VecOps::RVec<int> &x) { return x; },
              {"Jet_puId"});
  } else if (hasColumn(an, "Jet_puIdDisc")) {
    // Fallback: derive PU ID from discriminator (positive -> loose bit set).
    an.Define("Jet_puId_i",
              [](const ROOT::VecOps::RVec<float> &disc) {
                ROOT::VecOps::RVec<int> puId(disc.size(), 0);
                for (size_t i = 0; i < disc.size(); ++i) {
                  puId[i] = disc[i] > 0.0f ? 7 : 0;
                }
                return puId;
              },
              {"Jet_puIdDisc"});
  } else {
    // Missing entirely: assume all jets pass loose PU ID (bitmask 7).
    an.Define("Jet_puId_i",
              [](const ROOT::VecOps::RVec<float> &pt) {
                (void)pt;
                return ROOT::VecOps::RVec<int>(pt.size(), 7);
              },
              {"Jet_pt"});
  }

  // -----------------------------------------------------------------------
  // Jet_pt_raw — raw pT before JEC (Jet_pt × (1 - Jet_rawFactor))
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "Jet_pt_raw") && hasColumn(an, "Jet_rawFactor")) {
    an.Define("Jet_pt_raw",
              [](const ROOT::VecOps::RVec<float> &pt,
                 const ROOT::VecOps::RVec<float> &rawFactor) {
                ROOT::VecOps::RVec<float> rawPt(pt.size(), 0.0f);
                for (size_t i = 0; i < pt.size(); ++i) {
                  const float factor =
                      i < rawFactor.size() ? (1.0f - rawFactor[i]) : 1.0f;
                  rawPt[i] = pt[i] * factor;
                }
                return rawPt;
              },
              {"Jet_pt", "Jet_rawFactor"});
  }

  // -----------------------------------------------------------------------
  // Jet_mass_raw — raw mass before JEC (Jet_mass × (1 - Jet_rawFactor))
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "Jet_mass_raw") && hasColumn(an, "Jet_rawFactor")) {
    an.Define("Jet_mass_raw",
              [](const ROOT::VecOps::RVec<float> &mass,
                 const ROOT::VecOps::RVec<float> &rawFactor) {
                ROOT::VecOps::RVec<float> rawMass(mass.size(), 0.0f);
                for (size_t i = 0; i < mass.size(); ++i) {
                  const float factor =
                      i < rawFactor.size() ? (1.0f - rawFactor[i]) : 1.0f;
                  rawMass[i] = mass[i] * factor;
                }
                return rawMass;
              },
              {"Jet_mass", "Jet_rawFactor"});
  }

  // -----------------------------------------------------------------------
  // FatJet_pt_raw — raw pT before JEC (FatJet_pt × (1 - FatJet_rawFactor))
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "FatJet_pt_raw") && hasColumn(an, "FatJet_rawFactor")) {
    an.Define("FatJet_pt_raw",
              [](const ROOT::VecOps::RVec<float> &pt,
                 const ROOT::VecOps::RVec<float> &rawFactor) {
                ROOT::VecOps::RVec<float> rawPt(pt.size(), 0.0f);
                for (size_t i = 0; i < pt.size(); ++i) {
                  const float factor =
                      i < rawFactor.size() ? (1.0f - rawFactor[i]) : 1.0f;
                  rawPt[i] = pt[i] * factor;
                }
                return rawPt;
              },
              {"FatJet_pt", "FatJet_rawFactor"});
  }

  // -----------------------------------------------------------------------
  // FatJet_mass_raw — raw mass before JEC
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "FatJet_mass_raw") && hasColumn(an, "FatJet_rawFactor")) {
    an.Define("FatJet_mass_raw",
              [](const ROOT::VecOps::RVec<float> &mass,
                 const ROOT::VecOps::RVec<float> &rawFactor) {
                ROOT::VecOps::RVec<float> rawMass(mass.size(), 0.0f);
                for (size_t i = 0; i < mass.size(); ++i) {
                  const float factor =
                      i < rawFactor.size() ? (1.0f - rawFactor[i]) : 1.0f;
                  rawMass[i] = mass[i] * factor;
                }
                return rawMass;
              },
              {"FatJet_mass", "FatJet_rawFactor"});
  }

  // -----------------------------------------------------------------------
  // Muon_genPt — gen-matched pT from Muon_genPartIdx + GenPart_pt
  // NanoAOD stores Muon_genPartIdx (index into GenPart), not the pT directly.
  // -----------------------------------------------------------------------
  if (!hasColumn(an, "Muon_genPt") && hasColumn(an, "Muon_genPartIdx") &&
      hasColumn(an, "GenPart_pt")) {
    const auto genPartIdxType = columnType(an, "Muon_genPartIdx");
    if (genPartIdxType.find("Short") != std::string::npos ||
        genPartIdxType.find("short") != std::string::npos) {
      an.Define("Muon_genPartIdx_int",
                [](const ROOT::VecOps::RVec<Short_t> &source) {
                  ROOT::VecOps::RVec<int> converted(source.size(), -1);
                  for (size_t i = 0; i < source.size(); ++i) {
                    converted[i] = static_cast<int>(source[i]);
                  }
                  return converted;
                },
                {"Muon_genPartIdx"});
    } else {
      an.Define("Muon_genPartIdx_int",
                [](const ROOT::VecOps::RVec<int> &source) { return source; },
                {"Muon_genPartIdx"});
    }
    an.Define("Muon_genPt",
              [](const ROOT::VecOps::RVec<int> &genPartIdx,
                 const ROOT::VecOps::RVec<float> &genPartPt) {
                ROOT::VecOps::RVec<float> genPt(genPartIdx.size(), 0.0f);
                for (size_t i = 0; i < genPartIdx.size(); ++i) {
                  const int idx = genPartIdx[i];
                  if (idx >= 0 && static_cast<size_t>(idx) < genPartPt.size()) {
                    genPt[i] = genPartPt[idx];
                  }
                }
                return genPt;
              },
              {"Muon_genPartIdx_int", "GenPart_pt"});
  }
}

} // namespace cms
