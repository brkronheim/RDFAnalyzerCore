#pragma once
#include <ROOT/RVec.hxx>
class Analyzer;
namespace cms {
struct CmsCorrectionConfig;
namespace detail {
// Positive transverse momentum added by a calibrated, muon-subtracted
// PUPPI jet collection. The caller subtracts this vector from raw MET.
ROOT::VecOps::RVec<float> puppiType1JetSum(
    const ROOT::VecOps::RVec<float> &rawPt,
    const ROOT::VecOps::RVec<float> &correctedPt,
    const ROOT::VecOps::RVec<float> &eta,
    const ROOT::VecOps::RVec<float> &phi,
    const ROOT::VecOps::RVec<float> &muonSubtrFactor,
    const ROOT::VecOps::RVec<float> &muonSubtrDeltaEta,
    const ROOT::VecOps::RVec<float> &muonSubtrDeltaPhi,
    const ROOT::VecOps::RVec<float> &emFraction,
    float ptThreshold = 15.f, float maxEmFraction = .9f,
    float maxAbsEta = 9.9f);
ROOT::VecOps::RVec<float> leptonMetShift(
    const ROOT::VecOps::RVec<float> &originalPt,
    const ROOT::VecOps::RVec<float> &correctedPt,
    const ROOT::VecOps::RVec<float> &phi,
    const ROOT::VecOps::RVec<bool> &mask);
}
// Called after the jet and lepton correction columns have been materialized.
void defineCmsRebuiltPuppiMet(Analyzer &, const CmsCorrectionConfig &);
}
