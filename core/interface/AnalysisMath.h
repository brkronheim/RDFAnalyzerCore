#pragma once

#include <ROOT/RVec.hxx>
#include <Math/Vector4D.h>
#include "ArrayValidation.h"
#include <algorithm>
#include <array>
#include <cmath>
#include <stdexcept>
#include <utility>

namespace rdf {

/// Signed first minus second azimuth in radians, in [-pi, pi).
/// Nonfinite inputs produce NaN. Both pi ties map to -pi.
inline double signedDeltaPhi(double first, double second) {
  constexpr double pi = 3.14159265358979323846;
  const double delta = std::remainder(first - second, 2. * pi);
  return delta == pi ? -pi : delta;
}

inline double absDeltaPhi(double first, double second) {
  return std::abs(signedDeltaPhi(first, second));
}

inline double deltaR2(double eta1, double phi1, double eta2, double phi2) {
  const double deta = eta1 - eta2;
  const double dphi = signedDeltaPhi(phi1, phi2);
  return deta * deta + dphi * dphi;
}

/// Retrieve one four-vector from aligned collection columns. A missing index or
/// an inconsistent input schema raises a diagnostic before accessing the arrays.
template <class Pt, class Eta, class Phi, class Mass>
ROOT::Math::PtEtaPhiMVector indexedPtEtaPhiM(
    const Pt& pt, const Eta& eta, const Phi& phi, const Mass& mass, int index) {
  requireAlignedSizes("indexedPtEtaPhiM", pt.size(),
                      {{"eta", eta.size()}, {"phi", phi.size()}, {"mass", mass.size()}});
  const auto i = checkedIndex("indexedPtEtaPhiM", index, pt.size());
  return {pt[i], eta[i], phi[i], mass[i]};
}

/// Sum (px, py) in source order with double precision. Empty selections give
/// (0, 0). Only selected objects must have finite phi and finite nonnegative pt.
template <class Mask, class Pt, class Phi>
std::pair<double, double> selectedTransverseSum(const Mask& mask, const Pt& pt,
                                                const Phi& phi) {
  requireAlignedSizes("selectedTransverseSum", mask.size(),
                      {{"pt", pt.size()}, {"phi", phi.size()}});
  double px = 0., py = 0.;
  for (std::size_t i = 0; i < mask.size(); ++i) {
    if (!mask[i]) continue;
    const double momentum = pt[i], angle = phi[i];
    if (!std::isfinite(momentum) || momentum < 0. || !std::isfinite(angle))
      throw std::runtime_error("selectedTransverseSum: invalid pt or phi at index " +
                               std::to_string(i));
    px += momentum * std::cos(angle);
    py += momentum * std::sin(angle);
  }
  return {px, py};
}

inline float transverseVectorPt(float firstPt, float firstPhi, float secondPt, float secondPhi) {
  return std::hypot(firstPt*std::cos(firstPhi)+secondPt*std::cos(secondPhi),
                    firstPt*std::sin(firstPhi)+secondPt*std::sin(secondPhi));
}

inline float transverseVectorPhi(float firstPt, float firstPhi, float secondPt, float secondPhi) {
  return std::atan2(firstPt*std::sin(firstPhi)+secondPt*std::sin(secondPhi),
                    firstPt*std::cos(firstPhi)+secondPt*std::cos(secondPhi));
}

inline float transverseMass(float leptonPt, float leptonPhi, float metPt, float metPhi,
                            bool validateInputs=true, float invalid=-1.f) {
  if (validateInputs && (!std::isfinite(leptonPt) || !std::isfinite(leptonPhi) ||
      !std::isfinite(metPt) || !std::isfinite(metPhi) || leptonPt<0.f || metPt<0.f))
    return invalid;
  constexpr float pi=3.14159265358979323846f;
  const float angle=std::abs(std::remainder(leptonPhi-metPhi,2.f*pi));
  return std::sqrt(std::max(0.f,2.f*leptonPt*metPt*(1.f-std::cos(angle))));
}

// Covariance provenance belongs to the caller. This is only v^T C^-1 v.
inline float covarianceQuadratic(float pt, float phi, float xx, float xy, float yy,
                                 bool requirePositiveDiagonal = true,
                                 float invalid = -999.f) {
  const std::array<float, 5> values{pt, phi, xx, xy, yy};
  if (pt < 0.f || std::any_of(values.begin(), values.end(),
      [](float value) { return !std::isfinite(value); })) return invalid;
  const float determinant = xx * yy - xy * xy;
  if (!(determinant > 0.f) || (requirePositiveDiagonal && (!(xx > 0.f) || !(yy > 0.f))))
    return invalid;
  const float x = pt * std::cos(phi), y = pt * std::sin(phi);
  const float result = (yy*x*x - 2.f*xy*x*y + xx*y*y) / determinant;
  return std::isfinite(result) && result >= 0.f ? result : invalid;
}

template <class Indices, class Values>
float selectedProduct(const Indices &indices, const Values &values,
                      bool requirePositive = true) {
  float product = 1.f;
  for (const auto index : indices) {
    if (index < 0 || static_cast<std::size_t>(index) >= values.size())
      throw std::runtime_error("selected object has no aligned weight");
    const float value = values[index];
    if (!std::isfinite(value) || (requirePositive && value <= 0.f))
      throw std::runtime_error("selected object weight is invalid");
    product *= value;
  }
  return product;
}

// Scores are indexed by the held-out fold, not by the training-fold label.
template <class Scores>
auto heldOutScore(unsigned long long event, const Scores &scores) {
  if (scores.empty()) throw std::invalid_argument("no inference folds configured");
  return scores[event % scores.size()];
}

template <class Values>
auto indexedValue(const Values &values, int index, typename Values::value_type invalid) {
  return index >= 0 && static_cast<std::size_t>(index) < values.size() ? values[index] : invalid;
}
} // namespace rdf
