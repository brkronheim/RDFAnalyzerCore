/**
 * @file functions.h
 * @brief General utility functions and templates for mathematical and vector
 * operations.
 *
 * This header provides a collection of template functions and inline utilities
 * for use in analysis, including mathematical operations, vector manipulations,
 * and selection logic.
 */
#ifndef FUNCTIONS_H_INCLUDED
#define FUNCTIONS_H_INCLUDED

#include <algorithm>
#include <numeric>

#include <Math/Math.h>
#include <Math/Vector4D.h>
#include <ROOT/RDataFrame.hxx>

// =========================
// General Math Utilities
// =========================

/**
 * @brief Returns the sign of a value as a float (-1, 0, or 1).
 * @tparam T Numeric type
 * @param val Value to check
 * @return -1 if negative, 1 if positive, 0 if zero
 */
template <typename T> Float_t getSignFloat(T val) {
  return (T(0) < val) - (val < T(0));
}

/**
 * @brief Clips an integer value to the specified lower and upper bounds
 * (inclusive).
 * @tparam T Numeric type (typically integer)
 * @tparam lower Lower bound (inclusive)
 * @tparam upper Upper bound (inclusive)
 * @param val Value to clip
 * @return Value clipped to the range [lower, upper]
 */
template <typename T, int lower, int upper> T clipIntBounds(T val) {
  val = val > T(upper) ? T(upper) : val;
  val = val < T(lower) ? T(lower) : val;
  return (val);
}

/**
 * @brief Multiplies two values.
 * @tparam T Numeric type
 * @param val1 First value
 * @param val2 Second value
 * @return Product of val1 and val2
 */
template <typename T> T multiply(T val1, T val2) { return (val1 * val2); }

/**
 * @brief Divides two values.
 * @tparam T Numeric type
 * @param val1 Numerator
 * @param val2 Denominator
 * @return val1 / val2
 */
template <typename T> T divide(T val1, T val2) { return (val1 / val2); }

/**
 * @brief Adds two values.
 * @tparam T Numeric type
 * @param val1 First value
 * @param val2 Second value
 * @return Sum of val1 and val2
 */
template <typename T> T add(T val1, T val2) { return (val1 + val2); }

/**
 * @brief Returns the absolute difference between two values.
 * @tparam T Numeric type
 * @param val1 First value
 * @param val2 Second value
 * @return |val1 - val2|
 */
template <typename T> T absDiff(T val1, T val2) { return (fabs(val1 - val2)); }

// =========================
// Type/Value Utilities
// =========================

/**
 * @brief Returns a compile-time constant integer value.
 * @tparam index The integer value to return
 * @return The value of index
 */
template <Int_t index> Int_t constantInteger() { return (index); }

/**
 * @brief Casts a value to another type.
 * @tparam T Input type
 * @tparam S Output type
 * @param val1 Value to cast
 * @return Value cast to type S
 */
template <typename T, typename S> S castVar(T val1) { return (S(val1)); }

/**
 * @brief Creates a vector with a single value.
 * @tparam T Type of the value
 * @param val1 Value to place in the vector
 * @return Vector containing val1
 */
template <typename T> ROOT::VecOps::RVec<T> defineVector(T val1) {
  return (ROOT::VecOps::RVec<T>({val1}));
}

// =========================
// Vector Operations
// =========================

/**
 * @brief Returns the value at a fixed (compile-time) index in a vector, or
 * -9999.0 if out of bounds.
 * @tparam T Type of the vector elements
 * @tparam index Index to access (compile-time constant)
 * @param vector Input vector
 * @return Value at the given index or -9999.0 if out of bounds
 */
template <typename T, unsigned int index>
T fixedIndexVector(ROOT::VecOps::RVec<T> &vector) {
  if (index >= vector.size()) {
    return (T(-9999.0));
  }
  return (vector[index]);
}

/**
 * @brief Selects the indices of the top N largest values in a vector.
 * @tparam T Type of the vector elements
 * @tparam size Number of top elements to select
 * @param vector Input vector
 * @return Vector of indices for the top N elements, padded with -1 if needed
 */
template <typename T, long unsigned int size>
ROOT::VecOps::RVec<Int_t> selectTop(ROOT::VecOps::RVec<T> &vector) {
  ROOT::VecOps::RVec<Int_t> indexVector(vector.size());
  std::iota(indexVector.begin(), indexVector.end(), 0); // Initializing

  std::sort(indexVector.begin(), indexVector.end(),
            [&](Int_t i, Int_t j) { return vector[i] > vector[j]; });
  while (indexVector.size() < size) {
    indexVector.push_back(-1);
  }
  return (indexVector);
}

/**
 * @brief Returns the value at a given index in a vector, or -9999.0 if out of
 * bounds.
 * @tparam T Type of the vector elements
 * @tparam S Type of the index
 * @param vector Input vector
 * @param index Index to access
 * @return Value at the given index or -9999.0 if out of bounds
 */
template <typename T, typename S>
T indexVector(ROOT::VecOps::RVec<T> &vector, S index) {
  if (index < 0 || index >= vector.size()) {
    return (-9999.0);
  }
  return (vector[index]);
}

/**
 * @brief Returns a vector containing the element-wise maximum of two input
 * vectors.
 * @tparam T Type of the vector elements
 * @param val1 First input vector
 * @param val2 Second input vector
 * @return Vector of element-wise maxima
 */
template <typename T>
ROOT::VecOps::RVec<T> maximumVector(ROOT::VecOps::RVec<T> &val1,
                                    ROOT::VecOps::RVec<T> &val2) {
  ROOT::VecOps::RVec<T> maxVec(val1.size());
  for (long unsigned int i = 0; i < val1.size(); i++) {
    maxVec[i] = std::max(val1[i], val2[i]);
  }

  return (maxVec);
}

/**
 * @brief Takes elements from a vector at specified indices, pads with -9999.0
 * if out of bounds.
 * @tparam T Type of the vector elements
 * @tparam S Type of the index vector elements
 * @param vector Input vector
 * @param index Vector of indices
 * @return Vector of selected elements
 */
template <typename T, typename S>
ROOT::VecOps::RVec<T> take(ROOT::VecOps::RVec<T> &vector,
                           ROOT::VecOps::RVec<S> &index) {
  return (ROOT::VecOps::Take(vector, index, T(-9999.0)));
}

/**
 * @brief Adds a value to the end of a vector.
 * @tparam T Type of the vector elements
 * @param vec Input vector
 * @param newVal Value to add
 * @return Vector with newVal appended
 */
template <typename T>
ROOT::VecOps::RVec<T> addToVector(ROOT::VecOps::RVec<T> vec, T newVal) {
  vec.push_back(newVal);
  return (vec);
}

// =========================
// Logical Operations
// =========================

/**
 * @brief Returns true if the value is non-negative.
 * @tparam T Numeric type
 * @param val Value to check
 * @return True if val >= 0
 */
template <typename T> bool passPositive(T val) { return (val >= 0); }

/**
 * @brief Returns true if either value is non-negative.
 * @tparam T Numeric type
 * @param val1 First value
 * @param val2 Second value
 * @return True if either value is >= 0
 */
template <typename T> bool passPositiveOR(T val1, T val2) {
  return (val1 >= 0 || val2 >= 0);
}

/**
 * @brief Pass-through cut function for filters.
 * @param val1 Boolean value
 * @return The input value
 */
inline bool passCut(bool val1) { return (val1); }

/**
 * @brief Logical OR of two boolean values.
 * @param val1 First boolean value
 * @param val2 Second boolean value
 * @return True if either value is true
 */
inline bool orBranches(bool val1, bool val2) { return (val1 || val2); }

/**
 * @brief Logical AND of two boolean values.
 * @param val1 First boolean value
 * @param val2 Second boolean value
 * @return True if both values are true
 */
inline bool andBranches(bool val1, bool val2) { return (val1 && val2); }

/**
 * @brief Logical OR of three boolean values.
 * @param val1 First boolean value
 * @param val2 Second boolean value
 * @param val3 Third boolean value
 * @return True if any value is true
 */
inline bool orBranches3(bool val1, bool val2, bool val3) {
  return (val1 || val2 || val3);
}

/**
 * @brief Logical AND of three boolean values.
 * @param val1 First boolean value
 * @param val2 Second boolean value
 * @param val3 Third boolean value
 * @return True if all values are true
 */
inline bool andBranches3(bool val1, bool val2, bool val3) {
  return (val1 && val2 && val3);
}

// =========================
// Modulo Operations
// =========================

/**
 * @brief Returns val modulo modVal.
 * @tparam modVal Modulus value (compile-time constant)
 * @param val Value to take modulo
 * @return val % modVal
 */
template <int modVal> int modInt(int val) { return (val % modVal); }

/**
 * @brief Returns val modulo modVal if val >= 0, else returns val.
 * @tparam modVal Modulus value (compile-time constant)
 * @param val Value to take modulo
 * @return val % modVal if val >= 0, else val
 */
template <int modVal> int modIntPos(int val) {
  if (val < 0) {
    return (val);
  }
  return (val % modVal);
}

// =========================
// Physics/Analysis-Specific Functions
// =========================

/**
 * @brief Calculates a/(a+b) if both are non-negative, else returns -1.
 * @tparam T Numeric type
 * @param a First value
 * @param b Second value
 * @return a/(a+b) or -1 if a or b is negative
 */
template <typename T> T calcAvB(T a, T b) {
  if (a < 0 || b < 0) {
    return (-1);
  }
  return (a + b != 0 ? a / (a + b) : 0);
}

/**
 * @brief Computes the difference in phi, wrapped to [-pi, pi].
 * @tparam T Numeric type
 * @param phi0 First angle
 * @param phi1 Second angle
 * @return Difference in phi, wrapped to [-pi, pi]
 */
template <typename T> T EvalDeltaPhi(T phi0, T phi1) {

  double dPhi = fabs(phi0 - phi1);

  if (dPhi > ROOT::Math::Pi())
    dPhi = 2.0 * ROOT::Math::Pi() - dPhi;

  return dPhi;
}

/**
 * @brief Computes the delta-R separation between two objects in eta-phi space.
 * @tparam T Numeric type
 * @param eta0 First eta
 * @param phi0 First phi
 * @param eta1 Second eta
 * @param phi1 Second phi
 * @return Delta-R value
 */
template <typename T> T EvalDeltaR(T eta0, T phi0, T eta1, T phi1) {

  T dEta = fabs(eta0 - eta1);
  T dPhi = EvalDeltaPhi<T>(phi0, phi1);

  return sqrt(dEta * dEta + dPhi * dPhi);
}

/**
 * @brief Computes the Madgraph-style delta-R separation between two objects.
 * @tparam T Numeric type
 * @param eta0 First eta
 * @param phi0 First phi
 * @param eta1 Second eta
 * @param phi1 Second phi
 * @return Madgraph-style delta-R value
 */
template <typename T> T EvalDeltaR_MG(T eta0, T phi0, T eta1, T phi1) {

  T dEta = fabs(eta0 - eta1);
  T dPhi = EvalDeltaPhi<T>(phi0, phi1);

  return (2 * (cosh(dEta) - cos(dPhi)));
}

/**
 * @brief Computes the sum of two four-vectors given as pt, eta, phi, mass.
 * @tparam T Numeric type
 * @param Jet1_pt pt of first jet
 * @param Jet1_eta eta of first jet
 * @param Jet1_phi phi of first jet
 * @param Jet1_m mass of first jet
 * @param Jet2_pt pt of second jet
 * @param Jet2_eta eta of second jet
 * @param Jet2_phi phi of second jet
 * @param Jet2_m mass of second jet
 * @return Vector {pt, eta, phi, mass} of the sum
 */
template <typename T>
ROOT::VecOps::RVec<T> EvalVectorSum(T Jet1_pt, T Jet1_eta, T Jet1_phi, T Jet1_m,
                                    T Jet2_pt, T Jet2_eta, T Jet2_phi,
                                    T Jet2_m) {
  if (Jet2_pt < 0) {
    return (ROOT::VecOps::RVec<T>({Jet1_pt, Jet1_eta, Jet1_phi, Jet1_m}));
  } else if (Jet1_pt < 0) {
    return (ROOT::VecOps::RVec<T>({Jet2_pt, Jet2_eta, Jet2_phi, Jet2_m}));
  }
  auto vLorentz =
      ROOT::Math::PtEtaPhiMVector{Jet1_pt, Jet1_eta, Jet1_phi, Jet1_m} +
      ROOT::Math::PtEtaPhiMVector{Jet2_pt, Jet2_eta, Jet2_phi, Jet2_m};

  Float_t pt = vLorentz.Pt();
  Float_t eta = vLorentz.Eta();
  Float_t phi = vLorentz.Phi();
  Float_t mass = vLorentz.mass();
  return (ROOT::VecOps::RVec<T>({pt, eta, phi, mass}));
}

/**
 * @brief Computes the sum of two four-vectors given as 4-element vectors.
 * @tparam T Numeric type
 * @param Jet1 First jet as {pt, eta, phi, mass}
 * @param Jet2 Second jet as {pt, eta, phi, mass}
 * @return Vector {pt, eta, phi, mass} of the sum
 */
template <typename T>
ROOT::VecOps::RVec<T> EvalVectorSum_2(ROOT::VecOps::RVec<T> Jet1,
                                      ROOT::VecOps::RVec<T> Jet2) {
  if (Jet1.size() != 4 || Jet2.size() != 4) {
    throw std::runtime_error("Error: Jet sizes not 4 in EvalVectorSum");
    ROOT::VecOps::RVec<T>({0, 0, 0, 0});
  }

  auto vLorentz =
      ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<T>>{Jet1[0], Jet1[1],
                                                            Jet1[2], Jet1[3]} +
      ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<T>>{Jet2[0], Jet2[1],
                                                            Jet2[2], Jet2[3]};

  Float_t pt = vLorentz.Pt();
  Float_t eta = vLorentz.Eta();
  Float_t phi = vLorentz.Phi();
  Float_t mass = vLorentz.mass();
  return (ROOT::VecOps::RVec<T>({pt, eta, phi, mass}));
}

/**
 * @brief Creates a four-vector from pt, eta, phi, mass.
 * @tparam T Numeric type
 * @param pt Transverse momentum
 * @param eta Pseudorapidity
 * @param phi Azimuthal angle
 * @param mass Mass
 * @return Vector {pt, eta, phi, mass}
 */
template <typename T>
ROOT::VecOps::RVec<T> Fill4Vec(T pt, T eta, T phi, T mass) {

  return (ROOT::VecOps::RVec<T>({pt, eta, phi, mass}));
}

#endif
