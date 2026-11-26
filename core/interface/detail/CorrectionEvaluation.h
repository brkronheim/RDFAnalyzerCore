#pragma once

#include <correction.h>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace rdf::detail {
using CorrectionValue = correction::Variable::Type;
struct CorrectionScratch {
  std::vector<CorrectionValue> values;
  correction::CompoundCorrection::Workspace compound;
};

inline double evaluateWithScratch(const correction::Correction::Ref &correction, CorrectionScratch &scratch) {
  return correction->evaluate(scratch.values);
}

inline double evaluateWithScratch(const correction::CompoundCorrection::Ref &correction, CorrectionScratch &scratch) {
  return correction->evaluate_with_workspace(scratch.values, scratch.compound);
}

/// Evaluate a correction with safe fallback for out-of-range inputs.
///
/// Attempts `correction->evaluate(values)` and returns the result. Out-of-range
/// bin lookups return the fallback value. Depending on the Correctionlib
/// version, these are reported as either std::out_of_range or a runtime_error
/// whose message begins with "Index below/above bounds in". No range exception
/// propagates to the caller. Every std::out_of_range fallback is reported to
/// stderr, including runtime_error range failures and missing category keys.
///
/// For any other exception type the evaluation is still re-thrown as a
/// `std::runtime_error` with a descriptive message, since those indicate
/// genuine problems (corrupt files, schema mismatches, etc.).
template <typename Output = float, typename CorrectionRef>
Output safeEvaluate(const CorrectionRef &correction,
                     CorrectionScratch &scratch,
                     Output fallback = Output(1)) {
  const auto &values = scratch.values;
  const auto reportFallback = [&](const char *type, const std::exception &error) {
    std::ostringstream warning;
    warning << std::setprecision(17)
            << "Warning: CorrectionManager: " << type << " evaluating correction '"
            << correction->name() << "': " << error.what()
            << "; using fallback " << fallback << " for inputs [";
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (i) warning << ", ";
      std::visit([&warning](const auto &value) { warning << value; }, values[i]);
    }
    warning << "] (";
    for (std::size_t i = 0; i < correction->inputs().size(); ++i) {
      if (i) warning << ", ";
      warning << correction->inputs()[i].name();
    }
    warning << ")\n";
    // Emit complete records even when RDF evaluates corrections on many slots.
    static std::mutex warningMutex;
    const std::lock_guard<std::mutex> lock(warningMutex);
    std::cerr << warning.str();
  };
  try {
    const auto result = evaluateWithScratch(correction, scratch);
    if (!std::isfinite(result) || std::abs(result) > std::numeric_limits<Output>::max())
      throw std::runtime_error("non-finite or unrepresentable correction result");
    return result;
  } catch (const std::out_of_range &e) {
    // Preserve the neutral fallback, but report every failure: correctionlib
    // also uses this exception for missing string/category keys, which can
    // indicate a configuration error rather than an expected numeric range.
    reportFallback("std::out_of_range", e);
    return fallback;
  } catch (const std::runtime_error &e) {
    // The bundled Correctionlib throws runtime_error (rather than
    // out_of_range) for Binning/MultiBinning nodes configured with
    // flow: "error". Keep only those known range failures neutral; other
    // runtime errors still indicate a broken payload or schema mismatch.
    const std::string_view message(e.what());
    if (message.rfind("Index below bounds in ", 0) == 0 ||
        message.rfind("Index above bounds in ", 0) == 0) {
      reportFallback("range error", e);
      return fallback;
    }
    throw std::runtime_error(
        std::string("Error evaluating correction '") + correction->name() +
        "': " + e.what() +
        ". This indicates a genuine problem (corrupt file, schema "
        "mismatch, or missing correction) and should be investigated.");
  } catch (const std::exception &e) {
    throw std::runtime_error(
        std::string("Error evaluating correction '") + correction->name() +
        "': " + e.what() +
        ". This indicates a genuine problem (corrupt file, schema "
        "mismatch, or missing correction) and should be investigated.");
  }
}

} // namespace rdf::detail
