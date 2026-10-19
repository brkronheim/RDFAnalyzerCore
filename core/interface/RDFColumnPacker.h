/** Shared, typed single-Define vector packing for RDF helpers. */
#ifndef RDF_COLUMN_PACKER_H_INCLUDED
#define RDF_COLUMN_PACKER_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <string>
#include <algorithm>
#include <cctype>
#include <vector>
#include <utility>

namespace rdfcolumnpacker {
enum class InputTypeToken {
  Bool,
  Float,
  Double,
  Int,
  UInt,
  Short,
  UShort,
  Char,
  UChar,
  Long64,
  ULong64,
  Unsupported,
};

inline bool isRVecColumnType(const std::string &columnType) {
  return columnType.find("RVec") != std::string::npos;
}

inline std::string extractRVecElementType(const std::string &columnType) {
  const auto rvecPos = columnType.find("RVec<");
  if (rvecPos == std::string::npos) {
    return columnType;
  }

  const auto start = columnType.find('<', rvecPos);
  const auto end = columnType.rfind('>');
  if (start == std::string::npos || end == std::string::npos ||
      end <= start + 1) {
    return columnType;
  }
  return columnType.substr(start + 1, end - start - 1);
}

inline InputTypeToken tokenizeInputType(const std::string &typeName) {
  std::string normalized = typeName;
  normalized.erase(
      std::remove_if(normalized.begin(), normalized.end(),
                     [](unsigned char c) { return std::isspace(c) != 0; }),
      normalized.end());

  if (normalized == "Bool_t" || normalized == "bool") {
    return InputTypeToken::Bool;
  }
  if (normalized == "Float_t" || normalized == "float") {
    return InputTypeToken::Float;
  }
  if (normalized == "Double_t" || normalized == "double") {
    return InputTypeToken::Double;
  }
  if (normalized == "Int_t" || normalized == "int") {
    return InputTypeToken::Int;
  }
  if (normalized == "UInt_t" || normalized == "unsignedint") {
    return InputTypeToken::UInt;
  }
  if (normalized == "Short_t" || normalized == "short") {
    return InputTypeToken::Short;
  }
  if (normalized == "UShort_t" || normalized == "unsignedshort") {
    return InputTypeToken::UShort;
  }
  if (normalized == "Char_t" || normalized == "char") {
    return InputTypeToken::Char;
  }
  if (normalized == "UChar_t" || normalized == "unsignedchar") {
    return InputTypeToken::UChar;
  }
  if (normalized == "Long64_t" || normalized == "longlong") {
    return InputTypeToken::Long64;
  }
  if (normalized == "ULong64_t" || normalized == "unsignedlonglong") {
    return InputTypeToken::ULong64;
  }

  return InputTypeToken::Unsupported;
}


enum class PackingLayout { Scalars, Concatenate, Rows };

// Factories are cached by input types and layout, independent of column names.
ROOT::RDF::RNode definePackedVector(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns, const std::string &targetType,
    PackingLayout layout);

// Scalar product with sequential double arithmetic and a final normalization.
ROOT::RDF::RNode defineScalarProduct(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns, double normalization);

// Scalar conjunction in one Define, used for complete region filter chains.
ROOT::RDF::RNode defineScalarAll(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns);

// Row-major numeric features: scalars broadcast to the largest collection;
// shorter nonempty collections repeat their final value. An empty collection
// produces no rows. This preserves the original compiled flattening semantics.
ROOT::RDF::RNode defineFlattenedNumericInputs(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns, const std::string &context);

// Retain caller-supplied fallback behavior for nonstandard input types.
template <typename FallbackDefineFn>
ROOT::RDF::RNode defineFlattenedNumericInputs(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns, const std::string &context,
    FallbackDefineFn &&fallback) {
  for (const auto &column : columns) {
    const auto type = df.GetColumnType(column);
    if (tokenizeInputType(isRVecColumnType(type) ? extractRVecElementType(type) : type)
        == InputTypeToken::Unsupported)
      return fallback(std::move(df), name, columns);
  }
  return defineFlattenedNumericInputs(std::move(df), name, columns, context);
}
} // namespace rdfcolumnpacker
#endif
