/**
 * @file RDFColumnPacker.h
 * @brief Shared helpers for packing mixed scalar/RVec inputs into compiled
 *        RDataFrame-friendly flattened feature vectors.
 */
#ifndef RDF_COLUMN_PACKER_H_INCLUDED
#define RDF_COLUMN_PACKER_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>
#include <vector>

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

template <typename InputT>
ROOT::RDF::RNode defineObjectCountInit(ROOT::RDF::RNode df,
                                       const std::string &name,
                                       const std::string &column) {
  return df.Define(
      name,
      [](const ROOT::VecOps::RVec<InputT> &values) -> ULong64_t {
        return static_cast<ULong64_t>(values.size());
      },
      {column});
}

template <typename InputT>
ROOT::RDF::RNode defineObjectCountAppend(ROOT::RDF::RNode df,
                                         const std::string &name,
                                         const std::string &currentColumn,
                                         const std::string &nextColumn) {
  return df.Define(
      name,
      [](ULong64_t currentCount,
         const ROOT::VecOps::RVec<InputT> &values) -> ULong64_t {
        return std::max(currentCount,
                        static_cast<ULong64_t>(values.size()));
      },
      {currentColumn, nextColumn});
}

inline ROOT::RDF::RNode dispatchObjectCountInit(ROOT::RDF::RNode df,
                                                const std::string &name,
                                                const std::string &column,
                                                InputTypeToken inputType,
                                                const std::string &context) {
  switch (inputType) {
  case InputTypeToken::Bool:
    return defineObjectCountInit<Bool_t>(df, name, column);
  case InputTypeToken::Float:
    return defineObjectCountInit<Float_t>(df, name, column);
  case InputTypeToken::Double:
    return defineObjectCountInit<Double_t>(df, name, column);
  case InputTypeToken::Int:
    return defineObjectCountInit<Int_t>(df, name, column);
  case InputTypeToken::UInt:
    return defineObjectCountInit<UInt_t>(df, name, column);
  case InputTypeToken::Short:
    return defineObjectCountInit<Short_t>(df, name, column);
  case InputTypeToken::UShort:
    return defineObjectCountInit<UShort_t>(df, name, column);
  case InputTypeToken::Char:
    return defineObjectCountInit<Char_t>(df, name, column);
  case InputTypeToken::UChar:
    return defineObjectCountInit<UChar_t>(df, name, column);
  case InputTypeToken::Long64:
    return defineObjectCountInit<Long64_t>(df, name, column);
  case InputTypeToken::ULong64:
    return defineObjectCountInit<ULong64_t>(df, name, column);
  case InputTypeToken::Unsupported:
    throw std::runtime_error(
        context + ": unsupported vector input type in compiled flatten path");
  }

  throw std::runtime_error(context +
                           ": unreachable object-count init dispatch");
}

inline ROOT::RDF::RNode dispatchObjectCountAppend(ROOT::RDF::RNode df,
                                                  const std::string &name,
                                                  const std::string &currentColumn,
                                                  const std::string &nextColumn,
                                                  InputTypeToken inputType,
                                                  const std::string &context) {
  switch (inputType) {
  case InputTypeToken::Bool:
    return defineObjectCountAppend<Bool_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::Float:
    return defineObjectCountAppend<Float_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::Double:
    return defineObjectCountAppend<Double_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::Int:
    return defineObjectCountAppend<Int_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::UInt:
    return defineObjectCountAppend<UInt_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::Short:
    return defineObjectCountAppend<Short_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::UShort:
    return defineObjectCountAppend<UShort_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::Char:
    return defineObjectCountAppend<Char_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::UChar:
    return defineObjectCountAppend<UChar_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::Long64:
    return defineObjectCountAppend<Long64_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::ULong64:
    return defineObjectCountAppend<ULong64_t>(df, name, currentColumn, nextColumn);
  case InputTypeToken::Unsupported:
    throw std::runtime_error(
        context + ": unsupported vector input type in compiled flatten path");
  }

  throw std::runtime_error(context +
                           ": unreachable object-count append dispatch");
}

inline ROOT::RDF::RNode defineScalarObjectCount(ROOT::RDF::RNode df,
                                                const std::string &name) {
  return df.Define(name, []() -> ULong64_t { return 1U; },
                   std::vector<std::string>{});
}

template <typename InputT>
ROOT::RDF::RNode defineFlattenScalarInput(ROOT::RDF::RNode df,
                                          const std::string &name,
                                          const std::string &objectCountColumn,
                                          const std::string &inputColumn) {
  return df.Define(
      name,
      [](ULong64_t objectCount, InputT value) -> ROOT::VecOps::RVec<double> {
        return ROOT::VecOps::RVec<double>(static_cast<std::size_t>(objectCount),
                                          static_cast<double>(value));
      },
      {objectCountColumn, inputColumn});
}

template <typename InputT>
ROOT::RDF::RNode defineFlattenVectorInput(ROOT::RDF::RNode df,
                                          const std::string &name,
                                          const std::string &objectCountColumn,
                                          const std::string &inputColumn) {
  return df.Define(
      name,
      [](ULong64_t objectCount,
         const ROOT::VecOps::RVec<InputT> &values) -> ROOT::VecOps::RVec<double> {
        ROOT::VecOps::RVec<double> out;
        if (objectCount == 0U || values.empty()) {
          return out;
        }
        out.reserve(static_cast<std::size_t>(objectCount));
        for (ULong64_t index = 0; index < objectCount; ++index) {
          const auto clampedIndex = std::min<std::size_t>(
              static_cast<std::size_t>(index), values.size() - 1U);
          out.emplace_back(static_cast<double>(values[clampedIndex]));
        }
        return out;
      },
      {objectCountColumn, inputColumn});
}

inline ROOT::RDF::RNode dispatchFlattenScalarInput(
    ROOT::RDF::RNode df, const std::string &name,
    const std::string &objectCountColumn, const std::string &inputColumn,
    InputTypeToken inputType, const std::string &context) {
  switch (inputType) {
  case InputTypeToken::Bool:
    return defineFlattenScalarInput<Bool_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Float:
    return defineFlattenScalarInput<Float_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Double:
    return defineFlattenScalarInput<Double_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Int:
    return defineFlattenScalarInput<Int_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::UInt:
    return defineFlattenScalarInput<UInt_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Short:
    return defineFlattenScalarInput<Short_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::UShort:
    return defineFlattenScalarInput<UShort_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Char:
    return defineFlattenScalarInput<Char_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::UChar:
    return defineFlattenScalarInput<UChar_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Long64:
    return defineFlattenScalarInput<Long64_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::ULong64:
    return defineFlattenScalarInput<ULong64_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Unsupported:
    throw std::runtime_error(
        context + ": unsupported scalar input type in compiled flatten path");
  }

  throw std::runtime_error(context + ": unreachable scalar flatten dispatch");
}

inline ROOT::RDF::RNode dispatchFlattenVectorInput(
    ROOT::RDF::RNode df, const std::string &name,
    const std::string &objectCountColumn, const std::string &inputColumn,
    InputTypeToken inputType, const std::string &context) {
  switch (inputType) {
  case InputTypeToken::Bool:
    return defineFlattenVectorInput<Bool_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Float:
    return defineFlattenVectorInput<Float_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Double:
    return defineFlattenVectorInput<Double_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Int:
    return defineFlattenVectorInput<Int_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::UInt:
    return defineFlattenVectorInput<UInt_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Short:
    return defineFlattenVectorInput<Short_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::UShort:
    return defineFlattenVectorInput<UShort_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Char:
    return defineFlattenVectorInput<Char_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::UChar:
    return defineFlattenVectorInput<UChar_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Long64:
    return defineFlattenVectorInput<Long64_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::ULong64:
    return defineFlattenVectorInput<ULong64_t>(df, name, objectCountColumn, inputColumn);
  case InputTypeToken::Unsupported:
    throw std::runtime_error(
        context + ": unsupported vector input type in compiled flatten path");
  }

  throw std::runtime_error(context + ": unreachable vector flatten dispatch");
}

inline ROOT::RDF::RNode definePackedFeatureInit(ROOT::RDF::RNode df,
                                                const std::string &name,
                                                const std::string &featureColumn) {
  return df.Define(
      name,
      [](const ROOT::VecOps::RVec<double> &values) { return values; },
      {featureColumn});
}

inline ROOT::RDF::RNode definePackedFeatureAppend(ROOT::RDF::RNode df,
                                                  const std::string &name,
                                                  const std::string &currentColumn,
                                                  const std::string &nextColumn,
                                                  std::size_t currentStride,
                                                  const std::string &context) {
  return df.Define(
      name,
      [currentStride, context](const ROOT::VecOps::RVec<double> &current,
                               const ROOT::VecOps::RVec<double> &next) {
        const std::size_t rowCount = next.size();
        if (rowCount == 0U) {
          if (!current.empty()) {
            throw std::runtime_error(
                context +
                ": inconsistent flattened feature sizes while packing inputs");
          }
          return ROOT::VecOps::RVec<double>{};
        }
        if (current.size() != rowCount * currentStride) {
          throw std::runtime_error(
              context +
              ": inconsistent flattened feature sizes while packing inputs");
        }

        ROOT::VecOps::RVec<double> out;
        out.reserve(rowCount * (currentStride + 1U));
        for (std::size_t row = 0; row < rowCount; ++row) {
          const auto blockBegin = current.begin() +
                                  static_cast<std::ptrdiff_t>(row * currentStride);
          out.insert(out.end(), blockBegin,
                     blockBegin + static_cast<std::ptrdiff_t>(currentStride));
          out.emplace_back(next[row]);
        }
        return out;
      },
      {currentColumn, nextColumn});
}

template <typename FallbackDefineFn>
ROOT::RDF::RNode defineFlattenedNumericInputs(
    ROOT::RDF::RNode df, const std::string &outputColumn,
    const std::vector<std::string> &inputColumns, const std::string &context,
    FallbackDefineFn &&fallbackDefine) {
  std::vector<bool> isVectorInput;
  std::vector<InputTypeToken> inputTypes;
  isVectorInput.reserve(inputColumns.size());
  inputTypes.reserve(inputColumns.size());

  bool sawVectorInput = false;
  bool useFallback = false;
  for (const auto &column : inputColumns) {
    const std::string columnType = df.GetColumnType(column);
    const bool columnIsVector = isRVecColumnType(columnType);
    isVectorInput.push_back(columnIsVector);
    inputTypes.push_back(tokenizeInputType(
        columnIsVector ? extractRVecElementType(columnType) : columnType));
    sawVectorInput = sawVectorInput || columnIsVector;
    useFallback = useFallback || inputTypes.back() == InputTypeToken::Unsupported;
  }

  if (useFallback) {
    return fallbackDefine(df, outputColumn, inputColumns);
  }

  std::string objectCountColumn;
  if (!sawVectorInput) {
    objectCountColumn = outputColumn + "__object_count";
    df = defineScalarObjectCount(df, objectCountColumn);
  } else {
    bool objectCountDefined = false;
    std::string currentCountColumn;
    for (std::size_t index = 0; index < inputColumns.size(); ++index) {
      if (!isVectorInput[index]) {
        continue;
      }
      const std::string countColumnName =
          objectCountDefined ? outputColumn + "__object_count"
                             : outputColumn + "__object_count_0";
      if (!objectCountDefined) {
        df = dispatchObjectCountInit(df, countColumnName, inputColumns[index],
                                     inputTypes[index], context);
        currentCountColumn = countColumnName;
        objectCountDefined = true;
        continue;
      }

      const std::string nextCountColumn =
          (index + 1 == inputColumns.size())
              ? outputColumn + "__object_count"
              : outputColumn + "__object_count_" + std::to_string(index);
      df = dispatchObjectCountAppend(df, nextCountColumn, currentCountColumn,
                                     inputColumns[index], inputTypes[index],
                                     context);
      currentCountColumn = nextCountColumn;
    }
    objectCountColumn = currentCountColumn;
  }

  std::vector<std::string> flattenedColumns;
  flattenedColumns.reserve(inputColumns.size());
  for (std::size_t index = 0; index < inputColumns.size(); ++index) {
    const std::string flattenedColumn =
        outputColumn + "__feature_" + std::to_string(index);
    if (isVectorInput[index]) {
      df = dispatchFlattenVectorInput(df, flattenedColumn, objectCountColumn,
                                      inputColumns[index], inputTypes[index],
                                      context);
    } else {
      df = dispatchFlattenScalarInput(df, flattenedColumn, objectCountColumn,
                                      inputColumns[index], inputTypes[index],
                                      context);
    }
    flattenedColumns.push_back(flattenedColumn);
  }

  if (flattenedColumns.empty()) {
    return df.Define(outputColumn, []() { return ROOT::VecOps::RVec<double>{}; },
                     std::vector<std::string>{});
  }

  if (flattenedColumns.size() == 1U) {
    return definePackedFeatureInit(df, outputColumn, flattenedColumns.front());
  }

  std::string currentPackedColumn = outputColumn + "__packed_0";
  df = definePackedFeatureInit(df, currentPackedColumn, flattenedColumns.front());
  for (std::size_t index = 1; index < flattenedColumns.size(); ++index) {
    const std::string nextPackedColumn =
        (index + 1 == flattenedColumns.size())
            ? outputColumn
            : outputColumn + "__packed_" + std::to_string(index);
    df = definePackedFeatureAppend(df, nextPackedColumn, currentPackedColumn,
                                   flattenedColumns[index], index, context);
    currentPackedColumn = nextPackedColumn;
  }
  return df;
}

} // namespace rdfcolumnpacker

#endif