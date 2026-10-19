#include <RDFColumnPacker.h>
#include <TInterpreter.h>
#include <algorithm>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

namespace rdfcolumnpacker {
namespace {
bool isRowCollectionType(const std::string &type) {
  return isRVecColumnType(type) || type.find("vector<") != std::string::npos;
}

template <typename T>
ROOT::RDF::RNode defineSingleProduct(ROOT::RDF::RNode df, const std::string &name,
                                    const std::string &column, double norm) {
  return df.Define(name, [norm](T value) { return static_cast<double>(value) * norm; }, {column});
}

template <typename T>
ROOT::RDF::RNode definePairProduct(ROOT::RDF::RNode df, const std::string &name,
                                  const std::vector<std::string> &columns, double norm) {
  return df.Define(name, [norm](T a, T b) {
    return (static_cast<double>(a) * static_cast<double>(b)) * norm;
  }, columns);
}

enum class Reduction { None, Product, All };

using VectorPackingFactory = ROOT::RDF::RNode (*)(
    ROOT::RDF::RNode, const std::string &, const std::vector<std::string> &, double);

// Compile a typed, one-node Define once per packing signature. The generated
// function takes column names as arguments rather than embedding them in C++:
// different datasets and systematic variants can share the same callable.
ROOT::RDF::RNode defineKernel(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns,
    const std::string &targetType, PackingLayout layout,
    Reduction reduction, double normalization) {
  const bool product = reduction == Reduction::Product;
  const bool logicalAll = reduction == Reduction::All;
  std::vector<std::string> columnTypes;
  for (const auto &column : columns) columnTypes.push_back(df.GetColumnType(column));
  std::unordered_map<std::string, std::size_t> positions;
  std::vector<std::string> uniqueColumns, uniqueTypes;
  std::vector<std::size_t> indices;
  for (std::size_t i = 0; i < columns.size(); ++i) {
    if (columnTypes[i].find("CLING_UNKNOWN_TYPE_") != std::string::npos) {
      throw std::runtime_error(
          "DefineVector: input '" + columns[i] + "' has interpreter-unknown type '" +
          columnTypes[i] + "'; register its C++ type with ROOT before packing.");
    }
    const auto inserted = positions.emplace(columns[i], uniqueColumns.size());
    if (inserted.second) {
      uniqueColumns.push_back(columns[i]);
      uniqueTypes.push_back(columnTypes[i]);
    }
    indices.push_back(inserted.first->second);
  }

  std::ostringstream signature;
  signature << targetType << ":" << static_cast<int>(layout) << ":" << static_cast<int>(reduction) << ":";
  for (const auto &inputType : uniqueTypes)
    signature << inputType.size() << ':' << inputType << ';';
  signature << '/';
  for (const auto index : indices) signature << index << ',';
  const auto cacheKey = signature.str();

  static std::mutex factoryMutex;
  static std::unordered_map<std::string, VectorPackingFactory> factories;
  static std::size_t nextFactory = 0;
  VectorPackingFactory factory = nullptr;
  {
    // Graphs can be constructed on independent threads. Protect compilation as
    // well as the cache so a signature is never declared more than once.
    std::lock_guard<std::mutex> lock(factoryMutex);
    const auto found = factories.find(cacheKey);
    if (found != factories.end()) {
      factory = found->second;
    } else {
      const auto factoryName = "pack_" + std::to_string(nextFactory++);
      const auto function = "RDFAnalyzerVectorPackingJit::" + factoryName;
      std::ostringstream code;
      code << "#include <ROOT/RDataFrame.hxx>\n#include <ROOT/RVec.hxx>\n"
              "#include <string>\n#include <vector>\n#include <algorithm>\n"
              "namespace RDFAnalyzerVectorPackingJit {\n"
              "ROOT::RDF::RNode " << factoryName
           << "(ROOT::RDF::RNode node, const std::string& name, "
              "const std::vector<std::string>& columns, double normalization) {\n"
              "return node.Define(name, [normalization](";
      for (std::size_t i = 0; i < uniqueTypes.size(); ++i) {
        if (i) code << ',';
        code << "const " << uniqueTypes[i] << "& arg" << i;
      }
      code << ") -> " << (logicalAll ? "bool" : product ? targetType : "ROOT::VecOps::RVec<" + targetType + ">") << " {\n";
      if (logicalAll) {
        code << "return true";
        for (const auto index : indices)
          code << " && static_cast<bool>(arg" << index << ")";
        code << ";\n";
      } else if (product) {
        if (indices.empty()) code << "return normalization;\n";
        else {
          code << "double value = static_cast<double>(arg" << indices.front() << ");\n";
          for (std::size_t i = 1; i < indices.size(); ++i)
            code << "value *= static_cast<double>(arg" << indices[i] << ");\n";
          code << "return value * normalization;\n";
        }
      } else if (layout == PackingLayout::Scalars) {
        code << "return ROOT::VecOps::RVec<" << targetType << ">{";
        for (std::size_t i = 0; i < indices.size(); ++i) {
          if (i) code << ',';
          code << "static_cast<" << targetType << ">(arg" << indices[i] << ')';
        }
        code << "};\n";
      } else if (layout == PackingLayout::Rows) {
        const bool hasVector = std::any_of(uniqueTypes.begin(), uniqueTypes.end(),
            [](const std::string &type) { return isRowCollectionType(type); });
        code << "std::size_t rows = " << (hasVector ? 0 : 1) << ";\n";
        for (std::size_t i = 0; i < uniqueTypes.size(); ++i) {
          if (!isRowCollectionType(uniqueTypes[i])) continue;
          code << "if (arg" << i << ".empty()) return {};\n"
               << "rows = std::max(rows, arg" << i << ".size());\n";
        }
        code << "ROOT::VecOps::RVec<" << targetType << "> out;\n"
             << "out.reserve(rows * " << indices.size() << ");\n"
             << "for (std::size_t row = 0; row < rows; ++row) {\n";
        for (const auto index : indices) {
          code << "out.push_back(static_cast<" << targetType << ">(arg" << index;
          if (isRowCollectionType(uniqueTypes[index]))
            code << "[std::min(row, arg" << index << ".size() - 1)]";
          code << "));\n";
        }
        code << "}\nreturn out;\n";
      } else {
        code << "ROOT::VecOps::RVec<" << targetType << "> out;\n"
                "std::size_t total = 0;\n";
        for (const auto index : indices) code << "total += arg" << index << ".size();\n";
        code << "out.reserve(total);\n";
        for (const auto index : indices)
          code << "for (const auto& value : arg" << index << ") out.push_back(static_cast<"
               << targetType << ">(value));\n";
        code << "return out;\n";
      }
      code << "}, columns);\n}\n}\n";
      if (!gInterpreter->Declare(code.str().c_str())) {
        throw std::runtime_error("DefineVector: cannot compile packer for '" + name +
                                 "' with signature " + cacheKey);
      }
      TInterpreter::EErrorCode error = TInterpreter::kNoError;
      const auto address = gInterpreter->ProcessLine(
          ("reinterpret_cast<Longptr_t>(&" + function + ")").c_str(), &error);
      if (error != TInterpreter::kNoError || address == 0) {
        throw std::runtime_error("DefineVector: cannot resolve compiled packer for '" + name + "'");
      }
      factory = reinterpret_cast<VectorPackingFactory>(address);
      factories.emplace(cacheKey, factory);
    }
  }
  return factory(std::move(df), name, uniqueColumns, normalization);
}

} // namespace

ROOT::RDF::RNode definePackedVector(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns,
    const std::string &targetType, PackingLayout layout) {
  return defineKernel(std::move(df), name, columns, targetType, layout, Reduction::None, 1.);
}

ROOT::RDF::RNode defineScalarProduct(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns, double normalization) {
  if (columns.size() == 1) {
    // Keep tiny common products native: JIT setup would cost much more than
    // the graph savings in a job with only one weight component.
    switch (tokenizeInputType(df.GetColumnType(columns.front()))) {
    case InputTypeToken::Bool: return defineSingleProduct<Bool_t>(df, name, columns.front(), normalization);
    case InputTypeToken::Float: return defineSingleProduct<Float_t>(df, name, columns.front(), normalization);
    case InputTypeToken::Double: return defineSingleProduct<Double_t>(df, name, columns.front(), normalization);
    case InputTypeToken::Int: return defineSingleProduct<Int_t>(df, name, columns.front(), normalization);
    case InputTypeToken::UInt: return defineSingleProduct<UInt_t>(df, name, columns.front(), normalization);
    case InputTypeToken::Short: return defineSingleProduct<Short_t>(df, name, columns.front(), normalization);
    case InputTypeToken::UShort: return defineSingleProduct<UShort_t>(df, name, columns.front(), normalization);
    case InputTypeToken::Char: return defineSingleProduct<Char_t>(df, name, columns.front(), normalization);
    case InputTypeToken::UChar: return defineSingleProduct<UChar_t>(df, name, columns.front(), normalization);
    case InputTypeToken::Long64: return defineSingleProduct<Long64_t>(df, name, columns.front(), normalization);
    case InputTypeToken::ULong64: return defineSingleProduct<ULong64_t>(df, name, columns.front(), normalization);
    case InputTypeToken::Unsupported: break;
    }
  } else if (columns.size() == 2) {
    const auto first = tokenizeInputType(df.GetColumnType(columns[0]));
    if (first == tokenizeInputType(df.GetColumnType(columns[1]))) {
      if (first == InputTypeToken::Float)
        return definePairProduct<Float_t>(df, name, columns, normalization);
      if (first == InputTypeToken::Double)
        return definePairProduct<Double_t>(df, name, columns, normalization);
    }
  }
  return defineKernel(std::move(df), name, columns, "double", PackingLayout::Scalars,
                      Reduction::Product, normalization);
}

ROOT::RDF::RNode defineScalarAll(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns) {
  if (columns.size() == 1 && tokenizeInputType(df.GetColumnType(columns[0])) == InputTypeToken::Bool)
    return df.Define(name, [](bool value) { return value; }, columns);
  return defineKernel(std::move(df), name, columns, "bool", PackingLayout::Scalars,
                      Reduction::All, 1.);
}

ROOT::RDF::RNode defineFlattenedNumericInputs(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns, const std::string &context) {
  try {
    return definePackedVector(std::move(df), name, columns, "double", PackingLayout::Rows);
  } catch (const std::exception &error) {
    throw std::runtime_error(context + ": " + error.what());
  }
}
} // namespace rdfcolumnpacker
