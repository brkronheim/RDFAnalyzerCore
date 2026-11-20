/**
 * @file python_bindings.cpp
 * @brief Python bindings for RDFAnalyzerCore using pybind11
 * 
 * This module provides Python bindings for the Analyzer class, allowing users
 * to interact with the framework from Python. It supports:
 * - String-based Define/Filter expressions (handled by ROOT interpreter)
 * - Function pointer-based operations (compatible with numba)
 * - Vector handling via pointer + size interface
 */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/numpy.h>

#include <analyzer.h>
#include <functions.h>
#include <api/ISystematicManager.h>
#include <api/IDataFrameProvider.h>
#include <OnnxManager.h>
#include <BDTManager.h>
#include <CorrectionManager.h>
#include <WeightManager.h>
#include <RegionManager.h>
#include <RDFColumnPacker.h>
#include <ExtensionConfig.h>
#include <ExtensionPythonPlugins.h>
#include <unordered_set>
#include <TFile.h>
#include <TDirectory.h>
#include <TriggerManager.h>
#include <SofieManager.h>
#include <NDHistogramManager.h>
#include <PlottingUtility.h>

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <TInterpreter.h>
#include <TH1D.h>

#include <atomic>
#include <sstream>
#include <algorithm>
#include <set>
#include <iostream>
#include <cctype>
#include <cmath>
#include <limits>
#include <unordered_map>
#include <array>
#include <map>
#include <tuple>

namespace py = pybind11;

void bindHistogramIO(py::module_ &module);

// Whitelist of allowed element types for DefineFromVector
static const std::set<std::string> ALLOWED_ELEMENT_TYPES = {
    "float", "double", "int", "long", "unsigned int", "unsigned long",
    "int32_t", "int64_t", "uint32_t", "uint64_t",
    "Float_t", "Double_t", "Int_t", "Long_t", "UInt_t", "ULong_t"
};

// Helper function to validate function signature format
static bool isValidSignature(const std::string& signature) {
    // Basic validation: should match pattern like "type(type1, type2, ...)"
    // Must have opening and closing parentheses
    size_t open_paren = signature.find('(');
    size_t close_paren = signature.find(')');
    
    if (open_paren == std::string::npos || close_paren == std::string::npos) {
        return false;
    }
    
    if (open_paren >= close_paren) {
        return false;
    }
    
    // Return type must exist before '('
    if (open_paren == 0) {
        return false;
    }
    
    // Should not contain dangerous characters
    const std::string dangerous_chars = ";{}";
    if (signature.find_first_of(dangerous_chars) != std::string::npos) {
        return false;
    }
    
    return true;
}

static std::string replaceColumnIdentifiers(
    const std::string& expression,
    const std::unordered_map<std::string, std::string>& replacements,
    std::vector<std::string>* dependencies = nullptr) {
    std::string result;
    result.reserve(expression.size());

    enum class LexicalState { Code, String, Character, LineComment, BlockComment };
    LexicalState state = LexicalState::Code;
    bool escaped = false;

    for (std::size_t i = 0; i < expression.size();) {
        const char current = expression[i];
        const char next = i + 1 < expression.size() ? expression[i + 1] : '\0';

        if (state == LexicalState::String || state == LexicalState::Character) {
            result.push_back(current);
            ++i;
            if (escaped) {
                escaped = false;
            } else if (current == '\\') {
                escaped = true;
            } else if ((state == LexicalState::String && current == '"') ||
                       (state == LexicalState::Character && current == '\'')) {
                state = LexicalState::Code;
            }
            continue;
        }

        if (state == LexicalState::LineComment) {
            result.push_back(current);
            ++i;
            if (current == '\n') state = LexicalState::Code;
            continue;
        }

        if (state == LexicalState::BlockComment) {
            result.push_back(current);
            ++i;
            if (current == '*' && next == '/') {
                result.push_back(next);
                ++i;
                state = LexicalState::Code;
            }
            continue;
        }

        if (current == '"') {
            result.push_back(current);
            ++i;
            state = LexicalState::String;
            continue;
        }
        if (current == '\'') {
            result.push_back(current);
            ++i;
            state = LexicalState::Character;
            continue;
        }
        if (current == '/' && next == '/') {
            result.append("//");
            i += 2;
            state = LexicalState::LineComment;
            continue;
        }
        if (current == '/' && next == '*') {
            result.append("/*");
            i += 2;
            state = LexicalState::BlockComment;
            continue;
        }

        // Raw string contents are opaque to identifier replacement. Handle
        // standard raw-string prefixes before tokenizing the prefix itself.
        std::size_t rawPrefixLength = 0;
        if (expression.compare(i, 4, "u8R\"") == 0) rawPrefixLength = 4;
        else if (expression.compare(i, 3, "uR\"") == 0 ||
                 expression.compare(i, 3, "UR\"") == 0 ||
                 expression.compare(i, 3, "LR\"") == 0) rawPrefixLength = 3;
        else if (expression.compare(i, 2, "R\"") == 0) rawPrefixLength = 2;
        if (rawPrefixLength > 0) {
            const auto delimiterStart = i + rawPrefixLength;
            const auto openParen = expression.find('(', delimiterStart);
            if (openParen != std::string::npos && openParen - delimiterStart <= 16) {
                const auto delimiter = expression.substr(
                    delimiterStart, openParen - delimiterStart);
                if (delimiter.find_first_of(" ()\\\t\r\n") == std::string::npos) {
                    const auto terminator = ")" + delimiter + "\"";
                    const auto close = expression.find(terminator, openParen + 1);
                    if (close != std::string::npos) {
                        const auto end = close + terminator.size();
                        result.append(expression, i, end - i);
                        i = end;
                        continue;
                    }
                }
            }
        }

        const auto byte = static_cast<unsigned char>(current);
        if (std::isalpha(byte) || current == '_') {
            const auto start = i++;
            while (i < expression.size()) {
                const auto tokenByte = static_cast<unsigned char>(expression[i]);
                if (!std::isalnum(tokenByte) && expression[i] != '_') break;
                ++i;
            }
            const auto token = expression.substr(start, i - start);
            const auto replacement = replacements.find(token);
            auto previous = start;
            while (previous > 0 &&
                   std::isspace(static_cast<unsigned char>(expression[previous - 1]))) {
                --previous;
            }
            const bool isMemberName =
                (previous > 0 && expression[previous - 1] == '.') ||
                (previous >= 2 && expression.compare(previous - 2, 2, "->") == 0) ||
                (previous >= 2 && expression.compare(previous - 2, 2, "::") == 0);
            if (replacement != replacements.end() && !isMemberName) {
                result += replacement->second;
                if (dependencies && std::find(dependencies->begin(), dependencies->end(),
                                               token) == dependencies->end())
                    dependencies->push_back(token);
            } else {
                result += token;
            }
            continue;
        }

        result.push_back(current);
        ++i;
    }

    return result;
}

static std::vector<std::string> inferExpressionColumns(
    const std::string& expression, ROOT::RDF::RNode df) {
    std::unordered_map<std::string, std::string> candidates;
    for (const auto& column : df.GetColumnNames()) candidates.emplace(column, column);
    // ROOT's entry/slot columns are usable even when omitted by GetColumnNames.
    candidates.emplace("rdfentry_", "rdfentry_");
    candidates.emplace("rdfslot_", "rdfslot_");
    std::vector<std::string> columns;
    replaceColumnIdentifiers(expression, candidates, &columns);
    return columns;
}

static std::string declareJitExpressionFunction(
    const std::string& expression,
    const std::vector<std::string>& columns,
    ROOT::RDF::RNode& dataframe) {
    static std::atomic<unsigned int> expressionCounter{0};
    const auto functionName = "__rdf_jit_expression_" +
        std::to_string(expressionCounter.fetch_add(1));

    std::unordered_map<std::string, std::string> replacements;
    std::ostringstream declaration;
    declaration << "auto " << functionName << "(";
    for (std::size_t index = 0; index < columns.size(); ++index) {
        const auto& column = columns[index];
        if (!dataframe.HasColumn(column)) {
            throw std::invalid_argument(
                "DefineJIT: input column '" + column + "' is not available");
        }
        if (column.empty() ||
            !(std::isalpha(static_cast<unsigned char>(column.front())) ||
              column.front() == '_') ||
            !std::all_of(column.begin() + 1, column.end(), [](unsigned char ch) {
                return std::isalnum(ch) || ch == '_';
            })) {
            throw std::invalid_argument(
                "DefineJIT: input column '" + column +
                "' is not a C++ identifier and cannot be used in a JIT expression");
        }
        if (index != 0) declaration << ", ";
        const auto argument = "__rdf_jit_arg_" + std::to_string(index);
        const auto type = dataframe.GetColumnType(column);
        if (type.empty() || type.find_first_of(";{}") != std::string::npos) {
            throw std::invalid_argument(
                "DefineJIT: unsupported dataframe type '" + type +
                "' for input column '" + column + "'");
        }
        declaration << "const " << type << "& " << argument;
        replacements.emplace(column, argument);
    }

    const auto rewritten = replaceColumnIdentifiers(expression, replacements);
    declaration << "){ return (" << rewritten << "); }";
    if (!gInterpreter->Declare(declaration.str().c_str())) {
        throw std::runtime_error(
            "DefineJIT: Cling could not compile expression '" + expression + "'");
    }
    return functionName;
}

static std::string jitExpressionCall(
    const std::string& functionName,
    const std::vector<std::string>& columns) {
    std::ostringstream call;
    call << functionName << "(";
    for (std::size_t index = 0; index < columns.size(); ++index) {
        if (index != 0) call << ", ";
        call << columns[index];
    }
    call << ")";
    return call.str();
}

struct PythonActions {
    std::unordered_map<std::string, ROOT::RDF::RResultPtr<TH1D>> histograms;
    std::unordered_map<std::string, ROOT::RDF::RResultPtr<ULong64_t>> counts;
    std::unordered_map<std::string, ROOT::RDF::RResultPtr<double>> sums;
};

static void validatePersistentHistogramName(const std::string& name,
    const std::unordered_map<std::string,ROOT::RDF::RResultPtr<TH1D>>& existing) {
    if (name.empty() || name.find(';')!=std::string::npos ||
        std::any_of(name.begin(),name.end(),[](unsigned char c){return c<32;}))
        throw std::invalid_argument("Persistent histogram names must be nonempty ROOT paths without cycles or control characters");
    std::size_t start=0;
    while (start<=name.size()) {
        const auto slash=name.find('/',start);
        const auto component=name.substr(start,slash==std::string::npos ? std::string::npos:slash-start);
        if (component.empty() || component=="." || component=="..")
            throw std::invalid_argument("Persistent histogram paths cannot contain empty, '.' or '..' components");
        if (slash==std::string::npos)break;
        if (existing.count(name.substr(0,slash)))
            throw std::invalid_argument("Histogram and directory paths collide; use Nominal/name and variation/name");
        start=slash+1;
    }
    for (const auto& item:existing) if (item.first.rfind(name+"/",0)==0)
        throw std::invalid_argument("Histogram and directory paths collide; use Nominal/name and variation/name");
}

// Consume frontend-booked lazy actions inside the existing plugin lifecycle,
// before provenance finalization, including runs with neither skim nor ND output.
class PythonActionsManager final : public IPluggableManager {
public:
    explicit PythonActionsManager(std::shared_ptr<PythonActions> actions)
        : actions_(std::move(actions)) {}
    std::string type() const override { return "PythonActionsManager"; }
    void setContext(ManagerContext& context) override { context_=&context; }
    void setupFromConfigFile() override {}
    void finalize() override {
        if (persistHistograms())for (const auto& item:actions_->histograms)
            validatePersistentHistogramName(item.first,actions_->histograms);
        for (auto& item : actions_->histograms) item.second.GetValue();
        for (auto& item : actions_->counts) item.second.GetValue();
        for (auto& item : actions_->sums) item.second.GetValue();
        if (!persistHistograms()) return;
        const auto path=context_->metaSink.resolveOutputFile(context_->config,OutputChannel::Meta);
        if (path.empty()) throw std::runtime_error("Python histogram persistence requires a metadata output path");
        TFile output(path.c_str(),"UPDATE");
        if (output.IsZombie())throw std::runtime_error("Cannot persist Python histograms to "+path);
        // Own this namespace completely, avoiding stale slices on a rerun while
        // preserving counters, ND outputs and the provenance service's channel.
        output.Delete("python_histograms;*");
        auto* root=output.mkdir("python_histograms");
        if (!root)throw std::runtime_error("Cannot create Python histogram directory in "+path);
        for (auto& item:actions_->histograms) {
            auto* directory=root;
            const auto& name=item.first;
            std::size_t start=0,slash;
            while ((slash=name.find('/',start))!=std::string::npos) {
                const auto component=name.substr(start,slash-start);
                auto* next=directory->GetDirectory(component.c_str());
                if (!next)next=directory->mkdir(component.c_str());
                if (!next)throw std::runtime_error("Cannot create Python histogram directory "+name);
                directory=next;start=slash+1;
            }
            const auto key=name.substr(start);
            TH1D saved(item.second.GetValue());saved.SetDirectory(nullptr);saved.SetName(key.c_str());
            const TDirectory::TContext scope(directory);
            if (saved.Write(key.c_str(),TObject::kOverwrite)<=0)throw std::runtime_error("Cannot write Python histogram "+name);
        }
        output.Close();
    }
    std::unordered_map<std::string, std::string> collectProvenanceEntries() const override {
        return {{"authoring_api_version","4"},
                {"histograms", std::to_string(actions_->histograms.size())},
                {"counts", std::to_string(actions_->counts.size())},
                {"sums", std::to_string(actions_->sums.size())},
                {"histogram_storage",persistHistograms() ? "metadata:python_histograms (TH1D)":"memory"}};
    }
private:
    std::shared_ptr<PythonActions> actions_;
    ManagerContext* context_=nullptr;
    bool persistHistograms() const {
        if (!context_)return false;
        const auto& settings=context_->config.getConfigMap();
        const auto flag=settings.find("persistPythonHistograms");
        if (flag!=settings.end() && flag->second!="true" && flag->second!="false")
            throw std::invalid_argument("persistPythonHistograms must be true or false");
        return flag!=settings.end() && flag->second=="true";
    }
};

/**
 * @brief Wrapper to handle string-based Define calls (ROOT JIT compilation)
 * 
 * This allows Python users to pass C++ expressions as strings, which ROOT
 * will compile using its JIT compiler.
 */
class AnalyzerPythonWrapper {
    using HistogramRequest = std::tuple<std::string, std::string, int, double, double,
                                        std::string, std::string, std::string>;
    using MixedHistogramRequest = std::tuple<std::string, std::string, int, double, double,
                                            std::vector<double>, std::string, std::string, std::string>;
    struct HistogramSlice {
        ROOT::RDF::RNode node;
        ROOT::RDF::RResultPtr<ULong64_t> count;
    };
    using HistogramSlices = std::map<std::pair<std::string, std::string>, HistogramSlice>;
    using NDRequest = std::tuple<std::string, std::string, std::string, std::string,
                                 int, double, double, std::vector<double>, bool>;
    using NDCategory = std::tuple<std::string, int, double, double, std::vector<std::string>>;
public:
    AnalyzerPythonWrapper(const std::string& configFile, bool recordGraph = false)
        : analyzer_(configFile), recordGraph_(recordGraph) {
        analyzer_.addPlugin("__python_actions", std::make_unique<PythonActionsManager>(actions_));
    }

    // C++-style API: Define(name, expression, columns)
    AnalyzerPythonWrapper& Define(const std::string& name,
                                  const std::string& expression,
                                  const std::vector<std::string>& columns = {}) {
        return DefineJIT(name, expression, columns);
    }
    
    /**
     * @brief Define a variable using a string expression (ROOT JIT)
     * @param name Variable name
     * @param expression C++ expression as string
     * @param columns Input column names
     * @return Reference to this wrapper for chaining
     */
    AnalyzerPythonWrapper& DefineJIT(const std::string& name, 
                                     const std::string& expression,
                                     const std::vector<std::string>& columns = {}) {
        if(executed_ || running_)throw std::runtime_error("Build the graph before run()");
        return defineExpression(name, expression, columns, true);
    }

    AnalyzerPythonWrapper& DefineNominal(const std::string& name,
                                         const std::string& expression,
                                         const std::vector<std::string>& columns = {}) {
        if(executed_ || running_)throw std::runtime_error("Build the graph before run()");
        return defineExpression(name, expression, columns, false);
    }

    AnalyzerPythonWrapper& DefineCompiled(const std::string& name,
                                          const std::string& function,
                                          const std::vector<std::string>& columns,
                                          bool nominal) {
        if(executed_ || running_)throw std::runtime_error("Build the graph before run()");
        if (function.empty() || !std::all_of(function.begin(), function.end(),
            [](unsigned char c) { return std::isalnum(c) || c == '_' || c == ':'; }))
            throw std::invalid_argument("Expected a compiled function identifier");
        return defineExpression(name, jitExpressionCall(function, columns), columns,
                                !nominal, function);
    }

    AnalyzerPythonWrapper& defineExpression(const std::string& name,
                                             const std::string& expression,
                                             const std::vector<std::string>& requestedColumns,
                                             bool propagate,
                                             const std::string& compiledFunction = "") {
        auto df = analyzer_.getDF();
        auto& sysMgr = analyzer_.getSystematicManager();
        if (df.HasColumn(name)) {
            throw std::invalid_argument("Define: column '" + name + "' already exists");
        }
        const auto columns = requestedColumns.empty()
                                 ? inferExpressionColumns(expression, df)
                                 : requestedColumns;
        const auto inferred = inferExpressionColumns(expression, df);
        for (const auto& column : inferred) {
            if (std::find(columns.begin(), columns.end(), column) == columns.end())
                throw std::invalid_argument("Define: expression uses undeclared input '" + column + "'");
        }
        const auto expressionFunction = compiledFunction.empty()
            ? declareJitExpressionFunction(expression, columns, df) : compiledFunction;
        GraphOperation operation{};
        if (recordGraph_) operation = {compiledFunction.empty() ? "expression" : "compiled",
                                      name, graphStage_, propagate ? "propagate" : "nominal-only", columns};
        const auto define = [&](ROOT::RDF::RNode node, const std::string& output,
                                const std::vector<std::string>& inputs) {
            auto next = node.Define(output, jitExpressionCall(expressionFunction, inputs));
            if (recordGraph_) operation.materializations.push_back({output, inputs});
            return next;
        };
        if (propagate) {
            const auto names = df.GetColumnNames();
            sysMgr.autoRegisterSystematics({names.begin(), names.end()});
            df = rdf::detail::defineWithVariations(df, name, columns, sysMgr, define);
        } else {
            df = define(df, name, columns);
        }
        analyzer_.getDataFrameProvider().setDataFrame(df);
        if (recordGraph_) recordDefinition(std::move(operation), propagate);
        return *this;
    }
    
    /**
     * @brief Filter using a string expression (ROOT JIT)
     * @param name Filter name
     * @param expression C++ expression as string returning bool
     * @param columns Input column names
     * @return Reference to this wrapper for chaining
     */
    AnalyzerPythonWrapper& FilterJIT(const std::string& name,
                                     const std::string& expression,
                                     const std::vector<std::string>& columns = {}) {
        if(executed_ || running_)throw std::runtime_error("Build the graph before run()");
        // Define the expression under a private name, then delegate selection
        // masks and the row filter to Analyzer::Filter. This keeps the Python
        // JIT path aligned with the C++ variation-aware filter behavior.
        const std::string predicateName = "__python_filter_predicate_" + name;
        DefineJIT(predicateName, expression, columns);
        analyzer_.Filter(name, [](bool passed) { return passed; },
                         {predicateName});
        ++graphStage_;
        if (recordGraph_) {
            GraphOperation operation{"filter", name, graphStage_, "variation-union", {predicateName}};
            operation.masks = getSelectionMasks();
            auto& systematics = analyzer_.getSystematicManager();
            std::unordered_set<std::string> seen;
            for (const auto& label : systematics.getVariationLabels()) {
                const auto output = label == "Nominal" ? "pass_" + name
                    : systematics.getVariationColumnName("pass_" + name, label);
                operation.variations[label] = output;
                if (seen.insert(output).second)
                    operation.materializations.push_back({output,
                        {label == "Nominal" ? predicateName : systematics.getVariationColumnName(predicateName, label)}});
            }
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

    // Explicit bool-column path avoids the extra JIT expression alias while
    // delegating cumulative variation masks to the unchanged native Filter.
    AnalyzerPythonWrapper& FilterColumn(const std::string& name,
                                       const std::string& column) {
        if (executed_ || running_) throw std::runtime_error("Build the graph before run()");
        if (!analyzer_.HasColumn(column))
            throw std::invalid_argument("Filter column does not exist: " + column);
        const auto type = analyzer_.GetColumnType(column);
        if (type != "bool" && type != "Bool_t")
            throw std::invalid_argument("Filter column must be scalar bool: " + column + " (" + type + ")");
        analyzer_.Filter(name, [](bool passed) { return passed; }, {column});
        ++graphStage_;
        if (recordGraph_) {
            GraphOperation operation{"filter", name, graphStage_, "variation-union", {column}};
            operation.masks = getSelectionMasks();
            auto& systematics = analyzer_.getSystematicManager();
            std::unordered_set<std::string> seen;
            for (const auto& label : systematics.getVariationLabels()) {
                const auto output = label == "Nominal" ? "pass_" + name
                    : systematics.getVariationColumnName("pass_" + name, label);
                operation.variations[label] = output;
                if (seen.insert(output).second)
                    operation.materializations.push_back({output,
                        {label == "Nominal" ? column : systematics.getVariationColumnName(column, label)}});
            }
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

    // C++-style API: Filter(name, expression, columns)
    AnalyzerPythonWrapper& Filter(const std::string& name,
                                  const std::string& expression,
                                  const std::vector<std::string>& columns = {}) {
        return FilterJIT(name, expression, columns);
    }
    
    /**
     * @brief Define a variable using a function pointer (for numba compatibility)
     * @param name Variable name
     * @param func_ptr Function pointer as integer (address)
     * @param signature Function signature string (e.g., "float(float, float)")
     * @param columns Input column names
     * @return Reference to this wrapper for chaining
     * 
     * This allows users to pass numba-compiled functions by getting their address
     * via ctypes and passing as integer.
     */
    AnalyzerPythonWrapper& DefineFromPointer(const std::string& name,
                                             uintptr_t func_ptr,
                                             const std::string& signature,
                                             const std::vector<std::string>& columns = {}) {
        // Validate signature format to prevent code injection
        if (!isValidSignature(signature)) {
            throw std::invalid_argument(
                "Invalid function signature format: '" + signature + "'. "
                "Expected format: 'return_type(arg_type1, arg_type2, ...)'"
            );
        }
        if (func_ptr == 0) {
            throw std::invalid_argument(
                "DefineFromPointer: func_ptr must be a non-null function address"
            );
        }

        const size_t signatureClose = signature.rfind(')');
        const size_t signatureLast = signature.find_last_not_of(" \t\n\r");
        if (signatureClose != signatureLast ||
            signature.find('(', signature.find('(') + 1) != std::string::npos ||
            signature.find(')', signatureClose + 1) != std::string::npos) {
            throw std::invalid_argument(
                "Invalid function signature format: '" + signature + "'. "
                "Expected one complete return_type(arg_type, ...) signature."
            );
        }
        const size_t signatureOpen = signature.find('(');
        const std::string signatureArgs =
            signature.substr(signatureOpen + 1, signatureClose - signatureOpen - 1);
        const auto firstArgument = signatureArgs.find_first_not_of(" \t\n\r");
        const auto lastArgument = signatureArgs.find_last_not_of(" \t\n\r");
        if (signatureArgs.find_first_of("()") != std::string::npos ||
            (firstArgument != std::string::npos &&
             (signatureArgs[firstArgument] == ',' || signatureArgs[lastArgument] == ','))) {
            throw std::invalid_argument(
                "Invalid function signature format: malformed argument list"
            );
        }
        size_t argumentCount = 0;
        if (signatureArgs.find_first_not_of(" \t\n\r") != std::string::npos) {
            std::istringstream countArgs(signatureArgs);
            std::string argumentType;
            while (std::getline(countArgs, argumentType, ',')) {
                if (argumentType.find_first_not_of(" \t\n\r") == std::string::npos) {
                    throw std::invalid_argument(
                        "Invalid function signature format: empty argument type"
                    );
                }
                ++argumentCount;
            }
        }
        if (argumentCount != columns.size()) {
            throw std::invalid_argument(
                "DefineFromPointer: signature declares " +
                std::to_string(argumentCount) + " argument(s), but " +
                std::to_string(columns.size()) + " input column(s) were provided"
            );
        }
        
        // Build a proper C++ function pointer type from the simple signature string
        // e.g. "double(double,double)" -> "double(*)(double,double)"
        auto toFunctionPointerType = [](const std::string& sig)->std::string {
            size_t open = sig.find('(');
            size_t close = sig.rfind(')');
            std::string ret = (open==std::string::npos) ? sig : sig.substr(0, open);
            std::string args = (open==std::string::npos || close==std::string::npos) ? "" : sig.substr(open+1, close-open-1);
            // remove whitespace
            ret.erase(std::remove_if(ret.begin(), ret.end(), ::isspace), ret.end());
            args.erase(std::remove_if(args.begin(), args.end(), ::isspace), args.end());
            return ret + "(*)(" + args + ")";
        };
        const auto fptr = toFunctionPointerType(signature);

        // Declare the function pointer globally via ROOT's Cling interpreter.
        // A monotonically increasing ID ensures each declaration has a unique name.
        static std::atomic<unsigned int> fptr_counter{0};
        const unsigned int fptr_id = fptr_counter.fetch_add(1);
        const std::string fname = "__rdf_jit_fptr_" + std::to_string(fptr_id);

        const std::string decl = "auto " + fname +
                                 " = reinterpret_cast<" + fptr + ">(" +
                                 std::to_string(func_ptr) + ");";
        if (!gInterpreter->Declare(decl.c_str())) {
            throw std::runtime_error(
                "DefineFromPointer: failed to declare function pointer via Cling interpreter"
            );
        }

        // Build a typed wrapper function so that ROOT's JIT column-name
        // auto-detection can identify the column arguments.  A bare function
        // pointer call "fptr(col)" confuses ROOT's expression parser; a regular
        // function call "wrapper(col)" works reliably.
        //
        // Parse argument types from signature, e.g. "double(double,float)"
        // → retType="double", argTypes={"double","float"}
        const size_t open  = signature.find('(');
        const size_t close = signature.rfind(')');
        std::string retType = (open == std::string::npos) ? signature : signature.substr(0, open);
        std::string argsStr = (open == std::string::npos || close == std::string::npos)
                                  ? "" : signature.substr(open + 1, close - open - 1);
        retType.erase(std::remove_if(retType.begin(), retType.end(), ::isspace), retType.end());
        argsStr.erase(std::remove_if(argsStr.begin(), argsStr.end(), ::isspace), argsStr.end());

        // Split comma-separated arg types
        std::vector<std::string> argTypes;
        if (!argsStr.empty()) {
            std::istringstream argStream(argsStr);
            std::string tok;
            while (std::getline(argStream, tok, ',')) {
                if (!tok.empty()) argTypes.push_back(tok);
            }
        }

        // Build wrapper: "double __rdf_jit_fptr_0_w(double a0,float a1){return __rdf_jit_fptr_0(a0,a1);}"
        const std::string wname = fname + "_w";
        std::ostringstream wdecl;
        wdecl << retType << " " << wname << "(";
        for (size_t i = 0; i < argTypes.size(); ++i) {
            if (i > 0) wdecl << ",";
            wdecl << argTypes[i] << " a" << i;
        }
        wdecl << "){return " << fname << "(";
        for (size_t i = 0; i < argTypes.size(); ++i) {
            if (i > 0) wdecl << ",";
            wdecl << "a" << i;
        }
        wdecl << ");}";
        if (!gInterpreter->Declare(wdecl.str().c_str())) {
            throw std::runtime_error(
                "DefineFromPointer: failed to declare wrapper function via Cling interpreter"
            );
        }

        // Build the JIT expression using the wrapper function.
        // ROOT's column-name auto-detection works for regular function calls.
        std::stringstream jit_expr;
        jit_expr << wname << "(";
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) jit_expr << ", ";
            jit_expr << columns[i];
        }
        jit_expr << ")";

        return DefineJIT(name, jit_expr.str(), columns);
    }
    
    /**
     * @brief Define a variable from a vector using pointer + size
     * @param name Variable name
     * @param data_ptr Pointer to data array (as integer)
     * @param size Size of the array
     * @param element_type Type of array elements (e.g., "float", "double")
     * @return Reference to this wrapper for chaining
     */
    AnalyzerPythonWrapper& DefineFromVector(const std::string& name,
                                            uintptr_t data_ptr,
                                            size_t size,
                                            const std::string& element_type = "float") {
        if (size > 0 && data_ptr == 0) {
            throw std::invalid_argument(
                "DefineFromVector: data_ptr must be non-null when size is positive"
            );
        }
        // Validate element type against whitelist to prevent code injection
        if (ALLOWED_ELEMENT_TYPES.find(element_type) == ALLOWED_ELEMENT_TYPES.end()) {
            throw std::invalid_argument(
                "Invalid element_type: '" + element_type + "'. "
                "Allowed types: float, double, int, long, unsigned int, unsigned long, "
                "int32_t, int64_t, uint32_t, uint64_t, Float_t, Double_t, Int_t, Long_t, UInt_t, ULong_t"
            );
        }
        
        // Create an RVec from the pointer and size
        std::stringstream jit_expr;
        jit_expr << "ROOT::VecOps::RVec<" << element_type << ">("
                 << "reinterpret_cast<" << element_type << "*>(" << data_ptr << "), "
                 << size << ")";
        
        return DefineJIT(name, jit_expr.str(), {});
    }

    // C++-style API name parity with Analyzer::DefineVector
    AnalyzerPythonWrapper& DefineVector(const std::string& name,
                                        uintptr_t data_ptr,
                                        size_t size,
                                        const std::string& element_type = "float") {
        return DefineFromVector(name, data_ptr, size, element_type);
    }

    AnalyzerPythonWrapper& AddPlugin(const std::string& role,
                                     const std::string& pluginType) {
        const auto normalized = normalizePluginType(pluginType);
        auto& cfg = analyzer_.getConfigurationProvider();

        if (normalized == "onnx") {
            analyzer_.addPlugin(role, std::make_unique<OnnxManager>(cfg));
        } else if (normalized == "bdt") {
            analyzer_.addPlugin(role, std::make_unique<BDTManager>(cfg));
        } else if (normalized == "correction") {
            analyzer_.addPlugin(role, std::make_unique<CorrectionManager>(cfg));
        } else if (normalized == "trigger") {
            analyzer_.addPlugin(role, std::make_unique<TriggerManager>(cfg));
        } else if (normalized == "sofie") {
            analyzer_.addPlugin(role, std::make_unique<SofieManager>(cfg));
        } else if (normalized == "ndhistogram") {
            analyzer_.addPlugin(role, std::make_unique<NDHistogramManager>(cfg));
        } else if (normalized == "weights") {
            analyzer_.addPlugin(role, std::make_unique<WeightManager>());
        } else if (normalized == "regions") {
            analyzer_.addPlugin(role, std::make_unique<RegionManager>());
        } else if (rdf::addExtensionPythonPlugin(analyzer_, role, normalized)) {
            // The selected source extensions own their concrete managers.
        } else {
            throw std::invalid_argument(
                "Unavailable plugin type (check optional extensions): '" + pluginType + "'. Supported: "
                "OnnxManager, BDTManager, CorrectionManager, TriggerManager, SofieManager, NDHistogramManager, WeightManager, RegionManager"
            );
        }
        return *this;
    }

    std::unordered_map<std::string, std::string> configureExtension(
        const std::string& extension, const std::unordered_map<std::string, std::string>& settings) {
        if (executed_ || running_) throw std::runtime_error("Configure extensions before run()");
        if (!configuredExtensions_.insert(extension).second)
            throw std::runtime_error("Extension configuration already attempted; construct a fresh Analysis for a different recipe");
        return rdf::configureExtensionPythonAnalyzer(analyzer_, extension, settings);
    }

    std::unordered_map<std::string,std::string> configureWeights(
        const std::string& output,
        const std::vector<std::pair<std::string,std::string>>& components,
        const std::vector<std::pair<std::string,double>>& normalizations,
        const std::vector<std::array<std::string,4>>& variations) {
        if(executed_ || running_)throw std::runtime_error("Configure weights before run()");
        auto& data=analyzer_.getDataFrameProvider();auto& sys=analyzer_.getSystematicManager();
        auto node=data.getDataFrame();const auto labels=sys.getVariationLabels();
        std::unordered_set<std::string> names;
        auto checkColumn=[&](const std::string& column){
            if(!node.HasColumn(column))throw std::invalid_argument("Weight input column does not exist: "+column);
            const auto type=rdfcolumnpacker::tokenizeInputType(node.GetColumnType(column));
            if(type!=rdfcolumnpacker::InputTypeToken::Float && type!=rdfcolumnpacker::InputTypeToken::Double)
                throw std::invalid_argument("Weight input must be scalar float/double: "+column);
        };
        for(const auto& item:components){
            if(!names.insert(item.first).second)throw std::invalid_argument("Duplicate weight component: "+item.first);
            checkColumn(item.second);
        }
        std::unordered_set<std::string> paired;
        for(const auto& variation:variations){
            if(!names.count(variation[1]))throw std::invalid_argument("Unknown weight replacement component: "+variation[1]);
            if(!paired.insert(variation[0]).second)throw std::invalid_argument("Duplicate weight nuisance: "+variation[0]);
            checkColumn(variation[2]);checkColumn(variation[3]);
            for(const auto& direction:{"Up","Down"})
                if(std::find(labels.begin(),labels.end(),variation[0]+direction)!=labels.end())
                    throw std::invalid_argument("Weight nuisance overlaps an existing object/input nuisance: "+variation[0]+"; provide combined final weights explicitly");
        }
        std::vector<std::string> outputs{output};
        for(const auto& label:labels)if(label!="Nominal")outputs.push_back(output+"_"+label);
        for(const auto& name:paired){outputs.push_back(output+"_"+name+"Up");outputs.push_back(output+"_"+name+"Down");}
        for(const auto& column:outputs)if(node.HasColumn(column))throw std::invalid_argument("Weight output already exists: "+column);
        for(const auto& item:normalizations)if(!std::isfinite(item.second))throw std::invalid_argument("Nonfinite weight normalization: "+item.first);
        auto manager=WeightManager::create(analyzer_,"python_weights_"+output);
        std::vector<std::string> columns;
        for(const auto& item:components){manager->addScaleFactor(item.first,item.second);columns.push_back(item.second);}
        for(const auto& item:normalizations)manager->addNormalization(item.first,item.second);
        for(const auto& variation:variations)manager->addWeightVariation(variation[0],variation[1],variation[2],variation[3]);
        manager->defineNominalWeight(output);
        std::unordered_map<std::string,std::string> result{{"Nominal",output}};
        // The native manager owns nominal/component replacements and audits.
        // Propagate already-declared object-dependent factor shifts separately.
        for(const auto& label:labels){
            if(label=="Nominal")continue;
            std::vector<std::string> mapped;
            for(const auto& column:columns)mapped.push_back(sys.getVariationColumnName(column,label));
            if(mapped==columns)continue;
            const auto varied=output+"_"+label;
            data.setDataFrame(rdfcolumnpacker::defineScalarProduct(data.getDataFrame(),varied,mapped,manager->getTotalNormalization()));
            sys.registerVariationColumn(output,label,varied);result[label]=varied;
        }
        for(const auto& variation:variations){
            const auto up=output+"_"+variation[0]+"Up",down=output+"_"+variation[0]+"Down";
            manager->defineVariedWeight(variation[0],"up",up);manager->defineVariedWeight(variation[0],"down",down);
            sys.registerVariationColumns(output,variation[0],up,down);
            result[variation[0]+"Up"]=up;result[variation[0]+"Down"]=down;
        }
        if (recordGraph_) {
            GraphOperation operation{"weights", output, graphStage_, "component-product", columns};
            operation.materializations.push_back({output, columns});
            for (const auto& label : sys.getVariationLabels()) {
                const auto found = result.find(label);
                const auto mappedOutput = found == result.end() ? output : found->second;
                operation.variations[label] = mappedOutput;
                if (mappedOutput == output) continue;
                auto inputs = columns;
                for (std::size_t i = 0; i < inputs.size(); ++i) {
                    inputs[i] = sys.getVariationColumnName(columns[i], label);
                    for (const auto& replacement : variations) {
                        if (components[i].first == replacement[1]) {
                            if (label == replacement[0] + "Up") inputs[i] = replacement[2];
                            if (label == replacement[0] + "Down") inputs[i] = replacement[3];
                        }
                    }
                }
                operation.materializations.push_back({mappedOutput, inputs});
            }
            operations_.push_back(std::move(operation));
        }
        return result;
    }

    std::vector<std::string> declareRegion(const std::string& name,const std::string& column,const std::string& parent) {
        if(executed_ || running_)throw std::runtime_error("Declare regions before run()");
        auto manager=analyzer_.findPlugin<RegionManager>("python_regions");
        if(!manager)manager=RegionManager::create(analyzer_,"python_regions");
        manager->declareRegion(name,column,parent);return manager->getFilterChain(name);
    }

    AnalyzerPythonWrapper& AddDefaultPlugins() {
        AddPlugin("onnx", "OnnxManager");
        AddPlugin("bdt", "BDTManager");
        AddPlugin("correction", "CorrectionManager");
        AddPlugin("trigger", "TriggerManager");
        AddPlugin("sofie", "SofieManager");
        AddPlugin("hist", "NDHistogramManager");
        return *this;
    }

    AnalyzerPythonWrapper& SetupPlugin(const std::string& role) {
        if (auto mgr = analyzer_.getPlugin<OnnxManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto mgr = analyzer_.getPlugin<BDTManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto mgr = analyzer_.getPlugin<CorrectionManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto mgr = analyzer_.getPlugin<TriggerManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto mgr = analyzer_.getPlugin<SofieManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto mgr = analyzer_.getPlugin<NDHistogramManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        throw std::runtime_error("No supported plugin found for role '" + role + "'");
    }

    std::vector<std::string> GetColumnNames() {
        auto names = analyzer_.GetColumnNames();
        return std::vector<std::string>(names.begin(), names.end());
    }

    std::string GetColumnType(const std::string& column) {
        return analyzer_.GetColumnType(column);
    }

    bool HasColumn(const std::string& column) {
        if (running_) throw std::runtime_error("Inspect columns before or after run(), not while it is running");
        return analyzer_.HasColumn(column);
    }

    AnalyzerPythonWrapper& project(const std::string& name, const std::string& source,
                                   std::size_t index, bool nominal) {
        if(executed_ || running_)throw std::runtime_error("Build the graph before run()");
        if (analyzer_.HasColumn(name)) throw std::invalid_argument("Define: column '" + name + "' already exists");
        const auto type = analyzer_.GetColumnType(source);
        const auto element = rdfcolumnpacker::isRVecColumnType(type)
            ? rdfcolumnpacker::tokenizeInputType(rdfcolumnpacker::extractRVecElementType(type))
            : rdfcolumnpacker::InputTypeToken::Unsupported;
        if (element == rdfcolumnpacker::InputTypeToken::Double) {
            const auto select = [index](const ROOT::VecOps::RVec<double>& values) {
                if (index >= values.size()) throw std::out_of_range("project: vector index out of range");
                return values[index];
            };
            if (nominal) analyzer_.DefineNominal(name, select, {source});
            else analyzer_.Define(name, select, {source});
        } else if (element == rdfcolumnpacker::InputTypeToken::Float) {
            const auto select = [index](const ROOT::VecOps::RVec<float>& values) {
                if (index >= values.size()) throw std::out_of_range("project: vector index out of range");
                return values[index];
            };
            if (nominal) analyzer_.DefineNominal(name, select, {source});
            else analyzer_.Define(name, select, {source});
        } else throw std::invalid_argument("project currently requires RVec<float> or RVec<double>, got " + type);
        if (recordGraph_) {
            GraphOperation operation{"projection", name, graphStage_, nominal ? "nominal-only" : "propagate", {source}};
            operation.attributes["index"] = std::to_string(index);
            recordDefinition(std::move(operation), !nominal, true);
        }
        return *this;
    }

    AnalyzerPythonWrapper& registerSystematic(const std::string& syst,
                                              const std::vector<std::string>& affectedVariables) {
        analyzer_.getSystematicManager().registerSystematic(
            syst,
            std::set<std::string>(affectedVariables.begin(), affectedVariables.end())
        );
        return *this;
    }

    std::vector<std::string> getSystematics() {
        const auto& setRef = analyzer_.getSystematicManager().getSystematics();
        return std::vector<std::string>(setRef.begin(), setRef.end());
    }

    AnalyzerPythonWrapper& registerVariationColumns(
        const std::string& variable, const std::string& family,
        const std::string& up, const std::string& down) {
        if(executed_ || running_)throw std::runtime_error("Build the graph before run()");
        analyzer_.getSystematicManager().registerVariationColumns(variable, family, up, down);
        if (recordGraph_) {
            GraphOperation operation{"variation-registration", variable, graphStage_, "paired", {up, down}};
            operation.variations = {{family + "Up", up}, {family + "Down", down}};
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

    AnalyzerPythonWrapper& registerVariationColumn(
        const std::string& variable, const std::string& label,
        const std::string& column) {
        if(executed_ || running_)throw std::runtime_error("Build the graph before run()");
        analyzer_.getSystematicManager().registerVariationColumn(variable, label, column);
        if (recordGraph_) {
            GraphOperation operation{"variation-registration", variable, graphStage_, "explicit", {column}};
            operation.variations = {{label, column}};
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

    std::vector<std::string> getVariationLabels() {
        return analyzer_.getSystematicManager().getVariationLabels();
    }

    std::unordered_map<std::string, std::string> getSelectionMasks() {
        auto& systematics = analyzer_.getSystematicManager();
        std::unordered_map<std::string, std::string> masks;
        for (const auto& label : systematics.getVariationLabels()) {
            const auto column = systematics.getSelectionMaskColumn(label);
            if (!column.empty()) masks.emplace(label, column);
        }
        return masks;
    }

    // Read-only construction records. They describe this binding's authoring
    // path, not arbitrary RNode/plugin internals, and never book RDF work.
    py::dict graphDescription() {
        if (running_) throw std::runtime_error("Inspect the graph before or after run(), not while it is running");
        py::dict result;
        result["schema"] = "rdf-authoring-graph-v1";
        result["enabled"] = recordGraph_;
        result["completeness"] = "partial";
        result["stage_scope"] = "successful binding filter calls; initial configuration, services and other native/plugin mutations are opaque";
        py::list operations;
        for (const auto& operation : operations_) {
            py::dict item;
            item["kind"] = operation.kind;
            item["name"] = operation.name;
            item["stage"] = operation.stage;
            item["policy"] = operation.policy;
            item["inputs"] = operation.inputs;
            item["variations"] = operation.variations;
            item["selection_masks"] = operation.masks;
            item["attributes"] = operation.attributes;
            py::list materializations;
            for (const auto& definition : operation.materializations) {
                py::dict column;
                column["output"] = definition.first;
                column["inputs"] = definition.second;
                materializations.append(column);
            }
            item["materializations"] = materializations;
            operations.append(item);
        }
        result["operations"] = operations;
        return result;
    }

    std::vector<std::string> getVariablesForSystematic(const std::string& syst) {
        const auto& setRef = analyzer_.getSystematicManager().getVariablesForSystematic(syst);
        return std::vector<std::string>(setRef.begin(), setRef.end());
    }

    std::vector<std::string> getSystematicsForVariable(const std::string& var) {
        const auto& setRef = analyzer_.getSystematicManager().getSystematicsForVariable(var);
        return std::vector<std::string>(setRef.begin(), setRef.end());
    }

    AnalyzerPythonWrapper& registerExistingSystematics(const std::vector<std::string>& systConfig,
                                                       const std::vector<std::string>& columnList) {
        analyzer_.getSystematicManager().registerExistingSystematics(systConfig, columnList);
        return *this;
    }

    /// Python-friendly wrapper for autoRegisterSystematics().
    /// Returns a dict with keys "registered" (list of "baseVar:systName" strings),
    /// "missing_down" (list of column names), and "missing_up" (list of column names).
    py::dict autoRegisterSystematics(const std::vector<std::string>& columnList) {
        const SystematicValidationResult res =
            analyzer_.getSystematicManager().autoRegisterSystematics(columnList);

        py::list registered;
        for (const auto& p : res.registered) {
            registered.append(p.first + ":" + p.second);
        }
        py::list missingDown;
        for (const auto& col : res.missingDown) {
            missingDown.append(col);
        }
        py::list missingUp;
        for (const auto& col : res.missingUp) {
            missingUp.append(col);
        }
        py::dict result;
        result["registered"]   = registered;
        result["missing_down"] = missingDown;
        result["missing_up"]   = missingUp;
        return result;
    }

    std::vector<std::string> makeSystList(const std::string& branchName) {
        return analyzer_.getSystematicManager().makeSystList(branchName, analyzer_.getDataFrameProvider());
    }

    AnalyzerPythonWrapper& setConfig(const std::string& key, const std::string& value) {
        analyzer_.getConfigurationProvider().set(key, value);
        return *this;
    }

    std::unordered_map<std::string, std::string> getConfigMap() {
        const auto& cfg = analyzer_.getConfigurationProvider().getConfigMap();
        return std::unordered_map<std::string, std::string>(cfg.begin(), cfg.end());
    }

    std::vector<std::string> getConfigList(const std::string& key,
                                           const std::vector<std::string>& defaultValue = {},
                                           const std::string& delimiter = ",") {
        return analyzer_.getConfigurationProvider().getList(key, defaultValue, delimiter);
    }

    std::unordered_map<std::string, std::string> parsePairBasedConfig(const std::string& configFile) {
        return analyzer_.getConfigurationProvider().parsePairBasedConfig(configFile);
    }

    std::vector<std::string> parseVectorConfig(const std::string& configFile) {
        return analyzer_.getConfigurationProvider().parseVectorConfig(configFile);
    }

    std::vector<std::unordered_map<std::string, std::string>>
    parseMultiKeyConfig(const std::string& configFile,
                        const std::vector<std::string>& requiredEntryKeys) {
        return analyzer_.getConfigurationProvider().parseMultiKeyConfig(configFile, requiredEntryKeys);
    }

    AnalyzerPythonWrapper& applyAllOnnxModels(const std::string& role = "onnx",
                                              const std::string& outputSuffix = "") {
        requirePlugin<OnnxManager>(role, "OnnxManager").applyAllModels(outputSuffix);
        return *this;
    }

    AnalyzerPythonWrapper& applyOnnxModel(const std::string& role,
                                          const std::string& modelName,
                                          const std::string& outputSuffix = "") {
        requirePlugin<OnnxManager>(role, "OnnxManager").applyModel(modelName, outputSuffix);
        return *this;
    }

    std::vector<std::string> getOnnxModelNames(const std::string& role = "onnx") {
        return requirePlugin<OnnxManager>(role, "OnnxManager").getAllModelNames();
    }

    std::vector<std::string> getOnnxModelFeatures(const std::string& role,
                                                  const std::string& modelName) {
        const auto& features = requirePlugin<OnnxManager>(role, "OnnxManager").getModelFeatures(modelName);
        return std::vector<std::string>(features.begin(), features.end());
    }

    AnalyzerPythonWrapper& applyAllBDTs(const std::string& role = "bdt") {
        requirePlugin<BDTManager>(role, "BDTManager").applyAllBDTs();
        return *this;
    }

    AnalyzerPythonWrapper& applyBDT(const std::string& role,
                                    const std::string& bdtName) {
        requirePlugin<BDTManager>(role, "BDTManager").applyBDT(bdtName);
        return *this;
    }

    std::vector<std::string> getBDTNames(const std::string& role = "bdt") {
        return requirePlugin<BDTManager>(role, "BDTManager").getAllBDTNames();
    }

    std::vector<std::string> getBDTFeatures(const std::string& role,
                                            const std::string& bdtName) {
        const auto& features = requirePlugin<BDTManager>(role, "BDTManager").getBDTFeatures(bdtName);
        return std::vector<std::string>(features.begin(), features.end());
    }

    AnalyzerPythonWrapper& registerCorrection(const std::string& role,
                                               const std::string& name,
                                               const std::string& file,
                                               const std::string& correctionlibName,
                                               const std::vector<std::string>& inputVariables) {
        requirePlugin<CorrectionManager>(role, "CorrectionManager").registerCorrection(
            name, file, correctionlibName, inputVariables);
        return *this;
    }

    AnalyzerPythonWrapper& applyCorrection(const std::string& role,
                                           const std::string& correctionName,
                                           const std::vector<std::string>& stringArguments,
                                           const std::vector<std::string>& inputColumns = {},
                                           const std::string& outputBranch = "") {
        requirePlugin<CorrectionManager>(role, "CorrectionManager").applyCorrection(
            correctionName, stringArguments, inputColumns, outputBranch);
        return *this;
    }

    AnalyzerPythonWrapper& applyCorrectionVec(const std::string& role,
                                              const std::string& correctionName,
                                              const std::vector<std::string>& stringArguments,
                                              const std::vector<std::string>& inputColumns = {},
                                              const std::string& outputBranch = "") {
        requirePlugin<CorrectionManager>(role, "CorrectionManager").applyCorrectionVec(
            correctionName, stringArguments, inputColumns, outputBranch);
        return *this;
    }

    AnalyzerPythonWrapper& applyCorrections(const std::string& role,
                                            const std::string& correctionName,
                                            const std::vector<std::vector<std::string>>& argumentSets,
                                            const std::vector<std::string>& inputColumns = {},
                                            const std::vector<std::string>& outputBranches = {},
                                            const std::vector<bool>& propagateSystematics = {}) {
        if (executed_ || running_) throw std::runtime_error("Apply corrections before run()");
        auto& manager = requirePlugin<CorrectionManager>(role, "CorrectionManager");
        validateExplicitCorrectionOutputs(manager, correctionName, argumentSets,
                                          inputColumns, outputBranches, propagateSystematics);
        manager.applyCorrections(
            correctionName, argumentSets, inputColumns, outputBranches, propagateSystematics);
        return *this;
    }

    AnalyzerPythonWrapper& applyCorrectionsVec(const std::string& role,
                                               const std::string& correctionName,
                                               const std::vector<std::vector<std::string>>& argumentSets,
                                               const std::vector<std::string>& inputColumns = {},
                                               const std::vector<std::string>& outputBranches = {},
                                               const std::vector<bool>& propagateSystematics = {}) {
        if (executed_ || running_) throw std::runtime_error("Apply corrections before run()");
        auto& manager = requirePlugin<CorrectionManager>(role, "CorrectionManager");
        validateExplicitCorrectionOutputs(manager, correctionName, argumentSets,
                                          inputColumns, outputBranches, propagateSystematics);
        manager.applyCorrectionsVec(
            correctionName, argumentSets, inputColumns, outputBranches, propagateSystematics);
        return *this;
    }

    std::vector<std::string> getCorrectionFeatures(const std::string& role,
                                                   const std::string& correctionName) {
        const auto& features = requirePlugin<CorrectionManager>(role, "CorrectionManager").getCorrectionFeatures(correctionName);
        return std::vector<std::string>(features.begin(), features.end());
    }

    AnalyzerPythonWrapper& applyAllTriggers(const std::string& role = "trigger") {
        requirePlugin<TriggerManager>(role, "TriggerManager").applyAllTriggers();
        return *this;
    }

    std::vector<std::string> getTriggerGroups(const std::string& role = "trigger") {
        return requirePlugin<TriggerManager>(role, "TriggerManager").getAllGroups();
    }

    std::vector<std::string> getTriggers(const std::string& role,
                                         const std::string& group) {
        const auto& triggers = requirePlugin<TriggerManager>(role, "TriggerManager").getTriggers(group);
        return std::vector<std::string>(triggers.begin(), triggers.end());
    }

    std::vector<std::string> getVetoes(const std::string& role,
                                       const std::string& group) {
        const auto& vetoes = requirePlugin<TriggerManager>(role, "TriggerManager").getVetoes(group);
        return std::vector<std::string>(vetoes.begin(), vetoes.end());
    }

    std::string getTriggerGroupForSample(const std::string& role,
                                         const std::string& sample) {
        return requirePlugin<TriggerManager>(role, "TriggerManager").getGroupForSample(sample);
    }

    AnalyzerPythonWrapper& applyAllSofieModels(const std::string& role = "sofie") {
        requirePlugin<SofieManager>(role, "SofieManager").applyAllModels();
        return *this;
    }

    AnalyzerPythonWrapper& applySofieModel(const std::string& role,
                                           const std::string& modelName) {
        requirePlugin<SofieManager>(role, "SofieManager").applyModel(modelName);
        return *this;
    }

    std::vector<std::string> getSofieModelNames(const std::string& role = "sofie") {
        return requirePlugin<SofieManager>(role, "SofieManager").getAllModelNames();
    }

    std::vector<std::vector<std::string>> bookNDHistograms(
        const std::string& role,
        const std::vector<histInfo>& infos,
        const std::vector<selectionInfo>& selection,
        const std::string& suffix = "") {
        if (role=="histogramManager" && ndTyped_) {
            std::set<std::string> fresh;
            for (const auto& info : infos)
                if (!fresh.insert(info.name()+"_"+suffix).second || ndKeys_[role].count(info.name()+"_"+suffix))
                    throw std::invalid_argument("Raw ND destination collides with typed manager bookings");
        }
        auto infosCopy = infos;
        auto selectionCopy = selection;
        std::vector<std::vector<std::string>> regionNames;
        for (const auto& sel : selectionCopy) {
            regionNames.push_back(sel.regions());
        }
        if (regionNames.empty()) {
            regionNames.push_back({"Default"});
        }
        requirePlugin<NDHistogramManager>(role, "NDHistogramManager")
            .bookND(infosCopy, selectionCopy, suffix, regionNames);
        for (const auto& info : infos) ndKeys_[role].insert(info.name()+"_"+suffix);
        ndCounts_[role] += infos.size();
        return regionNames;
    }

    py::dict bookTypedNDHistograms(const std::vector<NDRequest>& requests,
                                  const std::vector<NDCategory>& categories,
                                  bool nominalOnly) {
        if (executed_ || running_) throw std::runtime_error("Book ND histograms before run()");
        if (requests.empty() || categories.size()!=3)
            throw std::invalid_argument("ND booking requires observables and channel/control/sample descriptors");
        const std::string role="histogramManager";
        auto registered=analyzer_.findPlugin<NDHistogramManager>(role);
        if (!registered) throw std::runtime_error(
            "nd_histograms requires NDHistogramManager explicitly registered as histogramManager; use native.AddPlugin('histogramManager','NDHistogramManager')");
        auto& manager=*registered;
        if (manager.GetHistos().size()!=ndCounts_[role] || ndKeys_[role].size()!=ndCounts_[role])
            throw std::invalid_argument("ND manager contains untracked or duplicate bookings; use a fresh typed manager or the raw native lifecycle");
        auto& sys=analyzer_.getSystematicManager();
        const auto& settings=analyzer_.getConfigurationProvider().getConfigMap();
        const auto configuredSystematics=settings.find("histogramSystematics");
        std::string enabled=configuredSystematics==settings.end() ? "true" : configuredSystematics->second;
        std::transform(enabled.begin(),enabled.end(),enabled.begin(),[](unsigned char c){return std::tolower(c);});
        const bool includesSystematics=enabled!="false" && enabled!="0" && enabled!="no";
        if (!nominalOnly && includesSystematics && ndFrozen_ && sys.getVariationLabels()!=ndRegistryLabels_)
            throw std::invalid_argument("Register all nuisance labels before the first all-systematics ND booking");
        if (!nominalOnly && includesSystematics && sys.isBranchNameMaterialized("SystematicCounter") &&
            sys.makeSystList("SystematicCounter",analyzer_.getDataFrameProvider())!=sys.getVariationLabels())
            throw std::invalid_argument("Existing ND systematic axis is frozen; register all labels before raw or typed all-systematics booking");
        auto df=analyzer_.getDF();
        auto flatName=[](const std::string& name) {
            if (name.empty() || !(name[0]=='_' || (name[0]>='a' && name[0]<='z') || (name[0]>='A' && name[0]<='Z'))) return false;
            return std::all_of(name.begin(),name.end(),[](char c) {
                return c=='_' || (c>='a' && c<='z') || (c>='A' && c<='Z') || (c>='0' && c<='9');
            });
        };
        auto requireColumn=[&](const std::string& name) {
            if (name.empty() || !df.HasColumn(name)) throw std::invalid_argument("Unknown ND input column: "+name);
        };
        std::set<std::string> keys;
        std::vector<histInfo> infos;
        for (const auto& row : requests) {
            const auto& name=std::get<0>(row);const auto key=name+"_";
            const auto bins=std::get<4>(row);const auto low=std::get<5>(row),high=std::get<6>(row);
            const auto& edges=std::get<7>(row);
            if (!flatName(name) || !keys.insert(key).second || ndKeys_[role].count(key))
                throw std::invalid_argument("Duplicate or invalid ND destination: "+name);
            for (const auto& configured : manager.getConfigHistograms())
                if (configured.name+"_"+configured.suffix==key)
                    throw std::invalid_argument("ND destination collides with histogramConfig: "+key);
            if (bins<=0 || !std::isfinite(static_cast<float>(low)) || !std::isfinite(static_cast<float>(high)) ||
                static_cast<float>(low)>=static_cast<float>(high))
                throw std::invalid_argument("ND observable requires finite increasing native float bounds and positive bins");
            if (!edges.empty()) {
                if (edges.size()!=static_cast<std::size_t>(bins)+1)
                    throw std::invalid_argument("ND variable edges must match bin count");
                for (std::size_t i=0;i<edges.size();++i)
                    if (!std::isfinite(edges[i]) || (i && edges[i]<=edges[i-1]))
                        throw std::invalid_argument("ND edges must be finite and strictly increasing");
            }
            requireColumn(std::get<1>(row));requireColumn(std::get<3>(row));
            infos.emplace_back(name.c_str(),std::get<1>(row).c_str(),std::get<2>(row).c_str(),
                std::get<3>(row).c_str(),bins,static_cast<float>(low),static_cast<float>(high),edges,std::get<8>(row));
        }
        std::vector<selectionInfo> selections;
        std::vector<std::vector<std::string>> labels;
        for (const auto& category : categories) {
            const auto& column=std::get<0>(category);const auto bins=std::get<1>(category);
            const auto low=std::get<2>(category),high=std::get<3>(category);
            const auto& names=std::get<4>(category);
            if (bins<=0 || !std::isfinite(static_cast<float>(low)) || !std::isfinite(static_cast<float>(high)) ||
                static_cast<float>(low)>=static_cast<float>(high) ||
                names.size()!=static_cast<std::size_t>(bins) || std::set<std::string>(names.begin(),names.end()).size()!=names.size())
                throw std::invalid_argument("ND categories require increasing bounds and unique labels for every bin");
            if (column!="zero__") requireColumn(column);
            selections.emplace_back(column,bins,low,high,names);labels.push_back(names);
        }
        manager.bookND(infos,selections,"",labels,nominalOnly);
        for (const auto& key : keys) ndKeys_[role].insert(key);
        ndCounts_[role] += requests.size();ndTyped_=true;
        if (!nominalOnly && includesSystematics) {ndFrozen_=true;ndRegistryLabels_=sys.getVariationLabels();}
        py::dict result;
        result["axis_labels"]=labels;
        result["native_keys"]=std::vector<std::string>(keys.begin(),keys.end());
        result["systematic_labels"]=labels.back();
        result["role"]=role;result["storage_directory"]="nd_histograms_v1";
        result["persistence"]="canonical manager saves during Analysis.run; contents not materialized here";
        result["stage"]=graphStage_;
        return result;
    }

    AnalyzerPythonWrapper& saveNDHistograms(
        const std::string& role,
        const std::vector<std::vector<histInfo>>& fullHistList,
        const std::vector<std::vector<std::string>>& allRegionNames) {
        auto fullHistListCopy = fullHistList;
        auto allRegionNamesCopy = allRegionNames;
        requirePlugin<NDHistogramManager>(role, "NDHistogramManager")
            .saveHists(fullHistListCopy, allRegionNamesCopy);
        return *this;
    }

    AnalyzerPythonWrapper& clearNDHistograms(const std::string& role) {
        if (role=="histogramManager" && ndTyped_)
            throw std::runtime_error("Cannot clear typed ND bookings; start a fresh Analysis");
        requirePlugin<NDHistogramManager>(role, "NDHistogramManager").Clear();
        ndKeys_.erase(role);ndCounts_.erase(role);
        return *this;
    }

    AnalyzerPythonWrapper& bookCount(const std::string& name) {
        if (executed_ || running_) throw std::runtime_error("Book counts before run()");
        if (actions_->counts.count(name)) throw std::invalid_argument("Duplicate count '" + name + "'");
        actions_->counts.emplace(name, analyzer_.getDF().Count());
        if (recordGraph_) {
            GraphOperation operation{"count", name, graphStage_, "variation-union", {}};
            operation.masks = getSelectionMasks();
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

    AnalyzerPythonWrapper& bookSum(const std::string& name, const std::string& column) {
        if (executed_ || running_) throw std::runtime_error("Book sums before run()");
        if (actions_->sums.count(name)) throw std::invalid_argument("Duplicate sum '" + name + "'");
        auto df = analyzer_.getDF();
        const auto mask = analyzer_.getSystematicManager().getSelectionMaskColumn("Nominal");
        if (!mask.empty()) df = df.Filter([](bool pass) { return pass; }, {mask});
        const auto type = df.GetColumnType(column);
        if (type != "double" && type != "Double_t")
            throw std::invalid_argument("Sum requires a scalar double column; define a double projection first");
        actions_->sums.emplace(name, df.Sum<double>(column));
        if (recordGraph_) {
            GraphOperation operation{"sum", name, graphStage_, "nominal-selection", {column}};
            operation.attributes["variation"] = "Nominal";
            operation.masks["Nominal"] = mask;
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

    py::dict sumResults() {
        if (!executed_) throw std::runtime_error("Call run() before requesting sum results");
        py::dict result;
        for (auto& [name, action] : actions_->sums) result[py::str(name)] = action.GetValue();
        return result;
    }

    AnalyzerPythonWrapper& bookHistogram(
        const std::string& name, const std::string& variable, int bins,
        double low, double high, const std::string& weight = "",
        const std::string& selection = "", const std::string& variation = "Nominal") {
        if (executed_ || running_) throw std::runtime_error("Book histograms before run()");
        if (actions_->histograms.count(name) || actions_->counts.count(name))
            throw std::invalid_argument("Duplicate histogram/count '" + name + "'");
        const auto& settings=analyzer_.getConfigurationProvider().getConfigMap();
        const auto persist=settings.find("persistPythonHistograms");
        if (persist!=settings.end() && persist->second=="true")validatePersistentHistogramName(name,actions_->histograms);
        if (bins <= 0 || !std::isfinite(low) || !std::isfinite(high) || low >= high)
            throw std::invalid_argument("Histogram requires positive bins and finite increasing bounds");
        auto df = analyzer_.getDF();
        auto& systematics = analyzer_.getSystematicManager();
        const auto labels = systematics.getVariationLabels();
        if (std::find(labels.begin(), labels.end(), variation) == labels.end())
            throw std::invalid_argument("Unknown histogram variation '" + variation + "'");
        const auto mask = systematics.getSelectionMaskColumn(variation);
        if (!mask.empty()) df = df.Filter([](bool pass) { return pass; }, {mask});
        if (!selection.empty())
            df = df.Filter([](bool pass) { return pass; },
                           {systematics.getVariationColumnName(selection, variation)});
        const auto value = systematics.getVariationColumnName(variable, variation);
        const auto w = weight.empty() ? weight : systematics.getVariationColumnName(weight, variation);
        const ROOT::RDF::TH1DModel model(name.c_str(), name.c_str(), bins, low, high);
        const auto valueType = df.GetColumnType(value);
        ROOT::RDF::RResultPtr<TH1D> result;
        // Avoid inferred-action Cling compilation for the common scalar types.
        if (w.empty()) {
            if (valueType == "double" || valueType == "Double_t") result = df.Histo1D<double>(model, value);
            else if (valueType == "float" || valueType == "Float_t") result = df.Histo1D<float>(model, value);
            else result = df.Histo1D(model, value);
        } else {
            const auto weightType = df.GetColumnType(w);
            const bool vDouble = valueType == "double" || valueType == "Double_t";
            const bool vFloat = valueType == "float" || valueType == "Float_t";
            const bool wDouble = weightType == "double" || weightType == "Double_t";
            const bool wFloat = weightType == "float" || weightType == "Float_t";
            if (vDouble && wDouble) result = df.Histo1D<double, double>(model, value, w);
            else if (vDouble && wFloat) result = df.Histo1D<double, float>(model, value, w);
            else if (vFloat && wDouble) result = df.Histo1D<float, double>(model, value, w);
            else if (vFloat && wFloat) result = df.Histo1D<float, float>(model, value, w);
            else result = df.Histo1D(model, value, w);
        }
        actions_->histograms.emplace(name, result);
        actions_->counts.emplace(name, df.Count());
        if (recordGraph_) {
            GraphOperation operation{"histogram", name, graphStage_, "variation-selection", {value}};
            if (!w.empty()) operation.inputs.push_back(w);
            if (!selection.empty()) operation.inputs.push_back(systematics.getVariationColumnName(selection, variation));
            operation.masks[variation] = mask;
            operation.attributes = {{"variation", variation}, {"variable", value}, {"weight", w},
                                    {"selection", selection.empty() ? "" : systematics.getVariationColumnName(selection, variation)}};
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

    AnalyzerPythonWrapper& bookHistograms(const std::vector<HistogramRequest>& requests) {
        if (executed_ || running_) throw std::runtime_error("Book histograms before run()");
        // This cache lives only within one call, pinning the exact booking node.
        // Definitions, filters, plugins or later calls can never reuse a stale slice.
        HistogramSlices slices;
        std::set<std::string> names;
        for (const auto& request : requests) {
            const auto& name = std::get<0>(request);
            if (!names.insert(name).second || actions_->histograms.count(name) || actions_->counts.count(name))
                throw std::invalid_argument("Duplicate histogram/count '" + name + "'");
            if (std::get<2>(request) <= 0 || !std::isfinite(std::get<3>(request)) ||
                !std::isfinite(std::get<4>(request)) || std::get<3>(request) >= std::get<4>(request))
                throw std::invalid_argument("Histogram requires positive bins and finite increasing bounds");
        }
        for (const auto& request : requests)
            std::apply([&](const auto&... args) { bookHistogramImpl(args..., &slices); }, request);
        return *this;
    }

    AnalyzerPythonWrapper& bookHistogramVariable(
        const std::string& name, const std::string& variable, const std::vector<double>& edges,
        const std::string& weight = "", const std::string& selection = "",
        const std::string& variation = "Nominal") {
        return bookHistogramVariableImpl(name, variable, edges, weight, selection, variation, nullptr);
    }

    AnalyzerPythonWrapper& bookHistogramsMixed(const std::vector<MixedHistogramRequest>& requests) {
        if (executed_ || running_) throw std::runtime_error("Book histograms before run()");
        // One cache for both axis kinds; exact masks/selections at this node only.
        HistogramSlices slices;
        std::set<std::string> names;
        for (const auto& request : requests) {
            const auto& name = std::get<0>(request);
            if (!names.insert(name).second || actions_->histograms.count(name) || actions_->counts.count(name))
                throw std::invalid_argument("Duplicate histogram/count '" + name + "'");
            const auto& edges = std::get<5>(request);
            if (edges.empty()) {
                if (std::get<2>(request) <= 0 || !std::isfinite(std::get<3>(request)) ||
                    !std::isfinite(std::get<4>(request)) || std::get<3>(request) >= std::get<4>(request))
                    throw std::invalid_argument("Histogram requires positive bins and finite increasing bounds");
            } else {
                validateHistogramEdges(edges);
                if (std::get<2>(request) != 0 || std::get<3>(request) != 0. || std::get<4>(request) != 0.)
                    throw std::invalid_argument("Mixed histogram request must specify exactly one axis kind");
            }
        }
        for (const auto& request : requests) {
            const auto& [name, variable, bins, low, high, edges, weight, selection, variation] = request;
            if (edges.empty()) bookHistogramImpl(name, variable, bins, low, high, weight, selection, variation, &slices);
            else bookHistogramVariableImpl(name, variable, edges, weight, selection, variation, &slices);
        }
        return *this;
    }

private:
    static void validateHistogramEdges(const std::vector<double>& edges) {
        if (edges.size() < 2 || edges.size() - 1 > static_cast<std::size_t>(std::numeric_limits<int>::max()))
            throw std::invalid_argument("Histogram requires at least two finite strictly increasing edges");
        for (std::size_t i = 0; i < edges.size(); ++i)
            if (!std::isfinite(edges[i]) || (i && edges[i - 1] >= edges[i]))
                throw std::invalid_argument("Histogram requires at least two finite strictly increasing edges");
    }

    AnalyzerPythonWrapper& bookHistogramImpl(
        const std::string& name, const std::string& variable, int bins,
        double low, double high, const std::string& weight,
        const std::string& selection, const std::string& variation, HistogramSlices* slices) {
        if (executed_ || running_) throw std::runtime_error("Book histograms before run()");
        if (actions_->histograms.count(name) || actions_->counts.count(name))
            throw std::invalid_argument("Duplicate histogram/count '" + name + "'");
        const auto& settings=analyzer_.getConfigurationProvider().getConfigMap();
        const auto persist=settings.find("persistPythonHistograms");
        if (persist!=settings.end() && persist->second=="true")validatePersistentHistogramName(name,actions_->histograms);
        if (bins <= 0 || !std::isfinite(low) || !std::isfinite(high) || low >= high)
            throw std::invalid_argument("Histogram requires positive bins and finite increasing bounds");
        auto df = analyzer_.getDF();
        auto& systematics = analyzer_.getSystematicManager();
        const auto labels = systematics.getVariationLabels();
        if (std::find(labels.begin(), labels.end(), variation) == labels.end())
            throw std::invalid_argument("Unknown histogram variation '" + variation + "'");
        const auto mask = systematics.getSelectionMaskColumn(variation);
        const auto selected = selection.empty() ? "" : systematics.getVariationColumnName(selection, variation);
        const auto key = std::make_pair(mask, selected);
        ROOT::RDF::RResultPtr<ULong64_t> sharedCount;
        const auto cached = slices ? slices->find(key) : HistogramSlices::iterator{};
        if (slices && cached != slices->end()) {
            df = cached->second.node;
            sharedCount = cached->second.count;
        } else {
            if (!mask.empty()) df = df.Filter([](bool pass) { return pass; }, {mask});
            if (!selected.empty()) df = df.Filter([](bool pass) { return pass; }, {selected});
            if (slices) {
                sharedCount = df.Count();
                slices->emplace(key, HistogramSlice{df, sharedCount});
            }
        }
        const auto value = systematics.getVariationColumnName(variable, variation);
        const auto w = weight.empty() ? weight : systematics.getVariationColumnName(weight, variation);
        const ROOT::RDF::TH1DModel model(name.c_str(), name.c_str(), bins, low, high);
        const auto valueType = df.GetColumnType(value);
        ROOT::RDF::RResultPtr<TH1D> result;
        // Avoid inferred-action Cling compilation for the common scalar types.
        if (w.empty()) {
            if (valueType == "double" || valueType == "Double_t") result = df.Histo1D<double>(model, value);
            else if (valueType == "float" || valueType == "Float_t") result = df.Histo1D<float>(model, value);
            else result = df.Histo1D(model, value);
        } else {
            const auto weightType = df.GetColumnType(w);
            const bool vDouble = valueType == "double" || valueType == "Double_t";
            const bool vFloat = valueType == "float" || valueType == "Float_t";
            const bool wDouble = weightType == "double" || weightType == "Double_t";
            const bool wFloat = weightType == "float" || weightType == "Float_t";
            if (vDouble && wDouble) result = df.Histo1D<double, double>(model, value, w);
            else if (vDouble && wFloat) result = df.Histo1D<double, float>(model, value, w);
            else if (vFloat && wDouble) result = df.Histo1D<float, double>(model, value, w);
            else if (vFloat && wFloat) result = df.Histo1D<float, float>(model, value, w);
            else result = df.Histo1D(model, value, w);
        }
        actions_->histograms.emplace(name, result);
        actions_->counts.emplace(name, slices ? sharedCount : df.Count());
        if (recordGraph_) {
            GraphOperation operation{"histogram", name, graphStage_, "variation-selection", {value}};
            if (!w.empty()) operation.inputs.push_back(w);
            if (!selection.empty()) operation.inputs.push_back(systematics.getVariationColumnName(selection, variation));
            operation.masks[variation] = mask;
            operation.attributes = {{"variation", variation}, {"variable", value}, {"weight", w},
                                    {"selection", selection.empty() ? "" : systematics.getVariationColumnName(selection, variation)}};
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

    // Separate implementation preserves the established regular booking path and
    // its lambda contexts. Both paths use the same call-local mixed-batch cache.
    AnalyzerPythonWrapper& bookHistogramVariableImpl(
        const std::string& name, const std::string& variable, const std::vector<double>& edges,
        const std::string& weight,
        const std::string& selection, const std::string& variation, HistogramSlices* slices) {
        if (executed_ || running_) throw std::runtime_error("Book histograms before run()");
        if (actions_->histograms.count(name) || actions_->counts.count(name))
            throw std::invalid_argument("Duplicate histogram/count '" + name + "'");
        const auto& settings=analyzer_.getConfigurationProvider().getConfigMap();
        const auto persist=settings.find("persistPythonHistograms");
        if (persist!=settings.end() && persist->second=="true")validatePersistentHistogramName(name,actions_->histograms);
        validateHistogramEdges(edges);
        auto df = analyzer_.getDF();
        auto& systematics = analyzer_.getSystematicManager();
        const auto labels = systematics.getVariationLabels();
        if (std::find(labels.begin(), labels.end(), variation) == labels.end())
            throw std::invalid_argument("Unknown histogram variation '" + variation + "'");
        const auto mask = systematics.getSelectionMaskColumn(variation);
        const auto selected = selection.empty() ? "" : systematics.getVariationColumnName(selection, variation);
        const auto key = std::make_pair(mask, selected);
        ROOT::RDF::RResultPtr<ULong64_t> sharedCount;
        const auto cached = slices ? slices->find(key) : HistogramSlices::iterator{};
        if (slices && cached != slices->end()) {
            df = cached->second.node;
            sharedCount = cached->second.count;
        } else {
            if (!mask.empty()) df = df.Filter([](bool pass) { return pass; }, {mask});
            if (!selected.empty()) df = df.Filter([](bool pass) { return pass; }, {selected});
            if (slices) {
                sharedCount = df.Count();
                slices->emplace(key, HistogramSlice{df, sharedCount});
            }
        }
        const auto value = systematics.getVariationColumnName(variable, variation);
        const auto w = weight.empty() ? weight : systematics.getVariationColumnName(weight, variation);
        const ROOT::RDF::TH1DModel model(name.c_str(), name.c_str(), static_cast<int>(edges.size() - 1), edges.data());
        const auto valueType = df.GetColumnType(value);
        ROOT::RDF::RResultPtr<TH1D> result;
        // Avoid inferred-action Cling compilation for the common scalar types.
        if (w.empty()) {
            if (valueType == "double" || valueType == "Double_t") result = df.Histo1D<double>(model, value);
            else if (valueType == "float" || valueType == "Float_t") result = df.Histo1D<float>(model, value);
            else result = df.Histo1D(model, value);
        } else {
            const auto weightType = df.GetColumnType(w);
            const bool vDouble = valueType == "double" || valueType == "Double_t";
            const bool vFloat = valueType == "float" || valueType == "Float_t";
            const bool wDouble = weightType == "double" || weightType == "Double_t";
            const bool wFloat = weightType == "float" || weightType == "Float_t";
            if (vDouble && wDouble) result = df.Histo1D<double, double>(model, value, w);
            else if (vDouble && wFloat) result = df.Histo1D<double, float>(model, value, w);
            else if (vFloat && wDouble) result = df.Histo1D<float, double>(model, value, w);
            else if (vFloat && wFloat) result = df.Histo1D<float, float>(model, value, w);
            else result = df.Histo1D(model, value, w);
        }
        actions_->histograms.emplace(name, result);
        actions_->counts.emplace(name, slices ? sharedCount : df.Count());
        if (recordGraph_) {
            GraphOperation operation{"histogram", name, graphStage_, "variation-selection", {value}};
            if (!w.empty()) operation.inputs.push_back(w);
            if (!selection.empty()) operation.inputs.push_back(systematics.getVariationColumnName(selection, variation));
            operation.masks[variation] = mask;
            operation.attributes = {{"variation", variation}, {"variable", value}, {"weight", w},
                                    {"selection", selection.empty() ? "" : systematics.getVariationColumnName(selection, variation)}};
            operations_.push_back(std::move(operation));
        }
        return *this;
    }

public:
    py::dict histogramResults() {
        if (!executed_) throw std::runtime_error("Call run() before requesting histogram results");
        py::dict result;
        for (auto& [name, action] : actions_->histograms) {
            const auto& histogram = action.GetValue();
            std::vector<double> sumw, sumw2, edges;
            for (int bin = 0; bin <= histogram.GetNbinsX() + 1; ++bin) {
                sumw.push_back(histogram.GetBinContent(bin));
                const auto error = histogram.GetBinError(bin);
                sumw2.push_back(error * error);
            }
            for (int bin = 1; bin <= histogram.GetNbinsX() + 1; ++bin)
                edges.push_back(histogram.GetXaxis()->GetBinLowEdge(bin));
            py::dict item;
            item["sumw"] = sumw;
            item["sumw2"] = sumw2;
            item["edges"] = edges;
            result[py::str(name)] = item;
        }
        return result;
    }

    py::dict countResults() {
        if (!executed_) throw std::runtime_error("Call run() before requesting count results");
        py::dict result;
        for (auto& [name, action] : actions_->counts) result[py::str(name)] = action.GetValue();
        return result;
    }

    py::dict threadingSnapshot() {
        if (running_)
            throw std::runtime_error("Inspect threading before or after run(), not while it is running");
        py::dict result;
        result["implicit_mt_enabled"] = ROOT::IsImplicitMTEnabled();
        result["pool_size"] = ROOT::GetThreadPoolSize();
        result["dataframe_slots"] = analyzer_.getDF().GetNSlots();
        return result;
    }

    unsigned int eventLoops() { return analyzer_.getDF().GetNRuns(); }
    
    /**
     * @brief Save the analysis results
     *
     * This calls analyzer_.run() which is the single entry point
     * for triggering computation and saving outputs.
     */
    AnalyzerPythonWrapper& save() {
        if (ndTyped_ && requirePlugin<NDHistogramManager>("histogramManager","NDHistogramManager").GetHistos().size()!=ndCounts_["histogramManager"])
            throw std::runtime_error("Typed ND manager changed outside recorded booking; start a fresh Analysis");
        if(running_.exchange(true))throw std::runtime_error("Analyzer is already running");
        if(executed_){running_=false;throw std::runtime_error("Analyzer has already run; start a fresh Analysis");}
        try {
            // Publish the running guard while still holding the GIL, so a
            // concurrent description cannot inspect a mutating native graph.
            {
                py::gil_scoped_release release;
                analyzer_.run();
            }
            executed_=true;running_=false;
        }catch(...){
            executed_=true;running_=false;throw;
        }
        return *this;
    }
    
    /**
     * @brief Get configuration value by key
     */
    std::string configMap(const std::string& key) {
        return analyzer_.configMap(key);
    }
    
    /**
     * @brief Access the underlying Analyzer object
     */
    Analyzer& getAnalyzer() {
        return analyzer_;
    }

private:
    // The Python facade requests fresh explicit outputs. Check their complete
    // native expansion before any pack or systematic registration is created.
    // Default-naming raw calls and already-existing nominal outputs retain the
    // manager's established idempotent behavior.
    void validateExplicitCorrectionOutputs(
        CorrectionManager& manager, const std::string& correction,
        const std::vector<std::vector<std::string>>& arguments,
        const std::vector<std::string>& inputs,
        const std::vector<std::string>& outputs,
        const std::vector<bool>& propagate) {
        if ((!outputs.empty() && outputs.size() != arguments.size()) ||
            (!propagate.empty() && propagate.size() != arguments.size()))
            throw std::invalid_argument("Correction batch output/propagation count mismatch");
        if (outputs.empty() || std::any_of(outputs.begin(), outputs.end(),
                                          [](const auto& value) { return value.empty(); })) return;
        auto node = analyzer_.getDataFrameProvider().getDataFrame();
        auto& systematics = analyzer_.getSystematicManager();
        const auto& resolved = inputs.empty() ? manager.getCorrectionFeatures(correction) : inputs;
        const auto paired = systematics.getSystematics();
        const auto explicitLabels = systematics.getExplicitVariationLabels();
        using Planned = std::pair<std::size_t, std::vector<std::string>>;
        std::map<std::string, Planned> destinations;
        const auto mappedInputs = [&](const std::string& label) {
            auto result = resolved;
            for (auto& input : result) input = systematics.getVariationColumnName(input, label);
            return result;
        };
        const auto add = [&](const std::string& output, std::size_t variant,
                             const std::vector<std::string>& columns) {
            if (node.HasColumn(output))
                throw std::invalid_argument("Correction output already exists: " + output);
            const auto found = destinations.find(output);
            if (found != destinations.end()) {
                if (found->second.first == variant && found->second.second == columns) return;
                throw std::invalid_argument("Correction batch destination collision: " + output);
            }
            destinations.emplace(output, Planned{variant, columns});
        };
        for (std::size_t i = 0; i < outputs.size(); ++i) {
            const auto& output = outputs[i];
            if (node.HasColumn(output)) continue; // raw partial/idempotent repeat
            add(output, i, resolved);
            if (!propagate.empty() && !propagate[i]) continue;
            for (const auto& family : paired) {
                const auto up = mappedInputs(family + "Up");
                const auto down = mappedInputs(family + "Down");
                if (up == resolved && down == resolved) continue;
                add(output + "_" + family + "Up", i, up);
                add(output + "_" + family + "Down", i, down);
            }
            for (const auto& label : explicitLabels) {
                const auto columns = mappedInputs(label);
                if (columns == resolved) continue;
                const auto mapped = systematics.getVariationColumnName(output, label);
                const auto registered = systematics.getExplicitVariationLabelsForVariable(output);
                if (mapped == output && std::find(registered.begin(), registered.end(), label) != registered.end())
                    throw std::invalid_argument("Correction batch destination aliases nominal output: " + output);
                add(mapped == output ? output + "_" + label : mapped, i, columns);
            }
        }
    }

    struct GraphOperation {
        std::string kind, name;
        std::size_t stage;
        std::string policy;
        std::vector<std::string> inputs;
        std::vector<std::pair<std::string, std::vector<std::string>>> materializations;
        std::unordered_map<std::string, std::string> variations, masks, attributes;
    };

    void recordDefinition(GraphOperation operation, bool propagate, bool reconstruct = false) {
        auto& systematics = analyzer_.getSystematicManager();
        for (const auto& label : systematics.getVariationLabels()) {
            operation.variations[label] = !propagate || label == "Nominal"
                ? operation.name : systematics.getVariationColumnName(operation.name, label);
        }
        // Typed projection delegates to Analyzer::Define; expose its registered
        // materializations using the same manager, without generating new work.
        if (reconstruct) {
            std::unordered_set<std::string> seen;
            for (const auto& label : systematics.getVariationLabels()) {
                const auto& output = operation.variations.at(label);
                if (!seen.insert(output).second) continue;
                auto inputs = operation.inputs;
                if (propagate && label != "Nominal")
                    for (auto& input : inputs) input = systematics.getVariationColumnName(input, label);
                operation.materializations.push_back({output, inputs});
            }
        }
        operations_.push_back(std::move(operation));
    }

    template <typename T>
    T& requirePlugin(const std::string& role, const std::string& typeName) {
        auto plugin = analyzer_.getPlugin<T>(role);
        if (plugin == nullptr) {
            throw std::runtime_error(
                "Plugin with role '" + role + "' not found or not of type '" + typeName +
                "'. Add it first with AddPlugin(role, pluginType)."
            );
        }
        return *plugin;
    }

    static std::string normalizePluginType(const std::string& pluginType) {
        std::string normalized;
        normalized.reserve(pluginType.size());
        for (char c : pluginType) {
            normalized.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
        if (normalized == "onnxmanager" || normalized == "onnx") return "onnx";
        if (normalized == "bdtmanager" || normalized == "bdt") return "bdt";
        if (normalized == "correctionmanager" || normalized == "correction") return "correction";
        if (normalized == "triggermanager" || normalized == "trigger") return "trigger";
        if (normalized == "sofiemanager" || normalized == "sofie") return "sofie";
        if (normalized == "ndhistogrammanager" || normalized == "ndhistogram" || normalized == "hist") return "ndhistogram";
        if (normalized == "weightmanager" || normalized == "weights") return "weights";
        if (normalized == "regionmanager" || normalized == "regions") return "regions";
        return normalized;
    }

    Analyzer analyzer_;
    std::atomic<bool> executed_{false};
    std::atomic<bool> running_{false};
    std::size_t graphStage_ = 0;
    bool recordGraph_;
    std::vector<GraphOperation> operations_;
    std::unordered_set<std::string> configuredExtensions_;
    std::map<std::string,std::set<std::string>> ndKeys_;
    std::map<std::string,std::size_t> ndCounts_;
    bool ndTyped_=false,ndFrozen_=false;
    std::vector<std::string> ndRegistryLabels_;
    std::shared_ptr<PythonActions> actions_ = std::make_shared<PythonActions>();
};

PYBIND11_MODULE(rdfanalyzer, m) {
    py::list enabledExtensions;
    for (const auto &extension : rdf::extensionBuildInfo) {
        if (extension.enabled) enabledExtensions.append(extension.name);
        if (extension.capability[0] != '\0')
            m.attr(extension.capability) = extension.enabled;
    }
    m.attr("enabled_extensions") = enabledExtensions;
    m.def("has_extension", [](const std::string &name) {
        for (const auto &extension : rdf::extensionBuildInfo)
            if (name == extension.name) return extension.enabled;
        return false;
    });
    m.def("__getattr__", [](const std::string &attribute) -> py::object {
        if (attribute.compare(0, 4, "has_") == 0) {
            const auto name = attribute.substr(4);
            for (const auto &extension : rdf::extensionBuildInfo)
                if (name == extension.name) return py::bool_(extension.enabled);
            return py::bool_(false);
        }
        throw py::attribute_error("module rdfanalyzer has no attribute " + attribute);
    });
    m.doc() = "Python bindings for RDFAnalyzerCore framework";
    m.attr("python_authoring_api_version")=4;
    bindHistogramIO(m);

    py::class_<histInfo>(m, "HistInfo",
        "Histogram booking descriptor equivalent to C++ histInfo")
        .def(py::init([](const std::string& name,
                         const std::string& variable,
                         const std::string& label,
                         const std::string& weight,
                         int bins,
                         float lowerBound,
                         float upperBound) {
                return histInfo(name.c_str(),
                                variable.c_str(),
                                label.c_str(),
                                weight.c_str(),
                                bins,
                                lowerBound,
                                upperBound);
            }),
            py::arg("name"),
            py::arg("variable"),
            py::arg("label"),
            py::arg("weight"),
            py::arg("bins"),
            py::arg("lowerBound"),
            py::arg("upperBound"))
        .def_property_readonly("name", &histInfo::name)
        .def_property_readonly("variable", &histInfo::variable)
        .def_property_readonly("label", &histInfo::label)
        .def_property_readonly("weight", &histInfo::weight)
        .def_property_readonly("bins", &histInfo::bins)
        .def_property_readonly("lowerBound", &histInfo::lowerBound)
        .def_property_readonly("upperBound", &histInfo::upperBound);

    py::class_<selectionInfo>(m, "SelectionInfo",
        "Selection descriptor equivalent to C++ selectionInfo")
        .def(py::init<>())
        .def(py::init<std::string, int, double, double, std::vector<std::string>>(),
             py::arg("variable"),
             py::arg("bins"),
             py::arg("lowerBound"),
             py::arg("upperBound"),
             py::arg("regions"))
        .def(py::init<std::string, int, double, double>(),
             py::arg("variable"),
             py::arg("bins"),
             py::arg("lowerBound"),
             py::arg("upperBound"))
        .def_property_readonly("variable", &selectionInfo::variable)
        .def_property_readonly("bins", &selectionInfo::bins)
        .def_property_readonly("lowerBound", &selectionInfo::lowerBound)
        .def_property_readonly("upperBound", &selectionInfo::upperBound)
        .def_property_readonly("regions", &selectionInfo::regions);
    
    // Bind the AnalyzerPythonWrapper class
    py::class_<AnalyzerPythonWrapper>(m, "Analyzer", 
        R"pbdoc(
        Main analysis class for event processing using ROOT's RDataFrame.
        
        This class provides Python access to the RDFAnalyzerCore framework,
        allowing for both string-based (ROOT JIT) and function pointer-based
        (numba-compatible) variable definitions and filters.
        
        Examples
        --------
        Basic usage with string expressions:
        
        >>> analyzer = rdfanalyzer.Analyzer("config.txt")
        >>> analyzer.DefineJIT("pt_gev", "pt / 1000.0", ["pt"])
        >>> analyzer.FilterJIT("high_pt", "pt_gev > 25.0", ["pt_gev"])
        >>> analyzer.save()
        
        Using numba-compiled functions:
        
        >>> import numba
        >>> import ctypes
        >>> 
        >>> @numba.cfunc("float64(float64)")
        >>> def convert_to_gev(pt):
        ...     return pt / 1000.0
        >>> 
        >>> func_ptr = ctypes.cast(convert_to_gev.address, ctypes.c_void_p).value
        >>> analyzer.DefineFromPointer("pt_gev", func_ptr, "double(double)", ["pt"])
        
        Using numpy arrays:
        
        >>> import numpy as np
        >>> data = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        >>> analyzer.DefineFromVector("my_vector", 
        ...                           data.ctypes.data, 
        ...                           len(data), 
        ...                           "float")
        )pbdoc")
        .def(py::init<const std::string&, bool>(),
             py::arg("configFile"), py::arg("record_graph") = false,
             "Construct an Analyzer from a configuration file")
           .def("Define", &AnalyzerPythonWrapper::Define,
               py::arg("name"),
               py::arg("expression"),
               py::arg("columns") = std::vector<std::string>(),
               R"pbdoc(
               Define a new variable using a C++ expression string.

               This method mirrors the C++ Analyzer::Define naming in Python.
               )pbdoc",
               py::return_value_policy::reference_internal)
        .def("DefineJIT", &AnalyzerPythonWrapper::DefineJIT,
             py::arg("name"), 
             py::arg("expression"),
             py::arg("columns") = std::vector<std::string>(),
             R"pbdoc(
             Define a new variable using a C++ expression string.
             
             The expression is compiled by ROOT's JIT compiler and can use
             any C++ syntax that ROOT supports.
             
             Parameters
             ----------
             name : str
                 Name of the new variable
             expression : str
                 C++ expression to compute the variable
             columns : list of str, optional
                 Input column names used in the expression
             
             Returns
             -------
             Analyzer
                 Self reference for method chaining
             
             Examples
             --------
             >>> analyzer.DefineJIT("pt_gev", "pt / 1000.0", ["pt"])
             >>> analyzer.DefineJIT("delta_r", "sqrt(delta_eta*delta_eta + delta_phi*delta_phi)", 
             ...                    ["delta_eta", "delta_phi"])
             )pbdoc",
             py::return_value_policy::reference_internal)
           .def("Filter", &AnalyzerPythonWrapper::Filter,
               py::arg("name"),
               py::arg("expression"),
               py::arg("columns") = std::vector<std::string>(),
               R"pbdoc(
               Apply a filter using a C++ expression string.

               This method mirrors the C++ Analyzer::Filter naming in Python.
               )pbdoc",
               py::return_value_policy::reference_internal)
        .def("FilterColumn", &AnalyzerPythonWrapper::FilterColumn,
             py::arg("name"), py::arg("column"),
             "Filter an existing scalar bool column with native variation masks.",
             py::return_value_policy::reference_internal)
        .def("FilterJIT", &AnalyzerPythonWrapper::FilterJIT,
             py::arg("name"),
             py::arg("expression"),
             py::arg("columns") = std::vector<std::string>(),
             R"pbdoc(
             Apply a filter using a C++ expression string.
             
             The expression is compiled by ROOT's JIT compiler and must
             return a boolean value.
             
             Parameters
             ----------
             name : str
                 Name of the filter
             expression : str
                 C++ expression returning bool
             columns : list of str, optional
                 Input column names used in the expression
             
             Returns
             -------
             Analyzer
                 Self reference for method chaining
             
             Examples
             --------
             >>> analyzer.FilterJIT("high_pt", "pt_gev > 25.0", ["pt_gev"])
             >>> analyzer.FilterJIT("quality", "quality_flag == 1", ["quality_flag"])
             )pbdoc",
             py::return_value_policy::reference_internal)
        .def("DefineFromPointer", &AnalyzerPythonWrapper::DefineFromPointer,
             py::arg("name"),
             py::arg("func_ptr"),
             py::arg("signature"),
             py::arg("columns") = std::vector<std::string>(),
             R"pbdoc(
             Define a variable using a function pointer (numba-compatible).
             
             This method allows you to use numba-compiled functions by passing
             their address. The function pointer is obtained using ctypes.
             
             Parameters
             ----------
             name : str
                 Name of the new variable
             func_ptr : int
                 Function pointer address (as integer)
             signature : str
                 C++ function signature (e.g., "float(float, float)")
             columns : list of str, optional
                 Input column names to pass to the function
             
             Returns
             -------
             Analyzer
                 Self reference for method chaining
             
             Examples
             --------
             >>> import numba
             >>> import ctypes
             >>> 
             >>> @numba.cfunc("float64(float64, float64)")
             >>> def delta_r(eta1, eta2):
             ...     return abs(eta1 - eta2)
             >>> 
             >>> func_ptr = ctypes.cast(delta_r.address, ctypes.c_void_p).value
             >>> analyzer.DefineFromPointer("deta", func_ptr, 
             ...                            "double(double, double)", 
             ...                            ["eta1", "eta2"])
             )pbdoc",
             py::return_value_policy::reference_internal)
        .def("DefineFromVector", &AnalyzerPythonWrapper::DefineFromVector,
             py::arg("name"),
             py::arg("data_ptr"),
             py::arg("size"),
             py::arg("element_type") = "float",
             R"pbdoc(
             Define a variable from a vector using pointer + size.
             
             This method creates an RVec from a raw pointer and size,
             useful for passing numpy arrays or other array data.
             
             Parameters
             ----------
             name : str
                 Name of the new variable
             data_ptr : int
                 Pointer to data array (as integer, e.g., numpy array.ctypes.data)
             size : int
                 Number of elements in the array
             element_type : str, optional
                 Type of array elements (default: "float")
             
             Returns
             -------
             Analyzer
                 Self reference for method chaining
             
             Examples
             --------
             >>> import numpy as np
             >>> weights = np.array([1.0, 2.0, 3.0], dtype=np.float32)
             >>> analyzer.DefineFromVector("event_weights", 
             ...                           weights.ctypes.data, 
             ...                           len(weights), 
             ...                           "float")
             )pbdoc",
             py::return_value_policy::reference_internal)
           .def("DefineVector", &AnalyzerPythonWrapper::DefineVector,
               py::arg("name"),
               py::arg("data_ptr"),
               py::arg("size"),
               py::arg("element_type") = "float",
               R"pbdoc(
               Define a vector-like variable from raw pointer + size.

               This method mirrors the C++ Analyzer::DefineVector naming in Python.
               )pbdoc",
               py::return_value_policy::reference_internal)
        .def("save", &AnalyzerPythonWrapper::save,
               "Trigger computation and save the analysis results",
               py::return_value_policy::reference_internal)
        .def("define_nominal", &AnalyzerPythonWrapper::DefineNominal,
             py::arg("name"), py::arg("expression"),
             py::arg("columns") = std::vector<std::string>(),
             "Define an explicitly nominal-only expression.",
             py::return_value_policy::reference_internal)
        .def("register_variation", &AnalyzerPythonWrapper::registerVariationColumn,
             py::arg("variable"), py::arg("label"), py::arg("column"),
             "Register an existing column for an exact non-paired label.",
             py::return_value_policy::reference_internal)
        .def("register_variation_pair", &AnalyzerPythonWrapper::registerVariationColumns,
             py::arg("variable"), py::arg("family"), py::arg("up"), py::arg("down"),
             "Register existing up/down columns with arbitrary names.",
             py::return_value_policy::reference_internal)
        .def_property_readonly("variation_labels", &AnalyzerPythonWrapper::getVariationLabels)
        .def_property_readonly("selection_masks", &AnalyzerPythonWrapper::getSelectionMasks)
        .def_property_readonly("columns", &AnalyzerPythonWrapper::GetColumnNames)
        .def("_graph_description", &AnalyzerPythonWrapper::graphDescription,
             "Describe tracked authoring operations without executing RDF work.")
        .def("book_count", &AnalyzerPythonWrapper::bookCount, py::arg("name"),
             py::return_value_policy::reference_internal)
        .def("book_sum", &AnalyzerPythonWrapper::bookSum, py::arg("name"), py::arg("column"),
             py::return_value_policy::reference_internal)
        .def_property_readonly("sums", &AnalyzerPythonWrapper::sumResults)
        .def("book_histogram", &AnalyzerPythonWrapper::bookHistogram,
             py::arg("name"), py::arg("variable"), py::arg("bins"), py::arg("low"), py::arg("high"),
             py::arg("weight") = "", py::arg("selection") = "", py::arg("variation") = "Nominal",
             "Book a lazy TH1D slice using the variation's acceptance mask.",
             py::return_value_policy::reference_internal)
        .def("book_histograms", &AnalyzerPythonWrapper::bookHistograms, py::arg("requests"),
             "Explicit batch at one node; share identical resolved selections and retain all named counts.",
             py::return_value_policy::reference_internal)
        .def("book_histogram_variable", &AnalyzerPythonWrapper::bookHistogramVariable,
             py::arg("name"), py::arg("variable"), py::arg("edges"),
             py::arg("weight") = "", py::arg("selection") = "", py::arg("variation") = "Nominal",
             "Book a lazy TH1D slice with finite strictly increasing variable bin edges.",
             py::return_value_policy::reference_internal)
        .def("book_histograms_mixed", &AnalyzerPythonWrapper::bookHistogramsMixed, py::arg("requests"),
             "Explicit batch of (name, variable, bins, low, high, edges, weight, selection, variation); variable axes use zero bins/bounds.",
             py::return_value_policy::reference_internal)
        .def_property_readonly("histograms", &AnalyzerPythonWrapper::histogramResults)
        .def_property_readonly("counts", &AnalyzerPythonWrapper::countResults)
        .def("_threading_snapshot", &AnalyzerPythonWrapper::threadingSnapshot)
        .def_property_readonly("event_loops", &AnalyzerPythonWrapper::eventLoops)
        .def("column_type", &AnalyzerPythonWrapper::GetColumnType, py::arg("column"))
        .def("_has_column", &AnalyzerPythonWrapper::HasColumn, py::arg("column"),
             "Check one column without exporting the entire column inventory.")
        .def("project", &AnalyzerPythonWrapper::project, py::arg("name"), py::arg("source"),
             py::arg("index"), py::arg("nominal") = false,
             py::return_value_policy::reference_internal)
        .def("_define_compiled", &AnalyzerPythonWrapper::DefineCompiled,
             py::arg("name"), py::arg("function"), py::arg("columns"), py::arg("nominal") = false,
             py::return_value_policy::reference_internal)
        .def("configMap", &AnalyzerPythonWrapper::configMap,
             py::arg("key"),
               "Get a configuration value by key")
           .def("configure_extension", &AnalyzerPythonWrapper::configureExtension,
                py::arg("extension"), py::arg("settings"),
                "Configure an enabled extension's native workflow once, before run")
           .def("_configure_weights", &AnalyzerPythonWrapper::configureWeights,
                py::arg("output"),py::arg("components"),py::arg("normalizations"),py::arg("variations"))
           .def("_declare_region", &AnalyzerPythonWrapper::declareRegion,
                py::arg("name"),py::arg("column"),py::arg("parent")="")
           .def("setConfig", &AnalyzerPythonWrapper::setConfig,
               py::arg("key"),
               py::arg("value"),
               "Set a configuration value by key",
               py::return_value_policy::reference_internal)
           .def("getConfigMap", &AnalyzerPythonWrapper::getConfigMap,
               "Get all configuration key/value pairs")
           .def("getConfigList", &AnalyzerPythonWrapper::getConfigList,
               py::arg("key"),
               py::arg("defaultValue") = std::vector<std::string>(),
               py::arg("delimiter") = ",",
               "Get a list-valued configuration entry")
           .def("parsePairBasedConfig", &AnalyzerPythonWrapper::parsePairBasedConfig,
               py::arg("configFile"),
               "Parse a pair-based config file")
           .def("parseVectorConfig", &AnalyzerPythonWrapper::parseVectorConfig,
               py::arg("configFile"),
               "Parse a vector config file")
           .def("parseMultiKeyConfig", &AnalyzerPythonWrapper::parseMultiKeyConfig,
               py::arg("configFile"),
               py::arg("requiredEntryKeys"),
               "Parse a multi-key config file")
           .def("registerSystematic", &AnalyzerPythonWrapper::registerSystematic,
               py::arg("syst"),
               py::arg("affectedVariables"),
               "Register a systematic and affected variables",
               py::return_value_policy::reference_internal)
           .def("getSystematics", &AnalyzerPythonWrapper::getSystematics,
               "Get all registered systematic names")
           .def("getVariablesForSystematic", &AnalyzerPythonWrapper::getVariablesForSystematic,
               py::arg("syst"),
               "Get variables affected by a systematic")
           .def("getSystematicsForVariable", &AnalyzerPythonWrapper::getSystematicsForVariable,
               py::arg("var"),
               "Get systematics affecting a variable")
           .def("registerExistingSystematics", &AnalyzerPythonWrapper::registerExistingSystematics,
               py::arg("systConfig"),
               py::arg("columnList"),
               "Register existing systematics from config",
               py::return_value_policy::reference_internal)
           .def("autoRegisterSystematics", &AnalyzerPythonWrapper::autoRegisterSystematics,
               py::arg("columnList"),
               "Automatically discover and register systematic variations from column names. "
               "Returns a dict with keys 'registered' (list of 'baseVar:systName' strings), "
               "'missing_down', and 'missing_up'.")
           .def("makeSystList", &AnalyzerPythonWrapper::makeSystList,
               py::arg("branchName"),
               "Compute systematic variation list for a branch")
           .def("GetColumnNames", &AnalyzerPythonWrapper::GetColumnNames,
               "Get current dataframe column names")
           .def("AddPlugin", &AnalyzerPythonWrapper::AddPlugin,
               py::arg("role"),
               py::arg("pluginType"),
               "Add a plugin manager by role and type",
               py::return_value_policy::reference_internal)
           .def("AddDefaultPlugins", &AnalyzerPythonWrapper::AddDefaultPlugins,
               "Add all standard plugin managers with default roles",
               py::return_value_policy::reference_internal)
           .def("SetupPlugin", &AnalyzerPythonWrapper::SetupPlugin,
               py::arg("role"),
               "Re-run setupFromConfigFile for a plugin role",
               py::return_value_policy::reference_internal)
           .def("applyAllOnnxModels", &AnalyzerPythonWrapper::applyAllOnnxModels,
               py::arg("role") = "onnx",
               py::arg("outputSuffix") = "",
               py::return_value_policy::reference_internal)
           .def("applyOnnxModel", &AnalyzerPythonWrapper::applyOnnxModel,
               py::arg("role"),
               py::arg("modelName"),
               py::arg("outputSuffix") = "",
               py::return_value_policy::reference_internal)
           .def("getOnnxModelNames", &AnalyzerPythonWrapper::getOnnxModelNames,
               py::arg("role") = "onnx")
           .def("getOnnxModelFeatures", &AnalyzerPythonWrapper::getOnnxModelFeatures,
               py::arg("role"),
               py::arg("modelName"))
           .def("applyAllBDTs", &AnalyzerPythonWrapper::applyAllBDTs,
               py::arg("role") = "bdt",
               py::return_value_policy::reference_internal)
           .def("applyBDT", &AnalyzerPythonWrapper::applyBDT,
               py::arg("role"),
               py::arg("bdtName"),
               py::return_value_policy::reference_internal)
           .def("getBDTNames", &AnalyzerPythonWrapper::getBDTNames,
               py::arg("role") = "bdt")
           .def("getBDTFeatures", &AnalyzerPythonWrapper::getBDTFeatures,
               py::arg("role"),
               py::arg("bdtName"))
           .def("applyCorrection", &AnalyzerPythonWrapper::applyCorrection,
               py::arg("role"),
               py::arg("correctionName"),
               py::arg("stringArguments"),
               py::arg("inputColumns") = std::vector<std::string>{},
               py::arg("outputBranch") = "",
               py::return_value_policy::reference_internal)
           .def("applyCorrectionVec", &AnalyzerPythonWrapper::applyCorrectionVec,
               py::arg("role"),
               py::arg("correctionName"),
               py::arg("stringArguments"),
               py::arg("inputColumns") = std::vector<std::string>{},
               py::arg("outputBranch") = "",
               py::return_value_policy::reference_internal)
           .def("applyCorrections", &AnalyzerPythonWrapper::applyCorrections,
               py::arg("role"), py::arg("correctionName"), py::arg("argumentSets"),
               py::arg("inputColumns") = std::vector<std::string>{},
               py::arg("outputBranches") = std::vector<std::string>{},
               py::arg("propagateSystematics") = std::vector<bool>{},
               py::return_value_policy::reference_internal)
           .def("applyCorrectionsVec", &AnalyzerPythonWrapper::applyCorrectionsVec,
               py::arg("role"), py::arg("correctionName"), py::arg("argumentSets"),
               py::arg("inputColumns") = std::vector<std::string>{},
               py::arg("outputBranches") = std::vector<std::string>{},
               py::arg("propagateSystematics") = std::vector<bool>{},
               py::return_value_policy::reference_internal)
           .def("registerCorrection", &AnalyzerPythonWrapper::registerCorrection,
               py::arg("role"),
               py::arg("name"),
               py::arg("file"),
               py::arg("correctionlibName"),
               py::arg("inputVariables"),
               py::return_value_policy::reference_internal)
           .def("getCorrectionFeatures", &AnalyzerPythonWrapper::getCorrectionFeatures,
               py::arg("role"),
               py::arg("correctionName"))
           .def("applyAllTriggers", &AnalyzerPythonWrapper::applyAllTriggers,
               py::arg("role") = "trigger",
               py::return_value_policy::reference_internal)
           .def("getTriggerGroups", &AnalyzerPythonWrapper::getTriggerGroups,
               py::arg("role") = "trigger")
           .def("getTriggers", &AnalyzerPythonWrapper::getTriggers,
               py::arg("role"),
               py::arg("group"))
           .def("getVetoes", &AnalyzerPythonWrapper::getVetoes,
               py::arg("role"),
               py::arg("group"))
           .def("getTriggerGroupForSample", &AnalyzerPythonWrapper::getTriggerGroupForSample,
               py::arg("role"),
               py::arg("sample"))
           .def("applyAllSofieModels", &AnalyzerPythonWrapper::applyAllSofieModels,
               py::arg("role") = "sofie",
               py::return_value_policy::reference_internal)
           .def("applySofieModel", &AnalyzerPythonWrapper::applySofieModel,
               py::arg("role"),
               py::arg("modelName"),
               py::return_value_policy::reference_internal)
           .def("getSofieModelNames", &AnalyzerPythonWrapper::getSofieModelNames,
               py::arg("role") = "sofie")
           .def("bookNDHistograms", &AnalyzerPythonWrapper::bookNDHistograms,
               py::arg("role"),
               py::arg("infos"),
               py::arg("selection"),
               py::arg("suffix") = "")
           .def("_book_nd_histograms", &AnalyzerPythonWrapper::bookTypedNDHistograms,
               py::arg("requests"),py::arg("categories"),py::arg("nominal_only"))
           .def("_nd_collection_layouts", [](const AnalyzerPythonWrapper&) {
               return std::vector<std::string>{"fixed", "per-slot"};
           })
           .def("saveNDHistograms", &AnalyzerPythonWrapper::saveNDHistograms,
               py::arg("role"),
               py::arg("fullHistList"),
               py::arg("allRegionNames"),
               py::return_value_policy::reference_internal)
           .def("clearNDHistograms", &AnalyzerPythonWrapper::clearNDHistograms,
               py::arg("role"),
               py::return_value_policy::reference_internal);
    
    // Version info
    m.attr("__version__") = "1.0.0";

    // Plotting calculations and rendering run entirely in C++.
    py::enum_<PlotKind>(m, "PlotKind")
        .value("Stack", PlotKind::Stack)
        .value("Overlay", PlotKind::Overlay)
        .value("Heatmap", PlotKind::Heatmap);
    py::enum_<PlotPanel>(m, "PlotPanel")
        .value("Ratio", PlotPanel::Ratio)
        .value("Pull", PlotPanel::Pull)
        .value("SignalBackground", PlotPanel::SignalBackground)
        .value("Significance", PlotPanel::Significance);
    // Python spelling aliases preserve the existing entry points.
    auto analyzerType = m.attr("Analyzer");
    analyzerType.attr("define") = analyzerType.attr("Define");
    analyzerType.attr("filter") = analyzerType.attr("Filter");
    analyzerType.attr("filter_column") = analyzerType.attr("FilterColumn");
    analyzerType.attr("run") = analyzerType.attr("save");

    py::enum_<PlotExperiment>(m, "PlotExperiment")
        .value("CMS", PlotExperiment::CMS)
        .value("ATLAS", PlotExperiment::ATLAS)
        .value("LHCb", PlotExperiment::LHCb)
        .value("ALICE", PlotExperiment::ALICE)
        .value("Custom", PlotExperiment::Custom);
    py::enum_<PlotFlow>(m, "PlotFlow")
        .value("Drop", PlotFlow::Drop)
        .value("Fold", PlotFlow::Fold);
    py::class_<PlotVariation>(m, "PlotVariation")
        .def(py::init<>())
        .def_readwrite("name", &PlotVariation::name)
        .def_readwrite("up", &PlotVariation::up)
        .def_readwrite("down", &PlotVariation::down);

    py::class_<PlotPanelConfig>(m, "PlotPanelConfig")
        .def(py::init<>())
        .def_readwrite("type", &PlotPanelConfig::type)
        .def_readwrite("minimum", &PlotPanelConfig::minimum)
        .def_readwrite("maximum", &PlotPanelConfig::maximum)
        .def_readwrite("autoRange", &PlotPanelConfig::autoRange);

    py::class_<PlotStyle>(m, "PlotStyle")
        .def(py::init<>())
        .def_readwrite("width", &PlotStyle::width)
        .def_readwrite("height", &PlotStyle::height)
        .def_readwrite("labelSize", &PlotStyle::labelSize)
        .def_readwrite("titleSize", &PlotStyle::titleSize)
        .def_readwrite("legendSize", &PlotStyle::legendSize)
        .def_readwrite("leftMargin", &PlotStyle::leftMargin)
        .def_readwrite("rightMargin", &PlotStyle::rightMargin)
        .def_readwrite("legendColumns", &PlotStyle::legendColumns)
        .def_readwrite("grid", &PlotStyle::grid)
        .def_readwrite("legendInside", &PlotStyle::legendInside)
        .def_readwrite("legendWidth", &PlotStyle::legendWidth)
        .def_readwrite("annotationBoxWidth", &PlotStyle::annotationBoxWidth);

    py::class_<PlotProcessConfig>(m, "PlotProcessConfig")
        .def(py::init<>())
        .def_readwrite("directory", &PlotProcessConfig::directory)
        .def_readwrite("histogramName", &PlotProcessConfig::histogramName)
        .def_readwrite("legendLabel", &PlotProcessConfig::legendLabel)
        .def_readwrite("color", &PlotProcessConfig::color)
        .def_readwrite("scale", &PlotProcessConfig::scale)
        .def_readwrite("normalizationHistogram", &PlotProcessConfig::normalizationHistogram)
        .def_readwrite("isData", &PlotProcessConfig::isData)
        .def_readwrite("files", &PlotProcessConfig::files)
        .def_readwrite("isSignal", &PlotProcessConfig::isSignal)
        .def_readwrite("overlay", &PlotProcessConfig::overlay)
        .def_readwrite("displayScale", &PlotProcessConfig::displayScale)
        .def_readwrite("group", &PlotProcessConfig::group)
        .def_readwrite("values", &PlotProcessConfig::values)
        .def_readwrite("variances", &PlotProcessConfig::variances)
        .def_readwrite("errorLow", &PlotProcessConfig::errorLow)
        .def_readwrite("errorHigh", &PlotProcessConfig::errorHigh)
        .def_readwrite("variations", &PlotProcessConfig::variations)
        .def_readwrite("selections", &PlotProcessConfig::selections)
        .def_readwrite("readSystematics", &PlotProcessConfig::readSystematics)
        .def_readwrite("colorHex", &PlotProcessConfig::colorHex)
        .def_readwrite("referenceCovariance", &PlotProcessConfig::referenceCovariance);

    py::class_<PlotRequest>(m, "PlotRequest")
        .def(py::init<>())
        .def_readwrite("metaFile", &PlotRequest::metaFile)
        .def_readwrite("outputFile", &PlotRequest::outputFile)
        .def_readwrite("title", &PlotRequest::title)
        .def_readwrite("xAxisTitle", &PlotRequest::xAxisTitle)
        .def_readwrite("yAxisTitle", &PlotRequest::yAxisTitle)
        .def_readwrite("logY", &PlotRequest::logY)
        .def_readwrite("drawRatio", &PlotRequest::drawRatio)
        .def_readwrite("processes", &PlotRequest::processes)
        .def_readwrite("kind", &PlotRequest::kind)
        .def_readwrite("binEdges", &PlotRequest::binEdges)
        .def_readwrite("binLabels", &PlotRequest::binLabels)
        .def_readwrite("panels", &PlotRequest::panels)
        .def_readwrite("blindBins", &PlotRequest::blindBins)
        .def_readwrite("systematicLow", &PlotRequest::systematicLow)
        .def_readwrite("systematicHigh", &PlotRequest::systematicHigh)
        .def_readwrite("experimentLabel", &PlotRequest::experimentLabel)
        .def_readwrite("luminosityLabel", &PlotRequest::luminosityLabel)
        .def_readwrite("annotations", &PlotRequest::annotations)
        .def_readwrite("style", &PlotRequest::style)
        .def_readwrite("density", &PlotRequest::density)
        .def_readwrite("strictNormalization", &PlotRequest::strictNormalization)
        .def_readwrite("signalOverlay", &PlotRequest::signalOverlay)
        .def_readwrite("flow", &PlotRequest::flow)
        .def_readwrite("additionalOutputs", &PlotRequest::additionalOutputs)
        .def_readwrite("matrix", &PlotRequest::matrix)
        .def_readwrite("yBinEdges", &PlotRequest::yBinEdges)
        .def_readwrite("yBinLabels", &PlotRequest::yBinLabels)
        .def_readwrite("heatmapYRole", &PlotRequest::heatmapYRole)
        .def_readwrite("zAxisTitle", &PlotRequest::zAxisTitle)
        .def_readwrite("horizontalSeparators", &PlotRequest::horizontalSeparators)
        .def_readwrite("symmetricColorRange", &PlotRequest::symmetricColorRange)
        .def_readwrite("logZ", &PlotRequest::logZ)
        .def_readwrite("referenceProcess", &PlotRequest::referenceProcess)
        .def_readwrite("experiment", &PlotRequest::experiment)
        .def_readwrite("experimentExtra", &PlotRequest::experimentExtra)
        .def_readwrite("rebin", &PlotRequest::rebin);

    py::class_<PlotResult>(m, "PlotResult")
        .def(py::init<>())
        .def_readwrite("success", &PlotResult::success)
        .def_readwrite("message", &PlotResult::message)
        .def_readwrite("mcIntegral", &PlotResult::mcIntegral)
        .def_readwrite("dataIntegral", &PlotResult::dataIntegral)
        .def_readwrite("mcVariance", &PlotResult::mcVariance)
        .def_readwrite("warnings", &PlotResult::warnings)
        .def_readwrite("outputs", &PlotResult::outputs);

    py::class_<PlotBatchOptions>(m, "PlotBatchOptions")
        .def(py::init<>())
        .def_readwrite("workers", &PlotBatchOptions::workers)
        .def_readwrite("maxPreparedBytes", &PlotBatchOptions::maxPreparedBytes)
        .def_readwrite("maxOpenFiles", &PlotBatchOptions::maxOpenFiles);

    py::class_<PlotSessionStats>(m, "PlotSessionStats")
        .def(py::init<>())
        .def_readwrite("plots", &PlotSessionStats::plots)
        .def_readwrite("workspaces", &PlotSessionStats::workspaces)
        .def_readwrite("drawableObjects", &PlotSessionStats::drawableObjects)
        .def_readwrite("bufferGrowths", &PlotSessionStats::bufferGrowths)
        .def_readwrite("fileOpens", &PlotSessionStats::fileOpens)
        .def_readwrite("preparationSeconds", &PlotSessionStats::preparationSeconds)
        .def_readwrite("renderingSeconds", &PlotSessionStats::renderingSeconds)
        .def_readwrite("exportSeconds", &PlotSessionStats::exportSeconds);

    auto plotting = py::class_<PlottingUtility>(m, "PlottingUtility",
        "Physics plots with reusable sessions and bounded parallel preparation.");
    plotting.def(py::init<>())
        .def("makeStackPlot", &PlottingUtility::makeStackPlot, py::arg("request"),
             py::call_guard<py::gil_scoped_release>())
        .def("makeStackPlots", &PlottingUtility::makeStackPlots,
             py::arg("requests"), py::arg("parallel") = false,
             "Reuse one session. Parallel mode prepares inputs on up to four workers; graphics are serialized.",
             py::call_guard<py::gil_scoped_release>())
        .def_static("readRequests", &PlottingUtility::readRequests, py::arg("configFile"))
        .def_static("parseRequests", &PlottingUtility::parseRequests, py::arg("yamlOrJson"));
    py::class_<PlottingUtility::Session>(plotting, "Session",
        "Thread-affine rendering session; reuse on its constructing/main thread.")
        .def(py::init<const PlotBatchOptions&>(), py::arg("options") = PlotBatchOptions{})
        .def("render", &PlottingUtility::Session::render, py::arg("request"),
             py::call_guard<py::gil_scoped_release>())
        .def("renderBatch", &PlottingUtility::Session::renderBatch, py::arg("requests"),
             py::call_guard<py::gil_scoped_release>())
        .def("stats", &PlottingUtility::Session::stats);
    m.attr("PlotSession") = plotting.attr("Session");
}
