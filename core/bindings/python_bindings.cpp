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
#include <TriggerManager.h>
#include <SofieManager.h>
#include <NDHistogramManager.h>
#include <PlottingUtility.h>

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <TInterpreter.h>

#include <atomic>
#include <sstream>
#include <algorithm>
#include <set>
#include <iostream>
#include <cctype>
#include <unordered_map>

namespace py = pybind11;

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

/**
 * @brief Wrapper to handle string-based Define calls (ROOT JIT compilation)
 * 
 * This allows Python users to pass C++ expressions as strings, which ROOT
 * will compile using its JIT compiler.
 */
class AnalyzerPythonWrapper {
public:
    AnalyzerPythonWrapper(const std::string& configFile) 
        : analyzer_(configFile) {}

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
        auto df = analyzer_.getDF();
        auto& sysMgr = analyzer_.getSystematicManager();
        
        const auto existingColumns = df.GetColumnNames();
        if (std::find(existingColumns.begin(), existingColumns.end(), name) != existingColumns.end()) {
            std::cerr << "Warning: Column '" << name << "' already exists, skipping Define" << std::endl;
            return *this;
        }
        
        // Handle systematics
        // Note on naming: Input columns use "_up"/"_down" suffixes (e.g., "pt_up", "pt_down")
        // while output columns include the systematic name (e.g., "newvar_jesUp", "newvar_jesDown")
        std::vector<std::string> systList(sysMgr.getSystematics().begin(), 
                                         sysMgr.getSystematics().end());
        if (!systList.empty()) {
            for (const auto& syst : systList) {
                int nAffected = 0;
                std::vector<std::string> newColumnsUp;
                std::vector<std::string> newColumnsDown;
                for (const auto& col : columns) {
                    if (sysMgr.getSystematicsForVariable(col).find(syst) != 
                        sysMgr.getSystematicsForVariable(col).end()) {
                        newColumnsUp.push_back(col + "_up");
                        newColumnsDown.push_back(col + "_down");
                        nAffected++;
                    } else {
                        newColumnsUp.push_back(col);
                        newColumnsDown.push_back(col);
                    }
                }
                if (nAffected > 0) {
                    const auto upName = name + "_" + syst + "Up";
                    const auto downName = name + "_" + syst + "Down";
                    if (std::find(existingColumns.begin(), existingColumns.end(), upName) == 
                        existingColumns.end()) {
                        // Use the string-expression overload (only name + expression). Columns are
                        // used for systematic bookkeeping above, ROOT's string-based Define does
                        // not accept an explicit columns argument.
                        df = df.Define(upName, expression);
                    }
                    if (std::find(existingColumns.begin(), existingColumns.end(), downName) == 
                        existingColumns.end()) {
                        df = df.Define(downName, expression);
                    }
                    sysMgr.registerSystematic(syst, {name});
                }
            }
        }
        
        // ROOT's string-based Define overload does not accept an explicit
        // column list (it only supports a name + expression). Therefore, we
        // rely on ROOT's expression parsing (and the earlier systematic handling
        // above) and ignore the provided column list here.
        df = df.Define(name, expression);
        analyzer_.getDataFrameProvider().setDataFrame(df);
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
        std::string filterName = "pass_" + name;
        DefineJIT(filterName, expression, columns);
        
        auto df = analyzer_.getDF();
        // Filter by the boolean column created above: keep rows where the column is true.
        df = df.Filter([](bool v){ return v; }, {filterName});
        analyzer_.getDataFrameProvider().setDataFrame(df);
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
        // This avoids embedding a complex reinterpret_cast inside the JIT expression
        // string, which ROOT's column-name auto-detection cannot handle reliably.
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

        // Generate a simple call expression using the declared function name.
        // ROOT's column-name auto-detection works correctly for expressions of
        // the form "funcname(col1, col2, ...)".
        std::stringstream jit_expr;
        jit_expr << fname << "(";
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
        } else {
            throw std::invalid_argument(
                "Unsupported plugin type: '" + pluginType + "'. Supported: "
                "OnnxManager, BDTManager, CorrectionManager, TriggerManager, SofieManager, NDHistogramManager"
            );
        }
        return *this;
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
        if (auto* mgr = analyzer_.getPlugin<OnnxManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto* mgr = analyzer_.getPlugin<BDTManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto* mgr = analyzer_.getPlugin<CorrectionManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto* mgr = analyzer_.getPlugin<TriggerManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto* mgr = analyzer_.getPlugin<SofieManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        if (auto* mgr = analyzer_.getPlugin<NDHistogramManager>(role)) {
            mgr->setupFromConfigFile();
            return *this;
        }
        throw std::runtime_error("No supported plugin found for role '" + role + "'");
    }

    std::vector<std::string> GetColumnNames() {
        auto names = analyzer_.getDF().GetColumnNames();
        return std::vector<std::string>(names.begin(), names.end());
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
        return regionNames;
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
        requirePlugin<NDHistogramManager>(role, "NDHistogramManager").Clear();
        return *this;
    }
    
    /**
     * @brief Save the analysis results
     */
    AnalyzerPythonWrapper& save() {
        analyzer_.save();
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
    template <typename T>
    T& requirePlugin(const std::string& role, const std::string& typeName) {
        auto* plugin = analyzer_.getPlugin<T>(role);
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
        return normalized;
    }

    Analyzer analyzer_;
};

PYBIND11_MODULE(rdfanalyzer, m) {
    m.doc() = "Python bindings for RDFAnalyzerCore framework";

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
        .def(py::init<const std::string&>(), 
             py::arg("configFile"),
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
        .def("configMap", &AnalyzerPythonWrapper::configMap,
             py::arg("key"),
               "Get a configuration value by key")
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

    // ---------------------------------------------------------------------------
    // PlottingUtility bindings
    // ---------------------------------------------------------------------------

    py::class_<PlotProcessConfig>(m, "PlotProcessConfig",
        R"pbdoc(
        Configuration for a single process in a stack plot.

        Parameters
        ----------
        directory : str
            TDirectory name inside the meta file (empty string = file root).
        histogramName : str
            Name of the TH1D histogram to retrieve.
        legendLabel : str
            Label shown in the plot legend.
        color : int
            ROOT color index used for fill/line.
        scale : float
            Manual normalization scale factor (multiplied on top of the
            normalization histogram scale when provided).
        normalizationHistogram : str
            Optional name of a TH1 whose bin-1 content is used to normalize
            the histogram (e.g. ``counter_weightSum_<sample>``).
        isData : bool
            When *True* the histogram is drawn as data points instead of a
            filled stack contribution.
        )pbdoc")
        .def(py::init<>())
        .def_readwrite("directory", &PlotProcessConfig::directory)
        .def_readwrite("histogramName", &PlotProcessConfig::histogramName)
        .def_readwrite("legendLabel", &PlotProcessConfig::legendLabel)
        .def_readwrite("color", &PlotProcessConfig::color)
        .def_readwrite("scale", &PlotProcessConfig::scale)
        .def_readwrite("normalizationHistogram", &PlotProcessConfig::normalizationHistogram)
        .def_readwrite("isData", &PlotProcessConfig::isData);

    py::class_<PlotRequest>(m, "PlotRequest",
        R"pbdoc(
        Full specification for a single stack plot.

        Parameters
        ----------
        metaFile : str
            Path to the ROOT meta output file produced by the analysis.
        outputFile : str
            Destination path for the saved plot (PDF, PNG, …).
        title : str
            Histogram/canvas title.
        xAxisTitle : str
            Label for the x-axis.
        yAxisTitle : str
            Label for the y-axis (default: ``"Counts"``).
        logY : bool
            Draw y-axis in log scale.
        drawRatio : bool
            Add a data/MC ratio (and pull) panel below the stack.
        processes : list[PlotProcessConfig]
            Ordered list of processes (MC first, data last).
        )pbdoc")
        .def(py::init<>())
        .def_readwrite("metaFile", &PlotRequest::metaFile)
        .def_readwrite("outputFile", &PlotRequest::outputFile)
        .def_readwrite("title", &PlotRequest::title)
        .def_readwrite("xAxisTitle", &PlotRequest::xAxisTitle)
        .def_readwrite("yAxisTitle", &PlotRequest::yAxisTitle)
        .def_readwrite("logY", &PlotRequest::logY)
        .def_readwrite("drawRatio", &PlotRequest::drawRatio)
        .def_readwrite("processes", &PlotRequest::processes);

    py::class_<PlotResult>(m, "PlotResult",
        R"pbdoc(
        Result returned by :py:meth:`PlottingUtility.makeStackPlot`.

        Attributes
        ----------
        success : bool
            *True* if the plot was created and saved successfully.
        message : str
            Error description when *success* is *False*.
        mcIntegral : float
            Integral of the total MC stack histogram.
        dataIntegral : float
            Integral of the data histogram (0 when no data process is given).
        )pbdoc")
        .def(py::init<>())
        .def_readwrite("success", &PlotResult::success)
        .def_readwrite("message", &PlotResult::message)
        .def_readwrite("mcIntegral", &PlotResult::mcIntegral)
        .def_readwrite("dataIntegral", &PlotResult::dataIntegral);

    py::class_<PlottingUtility>(m, "PlottingUtility",
        R"pbdoc(
        Utility for creating and saving analysis-style ROOT stack plots.

        Creates MC-stack + optional data-overlay plots with ratio and pull
        panels directly from the meta output ROOT file produced by the
        analysis framework.  Plots can be saved to any format supported by
        ROOT's ``TCanvas::SaveAs`` (PDF, PNG, …).

        Examples
        --------
        >>> import rdfanalyzer
        >>> pu = rdfanalyzer.PlottingUtility()
        >>>
        >>> proc_mc = rdfanalyzer.PlotProcessConfig()
        >>> proc_mc.directory = "signal"
        >>> proc_mc.histogramName = "pt"
        >>> proc_mc.legendLabel = "Signal MC"
        >>> proc_mc.color = 2  # kRed
        >>> proc_mc.normalizationHistogram = "counter_weightSum_signal"
        >>>
        >>> req = rdfanalyzer.PlotRequest()
        >>> req.metaFile = "meta_output.root"
        >>> req.outputFile = "pt_stack.pdf"
        >>> req.title = "Transverse Momentum"
        >>> req.xAxisTitle = "p_{T} [GeV]"
        >>> req.logY = False
        >>> req.drawRatio = False
        >>> req.processes = [proc_mc]
        >>>
        >>> result = pu.makeStackPlot(req)
        >>> assert result.success, result.message
        )pbdoc")
        .def(py::init<>())
        .def("makeStackPlot", &PlottingUtility::makeStackPlot,
             py::arg("request"),
             R"pbdoc(
             Create and save a single stack plot.

             Parameters
             ----------
             request : PlotRequest
                 Full plot specification.

             Returns
             -------
             PlotResult
                 Success flag, optional error message, and histogram integrals.
             )pbdoc")
        .def("makeStackPlots", &PlottingUtility::makeStackPlots,
             py::arg("requests"),
             py::arg("parallel") = false,
             R"pbdoc(
             Create and save multiple stack plots.

             Parameters
             ----------
             requests : list[PlotRequest]
                 List of plot specifications.
             parallel : bool
                 When *True*, plots are rendered concurrently using
                 ``std::async``.  ROOT thread-safety is enabled automatically.

             Returns
             -------
             list[PlotResult]
                 One result per request, in the same order.
             )pbdoc");
}
