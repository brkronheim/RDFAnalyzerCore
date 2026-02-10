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

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>

#include <sstream>
#include <algorithm>

namespace py = pybind11;

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
            return *this;
        }
        
        // Handle systematics
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
                        df = df.Define(upName, expression, newColumnsUp);
                    }
                    if (std::find(existingColumns.begin(), existingColumns.end(), downName) == 
                        existingColumns.end()) {
                        df = df.Define(downName, expression, newColumnsDown);
                    }
                    sysMgr.registerSystematic(syst, {name});
                }
            }
        }
        
        df = df.Define(name, expression, columns);
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
        df = df.Filter(passCut, {filterName});
        analyzer_.getDataFrameProvider().setDataFrame(df);
        return *this;
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
        // Create a JIT string that casts the pointer and calls it
        std::stringstream jit_expr;
        jit_expr << "reinterpret_cast<" << signature << "*>(" 
                 << func_ptr << ")";
        
        // If columns are provided, append them
        if (!columns.empty()) {
            jit_expr << "(";
            for (size_t i = 0; i < columns.size(); ++i) {
                if (i > 0) jit_expr << ", ";
                jit_expr << columns[i];
            }
            jit_expr << ")";
        } else {
            jit_expr << "()";
        }
        
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
        // Create an RVec from the pointer and size
        std::stringstream jit_expr;
        jit_expr << "ROOT::VecOps::RVec<" << element_type << ">("
                 << "reinterpret_cast<" << element_type << "*>(" << data_ptr << "), "
                 << size << ")";
        
        return DefineJIT(name, jit_expr.str(), {});
    }
    
    /**
     * @brief Save the analysis results
     */
    void save() {
        analyzer_.save();
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
    Analyzer analyzer_;
};

PYBIND11_MODULE(rdfanalyzer, m) {
    m.doc() = "Python bindings for RDFAnalyzerCore framework";
    
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
        .def("save", &AnalyzerPythonWrapper::save,
             "Trigger computation and save the analysis results")
        .def("configMap", &AnalyzerPythonWrapper::configMap,
             py::arg("key"),
             "Get a configuration value by key");
    
    // Version info
    m.attr("__version__") = "1.0.0";
}
