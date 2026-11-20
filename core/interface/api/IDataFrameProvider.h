#ifndef IDATAFRAMEPROVIDER_H_INCLUDED
#define IDATAFRAMEPROVIDER_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <algorithm>
#include <string>
#include <utility>
#include <vector>


#include <api/ISystematicManager.h>
#include <detail/DefineVariations.h>

/**
 * @brief Interface for dataframe providers to enable dependency injection
 * 
 * This interface abstracts dataframe operations, allowing for better testing
 * and more flexible dependency injection patterns.
 */
class IDataFrameProvider {
public:
    virtual ~IDataFrameProvider();
    
    /**
     * @brief Get the current RDataFrame node
     * @return The current RNode
     */
    virtual ROOT::RDF::RNode getDataFrame() = 0;
    
    /**
     * @brief Set the current RDataFrame node
     * @param node The RNode to set
     */
    virtual void setDataFrame(const ROOT::RDF::RNode &node) = 0;

    /// Custom operations reading branches outside declared RDF inputs must
    /// request learning before running. Normal RDF expressions/actions need
    /// no notification: ROOT resolves their physical reader dependencies.
    virtual void markInputDependenciesIncomplete(const std::string &) {}
    
    // TODO: Why are these defined here? Shouldn't they be defined in the final classes?

    /**
     * @brief Define exactly one column without systematic expansion.
     * Existing names retain their first definition for legacy compatibility.
     * For a new explicit calibration producer, check name ownership before
     * calling this method and use names distinct from auto-generated shifts.
     * Install variation mappings before defining downstream consumers.
     * @tparam F Callable type for the variable definition
     * @param name Name of the variable
     * @param f Callable to compute the variable
     * @param columns Input columns
     */
    template <typename F>
    void DefineNominal(std::string name, F f,
                       const std::vector<std::string> &columns = {}) {
        auto df = getDataFrame();
        if (df.HasColumn(name)) return;
        setDataFrame(df.Define(name, std::move(f), columns));
    }

    /**
     * @brief Define a column and propagate each registered input variation.
     * Use DefineNominal for already-resolved, terminal variation columns to
     * avoid constructing unused combinations of independent nuisances.
     */
    template <typename F>
    void Define(std::string name, F f, const std::vector<std::string> &columns, ISystematicManager &systematicManager) {
        setDataFrame(rdf::detail::defineWithVariations(
            getDataFrame(), name, columns, systematicManager,
            [&f](ROOT::RDF::RNode df, const std::string &output,
                 const std::vector<std::string> &inputs) {
                return df.Define(output, f, inputs);
            }));
    }
    
    /**
     * @brief Define a vector variable in the dataframe
     *
     * DataFrame providers should also define paired Up/Down and explicitly
     * labelled outputs for registered variations affecting inputs, leave unaffected
     * inputs nominal, and register the output as affected by that systematic.
     * @param name Name of the variable
     * @param columns Input columns
     * @param type Data type (default: Float_t)
     * @param systematicManager Reference to systematic manager
     */
    virtual void DefineVector(std::string name,
                             const std::vector<std::string> &columns,
                             std::string type,
                             ISystematicManager &systematicManager) = 0;

    /**
     * @brief Define an already-resolved vector without expanding systematics.
     *
     * Use for helper vectors whose elements already represent variations or
     * whose inputs intentionally repeat nominal values. Providers that do not
     * distinguish this mode may fall back to their ordinary vector definition.
     */
    virtual void DefineVectorNominal(
        std::string name, const std::vector<std::string> &columns,
        std::string type, ISystematicManager &systematicManager) {
        DefineVector(std::move(name), columns, std::move(type), systematicManager);
    }
    
    /**
     * @brief Filter the dataframe using the supplied nominal predicate
     * @param f Filter function
     * @param columns Input columns
     */
    template <typename F>
    void Filter(F f, const std::vector<std::string> &columns = {}) {
        auto df = getDataFrame();
        df = df.Filter(f, columns);
        setDataFrame(df);
    }
    
    /**
     * @brief Define a per-sample variable in the dataframe
     * @param name Name of the variable
     * @param f Function to define the variable
     */
    template <typename F> 
    void DefinePerSample(std::string name, F f) {
        auto df = getDataFrame();
        if (df.HasColumn(name)) {
            return;
        }
        df = df.DefinePerSample(name, f);
        setDataFrame(df);
    }
    
    /**
     * @brief Redefine a variable in the dataframe
     * @param name Name of the variable
     * @param f Function to redefine the variable
     * @param columns Input columns
     */
    template <typename F>
    void Redefine(std::string name, F f, const std::vector<std::string> &columns = {}) {
        auto df = getDataFrame();
        df = df.Redefine(name, f, columns);
        setDataFrame(df);
    }
};

#endif // IDATAFRAMEPROVIDER_H_INCLUDED
