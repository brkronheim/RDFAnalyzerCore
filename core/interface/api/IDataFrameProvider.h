#ifndef IDATAFRAMEPROVIDER_H_INCLUDED
#define IDATAFRAMEPROVIDER_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <algorithm>
#include <string>
#include <utility>
#include <vector>


#include <api/ISystematicManager.h>

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
    
    // TODO: Why are these defined here? Shouldn't they be defined in the final classes?

    /**
     * @brief Define a new variable in the dataframe
     * @tparam F Callable type for the variable definition
     * @param name Name of the variable
     * @param f Callable to compute the variable
     * @param columns Input columns
     * @param systematicManager Reference to systematic manager
     */
    template <typename F>
    void Define(std::string name, F f, const std::vector<std::string> &columns, ISystematicManager &systematicManager) {
        auto df = getDataFrame();
        const auto existingColumns = df.GetColumnNames();
        if (std::find(existingColumns.begin(), existingColumns.end(), name) != existingColumns.end()) {
            return;
        }

        std::vector<std::string> systList(systematicManager.getSystematics().begin(), systematicManager.getSystematics().end());
        if (!systList.empty()) {
            for (const auto &syst : systList) {
                int nAffected = 0;
                std::vector<std::string> newColumnsUp;
                std::vector<std::string> newColumnsDown;
                for (const auto &col : columns) {
                    const std::string upColumn =
                        systematicManager.getVariationColumnName(col, syst + "Up");
                    const std::string downColumn =
                        systematicManager.getVariationColumnName(col, syst + "Down");
                    if (upColumn != col || downColumn != col) {
                        newColumnsUp.push_back(upColumn);
                        newColumnsDown.push_back(downColumn);
                        nAffected++;
                    } else {
                        newColumnsUp.push_back(col);
                        newColumnsDown.push_back(col);
                    }
                }
                if (nAffected > 0) {
                    const auto upName = name + "_" + syst + "Up";
                    const auto downName = name + "_" + syst + "Down";
                    if (std::find(existingColumns.begin(), existingColumns.end(), upName) == existingColumns.end()) {
                        df = df.Define(upName, f, newColumnsUp);
                    }
                    if (std::find(existingColumns.begin(), existingColumns.end(), downName) == existingColumns.end()) {
                        df = df.Define(downName, f, newColumnsDown);
                    }
                    systematicManager.registerSystematic(syst, {name});
                }
            }
        }

        df = df.Define(name, f, columns);
        setDataFrame(df);
    }
    
    /**
     * @brief Define a vector variable in the dataframe
     *
     * DataFrame providers should also define paired Up/Down outputs for each
     * registered systematic affecting one or more inputs, leave unaffected
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
        const auto existingColumns = df.GetColumnNames();
        if (std::find(existingColumns.begin(), existingColumns.end(), name) != existingColumns.end()) {
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
