#ifndef IDATAFRAMEPROVIDER_H_INCLUDED
#define IDATAFRAMEPROVIDER_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <string>
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
        df = df.Define(name, f, columns);
        setDataFrame(df);
    }
    
    /**
     * @brief Define a vector variable in the dataframe
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
     * @brief Filter the dataframe
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