#ifndef IBDTMANAGER_H_INCLUDED
#define IBDTMANAGER_H_INCLUDED

#include <api/IDataFrameProvider.h>
#include <memory>
#include <string>
#include <vector>

// Forward declaration
namespace fastforest {
    class FastForest;
}

/**
 * @brief Interface for BDT managers to enable dependency injection
 * 
 * This interface abstracts BDT operations, allowing for better testing
 * and more flexible dependency injection patterns.
 */
class IBDTManager {
public:
    virtual ~IBDTManager() = default;
    
    /**
     * @brief Apply a BDT to the given dataframe provider
     * @param dataFrameProvider Reference to the dataframe provider
     * @param BDTName Name of the BDT
     */
    virtual void applyBDT(IDataFrameProvider& dataFrameProvider,
                          const std::string &BDTName) = 0;
    
    /**
     * @brief Get a BDT object by key
     * @param key BDT key
     * @return Shared pointer to the FastForest object
     */
    virtual std::shared_ptr<fastforest::FastForest> getBDT(const std::string &key) const = 0;
    
    /**
     * @brief Get the features for a BDT by key
     * @param key BDT key
     * @return Reference to the vector of feature names
     */
    virtual const std::vector<std::string> &getBDTFeatures(const std::string &key) const = 0;
    
    /**
     * @brief Get the run variable for a BDT
     * @param bdtName Name of the BDT
     * @return Reference to the run variable string
     */
    virtual const std::string &getRunVar(const std::string &bdtName) const = 0;
    
    /**
     * @brief Get all BDT names
     * @return Vector of all BDT names
     */
    virtual std::vector<std::string> getAllBDTNames() const = 0;
};

#endif // IBDTMANAGER_H_INCLUDED 