#ifndef ICORRECTIONMANAGER_H_INCLUDED
#define ICORRECTIONMANAGER_H_INCLUDED

#include <memory>
#include <string>
#include <vector>
#include <correction.h>

// Forward declarations
class IDataFrameProvider;
class ISystematicManager;
namespace correction {
    class Correction;
}

/**
 * @brief Interface for correction managers to enable dependency injection
 * 
 * This interface abstracts correction operations, allowing for better testing
 * and more flexible dependency injection patterns.
 */
class ICorrectionManager {
public:
    virtual ~ICorrectionManager() = default;
    
    /**
     * @brief Apply a correction to the given dataframe provider
     * @param dataFrameProvider Reference to the dataframe provider
     * @param correctionName Name of the correction
     * @param stringArguments String arguments
     */
    virtual void applyCorrection(IDataFrameProvider& dataFrameProvider,
                                 const std::string &correctionName,
                                 const std::vector<std::string> &stringArguments,
                                 ISystematicManager &systematicManager) = 0;
    
    /**
     * @brief Get a correction object by key
     * @param key Correction key
     * @return Correction reference
     */
    virtual correction::Correction::Ref getCorrection(const std::string &key) const = 0;
    
    /**
     * @brief Get the features for a correction by key
     * @param key Correction key
     * @return Reference to the vector of feature names
     */
    virtual const std::vector<std::string> &getCorrectionFeatures(const std::string &key) const = 0;
};

#endif // ICORRECTIONMANAGER_H_INCLUDED 