#ifndef ITRIGGERMANAGER_H_INCLUDED
#define ITRIGGERMANAGER_H_INCLUDED

#include <memory>
#include <string>
#include <vector>

/**
 * @brief Interface for trigger managers to enable dependency injection
 * 
 * This interface abstracts trigger operations, allowing for better testing
 * and more flexible dependency injection patterns.
 */
class ITriggerManager {
public:
    virtual ~ITriggerManager() = default;
    
    /**
     * @brief Get the triggers for a given group
     * @param group Name of the trigger group
     * @return Reference to the vector of trigger names
     */
    virtual const std::vector<std::string> &getTriggers(const std::string &group) const = 0;
    
    /**
     * @brief Get the vetoes for a given group
     * @param group Name of the trigger group
     * @return Reference to the vector of veto names
     */
    virtual const std::vector<std::string> &getVetoes(const std::string &group) const = 0;
    
    /**
     * @brief Get the group for a given sample
     * @param sample Name of the sample
     * @return Name of the group
     */
    virtual std::string getGroupForSample(const std::string &sample) const = 0;
    
    /**
     * @brief Get all groups
     * @return Vector of all group names
     */
    virtual std::vector<std::string> getAllGroups() const = 0;
};

#endif // ITRIGGERMANAGER_H_INCLUDED 