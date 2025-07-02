#ifndef INDHISTOGRAMMANAGER_H_INCLUDED
#define INDHISTOGRAMMANAGER_H_INCLUDED

#include <memory>
#include <string>
#include <vector>
#include <THnSparse.h>

// Forward declarations
class histInfo;
class selectionInfo;

/**
 * @brief Interface for ND histogram managers to enable dependency injection
 * 
 * This interface abstracts histogram operations, allowing for better testing
 * and more flexible dependency injection patterns.
 */
class INDHistogramManager {
public:
    virtual ~INDHistogramManager() = default;
    
    /**
     * @brief Book N-dimensional histograms
     * @param infos Vector of histogram info objects
     * @param selection Vector of selection info objects
     * @param suffix Suffix to append to histogram names
     * @param allRegionNames Vector of region name vectors
     */
    virtual void BookND(std::vector<histInfo> &infos,
                        std::vector<selectionInfo> &selection, 
                        const std::string &suffix,
                        std::vector<std::vector<std::string>> &allRegionNames,
                        ISystematicManager &systematicManager) = 0;
    
    /**
     * @brief Save histograms to file
     * @param fullHistList Vector of vectors of histogram info objects
     * @param allRegionNames Vector of vectors of region name vectors
     */
    virtual void SaveHists(std::vector<std::vector<histInfo>> &fullHistList,
                           std::vector<std::vector<std::string>> &allRegionNames) = 0;
    
    /**
     * @brief Get the vector of histogram result pointers
     * @return Reference to the vector of RResultPtr<THnSparseD>
     */
    virtual std::vector<ROOT::RDF::RResultPtr<THnSparseD>> &GetHistos() = 0;
    
    /**
     * @brief Clear all stored histograms
     */
    virtual void Clear() = 0;
};

#endif // INDHISTOGRAMMANAGER_H_INCLUDED 