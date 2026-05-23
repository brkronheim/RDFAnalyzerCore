#include "analyzer.h"
#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <functions.h>

#include <iostream>


// Example analysis demonstrating config-driven histograms
int main(int argc, char **argv) {

    // Main configuration is provided as command-line argument
    if (argc != 2) {
        std::cout << "Arguments: " << argc << std::endl;
        std::cerr << "Error: No configuration file provided. Please include a config file." << std::endl;
        return 1;
    }

    // Create analyzer with config file
    auto analyzer = Analyzer(argv[1]);

    // Add NDHistogramManager plugin using the helper function
    NDHistogramManager::create(analyzer);

    // Define analysis variables
    analyzer.Define("example_var", []() -> Float_t { return 5.0; }, {})
            .Define("example_weight", []() -> Float_t { return 1.0; }, {})
            .Define("channel_var", []() -> Float_t { return 1.0; }, {});

    // Apply event filters
    analyzer.Filter("event_quality", []() -> bool { return true; }, {});

    // Book histograms from config file
    // This should be called after all Define and Filter operations
    analyzer.bookConfigHistograms();

    // run() triggers the event loop exactly once: writes a skim when
    // enableSkim=1 is in the config, and saves all histograms booked on
    // the NDHistogramManager.
    analyzer.run();

    return 0;
}
