/*
 * @file analyzer.cc
 * @brief Implementation of the Analyzer class for event analysis using ROOT's
 * RDataFrame.
 *
 * This file contains the implementation of the Analyzer class, which manages
 * configuration, data loading, event selection, histogramming, and application
 * of corrections and BDTs. It is the core of the analysis logic, interfacing
 * with ROOT and other utilities.
 */
#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>
#include <TChain.h>
#include <TFile.h>
#include <memory>
#include <string>

#include <BDTManager.h>
#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <TriggerManager.h>
#include <analyzer.h>
#include <correction.h>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <util.h>

/**
 * @brief Construct a new Analyzer object
 *
 * @param configFile File containing the configuration information for this
 * analyzer
 */
Analyzer::Analyzer(std::string configFile)
    : configManager_m(configFile), dataManager_m(configManager_m),
      correctionManager_m(configManager_m), bdtManager_m(configManager_m),
      triggerManager_m(configManager_m),
      ndHistManager_m(dataManager_m, configManager_m) {

  // Set verbosity level
  auto verbosityStr = configManager_m.get("verbosity");
  if (!verbosityStr.empty()) {
    verbosityLevel_m = std::stoi(verbosityStr);
  } else {
    verbosityLevel_m = 1;
  }

// Display a progress bar depending on batch status and ROOT version
#if defined(HAS_ROOT_PROGRESS_BAR)
  auto batchStr = configManager_m.get("batch");
  if (batchStr.empty()) {
    ROOT::RDF::Experimental::AddProgressBar(dataManager_m.getDataFrame());
  } else if (batchStr != "True") {
    ROOT::RDF::Experimental::AddProgressBar(dataManager_m.getDataFrame());
  } else {
    if (verbosityLevel_m >= 1) {
      std::cout << "Batch mode, no progress bar" << std::endl;
    }
  }
#else
  if (verbosityLevel_m >= 1) {
    std::cout << "ROOT version does not support progress bar, update to at "
                 "least 6.28 to get it."
              << std::endl;
  }
#endif

  dataManager_m.finalizeSetup(configManager_m);

  if (verbosityLevel_m >= 1) {
    std::cout << "Done creating Analyzer object" << std::endl;
  }
}

/**
 * @brief Apply a Boosted Decision Tree (BDT) to the dataframe.
 *
 * This method defines the input vector for the BDT, creates a lambda for BDT
 * evaluation, and defines the BDT output variable in the dataframe.
 *
 * @param BDTName Name of the BDT to apply
 * @return Pointer to this Analyzer (for chaining)
 */
Analyzer *Analyzer::ApplyBDT(std::string BDTName) {
  // Use BDTManager for input features and BDT object
  const auto &inputFeatures = bdtManager_m.getBDTFeatures(BDTName);
  const auto &runVar = bdtManager_m.getRunVar(BDTName);
  bdtManager_m.applyBDT(
      BDTName, inputFeatures, runVar,
      [this](const std::string &name, const std::vector<std::string> &columns,
             const std::string &type) {
        this->DefineVector(name, columns, type);
      },
      [this](const std::string &name, auto f,
             const std::vector<std::string> &columns) {
        this->Define(name, f, columns);
      });
  return this;
}

Analyzer *Analyzer::ApplyAllBDTs() {
  // Loop over all BDTs in the manager
  for (const auto &bdtName : bdtManager_m.getAllBDTNames()) {
    ApplyBDT(bdtName);
  }
  return this;
}

inline bool passTriggerAndVeto(ROOT::VecOps::RVec<Bool_t> trigger,
                               ROOT::VecOps::RVec<Bool_t> triggerVeto) {
  for (const auto &trig : triggerVeto) {
    if (trig) {
      return (false);
    }
  }
  for (const auto &trig : trigger) {
    if (trig) {
      return (true);
    }
  }
  return (false);
}

inline bool passTrigger(ROOT::VecOps::RVec<Bool_t> trigger) {
  for (const auto &trig : trigger) {
    if (trig) {
      return (true);
    }
  }
  return (false);
}

Analyzer *Analyzer::ApplyAllTriggers() {
  std::string triggerName = configManager_m.get("type");
  if (!triggerName.empty()) { // data, need to account for duplicate events from
                              // different datasets
    std::cout << "looking for trigger for " << triggerName << std::endl;
    triggerName = triggerManager_m.getGroupForSample(triggerName);
    std::cout << "trigger is " << triggerName << std::endl;

    const auto &triggers = triggerManager_m.getTriggers(triggerName);
    const auto &vetos = triggerManager_m.getVetoes(triggerName);

    for (const auto &trig : triggers) {
      std::cout << "Keep Data trigger: " << trig << std::endl;
    }
    for (const auto &trig : vetos) {
      std::cout << "Veto Data trigger: " << trig << std::endl;
    }

    if (vetos.size() == 0) {
      std::cout << "No veto" << std::endl;
      DefineVector("allTriggersPassVector", triggers, "Bool_t");
      Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});
    } else {
      DefineVector(triggerName + "passVector", triggers, "Bool_t");
      DefineVector(triggerName + "vetoVector", vetos, "Bool_t");
      Filter("applyTrigger", passTriggerAndVeto,
             {triggerName + "pass vector", triggerName + " veto vector"});
    }
  } else { // MC, no duplicate events
    std::vector<std::string> allTriggers;
    for (const auto &group : triggerManager_m.getAllGroups()) {
      const auto &triggers = triggerManager_m.getTriggers(group);
      allTriggers.insert(allTriggers.end(), triggers.begin(), triggers.end());
    }
    for (const auto &trig : allTriggers) {
      std::cout << "Keep MC trigger: " << trig << std::endl;
    }
    DefineVector("allTriggersPassVector", allTriggers, "Bool_t");
    Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});
  }
  return (this);
}

ROOT::RDF::RNode Analyzer::getDF() { return (dataManager_m.getDataFrame()); }

std::string Analyzer::configMap(std::string key) {
  return (configManager_m.get(key));
}

Analyzer *Analyzer::DefineVector(std::string name,
                                 const std::vector<std::string> &columns,
                                 std::string type) {
  dataManager_m.DefineVector(name, columns, type);
  return this;
}

// TODO: I don't like the use of the lambdas here,
// ideally we will simply pass references to the appropriate
// manager classes instead, but the systematic implementations
// need to be finished first
Analyzer *Analyzer::ApplyCorrection(std::string correctionName,
                                    std::vector<std::string> stringArguments) {
  // Use CorrectionManager to apply the correction
  const auto &inputFeatures =
      correctionManager_m.getCorrectionFeatures(correctionName);
  correctionManager_m.applyCorrection(
      correctionName, stringArguments, inputFeatures,
      // DefineVector lambda
      [this](const std::string &name, const std::vector<std::string> &columns,
             const std::string &type) {
        this->DefineVector(name, columns, type);
      },
      // Define lambda
      [this](const std::string &name, auto f,
             const std::vector<std::string> &columns) {
        this->Define(name, f, columns);
      });
  return this;
}

/**
 * @brief Save the configured branches to the output file and trigger the
 * computation of the dataframe.
 *
 * @return Pointer to this Analyzer (for chaining)
 */
Analyzer *Analyzer::save() {
  auto df = dataManager_m.getDataFrame();
  saveDF(df, configManager_m, dataManager_m);
  return (this);
}
