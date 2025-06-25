/**
 * @file analyzer.h
 * @brief Declaration of the Analyzer class for event analysis.
 *
 The Analyzer class provides an interface for configuring, processing, and
 analyzing event data
 * using ROOT's RDataFrame. It supports defining variables, applying filters,
 handling systematics,
 * and managing histograms and corrections.
 */
#ifndef ANALYZER_H_INCLUDED
#define ANALYZER_H_INCLUDED

#include <ROOT/RResultPtr.hxx>

#include <RtypesCore.h>
#include <correction.h>
#include <fastforest.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>

#include <TChain.h>
#include <functions.h>
#include <plots.h>
#include <util.h>
#include <configParser.h>

/**
 * @class Analyzer
 * @brief Main analysis class for event processing using ROOT's RDataFrame.
 *
 * The Analyzer class manages configuration, data loading, event selection,
 * histogramming, application of corrections, BDTs, and systematics. It provides
 * a high-level interface for defining variables, applying filters, and managing
 * the analysis workflow.
 */
class Analyzer {
public:
  /**
   * @brief Construct a new Analyzer object
   *
   * @param configFile File containing the configuration information for this
   * analyzer
   */
  Analyzer(std::string configFile);

  /**
   * @brief Define a new variable in the dataframe, handling systematics if
   * present.
   * @tparam F Callable type for the variable definition
   * @param name Name of the new variable
   * @param f Callable to compute the variable
   * @param columns Input columns for the callable
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename F>
  Analyzer *Define(std::string name, F f,
                   const std::vector<std::string> &columns = {}) {
    // std::cout << "Defining: " << name << std::endl;
    // Find all systematics that affect this new variable
    std::unordered_set<std::string> systsToApply;
    std::vector<std::string> nomColumns;
    for (auto &variable : columns) {
      if (variableToSystematicMap_m.find(variable) !=
          variableToSystematicMap_m.end()) {
        for (auto &syst : variableToSystematicMap_m.at(variable)) {

          // register syst to process, register variable with systematics
          systsToApply.insert(syst);
          variableToSystematicMap_m[name].insert(syst);
          systematicToVariableMap_m[syst].insert(name);
        }
      }
      if (variable.find("_fixedNominal") !=
          std::string::npos) { // allow fixing a variable to nominal
        auto index = variable.find(
            "_fixedNominal"); // Need to do this for all similar operations

        // std::cout << "Fixing " << variable.substr(0, index) << " to nominal
        // for calculation of " << name << std::endl;
        nomColumns.push_back(variable.substr(0, index));
      } else {
        nomColumns.push_back(variable);
      }
    }

    // Define nominal variation
    Define_m<F>(name, f, nomColumns);

    // If systematics were found, define a new variable for each up and down
    // variation.
    for (auto &syst : systsToApply) {
      std::vector<std::string> systColumnsUp;
      std::vector<std::string> systColumnsDown;
      for (auto var : columns) { // check all variables in column, replace ones
                                 // affected by systematics
        if (systematicToVariableMap_m[syst].find(var) ==
            systematicToVariableMap_m[syst].end()) {
          if (var.find("_fixedNominal") !=
              std::string::npos) { // allow fixing a variable to nominal
            auto index = var.find(
                "_fixedNominal"); // Need to do this for all similar operations

            // std::cout << "Fixing " << var.substr(0, index) << " to nominal
            // for calculation of " << name << std::endl;
            systColumnsUp.push_back(var.substr(0, index));
            systColumnsDown.push_back(var.substr(0, index));
          } else {
            systColumnsUp.push_back(var);
            systColumnsDown.push_back(var);
          }
        } else {
          systColumnsUp.push_back(var + "_" + syst + "Up");
          systColumnsDown.push_back(var + "_" + syst + "Down");
        }
      }
      // Define up and down variations for current systematic
      Define_m<F>(name + "_" + syst + "Up", f, systColumnsUp);
      Define_m<F>(name + "_" + syst + "Down", f, systColumnsDown);
    }

    return (this);
  }

  /**
   * @brief Define a filter (selection) in the dataframe, handling systematics
   * if present.
   * @tparam F Callable type for the filter
   * @param name Name of the filter
   * @param f Callable to compute the filter
   * @param columns Input columns for the callable
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename F>
  Analyzer *Filter(std::string name, F f,
                   const std::vector<std::string> &columns = {}) {
    name = "pass_" + name;
    // make a branch for whether each systematic variation passes this cut
    Define(name, f, columns);
    if (variableToSystematicMap_m.find(name) !=
        variableToSystematicMap_m.end()) {
      for (auto &syst : variableToSystematicMap_m[name]) {

        Redefine_m(
            name, orBranches3,
            {name, name + "_" + syst + "Up", name + "_" + syst + "Down"});
      }
    }

    Filter_m(passCut, {name});
    return (this);
  }

  /**
   * @brief Define a variable per sample (e.g., for storing constants).
   * @tparam F Callable type for the variable definition
   * @param name Name of the variable
   * @param f Callable to compute the variable
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename F> Analyzer *DefinePerSample(std::string name, F f) {
    DefinePerSample_m<F>(name, f);
    return (this);
  }

  /**
   * @brief Save a constant variable per sample.
   * @tparam T Type of the variable
   * @param var Value to store
   * @param name Name of the variable
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename T> Analyzer *SaveVar(T var, std::string name) {
    auto storeVar = [var](unsigned int, const ROOT::RDF::RSampleInfo) -> T {
      return (var);
    };

    std::cout << "Defining variable " << name << " to be " << var << std::endl;
    DefinePerSample_m(name, storeVar);

    return (this);
  }

  /**
   * @brief Define a vector variable in the dataframe.
   * @param name Name of the variable
   * @param columns Input columns
   * @param type Data type (default: Float_t)
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *DefineVector(std::string name,
                         const std::vector<std::string> &columns = {},
                         std::string type = "Float_t");

  /**
   * @brief Apply a Boosted Decision Tree (BDT) to the dataframe.
   *
   * This method defines the input vector for the BDT, creates a lambda for BDT
   * evaluation, and defines the BDT output variable in the dataframe.
   *
   * @param BDTName Name of the BDT to apply
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *ApplyBDT(std::string BDTName);

  /**
   * @brief Apply a correction using correctionlib.
   * @param correctionName Name of the correction
   * @param stringArguments Arguments for the correction
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *ApplyCorrection(std::string correctionName,
                            std::vector<std::string> stringArguments);

  /**
   * @brief Apply all registered BDTs to the dataframe.
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *ApplyAllBDTs();

  /**
   * @brief Apply all registered triggers to the dataframe.
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *ApplyAllTriggers();

  /**
   * @brief Save the configured branches to the output file and trigger the
   * computation of the dataframe.
   *
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *save();

  /**
   * @brief Read histogram using bin indices and store in a branch.
   * @param histName Name of the histogram
   * @param outputBranchName Name of the output branch
   * @param inputBranchNames Input branches for lookup
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *readHistBins(std::string histName, std::string outputBranchName,
                         std::vector<std::string> inputBranchNames);

  /**
   * @brief Read histogram using value to find binand store in a branch.
   * @param histName Name of the histogram
   * @param outputBranchName Name of the output branch
   * @param inputBranchNames Input branches for lookup
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *readHistVals(std::string histName, std::string outputBranchName,
                         std::vector<std::string> inputBranchNames);

  /**
   * @brief Book N-dimensional histograms for a set of selections and regions.
   * @param infos Histogram info objects
   * @param selection Selection info objects
   * @param suffix Suffix for histogram names
   * @param allRegionNames List of region names
   */
  void bookND(std::vector<histInfo> &infos,
              std::vector<selectionInfo> &selection, std::string suffix,
              std::vector<std::vector<std::string>> &allRegionNames);

  /**
   * @brief Save all histograms to output files.
   * @param fullHistList List of all histograms
   * @param allRegionNames List of region names
   */
  void save_hists(std::vector<std::vector<histInfo>> &fullHistList,
                  std::vector<std::vector<std::string>> &allRegionNames);

  /**
   * @brief Get the underlying RDataFrame node.
   * @return The current RNode
   */
  ROOT::RDF::RNode getDF();

  /**
   * @brief Get a configuration value by key.
   * @param key Configuration key
   * @return Configuration value
   */
  std::string configMap(std::string key);

  /**
   * @brief Get a correction object by key.
   * @param key Correction key
   * @return Correction reference
   */
  correction::Correction::Ref correctionMap(std::string key);

  /**
   * @brief Make a systematic variation which is an indexing value for each
   * systematic. Return the names of all of these branches
   *
   * @param branchName The base name for the systematic branch
   * @return std::vector<std::string> A list of all of the systematic names
   */
  std::vector<std::string> makeSystList(std::string branchName);

  /**
   * @brief Book a systematic and associate it with affected variables.
   * @param syst Name of the systematic
   * @param affectedVariables Variables affected by the systematic
   */
  void bookSystematic(std::string syst,
                      std::vector<std::string> &affectedVariables);

private:
  /// Verbosity level for logging and debug output (higher = more verbose)
  UInt_t verbosityLevel_m;

  /// Map of configuration key-value pairs loaded from the config file
  std::unordered_map<std::string, std::string> configMap_m;
  /// Map of BDT names to FastForest objects
  std::unordered_map<std::string, std::shared_ptr<fastforest::FastForest>> bdts_m;
  /// Map of BDT input features by BDT name
  std::unordered_map<std::string, std::vector<std::string>> bdt_features_m;
  /// Map of BDT run variables by BDT name
  std::unordered_map<std::string, std::string> bdt_runVars_m;
  /// Map of correction names to correctionlib Correction references
  std::unordered_map<std::string, correction::Correction::Ref> corrections_m;
  /// Map of correction input features by correction name
  std::unordered_map<std::string, std::vector<std::string>> correction_features_m;

  /// Map of triggers by trigger name
  std::unordered_map<std::string, std::vector<std::string>> triggers_m;
  /// Map of trigger samples by sample type
  std::unordered_map<std::string, std::string> trigger_samples_m;
  /// Map of trigger vetoes by trigger name
  std::unordered_map<std::string, std::vector<std::string>> trigger_vetos_m;

  /// Static map of TH1F histograms by name
  std::unordered_map<std::string, std::shared_ptr<TH1F>> th1f_m;
  /// Static map of TH2F histograms by name
  std::unordered_map<std::string, std::shared_ptr<TH2F>> th2f_m;
  /// Static map of TH1D histograms by name
  std::unordered_map<std::string, std::shared_ptr<TH1D>> th1d_m;
  /// Static map of TH2D histograms by name
  std::unordered_map<std::string, std::shared_ptr<TH2D>> th2d_m;

  /// List of N-dimensional histogram result pointers
  std::vector<ROOT::Detail::RDF::RResultPtr<THnSparseD>> histos_m;

  /// List of TChain pointers for input files
  std::vector<std::unique_ptr<TChain>> chain_vec_m;
  /// Map of systematics to variables they affect
  std::unordered_map<std::string, std::unordered_set<std::string>> systematicToVariableMap_m;
  /// Map of variables to systematics that affect them
  std::unordered_map<std::string, std::unordered_set<std::string>> variableToSystematicMap_m;
  /// The underlying RDataFrame node for the analysis
  ROOT::RDF::RNode df_m;

  /**
   * @brief Define a new variable in the dataframe (internal helper).
   * @tparam F Callable type for the variable definition
   * @param name Name of the new variable
   * @param f Callable to compute the variable
   * @param columns Input columns for the callable
   */
  template <typename F>
  void Define_m(std::string name, F f,
                const std::vector<std::string> &columns = {}) {
    df_m = df_m.Define(name, f, columns);
  }

  /**
   * @brief Define a vector variable in the dataframe (internal helper).
   * @tparam T Data type of the vector
   * @param name Name of the variable
   * @param columns Input columns
   */
  template <typename T>
  void Define_m(std::string name,
                const std::vector<std::string> &columns = {}) {
    std::string arrayString = "ROOT::VecOps::RVec<Double_t>({";
    for (long unsigned int i = 0; i < columns.size() - 1; i++) {
      arrayString += columns[i] + ",";
    }
    arrayString += columns[columns.size() - 1] + "})";
    df_m = df_m.Define(name, arrayString);
  }

  /**
   * @brief Define a vector variable in the dataframe with explicit type (internal helper).
   * @param name Name of the variable
   * @param columns Input columns
   * @param type Data type as a string (e.g., "Float_t")
   */
  void Define_m(std::string name, const std::vector<std::string> &columns = {},
                std::string type = "Float_t") {
    std::string arrayString = "ROOT::VecOps::RVec<" + type + ">({";
    for (long unsigned int i = 0; i < columns.size() - 1; i++) {
      arrayString += +"static_cast<" + type + ">(" + columns[i] + "),";
    }

    arrayString += columns[columns.size() - 1] + "})";
    df_m = df_m.Define(name, arrayString);
  }

  /**
   * @brief Redefine an existing variable in the dataframe (internal helper).
   * @tparam F Callable type for the variable definition
   * @param name Name of the variable to redefine
   * @param f Callable to compute the variable
   * @param columns Input columns for the callable
   */
  template <typename F>
  void Redefine_m(std::string name, F f,
                  const std::vector<std::string> &columns = {}) {
    df_m = df_m.Redefine(name, f, columns);
  }

  /**
   * @brief Define a filter (selection) in the dataframe (internal helper).
   * @tparam F Callable type for the filter
   * @param f Callable to compute the filter
   * @param columns Input columns for the callable
   */
  template <typename F>
  void Filter_m(F f, const std::vector<std::string> &columns = {}) {
    df_m = df_m.Filter(f, columns);
  }

  /**
   * @brief Define a variable per sample (internal helper).
   * @tparam F Callable type for the variable definition
   * @param name Name of the variable
   * @param f Callable to compute the variable
   */
  template <typename F> void DefinePerSample_m(std::string name, F f) {
    df_m = df_m.DefinePerSample(name, f);
  }

  /**
   * @brief Read floatConfig and intConfig to register constant ints and floats.
   *
   * These are registered in the main config files as:
   * floatConfig=cfg/floats.txt
   * intConfig=cfg/ints.txt
   *
   * Within each of the files a constant is added like
   *
   * newConstant=3
   *
   */
  void registerConstants();

  /**
   * @brief Read aliasConfig to register aliases.
   *
   * This is registered in the main config files as:
   * aliasConfig=cfg/alias.txt
   *
   * Within each of the files an alias is added like
   *
   * newName=ptNew existingName=ptOld
   */
  void registerAliases();

  /**
   * @brief Read optionalBranchesConfig to register aliases.
   *
   * This is registered in the main config files as:
   * optionalBranchesConfig=cfg/optionalBranches.txt
   *
   * Within each of the files an optional branch is added like
   *
   * name=ptNew type=6 default=3.2
   *
   * The types are set as
   * types:  unsigned int=0, int=1, unsigned short=2, short=3, unsigned char=4,
   * char=5, float=6, double=7, bool=8 RVec +10;
   *
   * When using ROOT >=6.34 this will use DefaultValueFor which allows the
   * mixing of exisiting and non existing branches. Otherwise, it checks if the
   * branch exists and defines it, thus it won't work if it's only defined for
   * some branches.
   */
  void registerOptionalBranches();

  /**
   * @brief Read histConfig to register ROOT histograms.
   *
   * This is registered in the main config files as:
   * histConfig=cfg/hists.txt
   *
   * Within each of the files a histogram is added like
   *
   * file=aux/hists.root hists=TH2D:NLO_stitch_22:NLO_stitch_22
   *
   * The hists can be TH1F, TH2F, TH1D, or TH2D. The colons separate the
   * histogram type, the name of the histogram in the file and the name that
   * will be used to access the histogram in RDFAnalyzer
   *
   * This will likely be replaced by Correctionlib in the future
   */
  void registerHistograms();

  /**
   * @brief Read correctionlibConfig to register correctionlib corrections.
   *
   * This is registered in the main config files as:
   * correctionlibConfig=cfg/corrections.txt
   *
   * Each entry in the file should have the following keys:
   *   file=<json file with corrections> correctionName=<name in file>
   * name=<name to use in analysis> inputVariables=<comma-separated list>
   *
   * For each entry, loads the correction from the specified file, registers it
   * under the given name, and stores the list of input variables for use in the
   * analysis.
   */
  void registerCorrectionlib();

  /**
   * @brief Read bdtConfig to register BDTs for use in the analysis.
   *
   * This is registered in the main config files as:
   * bdtConfig=cfg/bdts.txt
   *
   * Each entry in the file should have the following keys:
   *   file=<txt file with BDT> name=<name to use in analysis>
   * inputVariables=<comma-separated list> runVar=<variable name>
   *
   * For each entry, loads the BDT from the specified file, registers it under
   * the given name, and stores the list of input variables and run variable for
   * use in the analysis.
   *
   * This will likely be replaced by ONNX in the future.
   */
  void registerBDTs();

  /**
   * @brief Read triggerConfig to register triggers and trigger vetos for use in
   * the analysis.
   *
   * Trigger vetos are used to prevent signal events from being counted twice in
   * the anlaysis. Different data files can in theory have the same events, so
   * we can only accept events that have not already been accepted.
   *
   * This is registered in the main config files as:
   * triggerConfig=cfg/triggers.txt
   *
   * Each entry in the file should have the following keys:
   *   name=<trigger group name> sample=<sample type> triggers=<comma-separated
   * list> [triggerVetos=<comma-separated list>]
   *
   * For each entry, registers the triggers and optional trigger vetos under the
   * given name, and associates the trigger group with the specified sample
   * type.
   */
  void registerTriggers();

  /** @brief Read existingSystematicsConfig to register existing systematic
   * corrections.
   *
   * This is registered in the main config files as:
   * existingSystematicsConfig=cfg/existingSystematics.txt
   *
   * Within each of the files each line contains just the name of an existing
   * systematic. For example if there are MassScaleUp and MassScaleDown
   * systematics, you would enter
   *
   * MassScale
   *
   * on its own line in the config file. This code will find all variables
   * affected by this systematic and register them. Note that if only looks for
   * the Up systematics, so both Up and Down need to be defined, and this won't
   * be checked.
   */
  void registerExistingSystematics();

  /**
   * @brief Add a BDT to the analysis.
   * @param key Name of the BDT
   * @param fileName Path to the BDT file
   * @param featureList List of input features
   * @param runVar Name of the run variable
   */
  void addBDT(std::string key, std::string fileName,
              std::vector<std::string> featureList, std::string runVar);

  /**
   * @brief Add a correction to the analysis.
   * @param key Name of the correction
   * @param fileName Path to the correction file
   * @param correctionName Name of the correction in the file
   * @param featureList List of input features
   */
  void addCorrection(std::string key, std::string fileName,
                     std::string correctionName,
                     std::vector<std::string> featureList);

  /**
   * @brief Add a trigger to the analysis.
   * @param key Name of the trigger
   * @param triggers List of trigger names
   */
  void addTrigger(std::string key, std::vector<std::string> triggers);
};

#endif
