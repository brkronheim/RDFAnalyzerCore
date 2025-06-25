/*
 * @file analyzer.cc
 * @brief Implementation of the Analyzer class for event analysis using ROOT's RDataFrame.
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
#include <TH1F.h>
#include <TH2F.h>
#include <memory>
#include <string>

#include <analyzer.h>
#include <correction.h>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <util.h>
#include <configParser.h>

/**
 * @brief Construct a new Analyzer object
 *
 * @param configFile File containing the configuration information for this
 * analyzer
 */
Analyzer::Analyzer(std::string configFile)
    : configMap_m(ConfigParser::processTopLevelConfig(configFile)),
      chain_vec_m(makeTChain(configMap_m)),
      df_m(ROOT::RDataFrame(*chain_vec_m[0])) {

  // Set verbosity level
  if (configMap_m.find("verbosity") != configMap_m.end()) {
    verbosityLevel_m = std::stoi(configMap_m["verbosity"]);
  } else {
    verbosityLevel_m = 1;
  }

// Display a progress bar depending on batch status and ROOT version
#if defined(HAS_ROOT_PROGRESS_BAR)
  if (configMap_m.find("batch") == configMap_m.end()) {
    ROOT::RDF::Experimental::AddProgressBar(df_m);
  } else if (configMap_m["batch"] != "True") {
    ROOT::RDF::Experimental::AddProgressBar(df_m);
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

  registerConstants();
  registerAliases();
  registerOptionalBranches();
  registerHistograms();
  registerCorrectionlib();
  registerExistingSystematics();
  registerBDTs();
  registerTriggers();

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
  Float_t eval(ROOT::VecOps::RVec<Float_t> & inputVector);

  // Get a vector of teh BDT inputs
  DefineVector("input_" + BDTName, bdt_features_m[BDTName], "Float_t");

  // Get the BDT and make an execution lambda
  auto bdt = bdts_m[BDTName];
  auto bdtLambda = [bdt](ROOT::VecOps::RVec<Float_t> &inputVector,
                             bool runVar) -> Float_t {
    if (runVar) {
      return (1. / (1. + std::exp(-((*bdt)(inputVector.data())))));
    } else {
      return (-1);
    }
  };

  // Define the BDT output
  Define(BDTName, bdtLambda, {"input_" + BDTName, bdt_runVars_m[BDTName]});
  return (this);
}

Analyzer *Analyzer::ApplyAllBDTs() {
  for (auto &pair : bdts_m) {
    ApplyBDT(pair.first);
  }
  return (this);
}

/**
 * @brief Read histogram using value to find binand store in a branch.
 * @param histName Name of the histogram
 * @param outputBranchName Name of the output branch
 * @param inputBranchNames Input branches for lookup
 * @return Pointer to this Analyzer (for chaining)
 */
Analyzer *Analyzer::readHistVals(std::string histName,
                                 std::string outputBranchName,
                                 std::vector<std::string> inputBranchNames) {
  if (inputBranchNames.size() == 1) {
    if (th1f_m.find(histName) != th1f_m.end()) {
      auto hist = th1f_m[histName];
      auto histLambda = [hist](Float_t val1) -> Float_t {
        auto bin1 = hist->GetXaxis()->FindBin(val1);
        return (hist->GetBinContent(bin1));
      };
      Define(outputBranchName, histLambda, inputBranchNames);
    } else if (th1d_m.find(histName) != th1d_m.end()) {
      auto hist = th1d_m[histName];
      auto histLambda = [hist](Float_t val1) -> Float_t {
        auto bin1 = hist->GetXaxis()->FindBin(val1);
        return (hist->GetBinContent(bin1));
      };
      Define(outputBranchName, histLambda, inputBranchNames);
    } else {
      std::cerr << "th1 val error" << std::endl;
    }
  } else if (inputBranchNames.size() == 2) {
    if (th2f_m.find(histName) != th2f_m.end()) {
      auto hist = th2f_m[histName];
      auto histLambda = [hist](Float_t val1, Float_t val2) -> Float_t {
        auto bin1 = hist->GetXaxis()->FindBin(val1);
        auto bin2 = hist->GetYaxis()->FindBin(val2);
        return (hist->GetBinContent(bin1, bin2));
      };
      Define(outputBranchName, histLambda, inputBranchNames);
    } else if (th2d_m.find(histName) != th2d_m.end()) {
      auto hist = th2d_m[histName];
      auto histLambda = [hist](Float_t val1, Float_t val2) -> Float_t {
        auto bin1 = hist->GetXaxis()->FindBin(val1);
        auto bin2 = hist->GetYaxis()->FindBin(val2);
        return (hist->GetBinContent(bin1, bin2));
      };
      Define(outputBranchName, histLambda, inputBranchNames);
    } else {
      std::cerr << "th2 val error" << std::endl;
    }
  } else {
    std::cerr << "Error! Access of histogram " << histName
              << " attempted with invalid branche combination" << std::endl;
  }
  return (this);
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

  if (trigger_samples_m.find(configMap_m["type"]) !=
      configMap_m.end()) { // data, need to account for duplicate events from
                           // different datasets
    std::cout << "looking for trigger for " << configMap_m["type"] << std::endl;
    std::string triggerName = trigger_samples_m.at(configMap_m["type"]);
    std::cout << "trigger is " << triggerName << std::endl;

    for (const auto &trig : triggers_m[triggerName]) {
      std::cout << "Keep Data trigger: " << trig << std::endl;
    }

    for (const auto &trig : trigger_vetos_m[triggerName]) {
      std::cout << "Veto Data trigger: " << trig << std::endl;
    }

    if (trigger_vetos_m[triggerName].size() == 0) {
      std::cout << "No veto" << std::endl;

      DefineVector("allTriggersPassVector", triggers_m[triggerName], "Bool_t");
      Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});

    } else {
      DefineVector(triggerName + "passVector", triggers_m[triggerName],
                   "Bool_t");
      DefineVector(triggerName + "vetoVector", trigger_vetos_m[triggerName],
                   "Bool_t");

      Filter("applyTrigger", passTriggerAndVeto,
             {triggerName + "pass vector", triggerName + " veto vector"});
    }
  } else {                                // MC, no duplicate events
    std::vector<std::string> allTriggers; // merge all the triggers
    for (const auto &pair : triggers_m) {
      allTriggers.insert(allTriggers.end(), pair.second.begin(),
                         pair.second.end());
    }

    for (const auto &trig : allTriggers) {
      std::cout << "Keep MC trigger: " << trig << std::endl;
    }

    DefineVector("allTriggersPassVector", allTriggers, "Bool_t");
    Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});
  }
  return (this);
}

ROOT::RDF::RNode Analyzer::getDF() { return (df_m); }

std::string Analyzer::configMap(std::string key) {
  if (configMap_m.find(key) != configMap_m.end()) {
    return (configMap_m[key]);
  } else {
    return ("");
  }
}

Analyzer *Analyzer::DefineVector(std::string name,
                                 const std::vector<std::string> &columns,
                                 std::string type) {
  // Find all systematics that affect this new variable
  std::unordered_set<std::string> systsToApply;
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
  }

  // Define nominal variation
  Define_m(name, columns, type);

  // If systematics were found, define a new variable for each up and down
  // variation.
  for (auto &syst : systsToApply) {
    std::vector<std::string> systColumnsUp;
    std::vector<std::string> systColumnsDown;
    for (auto var : columns) { // check all variables in column, replace ones
                               // affected by systematics
      if (systematicToVariableMap_m[syst].find(var) ==
          systematicToVariableMap_m[syst].end()) {
        systColumnsUp.push_back(var);
        systColumnsDown.push_back(var);
      } else {
        systColumnsUp.push_back(var + "_" + syst + "Up");
        systColumnsDown.push_back(var + "_" + syst + "Down");
      }
    }
    // Define up and down variations for current systematic

    // Define nominal variation
    Define_m(name + "_" + syst + "Up", systColumnsUp, type);
    // Define nominal variation
    Define_m(name + "_" + syst + "Down", systColumnsDown, type);
  }

  return (this);
}

Analyzer *Analyzer::ApplyCorrection(std::string correctionName,
                                    std::vector<std::string> stringArguments) {
  Float_t eval(ROOT::VecOps::RVec<Float_t> & inputVector);

  // Get a vector of teh BDT inputs
  DefineVector("input_" + correctionName, correction_features_m[correctionName],
               "double");

  // Get the BDT and make an execution lambda
  // auto bdt = bdts_m[BDTName];
  //
  /*int i = 0;
  for( const auto &val : corrections_m[correctionName]->inputs().typeStr()){
      std::cout << "input " << i << " is " << val << std::endl;
      i++;
  }*/

  auto correction = corrections_m[correctionName];

  auto correctionLambda =
      [correction,
       stringArguments](ROOT::VecOps::RVec<double> &inputVector) -> Float_t {
    std::vector<std::variant<int, double, std::string>> values;

    auto stringArgIt = stringArguments.begin();
    auto doubleArgIt = inputVector.begin();

    for (const auto &varType : correction->inputs()) {
      if (varType.typeStr() == "string") {
        values.push_back(*stringArgIt);
        stringArgIt++;

      } else if (varType.typeStr() == "int") {
        values.push_back(int(*doubleArgIt));
        doubleArgIt++;
      } else {
        values.push_back(*doubleArgIt);
        doubleArgIt++;
      }
    }

    return (correction->evaluate(values));
  };

  // Define the BDT output
  Define(correctionName, correctionLambda, {"input_" + correctionName});

  return (this);
}

/**
 * @brief Save the configured branches to the output file and trigger the
 * computation of the dataframe.
 *
 * @return Pointer to this Analyzer (for chaining)
 */
Analyzer *Analyzer::save() {
  saveDF(df_m, configMap_m, variableToSystematicMap_m);
  return (this);
}

/**
 * @brief Read histogram using bin indices and store in a branch.
 *
 * @param histName Name of the histogram
 * @param outputBranchName Name of the output branch
 * @param inputBranchNames Input branches for lookup
 * @return Pointer to this Analyzer (for chaining)
 */
Analyzer *Analyzer::readHistBins(std::string histName,
                                 std::string outputBranchName,
                                 std::vector<std::string> inputBranchNames) {
  if (inputBranchNames.size() == 1) {
    if (th1f_m.find(histName) != th1f_m.end()) {
      auto hist = th1f_m[histName];
      auto histLambda = [hist](Int_t bin1) -> Float_t {
        return (hist->GetBinContent(bin1 + 1));
      };
      Define(outputBranchName, histLambda, inputBranchNames);
    } else if (th1d_m.find(histName) != th1d_m.end()) {
      auto hist = th1d_m[histName];
      auto histLambda = [hist](Int_t bin1) -> Float_t {
        return (hist->GetBinContent(bin1 + 1));
      };
      Define(outputBranchName, histLambda, inputBranchNames);
    } else {
      std::cerr << "th1 bin error" << std::endl;
    }
  } else if (inputBranchNames.size() == 2) {
    if (th2f_m.find(histName) != th2f_m.end()) {
      auto hist = th2f_m[histName];
      auto histLambda = [hist](Int_t bin1, Int_t bin2) -> Float_t {
        return (hist->GetBinContent(bin1 + 1, bin2 + 1));
      };
      Define(outputBranchName, histLambda, inputBranchNames);
    } else if (th2d_m.find(histName) != th2d_m.end()) {
      auto hist = th2d_m[histName];
      auto histLambda = [hist](Int_t bin1, Int_t bin2) -> Float_t {
        return (hist->GetBinContent(bin1 + 1, bin2 + 1));
      };
      Define(outputBranchName, histLambda, inputBranchNames);
    } else {
      std::cerr << "th2 bin error" << std::endl;
    }
  } else {
    std::cerr << "Error. Access of histogram " << histName
              << " attempted with invalid branch combination" << std::endl;
  }
  return (this);
}

/**
 * @brief Get the correcionlib correction at key
 *
 * @param key Key for the correctionlib object
 * @return correction::Correction::Ref shared pointer to a correctionlib object
 */
correction::Correction::Ref Analyzer::correctionMap(std::string key) {
  auto correction = corrections_m.at(key);
  return (correction);
}

/**
 * @brief Read floatConfig and intConfig to register constant ints and floats.
 *
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
void Analyzer::registerConstants() {
  if (configMap_m.find("floatConfig") != configMap_m.end()) {
    const std::string floatFile = configMap_m.at("floatConfig");
    auto floatConfig = ConfigParser::parsePairBasedConfig(floatFile);
    for (auto &pair : floatConfig) {
      SaveVar<Float_t>(std::stof(pair.second), pair.first);
    }
  }
  if (configMap_m.find("intConfig") != configMap_m.end()) {
    const std::string intFile = configMap_m.at("intConfig");
    auto intConfig = ConfigParser::parsePairBasedConfig(intFile);
    for (auto &pair : intConfig) {
      SaveVar<Int_t>(std::stof(pair.second), pair.first);
    }
  }
}

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
void Analyzer::registerAliases() {

  auto aliasConfig =
      ConfigParser::parseMultiKeyConfig(configMap_m, "aliasConfig", {"existingName", "newName"});

  const auto columnNames = df_m.GetColumnNames();

  for (auto &column : columnNames) {
    std::cout << column << ": " << df_m.GetColumnType(column) << std::endl;
  }

  for (const auto &entryKeys : aliasConfig) {
    df_m = df_m.Alias(entryKeys.at("newName"), entryKeys.at("existingName"));
  }
}

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
 * When using ROOT >=6.34 this will use DefaultValueFor which allows the mixing
 * of exisiting and non existing branches. Otherwise, it checks if the branch
 * exists and defines it, thus it won't work if it's only defined for some
 * branches.
 */
void Analyzer::registerOptionalBranches() {

  const auto aliasConfig = ConfigParser::parseMultiKeyConfig(configMap_m, "optionalBranchesConfig",
                                       {"name", "type", "default"});

#if defined(HAS_DEFAULT_VALUE_FOR)

  for (const auto &entryKeys : aliasConfig) {
    const int varType = std::stoi(entryKeys.at("type"));

    const auto defaultValStr = entryKeys.at("default");
    const auto varName = entryKeys.at("name");
    const Bool_t defaultBool = defaultValStr == "1" ||
                               defaultValStr == "true" ||
                               defaultValStr == "True";

    switch (varType) {
    case 0:
      df_m = df_m.DefaultValueFor<UInt_t>(varName, std::stoul(defaultValStr));
      break;
    case 1:
      df_m = df_m.DefaultValueFor<Int_t>(varName, std::stoi(defaultValStr));
      break;
    case 2:
      df_m = df_m.DefaultValueFor<UShort_t>(varName, std::stoul(defaultValStr));
      break;
    case 3:
      df_m = df_m.DefaultValueFor<Short_t>(varName, std::stoi(defaultValStr));
      break;
    case 4:
      df_m = df_m.DefaultValueFor<UChar_t>(varName,
                                           UChar_t(std::stoul(defaultValStr)));
      break;
    case 5:
      df_m = df_m.DefaultValueFor<Char_t>(varName,
                                          Char_t(std::stoi(defaultValStr)));
      break;
    case 6:
      df_m = df_m.DefaultValueFor<Float_t>(varName, std::stof(defaultValStr));
      break;
    case 7:
      df_m = df_m.DefaultValueFor<Double_t>(varName, std::stod(defaultValStr));
      break;
    case 8:
      df_m = df_m.DefaultValueFor<Bool_t>(varName, defaultBool);
      break;
    case 10:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UInt_t>>(
          varName, {static_cast<UInt_t>(std::stoul(defaultValStr))});
      break;
    case 11:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Int_t>>(
          varName, {std::stoi(defaultValStr)});
      break;
    case 12:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UShort_t>>(
          varName, {static_cast<UShort_t>(std::stoul(defaultValStr))});
      break;
    case 13:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Short_t>>(
          varName, {static_cast<Short_t>(std::stoi(defaultValStr))});
      break;
    case 14:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UChar_t>>(
          varName, {UChar_t(std::stoul(defaultValStr))});
      break;
    case 15:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Char_t>>(
          varName, {Char_t(std::stoi(defaultValStr))});
      break;
    case 16:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Float_t>>(
          varName, {std::stof(defaultValStr)});
      break;
    case 17:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Double_t>>(
          varName, {std::stod(defaultValStr)});
      break;
    case 18:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Bool_t>>(varName,
                                                              {defaultBool});
      break;
    }
  }
#else
  const auto columnNames = df_m.GetColumnNames();
  std::unordered_set<std::string> columnSet;
  for (auto &column : columnNames) {
    columnSet.insert(column);
  }

  for (const auto &entryKeys : aliasConfig) {
    if (columnSet.find(entryKeys.at("name")) ==
        columnSet.end()) { // Add the branch if it doesn't exist
      const int varType = std::stoi(entryKeys.at("type"));

      // types:  unsigned int=0, int=1, unsigned short=2, short=3, unsigned
      // char=4, char=5, float=6, double=7, bool=8
      //     RVec +10;

      const auto defaultValStr = entryKeys.at("default");
      const auto varName = entryKeys.at("name");
      const Bool_t defaultBool = defaultValStr == "1" ||
                                 defaultValStr == "true" ||
                                 defaultValStr == "True";

      switch (varType) {
      case 0:
        SaveVar<UInt_t>(std::stoul(defaultValStr), varName);
        break;
      case 1:
        SaveVar<Int_t>(std::stoi(defaultValStr), varName);
        break;
      case 2:
        SaveVar<UShort_t>(std::stoul(defaultValStr), varName);
        break;
      case 3:
        SaveVar<Short_t>(std::stoi(defaultValStr), varName);
        break;
      case 4:
        SaveVar<UChar_t>(UChar_t(std::stoul(defaultValStr)), varName);
        break;
      case 5:
        SaveVar<Char_t>(Char_t(std::stoi(defaultValStr)), varName);
        break;
      case 6:
        SaveVar<Float_t>(std::stof(defaultValStr), varName);
        break;
      case 7:
        SaveVar<Double_t>(std::stod(defaultValStr), varName);
        break;
      case 8:
        SaveVar<Bool_t>(defaultBool, varName);
        break;
      case 10:
        SaveVar<ROOT::VecOps::RVec<UInt_t>>(
            {static_cast<UInt_t>(std::stoul(defaultValStr))}, varName);
        break;
      case 11:
        SaveVar<ROOT::VecOps::RVec<Int_t>>({std::stoi(defaultValStr)}, varName);
        break;
      case 12:
        SaveVar<ROOT::VecOps::RVec<UShort_t>>(
            {static_cast<UShort_t>(std::stoul(defaultValStr))}, varName);
        break;
      case 13:
        SaveVar<ROOT::VecOps::RVec<Short_t>>(
            {static_cast<Short_t>(std::stoi(defaultValStr))}, varName);
        break;
      case 14:
        SaveVar<ROOT::VecOps::RVec<UChar_t>>(
            {UChar_t(std::stoul(defaultValStr))}, varName);
        break;
      case 15:
        SaveVar<ROOT::VecOps::RVec<Char_t>>({Char_t(std::stoi(defaultValStr))},
                                            varName);
        break;
      case 16:
        SaveVar<ROOT::VecOps::RVec<Float_t>>({std::stof(defaultValStr)},
                                             varName);
        break;
      case 17:
        SaveVar<ROOT::VecOps::RVec<Double_t>>({std::stod(defaultValStr)},
                                              varName);
        break;
      case 18:
        SaveVar<ROOT::VecOps::RVec<Bool_t>>({defaultBool}, varName);
        break;
      }
    }
  }
#endif
}

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
 * The hists can be TH1F, TH2F, TH1D, or TH2D. The colons separate the histogram
 * type, the name of the histogram in the file and the name that will be used to
 * access the histogram in RDFAnalyzer
 *
 * This will likely be replaced by Correctionlib in the future
 */
void Analyzer::registerHistograms() {

  const auto histConfig =
      ConfigParser::parseMultiKeyConfig(configMap_m, "histConfig", {"file", "hists"});

  for (const auto &entryKeys : histConfig) {

    TFile *file(TFile::Open(entryKeys.at("file").c_str()));
    auto histVector = ConfigParser::splitString(entryKeys.at("hists"), ",");

    for (auto hist : histVector) {
      auto histData = ConfigParser::splitString(hist, ":");
      if (histData.size() == 3) {
        if (histData[0] == "TH1F") {
          TH1F *histPtr = file->Get<TH1F>(histData[1].c_str());
          histPtr->SetDirectory(0);
          th1f_m.emplace(histData[2], histPtr);
        } else if (histData[0] == "TH2F") {
          TH2F *histPtr = file->Get<TH2F>(histData[1].c_str());
          histPtr->SetDirectory(0);
          th2f_m.emplace(histData[2], histPtr);
        } else if (histData[0] == "TH1D") {
          TH1D *histPtr = file->Get<TH1D>(histData[1].c_str());
          histPtr->SetDirectory(0);
          th1d_m.emplace(histData[2], histPtr);
        } else if (histData[0] == "TH2D") {
          TH2D *histPtr = file->Get<TH2D>(histData[1].c_str());
          histPtr->SetDirectory(0);
          th2d_m.emplace(histData[2], histPtr);
        }
      }
    }

    delete file;
  }
}

/**
 * @brief Read correctionlibConfig to register correctionlib corrections.
 *
 * This is registered in the main config files as:
 * correctionlibConfig=cfg/corrections.txt
 *
 * Each entry in the file should have the following keys:
 *   file=<json file with corrections> correctionName=<name in file> name=<name
 * to use in analysis> inputVariables=<comma-separated list>
 *
 * For each entry, loads the correction from the specified file, registers it
 * under the given name, and stores the list of input variables for use in the
 * analysis.
 */
void Analyzer::registerCorrectionlib() {

  const auto correctionConfig =
      ConfigParser::parseMultiKeyConfig(configMap_m, "correctionlibConfig",
                  {"file", "correctionName", "name", "inputVariables"});

  for (const auto &entryKeys : correctionConfig) {

    // Split the variable list on commas, save to vector
    auto inputVariableVector = ConfigParser::splitString(entryKeys.at("inputVariables"), ",");

    // register as a correction

    // load correction object from json
    auto correctionF =
        correction::CorrectionSet::from_file(entryKeys.at("file"));
    auto correction = correctionF->at(entryKeys.at("correctionName"));

    // Add the correction and feature list to their maps
    std::cout << "Adding correction " << entryKeys.at("name") << "!"
              << std::endl;
    corrections_m.emplace(entryKeys.at("name"), correction);
    correction_features_m.emplace(entryKeys.at("name"), inputVariableVector);
  }
}

/**
 * @brief Read existingSystematicsConfig to register existing systematic
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
 * the Up systematics, so both Up and Down need to be defined, and this won't be
 * checked.
 */
void Analyzer::registerExistingSystematics() {

  std::string existingSystematicsConfig;
  if (configMap_m.find("existingSystematicsConfig") != configMap_m.end()) {
      existingSystematicsConfig = configMap_m.at("existingSystematicsConfig");
  }
  const auto systConfig =
      ConfigParser::parseVectorConfig(existingSystematicsConfig);
  const auto columnList = df_m.GetColumnNames();

  for (auto &syst : systConfig) { // process each line as a systematic

    const std::string upSyst = syst + "Up"; // Up version of systematic

    // Process a line if it has non zero length
    if (syst.size() > 0) {

      std::unordered_set<std::string> systVariableSet;

      // Register the systematic with the analyzer by checking all the existing
      // branches for variations of this systematic
      for (const auto &existingVars : columnList) {

        const size_t index = existingVars.find(upSyst);

        // Determine if the start of the upSyst is the correct distance from the
        // end of the string name
        if (index != std::string::npos &&
            index + upSyst.size() == existingVars.size() && index != 0) {

          // Get the base variable name
          const std::string systVar = existingVars.substr(0, index - 1);

          // register the systematics
          if (variableToSystematicMap_m.find(systVar) !=
              variableToSystematicMap_m.end()) {
            variableToSystematicMap_m[systVar].insert(syst);
          } else {
            variableToSystematicMap_m[systVar] = {syst};
          }

          systVariableSet.insert(systVar);
        }
      }
      systematicToVariableMap_m[syst] = systVariableSet;
    }
  }
}

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
 * For each entry, loads the BDT from the specified file, registers it under the
 * given name, and stores the list of input variables and run variable for use
 * in the analysis.
 *
 * This will likely be replaced by ONNX in the future.
 */
void Analyzer::registerBDTs() {

  const auto bdtConfig = ConfigParser::parseMultiKeyConfig(
      configMap_m, "bdtConfig", {"file", "name", "inputVariables", "runVar"});

  for (const auto &entryKeys : bdtConfig) {

    // Split the variable list on commas, save to vector
    auto inputVariableVector = ConfigParser::splitString(entryKeys.at("inputVariables"), ",");

    // Add BDT

    std::vector<std::string> features;
    for (long unsigned int i = 0; i < inputVariableVector.size(); i++) {
      features.push_back("f" + std::to_string(i));
    }

    // Load the BDT
    auto bdt = fastforest::load_txt(entryKeys.at("file"), features);

    // Add the BDT and feature list to their maps
    bdts_m.emplace(entryKeys.at("name"), &bdt);
    bdt_features_m.emplace(entryKeys.at("name"), inputVariableVector);
    bdt_runVars_m.emplace(entryKeys.at("name"), entryKeys.at("runVar"));
  }
}

/**
 * @brief Read triggerConfig to register triggers and trigger vetos for use in
 * the analysis.
 *
 * Trigger vetos are used to prevent signal events from being counted twice in
 * the anlaysis. Different data files can in theory have the same events, so we
 * can only accept events that have not already been accepted.
 *
 * This is registered in the main config files as:
 * triggerConfig=cfg/triggers.txt
 *
 * Each entry in the file should have the following keys:
 *   name=<trigger group name> sample=<sample type> triggers=<comma-separated
 * list> [triggerVetos=<comma-separated list>]
 *
 * For each entry, registers the triggers and optional trigger vetos under the
 * given name, and associates the trigger group with the specified sample type.
 */
void Analyzer::registerTriggers() {

  const auto triggerConfig =
      ConfigParser::parseMultiKeyConfig(configMap_m, "triggerConfig", {"name", "sample", "triggers"});

  for (const auto &entryKeys : triggerConfig) {

    auto triggerList = ConfigParser::splitString(entryKeys.at("triggers"), ",");

    if (entryKeys.find("triggerVetos") != entryKeys.end()) {
      auto triggerVetoList = ConfigParser::splitString(entryKeys.at("triggerVetos"), ",");
      trigger_vetos_m.emplace(entryKeys.at("name"), triggerVetoList);
    } else {
      trigger_vetos_m[entryKeys.at("name")] = {};
    }

    triggers_m.emplace(entryKeys.at("name"), triggerList);
    trigger_samples_m.emplace(entryKeys.at("sample"), entryKeys.at("name"));
  }
}

/**
 * @brief Book N-dimensional histograms for a set of selections and regions.
 * @param infos Histogram info objects
 * @param selection Selection info objects
 * @param suffix Suffix for histogram names
 * @param allRegionNames List of region names
 */
void Analyzer::bookND(std::vector<histInfo> &infos,
                      std::vector<selectionInfo> &selection, std::string suffix,
                      std::vector<std::vector<std::string>> &allRegionNames) {

  // Store the selection info in some vectors
  std::vector<int> binVectorBase;
  std::vector<double> lowerBoundVectorBase;
  std::vector<double> upperBoundVectorBase;
  std::vector<std::string> varVectorBase;

  for (auto const &selectionInfo : selection) {
    binVectorBase.push_back(selectionInfo.bins());
    lowerBoundVectorBase.push_back(selectionInfo.lowerBound());
    upperBoundVectorBase.push_back(selectionInfo.upperBound());
    varVectorBase.push_back(selectionInfo.variable());
  }

  for (auto const &info : infos) {
    std::string newName = info.name() + "." + suffix; // name of hist info
    std::vector<int> binVector(binVectorBase); // vectors from selectionInfos
    std::vector<double> lowerBoundVector(lowerBoundVectorBase);
    std::vector<double> upperBoundVector(upperBoundVectorBase);
    std::vector<std::string> varVector(varVectorBase);
    // add selection info from the histInfos object
    binVector.push_back(info.bins());
    lowerBoundVector.push_back(info.lowerBound());
    upperBoundVector.push_back(info.upperBound());
    varVector.push_back(info.variable());
    varVector.push_back(info.weight());

    // Book the THnSparseD histo
    // const ROOT::RDF::THnSparseDModel tempModel(newName.c_str(),
    // newName.c_str(), selection.size()+1,binVector.data(),
    // lowerBoundVector.data(), upperBoundVector.data());
    //
    Int_t numFills = 1;
    std::vector<std::string> systVector(varVector);
    for (const auto &syst : allRegionNames[allRegionNames.size() - 1]) {
      std::cout << syst << std::endl;
      std::string systBase = syst;
      if (syst.find("Up") != std::string::npos) {
        systBase = systBase.substr(0, syst.find("Up"));
      }
      if (syst.find("Down") != std::string::npos) {
        systBase = systBase.substr(0, syst.find("Down"));
      }
      if (syst == "Nominal") {
        continue;
      }
      const auto varSet = systematicToVariableMap_m[systBase];
      std::cout << "Affected variables for " << systBase << ": " << std::endl;
      for (const auto &var : varSet) {
        std::cout << var << std::endl;
      }
      Int_t affectedVariables = 0;
      std::vector<std::string> newVec;
      for (const auto &branch : varVector) {

        if (varSet.count(branch) != 0) {
          affectedVariables += 1;
          std::cout << branch + "_" + syst << std::endl;
          newVec.push_back(branch + "_" + syst);
        } else {
          std::cout << branch << std::endl;
          newVec.push_back(branch);
        }
      }
      if (affectedVariables > 1) {
        systVector.insert(systVector.end(), newVec.begin(), newVec.end());
        numFills++;
      }
    }

    std::string branchName = info.name() + "_" + suffix + "inputDoubleVector";
    this->DefineVector(branchName, systVector, "Double_t");
    THnMulti tempModel(df_m.GetNSlots(), newName.c_str(), newName.c_str(),
                       selection.size() + 1, numFills, binVector,
                       lowerBoundVector, upperBoundVector);
    histos_m.push_back(df_m.Book<ROOT::VecOps::RVec<Double_t>>(
        std::move(tempModel), {branchName}));

    // (newName+"inputDoubleVector").c_str()
    // tempModel.Exe

    // histos_m.emplace(histos_m.end(), df.HistoND<types..., float>(tempModel,
    // varVector));
  }
}

void Analyzer::save_hists(
    std::vector<std::vector<histInfo>> &fullHistList,
    std::vector<std::vector<std::string>> &allRegionNames) {
  std::string fileName = configMap("saveFile");
  std::vector<std::string> allNames;
  std::vector<std::string> allVariables;
  std::vector<std::string> allLabels;
  std::vector<int> allBins;
  std::vector<float> allLowerBounds;
  std::vector<float> allUpperBounds;
  // Get vectors of the hist names, variables, lables, bins, and bounds
  for (auto const &histList : fullHistList) {
    for (auto const &info : histList) {
      allNames.push_back(info.name());
      allVariables.push_back(info.variable());
      allLabels.push_back(info.label());
      allBins.push_back(info.bins());
      allLowerBounds.push_back(info.lowerBound());
      allUpperBounds.push_back(info.upperBound());
    }
  }

  // Open file
  TFile saveFile(fileName.c_str(), "RECREATE");

  // Save hists under hists
  // std::cout << df_m.Count().GetValue() << " Events processed" << std::endl;
  // // Trigger execution const Int_t histNumber = histos_m.size(); const Int_t
  // axisNumber = allRegionNames.size()+1;
  std::vector<Int_t> commonAxisSize = {};

  for (const auto &regionNameList : allRegionNames) {
    commonAxisSize.push_back(regionNameList.size());
  }

  int histIndex = 0;
  std::unordered_map<std::string, TH1F> histMap;
  std::unordered_set<std::string> dirSet;
  for (auto &histo_m : histos_m) {
    auto hist = histo_m.GetPtr();
    // const Int_t finalAxisSize = allBins[histIndex];
    const Int_t currentHistogramSize = hist->GetNbins();
    std::vector<Int_t> indices(commonAxisSize.size() + 1);
    for (int i = 0; i < currentHistogramSize; i++) {
      Float_t content = hist->GetBinContent(i, indices.data());
      if (content == 0) {
        continue;
      }
      Float_t error = hist->GetBinError2(i);
      /*std::cout << "Bin: " << indices << ": " << content << ", " << error  <<
      ": "; for(int j = 0; j< commonAxisSize.size()+1; j++){ std::cout <<
      indices[j] << ", ";
      }
      std::cout << std::endl;*/

      std::string dirName = "";
      // std::cout << "histName: " << histName
      const Int_t size = commonAxisSize.size() - 2;
      for (int i = 0; i < size; i++) {
        dirName += allRegionNames[i][indices[i] - 1] + "/";
        // std::cout << "histName: " << histName;
      }

      dirName += allRegionNames[commonAxisSize.size() - 2]
                               [indices[commonAxisSize.size() - 2] - 1];
      std::string histName = allVariables[histIndex];
      // bool isNominal=false;
      if (allRegionNames[commonAxisSize.size() - 1]
                        [indices[commonAxisSize.size() - 1] - 1] == "Nominal") {
        // isNominal= true;
      } else {
        histName += "_" +
                    allRegionNames[commonAxisSize.size() - 1]
                                  [indices[commonAxisSize.size() - 1] - 1];
      }
      if (histMap.count(dirName + "/" + histName) == 0) {
        histMap[dirName + "/" + histName] =
            TH1F(histName.c_str(),
                 (allVariables[histIndex] + ";" + allVariables[histIndex] +
                  ";Counts")
                     .c_str(),
                 allBins[histIndex], allLowerBounds[histIndex],
                 allUpperBounds[histIndex]);
        dirSet.emplace(dirName);
      }
      histMap[dirName + "/" + histName].SetBinContent(
          indices[commonAxisSize.size()], content);
      histMap[dirName + "/" + histName].SetBinError(
          indices[commonAxisSize.size()], sqrt(error));
    }

    histIndex++;
  }

  saveFile.cd();
  for (const auto &dirName : dirSet) {
    std::string newDir(dirName);
    newDir[dirName.find('/')] = '_';
    saveFile.mkdir(newDir.c_str());
  }
  std::unordered_map<std::string,
                     std::map<std::string, std::pair<Float_t, Float_t>>>
      systNormalizations;
  // store the nominal and sytematic normalization for each control region
  for (const auto &pair : histMap) {
    if (pair.first.find("Systematic") ==
        std::string::npos) { // Want the sytematic histogram
      continue;
    }
    std::string dirName = pair.first.substr(0, pair.first.find_last_of("/"));
    auto regionSplit = ConfigParser::splitString(pair.first, "/");
    std::string region =
        regionSplit[0] + "_" +
        regionSplit[2]; //  dirName.substr(0,dirName.find("/"));
    std::string histName =
        regionSplit[regionSplit.size() -
                    1]; // pair.first.substr(pair.first.find_last_of("/")+1);
    std::string nominalName = histName;
    std::string systName = "nominal";
    if (histName.find("Up") != std::string::npos ||
        histName.find("Down") != std::string::npos) {
      systName = nominalName.substr(nominalName.find_last_of("_") + 1);
      nominalName = nominalName.substr(0, nominalName.find_last_of("_"));
    }
    std::cout << nominalName << ", " << systName << std::endl;
    nominalName = dirName + "/" + nominalName;
    auto nominalHist = histMap[nominalName];
    Float_t nominalIntegral = nominalHist.Integral();
    Float_t systIntegral = pair.second.Integral();
    if (systNormalizations[region].count(systName) == 0) {
      systNormalizations[region][systName].first = nominalIntegral;
      systNormalizations[region][systName].second = systIntegral;
    } else {
      systNormalizations[region][systName].first += nominalIntegral;
      systNormalizations[region][systName].second += systIntegral;
    }
  }

  for (const auto &pair : histMap) {
    std::string dirName = pair.first.substr(0, pair.first.find_last_of("/"));
    auto regionSplit = ConfigParser::splitString(pair.first, "/");
    std::string region =
        regionSplit[0] + "_" +
        regionSplit[2]; //  dirName.substr(0,dirName.find("/"));
    std::string histName =
        regionSplit[regionSplit.size() -
                    1]; // pair.first.substr(pair.first.find_last_of("/")+1);
    std::string nominalName = histName;
    std::string systName = "nominal";
    if (histName.find("Up") != std::string::npos ||
        histName.find("Down") != std::string::npos) {
      systName = nominalName.substr(nominalName.find_last_of("_") + 1);
      nominalName = nominalName.substr(0, nominalName.find_last_of("_"));
    }
    nominalName = dirName + "/" + nominalName;
    std::cout << histName << ", " << systName << std::endl;
    std::string newDir(dirName);
    newDir[dirName.find('/')] = '_';
    saveFile.cd(newDir.c_str());
    pair.second.Write();
    //(pair.second*(systNormalizations[region][systName].first/systNormalizations[region][systName].second)).Write();
    //// Need to group this normalization the way combine wants it
    saveFile.cd();
  }
}

/**
 * @brief Make a systematic variation which is an indexing value for each
 * systematic. Return the names of all of these branches
 *
 * @param branchName The base name for the systematic branch
 * @return std::vector<std::string> A list of all of the systematic names
 */
std::vector<std::string> Analyzer::makeSystList(std::string branchName) {

  // Always have a nominal branch
  std::vector<std::string> systList = {"Nominal"};
  Int_t var = 0;
  this->SaveVar<Float_t>(var, branchName);

  // For each systematic add the Up and Down variations to the list and add
  // branches for them Register these as systematics
  for (const auto &pair : systematicToVariableMap_m) {
    systList.push_back(pair.first + "Up");
    systList.push_back(pair.first + "Down");
    var++;
    this->SaveVar<Float_t>(var, branchName + "_" + pair.first + "Up");
    var++;
    this->SaveVar<Float_t>(var, branchName + "_" + pair.first + "Down");
    systematicToVariableMap_m[pair.first].insert(branchName);
    variableToSystematicMap_m[branchName].insert(pair.first);
  }

  return (systList);
}

/**
 * @brief Map syst to affected variables and vice versa
 *
 * @param syst Name of syst (for PtScaleUp and PtScaleDown it would be PtScale)
 * @param affectedVariables List of all branches that have variations from this
 * systematic
 */
void Analyzer::bookSystematic(std::string syst,
                              std::vector<std::string> &affectedVariables) {

  std::unordered_set<std::string> systVariableSet;

  for (const auto &systVar : affectedVariables) {

    // Register the affected variable as one affected by this systematic
    systVariableSet.insert(systVar);

    // Add this systematic as one affecting the variable
    if (variableToSystematicMap_m.find(systVar) !=
        variableToSystematicMap_m.end()) {
      variableToSystematicMap_m[systVar].insert(syst);
    } else {
      variableToSystematicMap_m[systVar] = {syst};
    }
  }

  // Set all variables affected by this systematic
  systematicToVariableMap_m[syst] = systVariableSet;
}
