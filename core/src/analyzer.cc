#include <string>
#include <memory>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDFHelpers.hxx>
#include <TChain.h>
#include <TFile.h>
#include <TH1F.h>
#include <TH2F.h>

#include <unordered_map>
#include <unordered_set>
#include <analyzer.h>
#include <util.h>
#include <iostream>
#include <correction.h>
#include <systematics.h>


/**
 * @brief Construct a new Analyzer object
 * 
 * @param configFile File containing the configuration information for this analyzer
 */
Analyzer::Analyzer(std::string configFile):
    configMap_m(processConfig(configFile)),
    chain_m(makeTChain(configMap_m)),
    df_m(ROOT::RDataFrame(*chain_m)){

    // Set verbosity level
    if(configMap_m.find("verbosity")==configMap_m.end()){
        verbosityLevel_m = std::stoi(configMap_m["verbosity"]);
    } else {
        verbosityLevel_m = 1;
    }


    // Display a progress bar depending on batch status and ROOT version
    #if defined(HAS_ROOT_PROGRESS_BAR)
        if(configMap_m.find("batch")==configMap_m.end()){
            ROOT::RDF::Experimental::AddProgressBar(df_m);
        } else if (configMap_m["batch"]!="True"){
            ROOT::RDF::Experimental::AddProgressBar(df_m);
        } else {
            if(verbosityLevel_m >= 1){
                std::cout << "Batch mode, no progress bar" << std::endl;
            }
        }
    #else
        if(verbosityLevel_m >= 1){
            std::cout << "ROOT version does not support progress bar, update to at least 6.28 to get it." << std::endl;
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
    
    if(verbosityLevel_m >= 1){
        std::cout << "Done creating Analyzer object" << std::endl;
    }

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
void Analyzer::registerConstants(){
    if(configMap_m.find("floatConfig")!=configMap_m.end()){
        const std::string floatFile = configMap_m.at("floatConfig");
        auto floatConfig = readConfig(floatFile);
        for(auto &pair : floatConfig){
            SaveVar<Float_t>(std::stof(pair.second), pair.first);
        }
    }
    if(configMap_m.find("intConfig")!=configMap_m.end()){
        const std::string intFile = configMap_m.at("intConfig");
        auto intConfig = readConfig(intFile);
        for(auto &pair : intConfig){
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
void Analyzer::registerAliases(){

    auto aliasConfig = parseConfig(configMap_m, "aliasConfig", {"existingName", "newName"});

    for(const auto &entryKeys : aliasConfig){
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
 * types:  unsigned int=0, int=1, unsigned short=2, short=3, unsigned char=4, char=5, float=6, double=7, bool=8
 *            RVec +10;
 *
 * When using ROOT >=6.34 this will use DefaultValueFor which allows the mixing of exisiting and non existing branches.
 * Otherwise, it checks if the branch exists and defines it, thus it won't work if it's only defined for some branches.
 */
void Analyzer::registerOptionalBranches(){

    const auto aliasConfig = parseConfig(configMap_m, "optionalBranchesConfig", {"name", "type", "default"});


    #if defined(HAS_DEFAULT_VALUE_FOR)

        for(const auto &entryKeys : aliasConfig){
            const int varType = std::stoi(entryKeys.at("type"));
            
            const auto defaultValStr = entryKeys.at("default");
            const auto varName = entryKeys.at("name");
            const Bool_t defaultBool = defaultValStr=="1" || defaultValStr=="true" || defaultValStr=="True";

            switch(varType){
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
                    df_m = df_m.DefaultValueFor<UChar_t>(varName, UChar_t(std::stoul(defaultValStr)));
                    break;
                case 5:
                    df_m = df_m.DefaultValueFor<Char_t>(varName, Char_t(std::stoi(defaultValStr)));
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
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UInt_t>>(varName, {static_cast<UInt_t>(std::stoul(defaultValStr))});
                    break;
                case 11:
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Int_t>>(varName, {std::stoi(defaultValStr)});
                    break;
                case 12:
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UShort_t>>(varName, {static_cast<UShort_t>(std::stoul(defaultValStr))});
                    break;
                case 13:
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Short_t>>(varName, {static_cast<Short_t>(std::stoi(defaultValStr))});
                    break;
                case 14:
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UChar_t>>(varName, {UChar_t(std::stoul(defaultValStr))});
                    break;
                case 15:
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Char_t>>(varName, {Char_t(std::stoi(defaultValStr))});
                    break;
                case 16:
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Float_t>>(varName, {std::stof(defaultValStr)});
                    break;
                case 17:
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Double_t>>(varName, {std::stod(defaultValStr)});
                    break;
                case 18:
                    df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Bool_t>>(varName, {defaultBool});
                    break;

            }
        }
    #else 
        const auto columnNames = df_m.GetColumnNames();
        std::unordered_set<std::string> columnSet;
        for( auto &column : columnNames){
            columnSet.insert(column);
        }

        for(const auto &entryKeys : aliasConfig){
            if(columnSet.find(entryKeys.at("name")) == columnSet.end()){ // Add the branch if it doesn't exist
                const int varType = std::stoi(entryKeys.at("type"));
                
                // types:  unsigned int=0, int=1, unsigned short=2, short=3, unsigned char=4, char=5, float=6, double=7, bool=8
                //     RVec +10;

                const auto defaultValStr = entryKeys.at("default");
                const auto varName = entryKeys.at("name");
                const Bool_t defaultBool = defaultValStr=="1" || defaultValStr=="true" || defaultValStr=="True";

                switch(varType){
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
                        SaveVar<Double_t>(std::stod(defaultValStr),varName);
                        break;
                    case 8:
                        SaveVar<Bool_t>(defaultBool, varName);
                        break;
                    case 10:
                        SaveVar<ROOT::VecOps::RVec<UInt_t>>({static_cast<UInt_t>(std::stoul(defaultValStr))}, varName);
                        break;
                    case 11:
                        SaveVar<ROOT::VecOps::RVec<Int_t>>({std::stoi(defaultValStr)}, varName);
                        break;
                    case 12:
                        SaveVar<ROOT::VecOps::RVec<UShort_t>>({static_cast<UShort_t>(std::stoul(defaultValStr))}, varName);
                        break;
                    case 13:
                        SaveVar<ROOT::VecOps::RVec<Short_t>>({static_cast<Short_t>(std::stoi(defaultValStr))}, varName);
                        break;
                    case 14:
                        SaveVar<ROOT::VecOps::RVec<UChar_t>>({UChar_t(std::stoul(defaultValStr))}, varName);
                        break;
                    case 15:
                        SaveVar<ROOT::VecOps::RVec<Char_t>>({Char_t(std::stoi(defaultValStr))}, varName);
                        break;
                    case 16:
                        SaveVar<ROOT::VecOps::RVec<Float_t>>({std::stof(defaultValStr)}, varName);
                        break;
                    case 17:
                        SaveVar<ROOT::VecOps::RVec<Double_t>>({std::stod(defaultValStr)}, varName);
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
 * The hists can be TH1F, TH2F, TH1D, or TH2D. The colons separate the histogram type, the name of the histogram in the file
 * and the name that will be used to access the histogram in RDFAnalyzer
 *
 * This will likely be replaced by Correctionlib in the future
 */
void Analyzer::registerHistograms(){

    const auto histConfig = parseConfig(configMap_m, "histConfig", {"file", "hists"});

    for(const auto &entryKeys : histConfig){
        
        TFile* file( TFile::Open(entryKeys.at("file").c_str()));
        auto histVector = splitString(entryKeys.at("hists"),",");
        
        for(auto hist : histVector){
		    auto histData = splitString(hist,":");
            if(histData.size()==3){
                if(histData[0]=="TH1F"){
                    TH1F *histPtr = file->Get<TH1F>(histData[1].c_str());
                    histPtr->SetDirectory(0);
                    th1f_m.emplace(histData[2], histPtr);
                } else if(histData[0]=="TH2F") {
                    TH2F *histPtr = file->Get<TH2F>(histData[1].c_str());
                    histPtr->SetDirectory(0);
                    th2f_m.emplace(histData[2], histPtr);
                }  else if(histData[0]=="TH1D") {
                    TH1D *histPtr = file->Get<TH1D>(histData[1].c_str());
                    histPtr->SetDirectory(0);
                    th1d_m.emplace(histData[2], histPtr);
                }  else if(histData[0]=="TH2D") {
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
 * Within each of the files a correction is added like
 *
 * file=aux/hists.root hists=TH2D:NLO_stitch_22:NLO_stitch_22
 *
 * The hists can be TH1F, TH2F, TH1D, or TH2D. The colons separate the histogram type, the name of the histogram in the file
 * and the name that will be used to access the histogram in RDFAnalyzer
 *
 * This will likely be replaced by Correctionlib in the future
 *
 * TODO: Fix this documentation
 */
void Analyzer::registerCorrectionlib(){

    const auto correctionConfig = parseConfig(configMap_m, "correctionlibConfig", {"file", "correctionName", "name", "inputVariables"});

    for(const auto &entryKeys : correctionConfig){
        // Split the variable list on commas, save to vector
        auto inputVariableVector = splitString(entryKeys.at("inputVariables"),",");
        // register as a correction
        addCorrection(entryKeys.at("name"), entryKeys.at("file"), entryKeys.at("correctionName"), inputVariableVector);
    }
    
}

/**
 * @brief Read existingSystematicsConfig to register existing systematic corrections.
 * 
 * This is registered in the main config files as:
 * existingSystematicsConfig=cfg/existingSystematics.txt
 *
 * Within each of the files each line contains just the name of an existing systematic.
 * For example if there are MassScaleUp and MassScaleDown systematics, you would enter
 * 
 * MassScale
 * 
 * on its own line in the config file. This code will find all variables affected by this systematic and register them.
 * Note that if only looks for the Up systematics, so both Up and Down need to be defined, and this won't be checked.
 */
void Analyzer::registerExistingSystematics(){

    const auto systConfig = parseConfigVector(configMap_m, "existingSystematicsConfig");
    const auto columnList = df_m.GetColumnNames();    
    
    for( auto &syst : systConfig){ // process each line as a systematic
        
        const std::string upSyst = syst+"Up"; // Up version of systematic
        
        // Process a line if it has non zero length
        if(syst.size()>0){

            std::unordered_set<std::string> systVariableSet;

            // Register the systematic with the analyzer by checking all the existing branches for variations of this systematic
            for(const auto &existingVars : columnList){
                
                const size_t index = existingVars.find(upSyst);
                
                // Determine if the start of the upSyst is the correct distance from the end of the string name
                if(index!=std::string::npos && index+upSyst.size()==existingVars.size() && index!=0){
                    
                    // Get the base variable name
                    const std::string systVar = existingVars.substr(0,index-1);
                    
                    // register the systematics
                    if(variableToSystematicMap_m.find(systVar)!= variableToSystematicMap_m.end()){
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
 * @brief Read bdtConfig to register BDTs.
 * 
 * This is registered in the main config files as:
 * bdtConfig=cfg/bdts.txt
 *
 * Within each of the files a BDT is added like
 *
 * file=aux/hists.root name= inputVariables= runVar=
 *
 * The hists can be TH1F, TH2F, TH1D, or TH2D. The colons separate the histogram type, the name of the histogram in the file
 * and the name that will be used to access the histogram in RDFAnalyzer
 *
 * This will likely be replaced by Correctionlib in the future
 *
 * TODO: Fix this documentation
 */
void Analyzer::registerBDTs(){

    const auto bdtConfig = parseConfig(configMap_m, "bdtConfig", {"file", "name", "inputVariables", "runVar"});

    for(const auto &entryKeys : bdtConfig){
        // Split the variable list on commas, save to vector
        auto inputVariableVector = splitString(entryKeys.at("inputVariables"),",");
        // Add BDT
        addBDT(entryKeys.at("name"), entryKeys.at("file"), inputVariableVector, entryKeys.at("runVar")); // Register all of this as a BDT
    }
}



void Analyzer::registerTriggers(){
    // Check if there is a BDT config, process if there is
    if(configMap_m.find("triggerConfig")!=configMap_m.end()){
        
        // Get each line of the file in a vector
        auto bdtConfig =  configToVector(configMap_m["triggerConfig"]);
        for( auto &entry : bdtConfig){ // process each line as a trigger
            
            auto splitEntry = splitString(entry, " "); // split line on white space
            std::unordered_map<std::string, std::string> entryKeys; // map to hold keys and values
            for( auto &pair: splitEntry){ // iterate over each split entry
                
                auto splitPair = splitString(pair, "=");
                if(splitPair.size()==2){ // split entry on an equal sign, if two pieces the first is the key, the second is the value
                    entryKeys[splitPair[0]] =  splitPair[1];
                }
            }

            // Check that there is a file, a name, and an input variable list for each BDT entry
            if(entryKeys.find("name")!=entryKeys.end() && entryKeys.find("sample")!=entryKeys.end() && entryKeys.find("triggers")!=entryKeys.end()){//} && entryKeys.find("flags")!=entryKeys.end()){
                std::cout << "Adding trigger " << entryKeys["name"] << std::endl;                
                // Split the variable list on commas, save to vector
                auto triggerList = splitString(entryKeys["triggers"],",");
	        if(entryKeys.find("triggerVetos")!=entryKeys.end()){
                    auto triggerVetoList = splitString(entryKeys["triggerVetos"],",");
		    trigger_vetos_m.emplace(entryKeys["name"], triggerVetoList);
		} else {
                    trigger_vetos_m[entryKeys["name"]] = {};
	        }
		//auto sampleName = entryKeys.find("name");
                triggers_m.emplace(entryKeys["name"], triggerList);
                trigger_samples_m.emplace(entryKeys["sample"],entryKeys["name"]);
            }
        }
    }
}



Analyzer* Analyzer::DefineVector(std::string name, const std::vector<std::string> &columns, std::string type){
    // Find all systematics that affect this new variable
    std::unordered_set<std::string> systsToApply;
    for(auto &variable : columns){
        if(variableToSystematicMap_m.find(variable)!=variableToSystematicMap_m.end()){
            for(auto &syst : variableToSystematicMap_m.at(variable)){
                
                // register syst to process, register variable with systematics
                systsToApply.insert(syst);
                variableToSystematicMap_m[name].insert(syst);
                systematicToVariableMap_m[syst].insert(name); 
            }
        }
    }
    
    // Define nominal variation
    Define_m(name, columns, type);
    
    // If systematics were found, define a new variable for each up and down variation.
    for(auto &syst : systsToApply){
        std::vector<std::string> systColumnsUp;
        std::vector<std::string> systColumnsDown;
        for(auto var : columns){ // check all variables in column, replace ones affected by systematics
            if(systematicToVariableMap_m[syst].find(var)==systematicToVariableMap_m[syst].end()){
                systColumnsUp.push_back(var);
                systColumnsDown.push_back(var);
            } else {
                systColumnsUp.push_back(var+"_"+syst+"Up");
                systColumnsDown.push_back(var+"_"+syst+"Down");
            }
        }
        // Define up and down variations for current systematic

        // Define nominal variation
        Define_m(name+"_"+syst+"Up", systColumnsUp, type);
        // Define nominal variation
        Define_m(name+"_"+syst+"Down", systColumnsDown, type);
    }
    
    return(this);
}


Analyzer* Analyzer::ApplyBDT(std::string BDTName){
    Float_t eval(ROOT::VecOps::RVec<Float_t> &inputVector);
    
    // Get a vector of teh BDT inputs
    DefineVector("input_"+BDTName,bdt_features_m[BDTName], "Float_t");

    // Get the BDT and make an execution lambda
    //auto bdt = bdts_m[BDTName];
    auto bdtLambda = [BDTName](ROOT::VecOps::RVec<Float_t> &inputVector, bool runVar) -> Float_t {
        if(runVar){
            return(1./(1. + std::exp(-bdts_m[BDTName](inputVector.data()))));
        } else {
            return(-1);
        }
        
    };

    // Define the BDT output
    Define(BDTName,bdtLambda, {"input_"+BDTName, bdt_runVars_m[BDTName]});
    return(this);
}

Analyzer* Analyzer::ApplyCorrection(std::string correctionName, std::vector<std::string> stringArguments){
    Float_t eval(ROOT::VecOps::RVec<Float_t> &inputVector);
    
    // Get a vector of teh BDT inputs
    DefineVector("input_"+correctionName,correction_features_m[correctionName], "double");

    // Get the BDT and make an execution lambda
    //auto bdt = bdts_m[BDTName];
    //
    /*int i = 0;
    for( const auto &val : corrections_m[correctionName]->inputs().typeStr()){
        std::cout << "input " << i << " is " << val << std::endl;
	i++;
    }*/


    auto correctionLambda = [correctionName, stringArguments](ROOT::VecOps::RVec<double> &inputVector) -> Float_t {
        std::vector<std::variant<int, double, std::string>> values;

        auto stringArgIt = stringArguments.begin();
	    auto doubleArgIt = inputVector.begin();


        for(const auto &varType : corrections_m[correctionName]->inputs()){
            if(varType.typeStr() == "string"){
                values.push_back(*stringArgIt);
		stringArgIt++;
		
	    } else if(varType.typeStr() == "int"){
                values.push_back(int(*doubleArgIt));
		doubleArgIt++;
	    } else {
                values.push_back(*doubleArgIt);
                doubleArgIt++;
	    }

	}

	

        return(corrections_m[correctionName]->evaluate(values));
        
    };

    // Define the BDT output
    Define(correctionName, correctionLambda, {"input_"+correctionName});
    
    return(this);
}

Analyzer* Analyzer::ApplyAllBDTs(){
    for(auto &pair : bdts_m){
        ApplyBDT(pair.first);
    }
    return(this);
}

inline bool passTriggerAndVeto(ROOT::VecOps::RVec<Bool_t> trigger, ROOT::VecOps::RVec<Bool_t> triggerVeto){
    for(const auto &trig : triggerVeto){
        if(trig){
            return(false);
        }
    }
    for(const auto &trig : trigger){
        if(trig){
            return(true);
        }
    }
    return(false);
}


inline bool passTrigger(ROOT::VecOps::RVec<Bool_t> trigger){
    for(const auto &trig : trigger){
        if(trig){
            return(true);
        }
    }
    return(false);
}


Analyzer* Analyzer::ApplyAllTriggers(){
    
    if(trigger_samples_m.find(configMap_m["type"])!=configMap_m.end()){ // data, need to account for duplicate events from different datasets
	std::cout << "looking for trigger for " << configMap_m["type"] << std::endl;
	std::string triggerName = trigger_samples_m.at(configMap_m["type"]);
	std::cout << "trigger is " << triggerName << std::endl;

        for(const auto &trig : triggers_m[triggerName]){
            std::cout << "Keep Data trigger: " << trig << std::endl;
        }

	for(const auto &trig : trigger_vetos_m[triggerName]){
            std::cout << "Veto Data trigger: " << trig << std::endl;
        }

        if(trigger_vetos_m[triggerName].size()==0){
            std::cout << "No veto" << std::endl;

	    DefineVector("allTriggersPassVector",triggers_m[triggerName], "Bool_t");
            Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});

	} else {
            DefineVector(triggerName + "passVector", triggers_m[triggerName], "Bool_t");
            DefineVector(triggerName + "vetoVector", trigger_vetos_m[triggerName], "Bool_t");


            Filter("applyTrigger", passTriggerAndVeto, {triggerName +"pass vector", triggerName+" veto vector"});
       }
    } else { // MC, no duplicate events
        std::vector<std::string> allTriggers; // merge all the triggers
        for(const auto &pair: triggers_m){
            allTriggers.insert( allTriggers.end(), pair.second.begin(), pair.second.end() );
        }

         for(const auto &trig : allTriggers){
            std::cout << "Keep MC trigger: " << trig << std::endl;
        }


        DefineVector("allTriggersPassVector",allTriggers, "Bool_t");
        Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});

    }
    return(this);
}

Analyzer* Analyzer::save(){
    saveDF(df_m, configMap_m, variableToSystematicMap_m);
    return(this);
}

Analyzer* Analyzer::readHistBins(std::string histName, std::string outputBranchName, std::vector<std::string> inputBranchNames){
    if(inputBranchNames.size()==1){ 
        if(th1f_m.find(histName)!=th1f_m.end()){
            auto histLambda = [histName](Int_t bin1)-> Float_t {
                return(th1f_m[histName]->GetBinContent(bin1+1));
            };
            Define(outputBranchName,histLambda, inputBranchNames);
        } else if(th1d_m.find(histName)!=th1d_m.end()){
            auto histLambda = [histName](Int_t bin1)-> Float_t {
                return(th1d_m[histName]->GetBinContent(bin1+1));
            };
            Define(outputBranchName,histLambda, inputBranchNames);
        } else {
            std::cerr << "th1 bin error" << std::endl;
}
    } else if(inputBranchNames.size()==2){
        if(th2f_m.find(histName)!=th2f_m.end()){
            auto histLambda = [histName](Int_t bin1, Int_t bin2)-> Float_t {
                return(th2f_m[histName]->GetBinContent(bin1+1, bin2+1));
            };
            Define(outputBranchName,histLambda, inputBranchNames);
        } else if(th2d_m.find(histName)!=th2d_m.end()){
                auto histLambda = [histName](Int_t bin1, Int_t bin2)-> Float_t {
                return(th2d_m[histName]->GetBinContent(bin1+1, bin2+1));
            };
            Define(outputBranchName,histLambda, inputBranchNames);
        } else {
            std::cerr << "th2 bin error" << std::endl;
        }
    } else {
        std::cerr << "Error! Access of histogram " << histName << " attempted with invalid branche combination" << std::endl; 
    }
    return(this);
}

Analyzer* Analyzer::readHistVals(std::string histName, std::string outputBranchName, std::vector<std::string> inputBranchNames){
    if(inputBranchNames.size()==1){ 
        if(th1f_m.find(histName)!=th1f_m.end()){
            auto histLambda = [histName](Float_t val1)-> Float_t {
                auto bin1 = th1f_m[histName]->GetXaxis()->FindBin(val1);
                return(th1f_m[histName]->GetBinContent(bin1));
            };
            Define(outputBranchName,histLambda, inputBranchNames);
        } else if(th1d_m.find(histName)!=th1d_m.end()){
            auto histLambda = [histName](Float_t val1)-> Float_t {
                auto bin1 = th1d_m[histName]->GetXaxis()->FindBin(val1);
                return(th1d_m[histName]->GetBinContent(bin1));
            };
            Define(outputBranchName,histLambda, inputBranchNames);
        } else {
            std::cerr << "th1 val error" << std::endl;
        }
    } else if(inputBranchNames.size()==2){
        if(th2f_m.find(histName)!=th2f_m.end()){
            auto histLambda = [histName](Float_t val1, Float_t val2) -> Float_t {
                auto bin1 = th2f_m[histName]->GetXaxis()->FindBin(val1);
                auto bin2 = th2f_m[histName]->GetYaxis()->FindBin(val2);
                return(th2f_m[histName]->GetBinContent(bin1, bin2));
            };
            Define(outputBranchName,histLambda, inputBranchNames);
        } else if(th2d_m.find(histName)!=th2d_m.end()){
            auto histLambda = [histName](Float_t val1, Float_t val2) -> Float_t {
                auto bin1 = th2d_m[histName]->GetXaxis()->FindBin(val1);
                auto bin2 = th2d_m[histName]->GetYaxis()->FindBin(val2);
                return(th2d_m[histName]->GetBinContent(bin1, bin2));
            };
            Define(outputBranchName,histLambda, inputBranchNames);
        } else {
            std::cerr << "th2 val error" << std::endl;
        }
    } else {
        std::cerr << "Error! Access of histogram " << histName << " attempted with invalid branche combination" << std::endl; 
    }
    return(this);
}

ROOT::RDF::RNode Analyzer::getDF(){
    return(df_m);
}

std::string Analyzer::configMap(std::string key){
    if(configMap_m.find(key)!=configMap_m.end()){
        return(configMap_m[key]);
    } else {
        return("");
    }
} 


correction::Correction::Ref Analyzer::correctionMap(std::string key){
    
    std::cout << "Getting correction " << key << std::endl;
    auto correction = corrections_m.at(key);	
    std::cout << "got correction" << std::endl;
    return(correction);
    /*if(){
        
    } else {
        Thr
    }*/

}






void Analyzer::addBDT(std::string key, std::string fileName, std::vector<std::string> featureList, std::string runVar){
    // make the feature list (f0, f1, f2, ...)
    std::vector<std::string> features;
    for(long unsigned int i = 0; i<featureList.size(); i++){
        features.push_back("f"+std::to_string(i));
    }

    // Load the BDT
    auto bdt = fastforest::load_txt(fileName, features);

    // Add the BDT and feature list to their maps
    bdts_m.emplace(key, bdt);
    bdt_features_m.emplace(key, featureList);
    bdt_runVars_m.emplace(key, runVar);
}




void Analyzer::addCorrection(std::string key, std::string fileName, std::string correctionName, std::vector<std::string> featureList){
   
    // load correction object from json
    auto correctionF = correction::CorrectionSet::from_file(fileName);
    auto correction = correctionF->at(correctionName);

    // Add the correction and feature list to their maps
    std::cout << "Adding correction " << key << "!" << std::endl;
    corrections_m.emplace(key, correction);
    correction_features_m.emplace(key, featureList);
}




// Static variable declaration
std::unordered_map<std::string, std::unique_ptr<TH1F>> Analyzer::th1f_m;
std::unordered_map<std::string, std::unique_ptr<TH2F>> Analyzer::th2f_m;
std::unordered_map<std::string, std::unique_ptr<TH1D>> Analyzer::th1d_m;
std::unordered_map<std::string, std::unique_ptr<TH2D>> Analyzer::th2d_m;
std::unordered_map<std::string, fastforest::FastForest> Analyzer::bdts_m;
std::unordered_map<std::string, correction::Correction::Ref> Analyzer::corrections_m;


