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
#include <xgboostEvaluator.h>
#include <analyzer.h>
#include <systematics.h>
#include <util.h>
#include <iostream>
#include <correction.h>

//#include <Pythia8Plugins/JetMatching.h>s

Analyzer::Analyzer(std::string configFile):
    configMap_m(processConfig(configFile)),
    chain_m(makeTChain(configMap_m)),
    df_m(ROOT::RDataFrame(*chain_m)){

    // Display a progress bar
    // TODO: Make this depend on ROOT version
    
    
    #if defined(HAS_ROOT_PROGRESS_BAR)
        if(configMap_m.find("batch")==configMap_m.end()){
            ROOT::RDF::Experimental::AddProgressBar(df_m);
        } else if (configMap_m["batch"]!="True"){
            ROOT::RDF::Experimental::AddProgressBar(df_m);
        } else {
            std::cout << "Batch mode, no progress bar" << std::endl;
        }
    #else
        std::cout << "ROOT version does not support progress bar, update to at least 6.28 to get it." << std::endl;
    #endif

    df_m = addConstants(df_m, configMap_m);
    registerAliases();
    registerHistograms();
    registerOptionalBranches();
    registerCorrectionlib();
    registerSystematics();
    registerExistingSystematics();
    registerBDTs();
    registerTriggers();
    std::cout << "Done creating Analyzer object" << std::endl;

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

void Analyzer::registerOptionalBranches(){
    if(configMap_m.find("optionalBranchesConfig")!=configMap_m.end()){
        auto columnNames = df_m.GetColumnNames();
        std::unordered_set<std::string> columnSet;
        for( auto &column : columnNames){
            columnSet.insert(column);
        }
        // Get each line of the file in a vector
        auto branchConfig =  configToVector(configMap_m["optionalBranchesConfig"]);
        for( auto &branch : branchConfig){ // process each line as a variable
            
            auto splitEntry = splitString(branch, " "); // split line on white space
            std::unordered_map<std::string, std::string> entryKeys; // map to hold keys and values
            for( auto &pair: splitEntry){ // iterate over each split entry
                
                auto splitPair = splitString(pair, "=");
                if(splitPair.size()==2){ // split entry on an equal sign, if two pieces the first is the key, the second is the value
                    entryKeys[splitPair[0]] =  splitPair[1];
                }
            }

            // Check that there is a name, a type, and a default for each baranch
            if(entryKeys.find("name")!=entryKeys.end() && entryKeys.find("type")!=entryKeys.end() && entryKeys.find("default")!=entryKeys.end()){
                if(columnSet.find(entryKeys["name"])==columnSet.end()){ // Add the branch if it doesn't exist
                    const int varType = std::stoi(entryKeys["type"]);
                    
                    // types:  unsigned int=0, int=1, float=2, double=3, bool=4, char=5
                    //     RVec +6;

                    Bool_t defaultBool = false;
                    auto defaultValStr = entryKeys["default"];
                    if(defaultValStr=="1" || defaultValStr=="true" || defaultValStr=="True"){
                        defaultBool = true;
                    }

                    switch(varType){
                        case 0:
                            SaveVar<UInt_t>(std::stoul(defaultValStr), entryKeys["name"]);
                            break;
                        case 1:
                            SaveVar<Int_t>(std::stoi(defaultValStr), entryKeys["name"]);
                            break;
			            case 2:
                            SaveVar<UShort_t>(std::stoul(defaultValStr), entryKeys["name"]);
                            break;
                        case 3:
                            SaveVar<Short_t>(std::stoi(defaultValStr), entryKeys["name"]);
                            break;
			            case 4:
                            SaveVar<UChar_t>(UChar_t(std::stoul(entryKeys["default"])), entryKeys["name"]);
                            break;
                        case 5:
                            SaveVar<Char_t>(Char_t(std::stoi(entryKeys["default"])), entryKeys["name"]);
                            break;
                        case 6:
                            SaveVar<Float_t>(std::stof(defaultValStr), entryKeys["name"]);
                            break;
                        case 7:
                            SaveVar<Double_t>(std::stod(defaultValStr), entryKeys["name"]);
                            break;
                        case 8:
                            SaveVar<Bool_t>(defaultBool, entryKeys["name"]);
                            break;
                        case 10:
                            SaveVar<ROOT::VecOps::RVec<UInt_t>>({static_cast<UInt_t>(std::stoul(defaultValStr))}, entryKeys["name"]);
                            break;
                        case 11:
                            SaveVar<ROOT::VecOps::RVec<Int_t>>({std::stoi(defaultValStr)}, entryKeys["name"]);
                            break;
                        case 12:
                            SaveVar<ROOT::VecOps::RVec<UShort_t>>({static_cast<UShort_t>(std::stoul(defaultValStr))}, entryKeys["name"]);
                            break;
                        case 13:
                            SaveVar<ROOT::VecOps::RVec<Short_t>>({static_cast<Short_t>(std::stoi(defaultValStr))}, entryKeys["name"]);
                            break;
                        case 14:
                            SaveVar<ROOT::VecOps::RVec<UChar_t>>({UChar_t(std::stoul(entryKeys["default"]))}, entryKeys["name"]);
                            break;
                        case 15:
                            SaveVar<ROOT::VecOps::RVec<Char_t>>({Char_t(std::stoi(entryKeys["default"]))}, entryKeys["name"]);
                            break;
                        case 16:
                            SaveVar<ROOT::VecOps::RVec<Float_t>>({std::stof(defaultValStr)}, entryKeys["name"]);
                            break;
                        case 17:
                            SaveVar<ROOT::VecOps::RVec<Double_t>>({std::stod(defaultValStr)}, entryKeys["name"]);
                            break;
                        case 18:
                            SaveVar<ROOT::VecOps::RVec<Bool_t>>({defaultBool}, entryKeys["name"]);
                            break;

                    }
                }
            
            }
        }
    }
}

void Analyzer::registerSystematics(){
    // Check if there is a systematic config, process if there is
    if(configMap_m.find("systematicsConfig")!=configMap_m.end()){

        // Get each line of the file in a vector
        auto systConfig =  configToVector(configMap_m["systematicsConfig"]);
        for( auto &syst : systConfig){ // process each line as a systematic
            
            // Process a line if it has exactly one equal sign
            auto splitSyst = splitString(syst, "=");
            if(splitSyst.size()==2){
                
                // left side of equal sign is the name of the systematic, the right side has the affected branches
                auto systName = splitSyst[0];
                auto systVariables = splitString(splitSyst[1],",");
                std::unordered_set<std::string> systVariableSet;
                
                
                // Register the systematic with the analyzer
                for(auto systVar : systVariables){
                    if(variableToSystematicMap_m.find(systVar)!= variableToSystematicMap_m.end()){
                        variableToSystematicMap_m[systVar].insert(systName);
                    } else {
                        variableToSystematicMap_m[systVar] = {systName};
                    }
                    systVariableSet.insert(systVar);
                }
                systematicToVariableMap_m[systName] = systVariableSet;
                // Apply the systematic
                df_m = applySystematic(df_m, systName,systVariables);
            }
        }
    }
}

void Analyzer::registerExistingSystematics(){
    // Check if there is a systematic config, process if there is
    if(configMap_m.find("existingSystematicsConfig")!=configMap_m.end()){

        // Get each line of the file in a vector
        auto systConfig =  configToVector(configMap_m["existingSystematicsConfig"]);
        const auto columnList = df_m.GetColumnNames();
        for( auto &syst : systConfig){ // process each line as a systematic
            std::string upSyst = syst+"Up"; // Up version of systematic
            // Process a line if it has non zero length
            if(syst.size()>0){

                std::unordered_set<std::string> systVariableSet;


                // Register the systematic with the analyzer by checking all the existing branches for variations of this systematic
                for(auto existingVars : columnList){
                    const size_t index = existingVars.find(upSyst);
                    if(index!=std::string::npos && index+upSyst.size()==existingVars.size() && index!=0){
                        const std::string systVar = existingVars.substr(0,index-1);
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
}

void Analyzer::registerBDTs(){
    // Check if there is a BDT config, process if there is
    if(configMap_m.find("bdtConfig")!=configMap_m.end()){
        
        // Get each line of the file in a vector
        auto bdtConfig =  configToVector(configMap_m["bdtConfig"]);
        for( auto &entry : bdtConfig){ // process each line as a bdt
            
            auto splitEntry = splitString(entry, " "); // split line on white space
            std::unordered_map<std::string, std::string> entryKeys; // map to hold keys and values
            for( auto &pair: splitEntry){ // iterate over each split entry
                
                auto splitPair = splitString(pair, "=");
                if(splitPair.size()==2){ // split entry on an equal sign, if two pieces the first is the key, the second is the value
                    entryKeys[splitPair[0]] =  splitPair[1];
                }
            }

            // Check that there is a file, a name, and an input variable list for each BDT entry
            if(entryKeys.find("file")!=entryKeys.end() && entryKeys.find("name")!=entryKeys.end() && entryKeys.find("inputVariables")!=entryKeys.end() && entryKeys.find("runVar")!=entryKeys.end()){
                
                // Split the variable list on commas, save to vector
                auto inputVariableVector = splitString(entryKeys["inputVariables"],",");
                addBDT(entryKeys["name"], entryKeys["file"], inputVariableVector, entryKeys["runVar"]); // Register all of this as a BDT
            }
        }
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


void Analyzer::registerCorrectionlib(){
    // Check if there is a BDT config, process if there is
    if(configMap_m.find("correctionlibConfig")!=configMap_m.end()){
        
        // Get each line of the file in a vector
        auto correctionConfig =  configToVector(configMap_m["correctionlibConfig"]);
        for( auto &entry : correctionConfig){ // process each line as a bdt
            
            auto splitEntry = splitString(entry, " "); // split line on white space
            std::unordered_map<std::string, std::string> entryKeys; // map to hold keys and values
            for( auto &pair: splitEntry){ // iterate over each split entry
                
                auto splitPair = splitString(pair, "=");
                if(splitPair.size()==2){ // split entry on an equal sign, if two pieces the first is the key, the second is the value
                    entryKeys[splitPair[0]] =  splitPair[1];
                }
            }

            // Check that there is a file, a name, and an input variable list for each BDT entry
            if(entryKeys.find("file")!=entryKeys.end() && entryKeys.find("correctionName")!=entryKeys.end() && entryKeys.find("name")!=entryKeys.end() && entryKeys.find("inputVariables")!=entryKeys.end()){
                
                // Split the variable list on commas, save to vector
                auto inputVariableVector = splitString(entryKeys["inputVariables"],",");
                addCorrection(entryKeys["name"], entryKeys["file"], entryKeys["correctionName"], inputVariableVector); // Register all of this as a BDT
            }
        }
    }
}

void Analyzer::registerAliases(){
    // Check if there is a BDT config, process if there is
    if(configMap_m.find("aliasConfig")!=configMap_m.end()){
	std::cout << "Found alias config" << std::endl;
        // Get each line of the file in a vector
        auto aliasConfig =  configToVector(configMap_m["aliasConfig"]);
        for( auto &entry : aliasConfig){ // process each line as a bdt
            
            auto splitEntry = splitString(entry, " "); // split line on white space
            std::unordered_map<std::string, std::string> entryKeys; // map to hold keys and values
            for( auto &pair: splitEntry){ // iterate over each split entry
                
                auto splitPair = splitString(pair, "=");
                if(splitPair.size()==2){ // split entry on an equal sign, if two pieces the first is the key, the second is the value
                    entryKeys[splitPair[0]] =  splitPair[1];
                }
            }

            // Check that there is an existing name and a new name
            if(entryKeys.find("existingName")!=entryKeys.end() && entryKeys.find("newName")!=entryKeys.end()){
                // Apply the alias
		std::cout <<  entryKeys["existingName"] << " is now accesible from " << entryKeys["newName"] << std::endl;
                df_m = df_m.Alias(entryKeys["newName"], entryKeys["existingName"]);

            }
        }
    }
}

void Analyzer::registerHistograms(){
    // Check if there is a hist config, process if there is
    if(configMap_m.find("histConfig")!=configMap_m.end()){
        
        // Get each line of the file in a vector
        auto histConfig =  configToVector(configMap_m["histConfig"]);
        for( auto &entry : histConfig){ // process each line as the BDTs from a file
            
            auto splitEntry = splitString(entry, " "); // split line on white space
            std::unordered_map<std::string, std::string> entryKeys; // map to hold keys and values
            for( auto &pair: splitEntry){ // iterate over each split entry
                
                auto splitPair = splitString(pair, "=");
                if(splitPair.size()==2){ // split entry on an equal sign, if two pieces the first is the key, the second is the value
                    entryKeys[splitPair[0]] =  splitPair[1];
                }
            }

            // Check that there is a file, a name, and an input variable list for each BDT entry
            if(entryKeys.find("file")!=entryKeys.end() && entryKeys.find("hists")!=entryKeys.end()){
	        std::cout << "Opening file " << entryKeys["file"] << std::endl;
                // Open file with histograms
                TFile* file( TFile::Open(entryKeys["file"].c_str()) );
		std::cout << "Hist line: " << entryKeys["hists"] << std::endl;
                // Split the variable list on commas, save to vector
                auto histVector = splitString(entryKeys["hists"],",");
                for(auto hist : histVector){
		    std::cout << "Checking hist " << hist << std::endl;
                    auto histData = splitString(hist,":");
                    if(histData.size()==3){
                        if(histData[0]=="TH1F"){
                            TH1F *histPtr = file->Get<TH1F>(histData[1].c_str());
                            histPtr->SetDirectory(0);
                            th1f_m.emplace(histData[2], histPtr);
			    std::cout << "Adding th1f " << histData[2] << std::endl;
                        } else if(histData[0]=="TH2F") {
                            TH2F *histPtr = file->Get<TH2F>(histData[1].c_str());
                            histPtr->SetDirectory(0);
                            th2f_m.emplace(histData[2], histPtr);
			    std::cout << "Adding th2f " << histData[2] << std::endl;
                        }  else if(histData[0]=="TH1D") {
                            TH1D *histPtr = file->Get<TH1D>(histData[1].c_str());
                            histPtr->SetDirectory(0);
                            th1d_m.emplace(histData[2], histPtr);
			    std::cout << "Adding th1d " << histData[2] << std::endl;
                        }  else if(histData[0]=="TH2D") {
                            TH2D *histPtr = file->Get<TH2D>(histData[1].c_str());
                            histPtr->SetDirectory(0);
                            th2d_m.emplace(histData[2], histPtr);
			    std::cout << "Adding th2d " << histData[2] << std::endl;
                        }
                    }
                }
                delete file;

            }
        }
        std::cout << "Done adding histograms" << std::endl;
    }
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
