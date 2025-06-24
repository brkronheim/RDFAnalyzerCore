#ifndef ANALYZER_H_INCLUDED
#define ANALYZER_H_INCLUDED

#include <ROOT/RResultPtr.hxx>

#include <RtypesCore.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <fastforest.h>
#include <correction.h>

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDFHelpers.hxx>

#include <TChain.h>
#include <util.h>
#include <functions.h>
#include <plots.h>


class Analyzer{
    public:




        Analyzer(std::string configFile);

        
        template <typename F> 
        Analyzer* Define(std::string name, F f, const std::vector<std::string> &columns = {}){
            // std::cout << "Defining: " << name << std::endl;
            // Find all systematics that affect this new variable
            std::unordered_set<std::string> systsToApply;
            std::vector<std::string> nomColumns;
            for(auto &variable : columns){
                if(variableToSystematicMap_m.find(variable)!=variableToSystematicMap_m.end()){
                    for(auto &syst : variableToSystematicMap_m.at(variable)){
                        
                        // register syst to process, register variable with systematics
                        systsToApply.insert(syst);
                        variableToSystematicMap_m[name].insert(syst);
                        systematicToVariableMap_m[syst].insert(name); 
                    }
                }
                if(variable.find("_fixedNominal") != std::string::npos) { // allow fixing a variable to nominal
                    auto index = variable.find("_fixedNominal"); // Need to do this for all similar operations

                    //std::cout << "Fixing " << variable.substr(0, index) << " to nominal for calculation of " << name << std::endl;
                    nomColumns.push_back(variable.substr(0,index));
                } else {
                    nomColumns.push_back(variable);
                }
            }
            
            // Define nominal variation
            Define_m<F>(name, f, nomColumns);
            
            // If systematics were found, define a new variable for each up and down variation.
            for(auto &syst : systsToApply){
                std::vector<std::string> systColumnsUp;
                std::vector<std::string> systColumnsDown;
                for(auto var : columns){ // check all variables in column, replace ones affected by systematics
                    if(systematicToVariableMap_m[syst].find(var)==systematicToVariableMap_m[syst].end()){
                        if(var.find("_fixedNominal") != std::string::npos) { // allow fixing a variable to nominal
                            auto index = var.find("_fixedNominal"); // Need to do this for all similar operations

                            //std::cout << "Fixing " << var.substr(0, index) << " to nominal for calculation of " << name << std::endl;
                            systColumnsUp.push_back(var.substr(0,index));
                            systColumnsDown.push_back(var.substr(0,index));
                        } else {
                            systColumnsUp.push_back(var);
                            systColumnsDown.push_back(var);
                        }
                    } else {
                        systColumnsUp.push_back(var+"_"+syst+"Up");
                        systColumnsDown.push_back(var+"_"+syst+"Down");
                    }
                }
                // Define up and down variations for current systematic
                Define_m<F>(name+"_"+syst+"Up", f, systColumnsUp);
                Define_m<F>(name+"_"+syst+"Down", f, systColumnsDown);
            }
            
            return(this);
        }

        template <typename F> 
        Analyzer* Filter(std::string name, F f, const std::vector<std::string> &columns = {}){
            name = "pass_"+name;
            // make a branch for whether each systematic variation passes this cut
            Define(name, f, columns);
            if(variableToSystematicMap_m.find(name)!=variableToSystematicMap_m.end()){
                for(auto &syst : variableToSystematicMap_m[name]){
                
                    Redefine_m(name, orBranches3, {name, name+"_"+syst+"Up", name+"_"+syst+"Down"});
                }
            }

            Filter_m(passCut, {name});
            return(this);
        }

        template <typename F> 
        Analyzer* DefinePerSample(std::string name, F f){
            DefinePerSample_m<F>(name, f);
            return(this);
        }


        template<typename T>
        Analyzer* SaveVar(T var, std::string name){
            auto storeVar = [var](unsigned int, const ROOT::RDF::RSampleInfo) -> T {
                                    return(var);
                                };
            
            std::cout << "Defining variable " << name << " to be " << var << std::endl;
            DefinePerSample_m(name, storeVar);
            
            return(this);
        }

        Analyzer* DefineVector(std::string name, const std::vector<std::string> &columns = {}, std::string type="Float_t");

        Analyzer* ApplyBDT(std::string BDTName);

        Analyzer* ApplyCorrection(std::string correctionName, std::vector<std::string> stringArguments);

        

        Analyzer* ApplyAllBDTs();

        Analyzer* ApplyAllTriggers();

        Analyzer *save();

        Analyzer *readHistBins(std::string histName, std::string outputBranchName, std::vector<std::string> inputBranchNames);

        Analyzer *readHistVals(std::string histName, std::string outputBranchName, std::vector<std::string> inputBranchNames);



        



        void bookND(std::vector<histInfo> &infos, std::vector<selectionInfo> &selection, std::string suffix, std::vector<std::vector<std::string>> &allRegionNames);
     
        void save_hists(std::vector<std::vector<histInfo>> &fullHistList, std::vector<std::vector<std::string>> &allRegionNames);
  

        ROOT::RDF::RNode getDF();

        std::string configMap(std::string key);

        correction::Correction::Ref correctionMap(std::string key);

        std::vector<std::string> makeSystList(std::string branchName);
       

        void bookSystematic(std::string syst, std::vector<std::string> &affectedVariables);
        
    private:

        UInt_t verbosityLevel_m;


        std::unordered_map<std::string, std::string> configMap_m;
        static std::unordered_map<std::string, fastforest::FastForest> bdts_m;
        std::unordered_map<std::string, std::vector<std::string>> bdt_features_m;
        std::unordered_map<std::string, std::string> bdt_runVars_m;
        static std::unordered_map<std::string, correction::Correction::Ref> corrections_m;
        std::unordered_map<std::string, std::vector<std::string>> correction_features_m;

        std::unordered_map<std::string, std::vector<std::string>> triggers_m;
        std::unordered_map<std::string, std::string> trigger_samples_m;
        std::unordered_map<std::string, std::vector<std::string>> trigger_vetos_m;
        //static std::unordered_map<std::string, std::vector<std::string>> correction_strings_m;

        static std::unordered_map<std::string, std::unique_ptr<TH1F>> th1f_m;
        static std::unordered_map<std::string, std::unique_ptr<TH2F>> th2f_m;
        static std::unordered_map<std::string, std::unique_ptr<TH1D>> th1d_m;
        static std::unordered_map<std::string, std::unique_ptr<TH2D>> th2d_m;

        std::vector<ROOT::Detail::RDF::RResultPtr<THnSparseD>> histos_m;

        std::vector<std::unique_ptr<TChain>> chain_vec_m;
        std::unordered_map<std::string, std::unordered_set<std::string>> systematicToVariableMap_m;
        std::unordered_map<std::string, std::unordered_set<std::string>> variableToSystematicMap_m;
        ROOT::RDF::RNode df_m;
        


        template <typename F> 
        void Define_m(std::string name, F f, const std::vector<std::string> &columns = {}){
            df_m = df_m.Define(name, f, columns);
        }

        template <typename T>
        void Define_m(std::string name, const std::vector<std::string> &columns = {}){
            std::string arrayString = "ROOT::VecOps::RVec<Double_t>({";
            for(long unsigned int i = 0; i<columns.size()-1; i++){
                arrayString+=columns[i]+",";
            }

            arrayString += columns[columns.size()-1]+"})";
            df_m = df_m.Define(name, arrayString);
        }


        void Define_m(std::string name, const std::vector<std::string> &columns = {}, std::string type = "Float_t"){
            std::string arrayString = "ROOT::VecOps::RVec<"+type+">({";
            for(long unsigned int i = 0; i<columns.size()-1; i++){
                arrayString+=+"static_cast<"+type+">("+columns[i]+"),";
            }

            arrayString += columns[columns.size()-1]+"})";
            df_m = df_m.Define(name, arrayString);
        }

        template <typename F> 
        void Redefine_m(std::string name, F f, const std::vector<std::string> &columns = {}){
            df_m = df_m.Redefine(name, f, columns);
        }

        template <typename F> 
        void Filter_m(F f, const std::vector<std::string> &columns = {}){
            df_m = df_m.Filter(f, columns);
        }

        template <typename F> 
        void DefinePerSample_m(std::string name, F f){
            df_m = df_m.DefinePerSample(name, f);
        }

        void registerConstants();

        void registerAliases();

        void registerSystematics();

        void registerExistingSystematics();

        void registerBDTs();

        void registerCorrectionlib();
        
        void registerOptionalBranches();

        void registerHistograms();

        void registerTriggers();


        void addBDT(std::string key, std::string fileName, std::vector<std::string> featureList, std::string runVar);

        void addCorrection(std::string key, std::string fileName, std::string correctionName, std::vector<std::string> featureList);

        void addTrigger(std::string key, std::vector<std::string> triggers);
};



# endif
