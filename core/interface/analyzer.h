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

        Analyzer* DefineVector(std::string name, const std::vector<std::string> &columns = {}, std::string type="Float_t");


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

        Analyzer* ApplyBDT(std::string BDTName);

        Analyzer* ApplyCorrection(std::string correctionName, std::vector<std::string> stringArguments);

        

        Analyzer* ApplyAllBDTs();

        Analyzer* ApplyAllTriggers();

        Analyzer *save();

        Analyzer *readHistBins(std::string histName, std::string outputBranchName, std::vector<std::string> inputBranchNames);

        Analyzer *readHistVals(std::string histName, std::string outputBranchName, std::vector<std::string> inputBranchNames);

        void bookND(std::vector<histInfo> &infos, std::vector<selectionInfo> &selection, std::string suffix, std::vector<std::vector<std::string>> &allRegionNames){
            
            // Store the selection info in some vectors
            std::vector<int> binVectorBase;
            std::vector<double> lowerBoundVectorBase;
            std::vector<double> upperBoundVectorBase;
            std::vector<std::string> varVectorBase;

            for(auto const &selectionInfo : selection){
                binVectorBase.push_back(selectionInfo.bins());
                lowerBoundVectorBase.push_back(selectionInfo.lowerBound());
                upperBoundVectorBase.push_back(selectionInfo.upperBound());
                varVectorBase.push_back(selectionInfo.variable());
            }

            
            for(auto const &info : infos){
                std::string newName = info.name()+"."+suffix; // name of hist info
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
                //const ROOT::RDF::THnSparseDModel tempModel(newName.c_str(), newName.c_str(), selection.size()+1,binVector.data(), lowerBoundVector.data(), upperBoundVector.data());
                //
                Int_t numFills = 1;
                std::vector<std::string> systVector(varVector);
                for(const auto &syst : allRegionNames[allRegionNames.size()-1] ){
                    std::cout << syst << std::endl;
                    std::string systBase = syst;
                    if(syst.find("Up")!=std::string::npos){
                        systBase = systBase.substr(0,syst.find("Up"));
                    }
                    if(syst.find("Down")!=std::string::npos){
                        systBase = systBase.substr(0,syst.find("Down"));
                    }
                    if(syst=="Nominal"){
                        continue;
                    }
                    const auto varSet = systematicToVariableMap_m[systBase];
                    std::cout << "Affected variables for "  << systBase << ": " << std::endl;
                    for(const auto &var : varSet){
                        std::cout << var << std::endl;
                    }
                    Int_t affectedVariables = 0;
                    std::vector<std::string> newVec;
                    for(const auto &branch : varVector){
                        
                        if(varSet.count(branch)!=0){
                            affectedVariables +=1 ;
                            std::cout << branch+"_"+syst << std::endl;
                            newVec.push_back(branch+"_"+syst);
                        } else {
                            std::cout << branch << std::endl;
                            newVec.push_back(branch);
                        }
                    }
                    if(affectedVariables>1){
                        systVector.insert(systVector.end(), newVec.begin(), newVec.end());
                        numFills++;
                    }
                }
                
                
                std::string branchName = info.name()+"_"+suffix+"inputDoubleVector";
                this->DefineVector(branchName, systVector, "Double_t");
                THnMulti tempModel(df_m.GetNSlots(), newName.c_str(), newName.c_str(), selection.size()+1, numFills, binVector, lowerBoundVector, upperBoundVector);
                histos_m.push_back(df_m.Book<ROOT::VecOps::RVec<Double_t>>(std::move(tempModel), {branchName}));
                
                // (newName+"inputDoubleVector").c_str()
                //tempModel.Exe

                
                //histos_m.emplace(histos_m.end(), df.HistoND<types..., float>(tempModel, varVector));
                
            }
        }

        void save_hists(std::vector<std::vector<histInfo>> &fullHistList, std::vector<std::vector<std::string>> &allRegionNames){
            std::string fileName = configMap("saveFile");
            std::vector<std::string> allNames;
            std::vector<std::string> allVariables;
            std::vector<std::string> allLabels;
            std::vector<int> allBins;
            std::vector<float> allLowerBounds;
            std::vector<float> allUpperBounds;
            // Get vectors of the hist names, variables, lables, bins, and bounds
            for(auto const &histList : fullHistList){
                for(auto const &info : histList){
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
            //std::cout << df_m.Count().GetValue() << " Events processed" << std::endl; // Trigger execution
            //const Int_t histNumber = histos_m.size();
            //const Int_t axisNumber = allRegionNames.size()+1;
            std::vector<Int_t> commonAxisSize = {};


            for(const auto &regionNameList : allRegionNames){
                commonAxisSize.push_back(regionNameList.size());

            }
            
            int histIndex = 0;
            std::unordered_map<std::string, TH1F> histMap;
            std::unordered_set<std::string> dirSet;
            for(auto &histo_m : histos_m){
                auto hist = histo_m.GetPtr();
                //const Int_t finalAxisSize = allBins[histIndex];
                const Int_t currentHistogramSize = hist->GetNbins();
                std::vector<Int_t> indices(commonAxisSize.size()+1);
                for(int i = 0; i<currentHistogramSize; i++){
                    Float_t content = hist->GetBinContent(i,indices.data());
                    if(content==0){
                        continue;
                    }
                    Float_t error = hist->GetBinError2(i);
                    /*std::cout << "Bin: " << indices << ": " << content << ", " << error  << ": ";
                    for(int j = 0; j< commonAxisSize.size()+1; j++){
                        std::cout << indices[j] << ", ";
                    }
                    std::cout << std::endl;*/

                    std::string dirName = "";
                    //std::cout << "histName: " << histName
                    const Int_t size =commonAxisSize.size()-2;
                    for(int i =0; i<size; i++){
                        dirName += allRegionNames[i][indices[i]-1]+"/";
                        //std::cout << "histName: " << histName;
                    }

                    dirName += allRegionNames[commonAxisSize.size()-2][indices[commonAxisSize.size()-2]-1];
                    std::string histName = allVariables[histIndex];
                    //bool isNominal=false;
                    if(allRegionNames[commonAxisSize.size()-1][indices[commonAxisSize.size()-1]-1]=="Nominal"){
                        //isNominal= true;
                    } else {
                        histName+="_"+allRegionNames[commonAxisSize.size()-1][indices[commonAxisSize.size()-1]-1];
                    }
                    if(histMap.count(dirName+"/"+histName)==0){
                        histMap[dirName+"/"+histName] = TH1F(histName.c_str(),(allVariables[histIndex]+";"+allVariables[histIndex]+";Counts").c_str(),
                                        allBins[histIndex],allLowerBounds[histIndex],allUpperBounds[histIndex]);
                        dirSet.emplace(dirName);
                    }
                    histMap[dirName+"/"+histName].SetBinContent(indices[commonAxisSize.size()], content);
                    histMap[dirName+"/"+histName].SetBinError(indices[commonAxisSize.size()], sqrt(error));
                    
                }
                
                histIndex++;

            }
            
            saveFile.cd();
            for(const auto &dirName: dirSet){
                std::string newDir(dirName);
                newDir[dirName.find('/')] = '_';
                saveFile.mkdir(newDir.c_str());
            }
            std::unordered_map<std::string, std::map<std::string, std::pair<Float_t, Float_t>>> systNormalizations;
             // store the nominal and sytematic normalization for each control region
             for(const auto &pair: histMap){
                if(pair.first.find("Systematic")==std::string::npos){ // Want the sytematic histogram
                    continue;
                }
                std::string dirName = pair.first.substr(0, pair.first.find_last_of("/"));
                auto regionSplit = splitString(pair.first,"/");
                std::string region = regionSplit[0] + "_" + regionSplit[2]; //  dirName.substr(0,dirName.find("/"));
                std::string histName = regionSplit[regionSplit.size()-1]; //pair.first.substr(pair.first.find_last_of("/")+1);
                std::string nominalName = histName;
                std::string systName = "nominal";
                if(histName.find("Up")!=std::string::npos || histName.find("Down")!=std::string::npos){
                    systName = nominalName.substr(nominalName.find_last_of("_")+1);
                    nominalName = nominalName.substr(0,nominalName.find_last_of("_"));
                }
                std::cout << nominalName << ", " << systName << std::endl;
                nominalName = dirName+"/"+nominalName;
                auto nominalHist = histMap[nominalName];
                Float_t nominalIntegral = nominalHist.Integral();
                Float_t systIntegral = pair.second.Integral();
                if(systNormalizations[region].count(systName)==0){
                    systNormalizations[region][systName].first = nominalIntegral;
                    systNormalizations[region][systName].second = systIntegral;
                } else {
                    systNormalizations[region][systName].first += nominalIntegral;
                    systNormalizations[region][systName].second += systIntegral;
                }
            }
            
            
            for(const auto &pair: histMap){
                std::string dirName = pair.first.substr(0, pair.first.find_last_of("/"));
                auto regionSplit = splitString(pair.first,"/");
                std::string region = regionSplit[0] + "_" + regionSplit[2]; //  dirName.substr(0,dirName.find("/"));
                std::string histName = regionSplit[regionSplit.size()-1]; //pair.first.substr(pair.first.find_last_of("/")+1);
                std::string nominalName = histName;
                std::string systName = "nominal";
                if(histName.find("Up")!=std::string::npos || histName.find("Down")!=std::string::npos){
                    systName = nominalName.substr(nominalName.find_last_of("_")+1);
                    nominalName = nominalName.substr(0,nominalName.find_last_of("_"));
                }
                nominalName = dirName+"/"+nominalName;
                std::cout << histName << ", " << systName << std::endl;
                std::string newDir(dirName);
                newDir[dirName.find('/')] = '_';
                saveFile.cd(newDir.c_str());
                //pair.second.Write();
                (pair.second*(systNormalizations[region][systName].first/systNormalizations[region][systName].second)).Write(); // Need to group this normalization the way combine wants it
                saveFile.cd();
            }

        }


        ROOT::RDF::RNode getDF();

        std::string configMap(std::string key);

        std::vector<std::string> makeSystList(std::string branchName){
            std::vector<std::string> systList = {"Nominal"};
            Int_t var = 0;
            this->SaveVar<Float_t>(var,branchName);

            for(const auto &pair : systematicToVariableMap_m){
                systList.push_back(pair.first+"Up");
                systList.push_back(pair.first+"Down");
                var++;
                this->SaveVar<Float_t>(var,branchName+"_"+pair.first+"Up");
                var++;
                this->SaveVar<Float_t>(var,branchName+"_"+pair.first+"Down");
                systematicToVariableMap_m[pair.first].insert(branchName);
                variableToSystematicMap_m[branchName].insert(pair.first);
            }

            return(systList);
        }

        correction::Correction::Ref correctionMap(std::string key);

        void bookSystematic(std::string syst, std::vector<std::string> affectedVariables){
            std::unordered_set<std::string> systVariableSet;
            for(const auto &systVar : affectedVariables){
                systVariableSet.insert(systVar);
                if(variableToSystematicMap_m.find(systVar)!= variableToSystematicMap_m.end()){
                    variableToSystematicMap_m[systVar].insert(syst);
                } else {
                    variableToSystematicMap_m[systVar] = {syst};
                }
            }
            systematicToVariableMap_m[syst] = systVariableSet;

        }

        
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

        std::unique_ptr<TChain> chain_m;
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
