#include "analyzer.h"
#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <functions.h>

#include <iostream>


ROOT::VecOps::RVec<Int_t> getGoodMuons(const ROOT::VecOps::RVec<char> &muonFlag, const ROOT::VecOps::RVec<char> &muonPreselection, const ROOT::VecOps::RVec<Float_t> &jetSep, ROOT::VecOps::RVec<Float_t> &pt){
    ROOT::VecOps::RVec<Int_t> goodMuons;
    Int_t size = muonFlag.size();
    for(int i = 0; i<size; i++){
        if(muonFlag[i] == 1 && muonPreselection[i] == 1 && jetSep[i]>0.4 && pt[i] >= 20000){
            goodMuons.push_back(i);
        }
    }
    if(goodMuons.size()==2){
        Int_t ind0 = goodMuons[0];
        Int_t ind1 = goodMuons[1];
        if(pt[ind0]<pt[ind1]){
            goodMuons[1] = ind0;
            goodMuons[0] = ind1;
        }
    }
    return(goodMuons);
    
}

template<unsigned int ind>
inline ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<Float_t>> getMuon(ROOT::VecOps::RVec<Int_t> &goodMuons,
                                                                             ROOT::VecOps::RVec<Float_t> &muonPt, ROOT::VecOps::RVec<Float_t> &muonEta,
                                                                             ROOT::VecOps::RVec<Float_t> &muonPhi){
    Int_t index = goodMuons[ind];
    return(ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<Float_t>>(muonPt[index],muonEta[index],muonPhi[index],0));
    
}

bool twoMuons(ROOT::VecOps::RVec<Int_t> goodMuons){
    return(goodMuons.size()==2);
}

bool massFilter(Float_t mass){
    return(mass <=150000); // mass>= 70000 && 
}

Float_t scaleDown(Float_t mass){
    return(mass/1000.0);
}

int main(int argc, char **argv) {

    // Main configuration is provided as command-line argument
    if (argc != 2) {
        std::cout << "Arguments: " << argc << std::endl;
        std::cerr << "Error!!!!! No configuration file provided. Please include a "
                     "config file."
                  << std::endl;
        return (1);
    }

    auto configProvider = ManagerFactory::createConfigurationManager(argv[1]);
    auto systematicManager = ManagerFactory::createSystematicManager();
    auto dataFrameProvider = ManagerFactory::createDataManager(*configProvider);
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;

    auto an = Analyzer(argv[1]);

    an.Define("goodMuons", getGoodMuons, {"AnalysisMuonsAuxDyn.DFCommonMuonPassIDCuts",
                                          "AnalysisMuonsAuxDyn.DFCommonMuonPassPreselection",
                                          "AnalysisMuonsAuxDyn.DFCommonJetDr", "AnalysisMuonsAuxDyn.pt"})-> 
        Filter("twoMuons", twoMuons, {"goodMuons"})->
        Define("LeadingMuonVec", getMuon<0>, {"goodMuons", "AnalysisMuonsAuxDyn.pt", "AnalysisMuonsAuxDyn.eta", "AnalysisMuonsAuxDyn.phi"})->
        Define("SubleadingMuonVec", getMuon<1>, {"goodMuons", "AnalysisMuonsAuxDyn.pt", "AnalysisMuonsAuxDyn.eta", "AnalysisMuonsAuxDyn.phi"})->
        Define("ZBosonVec", sumLorentzVec<ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<Float_t>>>, {"LeadingMuonVec", "SubleadingMuonVec"})->
        Define("ZBosonMassScaled", getLorentzVecM<Float_t, ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<Float_t>>>, {"ZBosonVec"})->
        Filter("ZBosonMassFilter", massFilter, {"ZBosonMassScaled"})->
        Define("ZBosonMass", scaleDown, {"ZBosonMassScaled"});

    an.save();
    

}

