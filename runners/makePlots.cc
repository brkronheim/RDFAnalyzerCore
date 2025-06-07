#include <RtypesCore.h>
#include <utility>
#include <vector>
#include <string>
#include <iostream>

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDFHelpers.hxx>
#include <TFile.h>
#include <TDirectory.h>
#include <TDirectoryFile.h>
#include <TTree.h>

#include <util.h>
#include <plots.h>
#include <functions.h>
#include <analyzer.h>
#include <stitching.h>


using namespace std::chrono_literals;

bool filterJetsVpt(unsigned char Np, Float_t VpT){
    if(VpT<40 || Np==0){
        return(false);
    }
    return(true);
}

inline constexpr Float_t return0(){
    return(0.0);
}

inline Int_t getSampleCategory(Int_t type){
    if(type<10 && type >=0){
        return(0);
    } else if(type<40){
        return(1);
    } else if(type<200){
        return(2);
    } else {
        return(3);
    }
}


inline Int_t N_PS(Float_t pt1, Float_t pt2, Float_t pt3, Float_t pt4){
    if(pt1<0){
        return(0);
    } else if(pt2<0){
        return(1);
    } else if(pt3<0){
        return(2);
    } else if(pt4<0){
        return(3);
    }
    return(4);

}


inline Float_t dr1Up(Int_t NP_ME, Int_t NP_PS, Float_t minDR, Float_t FinalWeight, Int_t sampleCategory){
    return(NP_ME==3 && NP_PS<=2 && minDR<1 && sampleCategory==2 ? FinalWeight*2 : FinalWeight);

}

inline Float_t dr1Down(Int_t NP_ME, Int_t NP_PS, Float_t minDR, Float_t FinalWeight, Int_t sampleCategory){
    return(NP_ME==3 && NP_PS<=2  && minDR<1 && sampleCategory==2 ? FinalWeight*0.5 : FinalWeight);
}


inline Float_t dr2Up(Int_t NP_ME, Int_t NP_PS, Float_t minDR, Float_t FinalWeight, Int_t sampleCategory){
    return(NP_ME==3 && NP_PS<=2 && minDR>=1 && sampleCategory==2 ? FinalWeight*2 : FinalWeight);

}

inline Float_t dr2Down(Int_t NP_ME, Int_t NP_PS, Float_t minDR, Float_t FinalWeight, Int_t sampleCategory){
    return(NP_ME==3 && NP_PS<=2  && minDR>=1 && sampleCategory==2 ? FinalWeight*0.5 : FinalWeight);
}


inline Float_t minDR(Float_t dr1, Float_t dr2, Float_t dr3){
    return(std::min({dr1, dr2, dr3}));
}

inline Float_t vjWeight(Float_t weight, Int_t sampleCategory){
    return(sampleCategory==2 ? weight : 0);
}

//  a*(x-x_0) + b*(x^2 - x_0**2)
// low region: 0.5->0.9: 0 at 0.5
// mid region: 0.9->1.1 0 at 1
// high region: 1.1->2.0 0 at 2.0 

inline ROOT::VecOps::RVec<Float_t> curveCorrections(Int_t NP_ME, Int_t NP_PS, Float_t dr, Float_t FinalWeight, Int_t sampleCategory){
    ROOT::VecOps::RVec<Float_t> baseWeights = {FinalWeight, FinalWeight, FinalWeight, FinalWeight,FinalWeight, FinalWeight, // low
                                               FinalWeight, FinalWeight, FinalWeight, FinalWeight,FinalWeight, FinalWeight, // mid 
                                               FinalWeight, FinalWeight, FinalWeight, FinalWeight,FinalWeight, FinalWeight,}; //high
    
    /*Float_t ranges[4] = {0.79,0.9175,1.07,1.97};
    Float_t coefLow[2] = {-3.97920837,  4.13546965};
    Float_t coefMid[4] = {531.94430848, -1631.24542944,  1669.37113163,  -568.78358768}; 
    Float_t coefHigh[3] = { 0.88990597, -3.24359367,  3.9372251};
    if(sampleCategory!=2 || NP_PS>2 || dr<ranges[0] || dr>ranges[3] || NP_ME<2){
        return(baseWeights);
    } else if(dr<ranges[1]){
        Float_t val = coefLow[0]*dr + coefLow[1];
        if(val<0.4 || val > 2.5){
            std::cerr << "low: " << 1/val  << " dr: " << dr << std::endl;
        }
        baseWeights[0] *= 1/val;
        baseWeights[1] *= val;
    } else if(dr<ranges[2]){
        Float_t val = coefMid[0]*dr*dr*dr + coefMid[1]*dr*dr + coefMid[2]*dr + coefMid[3];
        if(val<0.5 || val > 2.0){
            std::cerr << "medium: " << 1/val  << " dr: " << dr << std::endl;
        }
        baseWeights[2] *= 1/val;
        baseWeights[3] *= val;
    } else {
        Float_t val = coefHigh[0]*dr*dr + coefHigh[1]*dr + coefHigh[2];
        if(val<0.5 || val > 2.0){
            std::cerr << "high: " << 1/val  << " dr: " << dr << std::endl;
        }
        baseWeights[4] *= 1/val;
        baseWeights[5] *= val;
    } 
    return(baseWeights);*/
    
    /*
    Float_t crossOver = 2.0;
    Int_t index = 8;
    Float_t scaleLin = 1;
    Float_t scaleQuad = 2;

    if(sampleCategory!=2 || NP_ME<2 || NP_PS!=2 || dr<=0.75 || dr>=2.0){
        return(baseWeights);
    } else if(dr<0.9) {
        crossOver = 0.75;
        index = 0;
        scaleLin = 10;
        scaleQuad = 10;

    } else if(dr<1.1){
        crossOver = 1.0;
        index = 4;
        scaleLin = 8;
        scaleQuad = 8;
    }
    baseWeights[index]*= (1 + scaleLin*(dr-crossOver));
    baseWeights[index+1]/= (1 + scaleLin*(dr-crossOver));
    baseWeights[index+2]*= (1 + scaleQuad*(dr-crossOver)*(dr-crossOver));
    baseWeights[index+3]/= (1 + scaleQuad*(dr-crossOver)*(dr-crossOver));
    //return(NP_ME==3 && NP_PS<=2  && minDR>=1 && sampleCategory==2 ? FinalWeight*0.5 : FinalWeight);
    */

    Float_t crossOver = 2.0;
    Int_t index = 12;
    Float_t scaleConst = 2;
    Float_t scaleLin = 2;
    Float_t scaleQuad = 2;

    if(sampleCategory!=2 || NP_ME<2 || NP_PS!=2 || dr<=0.75 || dr>=2.0){
        return(baseWeights);
    } else if(dr<0.9) {
        crossOver = 0.75;
        index = 0;
        //scaleLin = 10;
        //caleQuad = 10;

    } else if(dr<1.1){
        crossOver = 1.0;
        index = 6;
        //scaleLin = 8;
        //scaleQuad = 8;
    }
    

    
    baseWeights[index]/= scaleConst;
    baseWeights[index+1]/= 1/scaleConst;
    
    if(index==12){
        baseWeights[index+2]/= (1 + scaleLin*(crossOver-dr));
        baseWeights[index+3]/= (1 - scaleLin*(crossOver-dr));
        baseWeights[index+4]/= (scaleQuad + (crossOver-dr));
        baseWeights[index+5]/= (1/scaleQuad + (crossOver-dr));
    } else {
        baseWeights[index+2]/= (1 + scaleLin*(dr-crossOver));
        baseWeights[index+3]/= (1 - scaleLin*(dr-crossOver));
        baseWeights[index+4]/= (scaleQuad + (dr-crossOver));
        baseWeights[index+5]/= (1/scaleQuad + (dr-crossOver));
    }
    //return(NP_ME==3 && NP_PS<=2  && minDR>=1 && sampleCategory==2 ? FinalWeight*0.5 : FinalWeight);
    return(baseWeights);
    
}


inline ROOT::VecOps::RVec<Float_t> binCorrections(Int_t NP_ME, Int_t NP_PS, Float_t dr,  Float_t closestDr, Float_t FinalWeight, Int_t sampleCategory){
    ROOT::VecOps::RVec<Float_t> baseWeights = {FinalWeight, FinalWeight, FinalWeight, FinalWeight,FinalWeight, FinalWeight, // low
                                               FinalWeight, FinalWeight, FinalWeight, FinalWeight,FinalWeight, FinalWeight, // mid 
                                               FinalWeight, FinalWeight, FinalWeight, FinalWeight,FinalWeight, FinalWeight,
                                               FinalWeight, FinalWeight}; //high

    Float_t binEdges[9] = {0.85,0.89,0.92,0.94,0.96,1.0,1.05,1.10,1.15};

    if(sampleCategory!=2 || NP_PS>2 || closestDr>1.0 || dr<0.8 ||  dr>1.2 || NP_ME<=2 || fabs(dr-closestDr)<1e-6){
        //std::cerr << "Base" << std::endl;
        return(baseWeights);
    }
    for(int x = 0; x<9; x++){
        if(dr<binEdges[x]){
            baseWeights[2*x]*=10;
            baseWeights[2*x+1]*=0.1;
            //std::cerr << x << std::endl;
            return(baseWeights);
        } 
    } 
    baseWeights[2*9]*=10;
    baseWeights[2*9+1]*=0.1;
    //std::cerr << 9 << std::endl;
    return(baseWeights);
}


// helper: expands Define for Is = 0…N-1
template<typename T, std::size_t... Is>
void DefineMultiple(std::string const& baseName,
                  const std::vector<std::string>& cols,
                  std::index_sequence<Is...>,
                Analyzer *an)
{
    // this fold-expansion becomes:
    //   ( Define(baseName+"0", fixedIndexVector<T,0>, cols),
    //     Define(baseName+"1", fixedIndexVector<T,1>, cols),
    //     …
    //     Define(baseName+"9", fixedIndexVector<T,9>, cols) );
    //
    // we cast to void to ignore the comma‑fold’s final result:
    (void)( an->Define(baseName +std::to_string(Is)+"Up",
                   fixedIndexVector<T, Is*2>,
                   cols)
            , ... );
    (void)( an->Define(baseName +std::to_string(Is)+"Down",
            fixedIndexVector<T, Is*2 + 1>,
            cols)
     , ... );
}



int main(int argc, char **argv) {

    
    if(argc!=2){
        std::cout << "Arguments: " << argc << std::endl;
        std::cerr << "Error!!!!! No configuration file provided. Please include a config file." << std::endl;
        return(1);
    }
    
    Analyzer an(argv[1]);



    an.Define("channel", return0, {})->
    Define("controlRegion", return0, {})->
    Define("sampleCategory", getSampleCategory, {"type"})->
    
    Define("N_PS",N_PS, {"partonShowerJets_0_pt",
                                         "partonShowerJets_1_pt",
                                          "partonShowerJets_2_pt",
                                          "partonShowerJets_3_pt"})->
    Define("ME_dR01",EvalDeltaR_MG<Float_t>,  {"OutgoingPartons_0_eta",
                                           "OutgoingPartons_0_phi",
                                           "OutgoingPartons_1_eta",
                                           "OutgoingPartons_1_phi"})-> 
    Define("ME_dR12",EvalDeltaR_MG<Float_t>,  {"OutgoingPartons_2_eta",
                                            "OutgoingPartons_2_phi",
                                            "OutgoingPartons_1_eta",
                                            "OutgoingPartons_1_phi"})->
    Define("ME_dR02",EvalDeltaR_MG<Float_t>,  {"OutgoingPartons_0_eta",
                                            "OutgoingPartons_0_phi",
                                            "OutgoingPartons_2_eta",
                                            "OutgoingPartons_2_phi"})-> 
    Define("minDR",minDR, {"ME_dR01","ME_dR12","ME_dR02"})->
    Define("PS_dR01",EvalDeltaR_MG<Float_t>,  {"partonShowerJets_0_eta",
        "partonShowerJets_0_phi",
        "partonShowerJets_1_eta",
        "partonShowerJets_1_phi"})->
    Define("binWeights", binCorrections, {"NP_ME", "N_PS", "ME_dR01", "minDR","FinalWeight", "sampleCategory"});


    DefineMultiple<Float_t>("FinalWeight_drBin", {"binWeights"}, std::make_index_sequence<10>{}, &an);
    
    /*Define("FinalWeight_lowdrUp",fixedIndexVector<Float_t, 0>,{"curveWeights"})->
    Define("FinalWeight_lowdrDown",fixedIndexVector<Float_t, 1>,{"curveWeights"})->
    Define("FinalWeight_meddrUp",fixedIndexVector<Float_t, 2>,{"curveWeights"})->
    Define("FinalWeight_meddrDown",fixedIndexVector<Float_t, 3>,{"curveWeights"})->
    Define("FinalWeight_highdrUp",fixedIndexVector<Float_t, 4>,{"curveWeights"})->
    Define("FinalWeight_highdrDown",fixedIndexVector<Float_t, 5>,{"curveWeights"});*/
    /*Define("FinalWeight_lowdrconUp",fixedIndexVector<Float_t, 0>,{"curveWeights"})->
    Define("FinalWeight_lowdrconDown",fixedIndexVector<Float_t, 1>,{"curveWeights"})->
    Define("FinalWeight_lowdrlinUp",fixedIndexVector<Float_t, 2>,{"curveWeights"})->
    Define("FinalWeight_lowdrlinDown",fixedIndexVector<Float_t, 3>,{"curveWeights"})->
    Define("FinalWeight_lowdrquadUp",fixedIndexVector<Float_t, 4>,{"curveWeights"})->
    Define("FinalWeight_lowdrquadDown",fixedIndexVector<Float_t, 5>,{"curveWeights"})->
    Define("FinalWeight_meddrconUp",fixedIndexVector<Float_t, 6>,{"curveWeights"})->
    Define("FinalWeight_meddrconDown",fixedIndexVector<Float_t, 7>,{"curveWeights"})->
    Define("FinalWeight_meddrlinUp",fixedIndexVector<Float_t, 8>,{"curveWeights"})->
    Define("FinalWeight_meddrlinDown",fixedIndexVector<Float_t, 9>,{"curveWeights"})->
    Define("FinalWeight_meddrquadUp",fixedIndexVector<Float_t, 10>,{"curveWeights"})->
    Define("FinalWeight_meddrquadDown",fixedIndexVector<Float_t, 11>,{"curveWeights"})->
    Define("FinalWeight_highdrconUp",fixedIndexVector<Float_t, 12>,{"curveWeights"})->
    Define("FinalWeight_highdrconDown",fixedIndexVector<Float_t, 13>,{"curveWeights"})->
    Define("FinalWeight_highdrlinUp",fixedIndexVector<Float_t, 14>,{"curveWeights"})->
    Define("FinalWeight_highdrlinDown",fixedIndexVector<Float_t, 15>,{"curveWeights"})->
    Define("FinalWeight_highdrquadUp",fixedIndexVector<Float_t, 16>,{"curveWeights"})->
    Define("FinalWeight_highdrquadDown",fixedIndexVector<Float_t, 17>,{"curveWeights"});*/
    //Define("Systematic", return0, {});

    // Hold all the histograms
    histHolder hists;

    const int baseBins = 20;

    an.Define("FinalWeight_dr1Up",dr1Up, {"NP_ME", "N_PS", "minDR", "FinalWeight", "sampleCategory"});
    an.Define("FinalWeight_dr1Down",dr1Down, {"NP_ME", "N_PS", "minDR", "FinalWeight", "sampleCategory"});
    an.Define("FinalWeight_dr2Up",dr2Up, {"NP_ME", "N_PS", "minDR", "FinalWeight", "sampleCategory"});
    an.Define("FinalWeight_dr2Down",dr2Down, {"NP_ME", "N_PS", "minDR", "FinalWeight", "sampleCategory"});

    an.bookSystematic("dr1", {"FinalWeight"});
    an.bookSystematic("dr2", {"FinalWeight"});

    for(int i = 0; i<10; i++){
        an.bookSystematic("drBin"+std::to_string(i), {"FinalWeight"});
    }

    /*an.bookSystematic("lowdr", {"FinalWeight"});
    an.bookSystematic("meddr", {"FinalWeight"});
    an.bookSystematic("highdr", {"FinalWeight"});*/

    /*an.bookSystematic("lowdrcon", {"FinalWeight"});
    an.bookSystematic("lowdrlin", {"FinalWeight"});
    an.bookSystematic("lowdrquad", {"FinalWeight"});
    an.bookSystematic("meddrcon", {"FinalWeight"});
    an.bookSystematic("meddrlin", {"FinalWeight"});
    an.bookSystematic("meddrquad", {"FinalWeight"});
    an.bookSystematic("highdrcon", {"FinalWeight"});
    an.bookSystematic("highdrlin", {"FinalWeight"});
    an.bookSystematic("highdrquad", {"FinalWeight"});*/
    const std::vector<std::string> systList = an.makeSystList("Systematic");
    an.Define("vjWeight", vjWeight, {"FinalWeight", "sampleCategory"});
    std::vector<histInfo> histInfos = {

        // V vars:
        histInfo("Reco_JJ_dR",                          "Reco_JJ_dR",     "dR",                          "FinalWeight",             80,   0.4, 4.4),
        histInfo("RecoJJ_mass",                          "RecoJJ_mass",     "mass [GeV]",                          "FinalWeight",             100,   50, 250),
        histInfo("Systematic",                          "Systematic",     "Systematic",                  "FinalWeight",             systList.size(), 0.0,systList.size()),
        histInfo("ME_dR01",                             "ME_dR01",        "ME_dR01",                     "vjWeight",                40,   0.4, 4.4),
        histInfo("ME_dR12",                          "ME_dR12",     "ME_dR12",                  "vjWeight",                40,   0.4, 4.4),
        histInfo("ME_dR02",                          "ME_dR02",     "ME_dR02",                  "vjWeight",                40,   0.4, 4.4),
        histInfo("minDR",                          "minDR",     "minDR",                  "vjWeight",             40,   0.4, 4.4),
        histInfo("PS_dR01",                          "PS_dR01",     "PS_dR01",                  "vjWeight",             40,   0.4, 4.4),
    };   


    std::vector<std::vector<histInfo>> fullHistList =  {histInfos};


    

    // Define selection categories
    selectionInfo channelBounds("channel",1, 0.0,1.0);
    selectionInfo controlBounds("controlRegion", 1, 0.0,1.0);
    selectionInfo categoryBounds("sampleCategory", 4, 0.0,4.0);
    selectionInfo systematicBounds("Systematic", systList.size(), 0.0,systList.size());


    // List of all region names. TODO: Make this something other than a flat vector
    std::vector<std::vector<std::string>> allRegionNames = {{"Zmm"},
                                                            {"LF"},
                                                            {"data_obs","VV","DY","TT"},
                                                            systList};


    // vector of selection for each ND histogram
    std::vector<selectionInfo> selection = {channelBounds, controlBounds, categoryBounds, systematicBounds};
    
    // types are of the selections and then the variable
    an.bookND(histInfos, selection, "All", allRegionNames);
    an.save_hists(fullHistList, allRegionNames);

}
