#include <ROOT/RResultPtr.hxx>
#include <ROOT/RDF/RColumnRegister.hxx>
#include <iostream>

#include <util.h>
#include <analyzer.h>
#include <vjets.h>
#include <gen.h>
#include <vh.h>
#include <selections.h>
#include <corrections.h>
#include <stitching.h>
#include <trigger.h>

Float_t returnOne(){
    return(1.0);
}

int main(int argc, char **argv) {

    if(argc!=2){
        std::cout << "Arguments: " << argc << std::endl;
        std::cerr << "Error!!!!! No configuration file provided. Please include a config file." << std::endl;
        return(1);
    }
    
    Analyzer an(argv[1]);
    an.ApplyAllTriggers();

    Corrections::applyJetVeto(&an);
    //Corrections::applyL1ToL3Corrections(&an);
    
    VH::leptonSelection(&an);
    VH::jetSelectionBBCC(&an);
    VH::VHKinematics(&an);
    
    // Get VJet GenInfo
    Gen::getVJetInfo(&an);

    // perform stitching
    Stitching::applyStitchingRun3(&an);
    an.Define("weight", multiply<Float_t>, {"genWeight", "normScale"});
    an.Define("FinalWeight", multiply<Float_t>, {"weight", "stitchWeight"});
   
    an.save();

    return(0);   
}
