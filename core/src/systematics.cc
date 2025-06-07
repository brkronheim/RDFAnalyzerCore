#include <Math/Vector4D.h>
#include <Math/Polar2D.h>
#include <ROOT/RDataFrame.hxx>
#include <RtypesCore.h>
#include <vector>
#include <string>
#include <iostream>
#include <cassert>

#include <systematics.h>

Float_t smearUp(Float_t var){
    return(var*1.1);
}

Float_t smearDown(Float_t var){
    return(var*0.9);
}



// Select background events, sample number > 0
ROOT::RDF::RNode applySystematic(ROOT::RDF::RNode df, std::string systName, std::vector<std::string> branchNames){
    std::cout << "Checking systemaitc: " << systName << std::endl;
    if(systName=="metSmear"){
        assert(branchNames.size()==1);
        df = df.Define(branchNames[0]+"_"+systName+"_Up",smearUp,{branchNames[0]});
        df = df.Define(branchNames[0]+"_"+systName+"_Down",smearDown,{branchNames[0]});
        return(df);
    } else if(systName=="electronSmear"){
        assert(branchNames.size()==2);
        df = df.Define(branchNames[0]+"_"+systName+"_Up",smearUp,{branchNames[0]});
        df = df.Define(branchNames[0]+"_"+systName+"_Down",smearDown,{branchNames[0]});
        df = df.Define(branchNames[1]+"_"+systName+"_Up",smearUp,{branchNames[1]});
        df = df.Define(branchNames[1]+"_"+systName+"_Down",smearDown,{branchNames[1]});
        return(df);
    } else if(systName=="smearLHEVpt"){
        assert(branchNames.size()==1);
        df = df.Define(branchNames[0]+"_"+systName+"_Up",smearUp,{branchNames[0]});
        df = df.Define(branchNames[0]+"_"+systName+"_Down",smearDown,{branchNames[0]});
        return(df);
    } else {
        return(df);
    }
    
}
