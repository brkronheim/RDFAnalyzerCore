#ifndef FUNCTIONS_H_INCLUDED
#define FUNCTIONS_H_INCLUDED

#include <algorithm>
#include <numeric>

#include <Math/Math.h>
#include <Math/Vector4D.h>
#include <ROOT/RDataFrame.hxx>


template <typename T> 
Float_t getSignFloat(T val) {
    return(T(0) < val) - (val < T(0));
}


// General useful functions
template<typename T, int lower, int upper>
T clipIntBounds(T val){
    val = val>T(upper) ? T(upper) : val;
    val = val<T(lower) ? T(lower) : val;
    return(val);
}

template<Int_t index>
Int_t constantInteger(){
    
    return(index);
}

template<typename T, unsigned int index>
T fixedIndexVector(ROOT::VecOps::RVec<T> &vector){
    if(index >= vector.size()){
        return(std::optional<T>(-9999.0));
    }

    return(vector[index]);
}

// Returns the indices for the size largest values in vector, pads with -1 at the end
template<typename T, long unsigned int size>
ROOT::VecOps::RVec<Int_t> selectTop(ROOT::VecOps::RVec<T> &vector){
    ROOT::VecOps::RVec<Int_t> indexVector(vector.size());
    std::iota(indexVector.begin(),indexVector.end(),0); //Initializing
    
    std::sort( indexVector.begin(), indexVector.end(), [&](Int_t i, Int_t j){
            return vector[i]>vector[j];
        });
    while(indexVector.size()<size){
        indexVector.push_back(-1);
    }
    return(indexVector);
}

template<typename T, typename S>
T indexVector(ROOT::VecOps::RVec<T> &vector, S index){
    if(index<0 || index >= vector.size()){
        return(-9999.0);
    }
    return(vector[index]);
}
template<typename T>
ROOT::VecOps::RVec<T> maximumVector(ROOT::VecOps::RVec<T> &val1, ROOT::VecOps::RVec<T> &val2){
    ROOT::VecOps::RVec<T> maxVec(val1.size());
    for(long unsigned int i = 0; i<val1.size(); i++){
        maxVec[i] = std::max(val1[i], val2[i]);
    }

    return(maxVec);
}

template<typename T, typename S>
ROOT::VecOps::RVec<T> take(ROOT::VecOps::RVec<T> &vector, ROOT::VecOps::RVec<S> &index){
    return(ROOT::VecOps::Take(vector, index, T(-9999.0)));
}

template<typename T>
bool passPositive(T val){
    return(val>=0);
}

template<typename T>
bool passPositiveOR(T val1, T val2){
    return(val1>=0 || val2>=0);
}

template<typename T>
T calcAvB(T a, T b){
     if(a<0 || b<0){
         return(-1);
     }
     return(a+b!=0 ? a/(a+b) : 0);
}


template<typename T, typename S>
S castVar(T val1){
    return(S(val1));
}

template<typename T>
ROOT::VecOps::RVec<T> defineVector(T val1){
    return(ROOT::VecOps::RVec<T>({val1}));
}

template<typename T>
ROOT::VecOps::RVec<T> addToVector(ROOT::VecOps::RVec<T> vec, T newVal){
    vec.push_back(newVal);
    return(vec);
}


inline bool orBranches(bool val1, bool val2){
    return(val1 || val2);
}

inline bool andBranches(bool val1, bool val2){
    return(val1 && val2);
}


inline bool orBranches3(bool val1, bool val2, bool val3){
    return(val1 || val2 || val3);
}

inline bool andBranches3(bool val1, bool val2, bool val3){
    return(val1 && val2 && val3);
}

inline bool passCut(bool val1){
    return(val1);
}

template<int modVal>
int modInt(int val){
    return(val%modVal);
}

template<int modVal>
int modIntPos(int val){
    if(val<0){
        return(val);
    }
    return(val%modVal);
}

template<typename T>
T multiply(T val1, T val2){
    return(val1*val2);
}

template<typename T>
T divide(T val1, T val2){
    return(val1/val2);
}

template<typename T>
T add(T val1, T val2){
    return(val1 + val2);
}

template<typename T>
T absDiff(T val1, T val2){
    return(fabs(val1-val2));
}

/*
template<typename T>
ROOT::RDF::RNode saveVar(T var, std::string name, ROOT::RDF::RNode df){
    auto storeVar = [var](unsigned int, const ROOT::RDF::RSampleInfo) -> T {
                                    return(var);
                                };
    auto newDf = df.DefinePerSample(name, storeVar);
    return(newDf);
}
*/

template<typename T>
T EvalDeltaPhi(T phi0, T phi1){

    double dPhi = fabs(phi0-phi1);

    if(dPhi > ROOT::Math::Pi())
        dPhi = 2.0*ROOT::Math::Pi() - dPhi;

    return dPhi;
}

template<typename T>
T EvalDeltaR(T eta0, T phi0, T eta1, T phi1) {
    
    T dEta = fabs(eta0-eta1);
    T dPhi = EvalDeltaPhi<T>(phi0, phi1);

    return sqrt(dEta*dEta+dPhi*dPhi);
}


// Madgraph Style dR
template<typename T>
T EvalDeltaR_MG(T eta0, T phi0, T eta1, T phi1) {

    T dEta = fabs(eta0-eta1);
    T dPhi = EvalDeltaPhi<T>(phi0, phi1);

    return(2*(cosh(dEta) - cos(dPhi)));
}


template<typename T>
ROOT::VecOps::RVec<T> EvalVectorSum(T Jet1_pt, T Jet1_eta, T Jet1_phi, T Jet1_m, T Jet2_pt, T Jet2_eta, T Jet2_phi, T Jet2_m){
    if(Jet2_pt<0){
        return(ROOT::VecOps::RVec<T>({Jet1_pt, Jet1_eta, Jet1_phi, Jet1_m}));
    } else if(Jet1_pt<0){
        return(ROOT::VecOps::RVec<T>({Jet2_pt, Jet2_eta, Jet2_phi, Jet2_m}));
    }
    auto vLorentz = ROOT::Math::PtEtaPhiMVector{Jet1_pt,Jet1_eta,
                                                    Jet1_phi,Jet1_m} + ROOT::Math::PtEtaPhiMVector{Jet2_pt,Jet2_eta,
                                                    Jet2_phi,Jet2_m};

    Float_t pt = vLorentz.Pt();
    Float_t eta = vLorentz.Eta();
    Float_t phi = vLorentz.Phi();
    Float_t mass = vLorentz.mass();
    return(ROOT::VecOps::RVec<T>({pt, eta, phi, mass}));
}

template<typename T>
ROOT::VecOps::RVec<T> EvalVectorSum_2(ROOT::VecOps::RVec<T> Jet1, ROOT::VecOps::RVec<T> Jet2){
    if(Jet1.size()!=4 || Jet2.size()!=4){
        std::cerr << "Jet sizes not 4 in EvalVectorSum" << std::endl;
	    ROOT::VecOps::RVec<T>({0,0,0,0});
    }

    auto vLorentz = ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<T>>{Jet1[0], Jet1[1], Jet1[2], Jet1[3]} + ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<T>>{Jet2[0], Jet2[1], Jet2[2], Jet2[3]};

    Float_t pt = vLorentz.Pt();
    Float_t eta = vLorentz.Eta();
    Float_t phi = vLorentz.Phi();
    Float_t mass = vLorentz.mass();
    return(ROOT::VecOps::RVec<T>({pt, eta, phi, mass}));
}


template<typename T>
ROOT::VecOps::RVec<T> Fill4Vec(T pt, T eta, T phi, T mass) {

    return(ROOT::VecOps::RVec<T>({pt, eta, phi, mass}));
}



# endif
