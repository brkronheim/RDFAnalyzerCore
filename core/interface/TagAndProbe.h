#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

// Container-generic, experiment-independent helpers. No ROOT or CMS dependency.
namespace rdf { namespace tagprobe {
struct PairConfig {
  double minMass=60., maxMass=120., referenceMass=91.1876, minDeltaR=0.;
  bool oppositeCharge=true;
  std::uint64_t seed=1;
};
inline double deltaR(double eta,double phi,double otherEta,double otherPhi) {
  return std::hypot(eta-otherEta,std::remainder(phi-otherPhi,2*std::acos(-1.)));
}
inline std::uint64_t mix(std::uint64_t x) {
  x+=UINT64_C(0x9e3779b97f4a7c15);x=(x^(x>>30))*UINT64_C(0xbf58476d1ce4e5b9);
  x=(x^(x>>27))*UINT64_C(0x94d049bb133111eb);return x^(x>>31);
}
inline std::uint64_t eventKey(std::uint64_t run,std::uint64_t lumi,std::uint64_t event,std::uint64_t seed) {
  return mix(event^mix(run)^mix(lumi+UINT64_C(0x100000000))^mix(seed));
}
template<class Pt,class Eta,class Phi,class Mass>
double invariantMass(const Pt &pt,const Eta &eta,const Phi &phi,const Mass &mass,int i,int j) {
  double px=0,py=0,pz=0,e=0;
  for (const auto k:{i,j}) {
    const double z=pt.at(k)*std::sinh(eta.at(k));
    px+=pt.at(k)*std::cos(phi.at(k));py+=pt.at(k)*std::sin(phi.at(k));pz+=z;
    e+=std::sqrt(pt.at(k)*pt.at(k)+z*z+mass.at(k)*mass.at(k));
  }
  return std::sqrt(std::max(0.,e*e-px*px-py*py-pz*pz));
}
// Result {tag, probe, number of eligible unordered denominator pairs}.
// Select the denominator pair and orientation BEFORE examining tag decisions.
// Do not swap/fall back when the chosen tag fails: doing so biases probe rates.
template<class Pt,class Eta,class Phi,class Mass,class Charge,class Denominator,class Tags>
std::vector<int> selectPair(const Pt &pt,const Eta &eta,const Phi &phi,const Mass &mass,
    const Charge &charge,const Denominator &denominator,const Tags &tags,
    std::uint64_t run,std::uint64_t lumi,std::uint64_t event,const PairConfig &c={}) {
  const auto n=pt.size();
  if (eta.size()!=n||phi.size()!=n||mass.size()!=n||charge.size()!=n||denominator.size()!=n||tags.size()!=n)
    throw std::invalid_argument("tag-and-probe object collections are not aligned");
  if (!std::isfinite(c.minMass)||!std::isfinite(c.maxMass)||!std::isfinite(c.referenceMass)||
      !std::isfinite(c.minDeltaR)||c.minMass<0||c.maxMass<=c.minMass||c.minDeltaR<0)
    throw std::invalid_argument("invalid tag-and-probe pair configuration");
  int first=-1,second=-1,count=0;double best=std::numeric_limits<double>::infinity();
  for (std::size_t i=0;i<n;++i) for (std::size_t j=i+1;j<n;++j) {
    if (!denominator[i]||!denominator[j])continue;
    if (!std::isfinite(pt[i])||!std::isfinite(pt[j])||pt[i]<=0||pt[j]<=0||
        !std::isfinite(eta[i])||!std::isfinite(eta[j])||!std::isfinite(phi[i])||!std::isfinite(phi[j])||
        !std::isfinite(mass[i])||!std::isfinite(mass[j])||mass[i]<0||mass[j]<0)continue;
    if (c.oppositeCharge && (charge[i]==0||charge[j]==0||((charge[i]>0)==(charge[j]>0))))continue;
    if (deltaR(eta[i],phi[i],eta[j],phi[j])<c.minDeltaR)continue;
    const double m=invariantMass(pt,eta,phi,mass,i,j);
    if (!std::isfinite(m)||m<c.minMass||m>c.maxMass)continue;
    ++count;const double distance=std::abs(m-c.referenceMass);
    if (distance<best) {best=distance;first=int(i);second=int(j);}
  }
  if (first<0)return {-1,-1,count};
  if (eventKey(run,lumi,event,c.seed)&1)std::swap(first,second);
  if (!tags[first])return {-1,-1,count};
  return {first,second,count};
}

struct TriggerMatchConfig {
  int absoluteId=0;
  std::uint32_t requiredBits=0;
  double minPt=0.,maxDeltaR=.1;
};
// Closest qualifying object index, or -1. Event/path gate is supplied explicitly.
template<class Eta,class Phi,class TP,class TE,class TF,class TI,class TB>
std::vector<int> matchTriggerObjects(const Eta &eta,const Phi &phi,const TP &pt,const TE &te,
    const TF &tf,const TI &id,const TB &bits,bool fired,const TriggerMatchConfig &c) {
  if (eta.size()!=phi.size()||pt.size()!=te.size()||pt.size()!=tf.size()||pt.size()!=id.size()||pt.size()!=bits.size())
    throw std::invalid_argument("trigger matching collections are not aligned");
  if (c.absoluteId<=0||!std::isfinite(c.minPt)||!std::isfinite(c.maxDeltaR)||c.minPt<0||c.maxDeltaR<=0)
    throw std::invalid_argument("invalid trigger match configuration");
  std::vector<int> result(eta.size(),-1);if(!fired)return result;
  for (std::size_t i=0;i<eta.size();++i) {
    if(!std::isfinite(eta[i])||!std::isfinite(phi[i]))continue;
    double closest=c.maxDeltaR;
    for (std::size_t j=0;j<pt.size();++j) {
      if(std::abs(int(id[j]))!=c.absoluteId||!std::isfinite(pt[j])||pt[j]<c.minPt||
          !std::isfinite(te[j])||!std::isfinite(tf[j])||
          (std::uint32_t(bits[j])&c.requiredBits)!=c.requiredBits)continue;
      const double dr=deltaR(eta[i],phi[i],te[j],tf[j]);
      if(dr<closest) {closest=dr;result[i]=int(j);}
    }
  }
  return result;
}
}} // namespace rdf::tagprobe
