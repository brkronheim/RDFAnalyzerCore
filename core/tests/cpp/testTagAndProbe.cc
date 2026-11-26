#include "TagAndProbe.h"
#include <gtest/gtest.h>
#include <vector>

TEST(TagAndProbe,OrientationIsIndependentOfTagAndNoFallback) {
  std::vector<float> pt{45,45},eta{0,0},phi{0,3.14159265f},mass{0,0};
  std::vector<int> charge{1,-1};std::vector<bool> denom{true,true},both{true,true};
  int orientations[2]={0,0};
  for (unsigned event=0;event<20000;++event) {
    auto pair=rdf::tagprobe::selectPair(pt,eta,phi,mass,charge,denom,both,1,2,event);
    ++orientations[pair[0]];
    auto one=both;one[pair[0]]=false;
    auto rejected=rdf::tagprobe::selectPair(pt,eta,phi,mass,charge,denom,one,1,2,event);
    EXPECT_EQ(rejected[0],-1);EXPECT_EQ(rejected[2],1);
  }
  EXPECT_GT(orientations[0],9500);EXPECT_LT(orientations[0],10500);
}
TEST(TagAndProbe,EfficiencyIsUnbiasedWithOnePairPerEvent) {
  std::vector<float> pt{45,45},eta{0,0},phi{0,3.14159265f},mass{0,0};
  std::vector<int> charge{1,-1};std::vector<bool> denom{true,true};int pass=0,total=0;
  for (unsigned e=0;e<100000;++e) {
    std::vector<bool> decision{rdf::tagprobe::mix(e+200000)%10<7,rdf::tagprobe::mix(e+500000)%10<7};
    auto p=rdf::tagprobe::selectPair(pt,eta,phi,mass,charge,denom,decision,1,2,e);
    if(p[0]>=0) {++total;pass+=decision[p[1]];}
  }
  EXPECT_NEAR(double(pass)/total,.7,.01);
}
TEST(TagAndProbe,ChargeMassAndAlignment) {
  std::vector<float> pt{45,45},eta{0,0},phi{0,3.14159265f},mass{0,0};
  std::vector<int> q{1,1};std::vector<bool> mask{true,true};
  EXPECT_EQ(rdf::tagprobe::selectPair(pt,eta,phi,mass,q,mask,mask,1,1,1)[0],-1);
  EXPECT_THROW(rdf::tagprobe::selectPair(pt,std::vector<float>{0},phi,mass,q,mask,mask,1,1,1),std::invalid_argument);
}
TEST(TagAndProbe,TriggerMatchingRequiresAllBitsAndFiredPath) {
  std::vector<float> eta{0},phi{3.13f},pt{31,31},te{0,0},tf{-3.14f,3.13f};
  std::vector<int> id{11,11},bits{2,1};rdf::tagprobe::TriggerMatchConfig c{11,2,30,.1};
  auto m=rdf::tagprobe::matchTriggerObjects(eta,phi,pt,te,tf,id,bits,true,c);EXPECT_EQ(m[0],0);
  EXPECT_EQ(rdf::tagprobe::matchTriggerObjects(eta,phi,pt,te,tf,id,bits,false,c)[0],-1);
  c.requiredBits=3;EXPECT_EQ(rdf::tagprobe::matchTriggerObjects(eta,phi,pt,te,tf,id,bits,true,c)[0],-1);
}
