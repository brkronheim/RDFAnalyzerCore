// Independent Run 3 NanoAOD workload for full-worker timing. These selections
// exercise a long cutflow and a broad snapshot on the same graph.
#include <CutflowManager.h>
#include <analyzer.h>
#include <iostream>

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "Usage: benchmarkNanoSkim worker.yaml\n";
    return 2;
  }
  Analyzer analyzer(argv[1]);
  auto cuts = CutflowManager::create(analyzer);
  analyzer.Define("hasLepton", [](int muons, int electrons) {
    return muons > 0 || electrons > 0;
  }, {"nMuon", "nElectron"});
  analyzer.Define("hasJet", [](int jets) { return jets > 0; }, {"nJet"});
  for (const auto column : {
           "Flag_goodVertices", "Flag_globalSuperTightHalo2016Filter",
           "Flag_HBHENoiseFilter", "Flag_HBHENoiseIsoFilter",
           "Flag_EcalDeadCellTriggerPrimitiveFilter", "Flag_BadPFMuonFilter",
           "Flag_BadPFMuonDzFilter", "Flag_hfNoisyHitsFilter",
           "Flag_eeBadScFilter", "Flag_ecalBadCalibFilter", "hasLepton", "hasJet"})
    cuts->addCut(column, column);
  analyzer.run();
}
