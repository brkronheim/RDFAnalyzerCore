#include <RtypesCore.h>
#include <analyzer.h>
#include <plots.h>

Size_t size(std::map<std::pair<Int_t, Int_t>, Int_t> &map) {
  // std::cerr << map.size() << std::endl;
  return (map.size());
}

inline constexpr Float_t return0() { return (0.0); }

int main(int argc, char **argv) {

  if (argc != 2) {
    std::cout << "Arguments: " << argc << std::endl;
    std::cerr << "Error!!!!! No configuration file provided. Please include a "
                 "config file."
              << std::endl;
    return (1);
  }

  Analyzer an(argv[1]);

  an.Define("channel", return0, {})
      ->Define("controlRegion", return0, {})
      ->Define("sampleCategory", return0, {})
      ->Define("size", size, {"nPixelClusters"});

  Int_t bins = 80;
  Float_t lowerBound = 0.0;
  Float_t upperBound = 1.0;

  // Define base histograms
  std::vector<histInfo> histInfos = {
      // V vars:
      // histInfo("HistName", "PlotBranchName", "xLabel", "WeightBranchName",
      // bins, lowerBound, upperBound),
      histInfo("HistName", "bunchCrossing", "xLabel", "size", 4000, 0, 4000),
  };
  std::vector<std::vector<histInfo>> fullHistList = {histInfos};

  // Define selection categories: branchName, numBins, lowerBound, upperBound
  selectionInfo channelBounds("channel", 1, 0.0, 1.0);
  selectionInfo controlBounds("controlRegion", 1, 0.0, 1.0);
  selectionInfo categoryBounds("sampleCategory", 4, 0.0, 4.0);

  // List of all region names, should correspond to the bins of the axes other
  // than the systematic and main variable
  std::vector<std::vector<std::string>> allRegionNames = {
      {"Channel"},
      {"Control Region"},
      {"data_obs", "Process 1", "Process 2", "Process 3"}
  };

  // vector of selection for each ND histogram
  std::vector<selectionInfo> selection = {channelBounds, controlBounds,
                                          categoryBounds};

  // Book all the histograms, provide a suffix of "All" to these. This can be
  // called multiple times
  an.BookND(histInfos, selection, "All", allRegionNames);

  // Trigger the execution loop and save the histograms
  an.SaveHists(fullHistList, allRegionNames);

  return (0);
}