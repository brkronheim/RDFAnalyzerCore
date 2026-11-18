// Installed Analyzer example using existing Analyzer and RunResult APIs.
#include <analyzer.h>
#include <RunResult.h>
#include <TH1D.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>

int main(int argc,char** argv) {
  using Json=rdfauthoring::Json;
  using Clock=std::chrono::steady_clock;
  if (argc==1) {
    Json(Json::Object{{"scope","Analyzer SDK scalar example"},
        {"execution_requested",false},{"required_mode","--run CONFIG OUTPUT.json"},
        {"physics","double weight, value=2*weight, nonzero selection, signed histogram with flows/Sumw2"}}).write(std::cout);
    std::cout<<'\n';return 0;
  }
  try {
    if (argc!=4 || std::string(argv[1])!="--run") throw std::invalid_argument("usage: analyzer-onboarding --run CONFIG OUTPUT.json");
    const std::filesystem::path output(argv[3]);
    if (std::filesystem::exists(output) || !std::filesystem::is_directory(output.parent_path()))
      throw std::invalid_argument("fresh output and existing parent directory required");
    const auto started=Clock::now();
    Analyzer analysis(argv[2]);
    analysis.DefineNominal("sdk_weight",[](Float_t weight){return double(weight);},{"genWeight"});
    analysis.DefineNominal("sdk_value",[](double weight){return 2.*weight;},{"sdk_weight"});
    auto raw=analysis.getDataFrameProvider().getDataFrame();
    auto input=raw.Count();auto input_weight=raw.Sum<double>("sdk_weight");
    analysis.Filter("sdk_nonzero",[](double value){return value!=0.;},{"sdk_value"});
    auto selected=analysis.getDataFrameProvider().getDataFrame();
    auto count=selected.Count();auto selected_weight=selected.Sum<double>("sdk_weight");
    auto histogram=selected.Histo1D<double,double>({"observable","",4,-8.,8.},"sdk_value","sdk_weight");
    const auto ready=Clock::now();
    analysis.run();
    const auto n=input.GetValue(), nselected=count.GetValue();
    const auto total=input_weight.GetValue(), passing=selected_weight.GetValue();
    const auto& hist=histogram.GetValue();
    const auto executed=Clock::now();
    if (raw.GetNRuns()!=1 || hist.GetSumw2N()==0) throw std::runtime_error("one loop and signed weighted Sumw2 required");
    Json::Array sumw,sumw2;
    for (int bin=0;bin<=hist.GetNbinsX()+1;++bin) {
      sumw.emplace_back(hist.GetBinContent(bin));sumw2.emplace_back(hist.GetSumw2()->At(bin));
    }
    rdfauthoring::RunResult result("analyzer-sdk-cpp",rdfauthoring::Status::Succeeded,
        "completed Analyzer scalar example");
    result.counts={{"input",n},{"selected",nselected}};
    result.sums={{"input_weight",total},{"selected_weight",passing}};
    result.histograms={{"observable",Json::Object{{"edges",Json::Array{-8.,-4.,0.,4.,8.}},
        {"sumw",std::move(sumw)},{"sumw2",std::move(sumw2)},{"flow",true}}}};
    result.event_loops=raw.GetNRuns();
    result.timings={{"setup_s",std::chrono::duration<double>(ready-started).count()},
        {"processing_s",std::chrono::duration<double>(executed-ready).count()},
        {"detached_export_s",std::chrono::duration<double>(Clock::now()-executed).count()}};
    result.details={{"actions_booked",5},{"physics_source","C++; independent Python source is retained"},
        {"export_scope","detached extraction through result construction; excludes JSON encoding/file write"}};
    std::ofstream stream(output);
    if (!stream) throw std::runtime_error("cannot open output");
    stream<<result.to_json()<<'\n';
    if (!stream) throw std::runtime_error("cannot write output");
    return 0;
  } catch(const std::exception& error) {
    std::cerr<<error.what()<<'\n';return 1;
  }
}
