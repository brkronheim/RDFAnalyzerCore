#include "analysis.h"

#include <ROOT/RDataFrame.hxx>
#include <TH1D.h>
#include <array>
#include <chrono>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace native_results_example {
namespace {
constexpr std::array<double, 8> values{{-2., -1., 0., .5, 1., 2., 3., 5.}};
constexpr std::array<double, 8> weights{{1., -.5, 2., -1., .5, 1.5, -2., .25}};
using Clock = std::chrono::steady_clock;
using Json = rdfauthoring::Json;
// Detached JSON construction must never evaluate a ROOT action implicitly.
static_assert(!std::is_constructible_v<Json, ROOT::RDF::RResultPtr<ULong64_t>&>);
}

struct Analysis::Impl {
  ROOT::RDataFrame frame{8};
  ROOT::RDF::RNode all;
  ROOT::RDF::RNode selected;
  ROOT::RDF::RResultPtr<ULong64_t> input_count, selected_count;
  ROOT::RDF::RResultPtr<double> input_weight, selected_weight, selected_observable;
  ROOT::RDF::RResultPtr<TH1D> histogram;
  bool executed = false;

  Impl()
      : all(frame.Define("x", [](ULong64_t entry) { return values.at(entry); }, {"rdfentry_"})
                 .Define("weight", [](ULong64_t entry) { return weights.at(entry); }, {"rdfentry_"})),
        selected(all.Define("scaled", [](double x) { return 2. * x; }, {"x"})
                    .Filter([](double x) { return x >= 0.; }, {"x"}, "nonnegative x")
                    .Define("observable", [](double scaled) { return scaled - .5; }, {"scaled"})
                    .Filter([](double observable) { return observable < 7.; }, {"observable"}, "observable below seven")),
        input_count(all.Count()), selected_count(selected.Count()),
        input_weight(all.Sum<double>("weight")), selected_weight(selected.Sum<double>("weight")),
        selected_observable(selected.Sum<double>("observable")),
        histogram(selected.Histo1D<double, double>({"signed_observable", "Signed observable;observable;weighted events", 3, 0., 3.},
                                                  "observable", "weight")) {}
};

Analysis::Analysis() : impl_(std::make_unique<Impl>()) {}
Analysis::~Analysis() = default;

std::string Analysis::describe() const {
  // Metadata only: reading a result here would accidentally trigger the graph.
  Json::Object document{{"schema", "native-results-description-v1"}, {"fixture_entries", 8},
      {"actions_booked", 6}, {"event_loops", impl_->frame.GetNRuns()},
      {"graph", Json::Array{"define x", "define weight", "define scaled", "filter x",
                            "define observable", "filter observable", "book counts, sums and histogram"}},
      {"execution_requested", false}, {"analysis_library", "separately compiled C++17 static library"}};
  std::ostringstream stream;
  Json(std::move(document)).write(stream);
  return stream.str();
}

rdfauthoring::RunResult Analysis::run() {
  if (impl_->executed || impl_->frame.GetNRuns() != 0)
    throw std::logic_error("this example executes its prepared graph once");
  impl_->executed = true;
  const auto started = Clock::now();
  // Every action above is already booked. This is the sole explicit trigger.
  const auto input_count = impl_->input_count.GetValue();
  const auto selected_count = impl_->selected_count.GetValue();
  const auto input_weight = impl_->input_weight.GetValue();
  const auto selected_weight = impl_->selected_weight.GetValue();
  const auto selected_observable = impl_->selected_observable.GetValue();
  const auto& histogram = impl_->histogram.GetValue();
  if (impl_->frame.GetNRuns() != 1 || histogram.GetSumw2N() == 0)
    throw std::logic_error("expected one ROOT loop and weighted Sumw2 storage");
  Json::Array contents, variances;
  for (int bin = 0; bin <= histogram.GetNbinsX() + 1; ++bin) {
    contents.emplace_back(histogram.GetBinContent(bin));
    variances.emplace_back(histogram.GetSumw2()->At(bin));
  }
  // These are copied, already-evaluated values. The header owns no ROOT runtime
  // and neither constructs a graph nor triggers lazy actions.
  rdfauthoring::RunResult result("linked-native-cpp", rdfauthoring::Status::Succeeded,
      "one generated eight-event graph; detached in-memory counters, sums and histogram flows");
  result.counts = {{"input", input_count}, {"selected", selected_count}};
  result.sums = {{"input_weight", input_weight}, {"selected_weight", selected_weight},
                 {"selected_observable", selected_observable}};
  result.histograms = {{"observable", Json::Object{{"edges", Json::Array{0., 1., 2., 3.}},
      {"sumw", std::move(contents)}, {"sumw2", std::move(variances)}, {"flow", true}}}};
  result.timings = {{"execute_and_materialize_s", std::chrono::duration<double>(Clock::now() - started).count()}};
  result.provenance = {{"fixture", "generated-eight-events-v1"}, {"analysis", "linked-static-cpp17"},
                       {"weighting", "signed dyadic weights"}};
  result.details = {{"before_event_loops", 0}, {"actions_booked", 6},
      {"histogram_entries", histogram.GetEntries()}, {"histogram_storage", "TH1D; Sumw2 includes flows"},
      {"timing_scope", "explicit trigger through detached value extraction; not a pure loop benchmark"}};
  result.event_loops = impl_->frame.GetNRuns();
  return result;
}
} // namespace native_results_example
