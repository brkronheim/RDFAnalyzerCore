// Optional I/O microbenchmark; never registered with CTest.
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <TFile.h>
#include <TStopwatch.h>
#include <iostream>

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "Usage: benchmarkInputCache CONFIG 'scalar C++ RDF expression'\n"
                 "Example expression: Sum(Jet_pt) + MET_pt\n";
    return 2;
  }
  try {
    ConfigurationManager config(argv[1]);
    DataManager data(config);
    if (ROOT::IsImplicitMTEnabled()) {
      std::cerr << "Set threads=1 for this single-CPU benchmark\n";
      return 2;
    }
    auto df = data.getDataFrame().Define("__io_benchmark", "static_cast<double>(" + std::string(argv[2]) + ")");
    auto sum = df.Sum<double>("__io_benchmark");
    auto count = df.Count();
    const auto bytes = TFile::GetFileBytesRead();
    const auto reads = TFile::GetFileReadCalls();
    TStopwatch clock;
    clock.Start();
    const auto checksum = *sum;
    clock.Stop();
    const auto wall = clock.RealTime();
    const auto cpu = clock.CpuTime();
    std::cout << "wall_s,cpu_s,cpu_efficiency,events,events_per_s,bytes_read,read_calls,checksum\n"
              << wall << ',' << cpu << ',' << (wall > 0 ? cpu / wall : 0) << ','
              << *count << ',' << (wall > 0 ? *count / wall : 0) << ','
              << TFile::GetFileBytesRead() - bytes << ','
              << TFile::GetFileReadCalls() - reads << ',' << checksum << '\n';
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
