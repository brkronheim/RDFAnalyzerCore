#include "analysis.h"
#include <exception>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
  const std::string mode = argc == 1 ? "--describe" : argv[1];
  if (argc > 2 || (mode != "--describe" && mode != "--preflight" && mode != "--run")) {
    std::cerr << "usage: rdf-native-results [--describe|--preflight|--run]\n";
    return 2;
  }
  try {
    native_results_example::Analysis analysis;
    if (mode != "--run") {
      std::cout << analysis.describe() << '\n';
      return 0;
    }
    const auto result = analysis.run();
    std::cout << result.to_json() << '\n';
    return 0;
  } catch (const std::exception& error) {
    rdfauthoring::RunResult result("linked-native-cpp", rdfauthoring::Status::Failed,
                                  "example setup or explicit execution failed; summaries unavailable");
    result.issues.push_back({{"severity", "error"}, {"message", std::string(error.what())}});
    std::cout << result.to_json() << '\n';
    return 1;
  }
}
