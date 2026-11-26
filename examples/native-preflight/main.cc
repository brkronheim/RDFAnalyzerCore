#include "inspection.h"
#include <CommonSettingsSchema.h>
#include <fstream>
#include <iostream>

int main(int argc, char **argv) {
  try {
    const std::string mode = argc > 1 ? argv[1] : "--help";
    if (mode == "--help" && argc <= 2) {
      std::cout << "rdf-native-preflight --check-input INPUT REPORT.json\n"
                   "rdf-native-preflight --describe-settings\n\n"
                << rdf::config::commonSettingsHelp();
      return 0;
    }
    if (mode == "--describe-settings" && argc == 2) {
      std::cout << rdf::config::commonSettingsDescriptionJson() << '\n';
      return 0;
    }
    if (mode != "--check-input" || argc != 4)
      throw std::invalid_argument("Expected --check-input INPUT REPORT.json, --describe-settings or --help");
    const auto output = std::filesystem::path(argv[3]);
    if (std::filesystem::exists(output)) throw std::invalid_argument("Choose a fresh report path: " + output.string());
    const auto inspection = inspectInput(argv[2]);
    std::ofstream stream(output);
    if (!stream) throw std::runtime_error("Cannot create report: " + output.string());
    stream << inspection.report.toJson() << '\n';
    stream.close();
    if (!stream) throw std::runtime_error("Cannot finish report: " + output.string());
    std::cout << inspection.report.toJson() << '\n';
    std::cerr << "event_loops=" << inspection.eventLoops << '\n';
    return inspection.report.passed() ? 0 : 2;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
