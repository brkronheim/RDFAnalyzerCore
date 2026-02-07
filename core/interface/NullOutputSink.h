#ifndef NULLOUTPUTSINK_H_INCLUDED
#define NULLOUTPUTSINK_H_INCLUDED

#include "api/IOutputSink.h"

/**
 * @brief No-op output sink for early wiring.
 */
class NullOutputSink : public IOutputSink {
public:
  void writeDataFrame(ROOT::RDF::RNode&, const OutputSpec&) override {}
  void writeDataFrame(ROOT::RDF::RNode&,
                      const IConfigurationProvider&,
                      const IDataFrameProvider*,
                      const ISystematicManager*,
                      OutputChannel) override {}
  std::string resolveOutputFile(const IConfigurationProvider& configProvider,
                                OutputChannel channel) override {
    if (channel == OutputChannel::Meta) {
      std::string metaFile = configProvider.get("metaFile");
      if (!metaFile.empty()) {
        return metaFile;
      }
      const std::string saveFile = configProvider.get("saveFile");
      if (saveFile.empty()) {
        return "";
      }
      const auto pos = saveFile.rfind('.');
      if (pos != std::string::npos) {
        return saveFile.substr(0, pos) + "_meta" + saveFile.substr(pos);
      }
      return saveFile + "_meta.root";
    }
    return configProvider.get("saveFile");
  }
};

#endif // NULLOUTPUTSINK_H_INCLUDED
