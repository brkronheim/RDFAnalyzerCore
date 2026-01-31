#include <CounterService.h>
#include <api/IConfigurationProvider.h>
#include <api/ILogger.h>

void CounterService::initialize(ManagerContext& ctx) {
  ctx_m = &ctx;

  if (ctx.config.getConfigMap().find("sample") != ctx.config.getConfigMap().end()) {
    sampleName_m = ctx.config.getConfigMap().at("sample");
  } else if (ctx.config.getConfigMap().find("type") != ctx.config.getConfigMap().end()) {
    sampleName_m = ctx.config.getConfigMap().at("type");
  } else {
    sampleName_m = "unknown";
  }
}

void CounterService::finalize(ROOT::RDF::RNode& df) {
  if (!ctx_m) {
    return;
  }

  auto countResult = df.Count();
  auto countValue = countResult.GetValue();

  ctx_m->logger.log(ILogger::Level::Info,
                    "CounterService: sample=" + sampleName_m +
                    " entries=" + std::to_string(countValue));
}
