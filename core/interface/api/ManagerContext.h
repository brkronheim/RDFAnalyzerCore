#ifndef MANAGERCONTEXT_H_INCLUDED
#define MANAGERCONTEXT_H_INCLUDED

class IConfigurationProvider;
class IDataFrameProvider;
class ISystematicManager;
class ILogger;
class IOutputSink;

/**
 * @brief Shared context injected into managers/services.
 */
struct ManagerContext {
  IConfigurationProvider& config;
  IDataFrameProvider& data;
  ISystematicManager& systematics;
  ILogger& logger;
  IOutputSink& skimSink;
  IOutputSink& metaSink;
};

#endif // MANAGERCONTEXT_H_INCLUDED
