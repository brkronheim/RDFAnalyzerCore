#ifndef ILOGGER_H_INCLUDED
#define ILOGGER_H_INCLUDED

#include <string>

/**
 * @brief Lightweight logger interface.
 *
 * Intended to be injected via ManagerContext.
 */
class ILogger {
public:
  enum class Level { Trace, Debug, Info, Warn, Error };
  virtual ~ILogger() = default;
  virtual void log(Level level, const std::string& message) = 0;
};

#endif // ILOGGER_H_INCLUDED
