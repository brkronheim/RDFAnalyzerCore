#ifndef DEFAULTLOGGER_H_INCLUDED
#define DEFAULTLOGGER_H_INCLUDED

#include "api/ILogger.h"
#include <iostream>

/**
 * @brief Simple stdout logger.
 */
class DefaultLogger : public ILogger {
public:
  void log(Level level, const std::string& message) override {
    std::ostream& os = (level == Level::Error || level == Level::Warn) ? std::cerr : std::cout;
    os << message << std::endl;
  }
};

#endif // DEFAULTLOGGER_H_INCLUDED
