#ifndef ICONTEXTAWARE_H_INCLUDED
#define ICONTEXTAWARE_H_INCLUDED

#include "ManagerContext.h"

/**
 * @brief Interface for components that receive ManagerContext injection.
 */
class IContextAware {
public:
  virtual ~IContextAware() = default;
  virtual void setContext(ManagerContext& ctx) = 0;
};

#endif // ICONTEXTAWARE_H_INCLUDED
