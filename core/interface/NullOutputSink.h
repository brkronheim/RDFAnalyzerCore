#ifndef NULLOUTPUTSINK_H_INCLUDED
#define NULLOUTPUTSINK_H_INCLUDED

#include "api/IOutputSink.h"

/**
 * @brief No-op output sink for early wiring.
 */
class NullOutputSink : public IOutputSink {
public:
  void writeDataFrame(ROOT::RDF::RNode&, const OutputSpec&) override {}
};

#endif // NULLOUTPUTSINK_H_INCLUDED
