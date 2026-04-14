#ifndef TRACE_MACROS_H
#define TRACE_MACROS_H

#include "trace.h"

#ifdef TRACE

class TraceScope {
public:
    TraceScope(const char* module, const char* func) {
        TRACE_ENTER(module, func);
    }

    ~TraceScope() {
        TRACE_EXIT();
    }
};

#define TRACE_FUNC(module) \
    TraceScope __trace_scope(module, __func__)

#else

#define TRACE_FUNC(module)

#endif

#endif