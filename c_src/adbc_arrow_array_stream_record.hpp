#ifndef ADBC_ARROW_ARRAY_STREAM_RECORD_HPP
#define ADBC_ARROW_ARRAY_STREAM_RECORD_HPP
#pragma once

#include <adbc.h>

struct ArrowArrayStreamRecord {
    struct ArrowSchema *schema;
    struct ArrowArray *values;
};

#endif  // ADBC_ARROW_ARRAY_STREAM_RECORD_HPP
