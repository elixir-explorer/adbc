#ifndef ADBC_ARROW_ARRAY_STREAM_RECORD_HPP
#define ADBC_ARROW_ARRAY_STREAM_RECORD_HPP
#pragma once

#include <adbc.h>

struct ArrowArrayStreamRecord {
    struct ArrowSchema *schema = nullptr;
    struct ArrowArray *values = nullptr;
};

#endif  // ADBC_ARROW_ARRAY_STREAM_RECORD_HPP
