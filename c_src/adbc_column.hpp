#ifndef ADBC_COLUMN_HPP
#pragma once

#include <time.h>
#include <cstdbool>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <adbc.h>
#include <erl_nif.h>
#include "adbc_consts.h"
#include "nif_utils.hpp"

ERL_NIF_TERM make_adbc_column(ErlNifEnv *env, ERL_NIF_TERM name_term, ERL_NIF_TERM type_term, bool nullable, ERL_NIF_TERM metadata, ERL_NIF_TERM data) {
    ERL_NIF_TERM nullable_term = nullable ? kAtomTrue : kAtomFalse;

    ERL_NIF_TERM keys[] = {
        kAtomStructKey,
        kAtomNameKey,
        kAtomTypeKey,
        kAtomNullableKey,
        kAtomMetadataKey,
        kAtomDataKey,
    };
    ERL_NIF_TERM values[] = {
        kAtomAdbcColumnModule,
        name_term,
        type_term,
        nullable_term,
        metadata,
        data,
    };

    ERL_NIF_TERM adbc_column;
    enif_make_map_from_arrays(env, keys, values, sizeof(keys)/sizeof(keys[0]), &adbc_column);
    return adbc_column;
}

ERL_NIF_TERM make_adbc_column(ErlNifEnv *env, ERL_NIF_TERM name_term, const char * type, bool nullable, ERL_NIF_TERM metadata, ERL_NIF_TERM data) {
    ERL_NIF_TERM type_term = erlang::nif::make_binary(env, type);
    return make_adbc_column(env, name_term, type_term, nullable, metadata, data);
}

ERL_NIF_TERM make_adbc_column(ErlNifEnv *env, const char * name, const char * type, bool nullable, ERL_NIF_TERM metadata, ERL_NIF_TERM data) {
    ERL_NIF_TERM name_term = erlang::nif::make_binary(env, name == nullptr ? "" : name);
    return make_adbc_column(env, name_term, type, nullable, metadata, data);
}

template <typename Integer, typename std::enable_if<
        std::is_integral<Integer>{} && std::is_signed<Integer>{}, bool>::type = true>
int get_list_integer(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<void(Integer val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        int64_t val;
        if (!erlang::nif::get(env, head, &val)) {
            if (nullable && enif_is_identical(head, kAtomNil)) {
                callback(0, true);
            } else {
                return 1;
            }
        }
        callback((Integer)val, false);
    }
    return 0;
}

template <typename Integer, typename std::enable_if<
        std::is_integral<Integer>{} && !std::is_signed<Integer>{}, bool>::type = true>
int get_list_integer(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<void(Integer val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        uint64_t val;
        if (!erlang::nif::get(env, head, &val)) {
            if (nullable && enif_is_identical(head, kAtomNil)) {
                callback(0, true);
            } else {
                return 1;
            }
        }
        callback((Integer)val, false);
    }
    return 0;
}

template <typename T>
int do_get_list_integer(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    if (nullable) {
        return get_list_integer<T>(env, list, nullable, [&array_out](T val, bool is_nil) -> void {
            ArrowArrayAppendInt(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_integer<T>(env, list, nullable, [&array_out](T val, bool) -> void {
            ArrowArrayAppendInt(array_out, val);
        });
    }
}

int get_list_float(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<void(double val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        double val;
        if (!erlang::nif::get(env, head, &val)) {
            if (nullable && enif_is_identical(head, kAtomNil)) {
                callback(0, true);
            } else {
                return 1;
            }
        }
        callback(val, false);
    }
    return 0;
}

int do_get_list_float(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    if (nullable) {
        return get_list_float(env, list, nullable, [&array_out](double val, bool is_nil) -> void {
            ArrowArrayAppendDouble(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_float(env, list, nullable, [&array_out](double val, bool) -> void {
            ArrowArrayAppendDouble(array_out, val);
        });
    }
}

int get_list_decimal(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, int32_t bitwidth, int32_t precision, int32_t scale, const std::function<void(struct ArrowDecimal * val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        struct ArrowDecimal val{};
        ArrowDecimalInit(&val, bitwidth, precision, scale);
        ErlNifBinary bytes;
        if (enif_is_binary(env, head) && enif_inspect_binary(env, head, &bytes)) {
            if (nanoarrow_type == NANOARROW_TYPE_DECIMAL128) {
                if (bytes.size != 16) {
                    return 1;
                }
                ArrowDecimalSetBytes(&val, (const uint8_t *)bytes.data);
            } else if (nanoarrow_type == NANOARROW_TYPE_DECIMAL256) {
                if (bytes.size != 32) {
                    return 1;
                }
                ArrowDecimalSetBytes(&val, (const uint8_t *)bytes.data);
            } else {
                return 1;
            }
            callback(&val, false);
        } else if (nullable && enif_is_identical(head, kAtomNil)) {
            callback(&val, true);
        } else {
            return 1;
        }
    }
    return 0;
}

int do_get_list_decimal(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, int32_t bitwidth, int32_t precision, int32_t scale, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDecimal(schema_out, nanoarrow_type, precision, scale));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    if (nullable) {
        return get_list_decimal(env, list, nullable, nanoarrow_type, bitwidth, precision, scale, [&array_out](struct ArrowDecimal * val, bool is_nil) -> void {
            ArrowArrayAppendDecimal(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_decimal(env, list, nullable, nanoarrow_type,  bitwidth, precision, scale, [&array_out](struct ArrowDecimal * val, bool) -> void {
            ArrowArrayAppendDecimal(array_out, val);
        });
    }
}

int get_list_string(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<void(struct ArrowStringView val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        ErlNifBinary bytes;
        struct ArrowStringView val{};
        if (enif_is_binary(env, head) && enif_inspect_binary(env, head, &bytes)) {
            val.data = (const char *)bytes.data;
            val.size_bytes = static_cast<int64_t>(bytes.size);
            callback(val, false);
        } else if (nullable && enif_is_identical(head, kAtomNil)) {
            callback(val, true);
        } else {
            return 1;
        }
    }
    return 0;
}

int do_get_list_string(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    if (nullable) {
        return get_list_string(env, list, nullable, [&array_out](struct ArrowStringView val, bool is_nil) -> void {
            ArrowArrayAppendString(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_string(env, list, nullable, [&array_out](struct ArrowStringView val, bool) -> void {
            ArrowArrayAppendString(array_out, val);
        });
    }
}

int get_list_boolean(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<void(bool val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomTrue)) {
            callback(true, false);
        } else if (enif_is_identical(head, kAtomFalse)) {
            callback(false, false);
        } else if (enif_is_identical(head, kAtomNil)) {
            callback(true, true);
        } else {
            return 1;
        }
    }
    return 0;
}

int do_get_list_boolean(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    if (nullable) {
        return get_list_boolean(env, list, nullable, [&array_out](bool val, bool is_nil) -> void {
            ArrowArrayAppendInt(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_boolean(env, list, nullable, [&array_out](bool val, bool) -> void {
            ArrowArrayAppendInt(array_out, val);
        });
    }
}

int get_list_fixed_size_binary(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<void(struct ArrowBufferView val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        ErlNifBinary bytes;
        struct ArrowBufferView val{};
        if (enif_is_binary(env, head) && enif_inspect_binary(env, head, &bytes)) {
            val.data.data = bytes.data;
            val.size_bytes = static_cast<int64_t>(bytes.size);
            callback(val, false);
        } else if (nullable && enif_is_identical(head, kAtomNil)) {
            callback(val, true);
        } else {
            return 1;
        }
    }
    return 0;
}

int do_get_list_fixed_size_binary(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, int32_t fixed_size, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeFixedSize(schema_out, nanoarrow_type, fixed_size));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    if (nullable) {
        return get_list_fixed_size_binary(env, list, nullable, [&array_out](struct ArrowBufferView val, bool is_nil) -> void {
            ArrowArrayAppendBytes(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_fixed_size_binary(env, list, nullable, [&array_out](struct ArrowBufferView val, bool) -> void {
            ArrowArrayAppendBytes(array_out, val);
        });
    }
}

int get_utc_offset() {
    time_t zero = 24 * 60 * 60L;
    struct tm * timeptr;
    int gmtime_hours;
    timeptr = localtime(&zero);
    gmtime_hours = timeptr->tm_hour;
    if (timeptr->tm_mday < 2) {
        gmtime_hours -= 24;
    }
    return gmtime_hours;
}

int get_list_date(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int64_t(int64_t)> &normalize_ex_value, const std::function<void(int64_t val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            callback(0, true);
        } else {
            int64_t val;
            if (erlang::nif::get(env, head, &val)) {
                callback(val, false);
            } else if (enif_is_map(env, head)) {
                ERL_NIF_TERM struct_name_term, calendar_term, year_term, month_term, day_term;
                if (!enif_get_map_value(env, head, kAtomStructKey, &struct_name_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_is_identical(struct_name_term, kAtomDateModule)) {
                    return kErrorBufferWrongStruct;
                }

                if (!enif_get_map_value(env, head, kAtomCalendarKey, &calendar_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_is_identical(calendar_term, kAtomCalendarISO)) {
                    return kErrorExpectedCalendarISO;
                }

                if (!enif_get_map_value(env, head, kAtomYearKey, &year_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomMonthKey, &month_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomDayKey, &day_term)) {
                    return kErrorBufferGetMapValue;
                }

                tm time{};
                if (!erlang::nif::get(env, year_term, &time.tm_year) || !erlang::nif::get(env, month_term, &time.tm_mon) || !erlang::nif::get(env, day_term, &time.tm_mday)) {
                    return kErrorBufferGetMapValue;
                }
                time.tm_year -= 1900;
                time.tm_mon -= 1;
                // mktime always gives local time
                // so we need to adjust it to UTC
                val = mktime(&time) + get_utc_offset() * 3600;
                callback(normalize_ex_value(val), false);
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_date(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    std::function<int64_t(int64_t)> normalize_ex_value;
    if (nanoarrow_type == NANOARROW_TYPE_DATE32) {
        normalize_ex_value = [](int64_t val) -> int64_t {
            return val / (24 * 60 * 60);
        };
    } else {
        normalize_ex_value = [](int64_t val) -> int64_t {
            return val * 1000;
        };
    }
    if (nullable) {
        return get_list_date(env, list, nullable, normalize_ex_value, [&array_out](int64_t val, bool is_nil) -> void {
            ArrowArrayAppendInt(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_date(env, list, nullable, normalize_ex_value, [&array_out](int64_t val, bool) -> void {
            ArrowArrayAppendInt(array_out, val);
        });
    }
}

int get_list_time(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int64_t(int64_t, uint64_t)> &normalize_ex_value, const std::function<void(int64_t val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            callback(0, true);
        } else {
            int64_t val;
            if (erlang::nif::get(env, head, &val)) {
                callback(val, false);
            } else if (enif_is_map(env, head)) {
                ERL_NIF_TERM struct_name_term, calendar_term, hour_term, minute_term, second_term, microsecond_term;
                if (!enif_get_map_value(env, head, kAtomStructKey, &struct_name_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_is_identical(struct_name_term, kAtomTimeModule)) {
                    return kErrorBufferWrongStruct;
                }

                if (!enif_get_map_value(env, head, kAtomCalendarKey, &calendar_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_is_identical(calendar_term, kAtomCalendarISO)) {
                    return kErrorExpectedCalendarISO;
                }

                if (!enif_get_map_value(env, head, kAtomHourKey, &hour_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomMinuteKey, &minute_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomSecondKey, &second_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomMicrosecondKey, &microsecond_term)) {
                    return kErrorBufferGetMapValue;
                }

                tm time{};
                if (!erlang::nif::get(env, hour_term, &time.tm_hour) || !erlang::nif::get(env, minute_term, &time.tm_min) || !erlang::nif::get(env, second_term, &time.tm_sec)) {
                    return kErrorBufferGetMapValue;
                }

                const ERL_NIF_TERM *us_tuple = nullptr;
                int us_arity;
                uint64_t us;
                int us_precision;
                if (!enif_get_tuple(env, microsecond_term, &us_arity, &us_tuple) || us_arity != 2) {
                    return kErrorBufferGetMapValue;
                }
                if (!erlang::nif::get(env, us_tuple[0], &us) || !erlang::nif::get(env, us_tuple[1], &us_precision)) {
                    return kErrorBufferGetMapValue;
                }

                val = time.tm_hour * 3600 + time.tm_min * 60 + time.tm_sec;
                callback(normalize_ex_value(val, us), false);
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_time(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, enum ArrowTimeUnit time_unit, uint64_t unit, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out, nanoarrow_type, time_unit, NULL));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    auto normalize_ex_value = [=](int64_t val, uint64_t us) -> int64_t {
        if (time_unit == NANOARROW_TIME_UNIT_SECOND) {
            return val;
        }
        val = (val * 1000000 + us) * 1000 / unit;
        return val;
    };
    if (nullable) {
        return get_list_time(env, list, nullable, normalize_ex_value, [&array_out](int64_t val, bool is_nil) -> void {
            ArrowArrayAppendInt(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_time(env, list, nullable, normalize_ex_value, [&array_out](int64_t val, bool) -> void {
            ArrowArrayAppendInt(array_out, val);
        });
    }
}

int get_list_timestamp(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int64_t(int64_t, uint64_t)> &normalize_ex_value, const std::function<void(int64_t val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            callback(0, true);
        } else {
            int64_t val;
            if (erlang::nif::get(env, head, &val)) {
                callback(val, false);
            } else if (enif_is_map(env, head)) {
                ERL_NIF_TERM struct_name_term, calendar_term, year_term, month_term, day_term, hour_term, minute_term, second_term, microsecond_term;
                if (!enif_get_map_value(env, head, kAtomStructKey, &struct_name_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_is_identical(struct_name_term, kAtomNaiveDateTimeModule)) {
                    return kErrorBufferWrongStruct;
                }

                if (!enif_get_map_value(env, head, kAtomCalendarKey, &calendar_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_is_identical(calendar_term, kAtomCalendarISO)) {
                    return kErrorExpectedCalendarISO;
                }

                if (!enif_get_map_value(env, head, kAtomYearKey, &year_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomMonthKey, &month_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomDayKey, &day_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomHourKey, &hour_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomMinuteKey, &minute_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomSecondKey, &second_term)) {
                    return kErrorBufferGetMapValue;
                }
                if (!enif_get_map_value(env, head, kAtomMicrosecondKey, &microsecond_term)) {
                    return kErrorBufferGetMapValue;
                }

                tm time{};
                if (!erlang::nif::get(env, year_term, &time.tm_year) ||
                    !erlang::nif::get(env, month_term, &time.tm_mon) ||
                    !erlang::nif::get(env, day_term, &time.tm_mday) ||
                    !erlang::nif::get(env, hour_term, &time.tm_hour) ||
                    !erlang::nif::get(env, minute_term, &time.tm_min) ||
                    !erlang::nif::get(env, second_term, &time.tm_sec)) {
                    return kErrorBufferGetMapValue;
                }

                const ERL_NIF_TERM *us_tuple = nullptr;
                int us_arity;
                uint64_t us;
                int us_precision;
                if (!enif_get_tuple(env, microsecond_term, &us_arity, &us_tuple) || us_arity != 2) {
                    return kErrorBufferGetMapValue;
                }
                if (!erlang::nif::get(env, us_tuple[0], &us) || !erlang::nif::get(env, us_tuple[1], &us_precision)) {
                    return kErrorBufferGetMapValue;
                }

                time.tm_year -= 1900;
                time.tm_mon -= 1;
                // mktime always gives local time
                // so we need to adjust it to UTC
                val = mktime(&time) + get_utc_offset() * 3600;
                callback(normalize_ex_value(val, us), false);
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_timestamp(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, enum ArrowTimeUnit time_unit, uint64_t unit, const char * timezone, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out, nanoarrow_type, time_unit, timezone));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    auto normalize_ex_value = [=](int64_t val, uint64_t us) -> int64_t {
        if (time_unit == NANOARROW_TIME_UNIT_SECOND) {
            return val;
        }
        val = (val * 1000000 + us) * 1000 / unit;
        return val;
    };
    if (nullable) {
        return get_list_timestamp(env, list, nullable, normalize_ex_value, [&array_out](int64_t val, bool is_nil) -> void {
            ArrowArrayAppendInt(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_timestamp(env, list, nullable, normalize_ex_value, [&array_out](int64_t val, bool) -> void {
            ArrowArrayAppendInt(array_out, val);
        });
    }
}

int get_list_duration(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<void(int64_t val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            callback(0, true);
        } else {
            int64_t val;
            if (erlang::nif::get(env, head, &val)) {
                callback(val, false);
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_duration(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, enum ArrowTimeUnit time_unit, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out, nanoarrow_type, time_unit, NULL));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    if (nullable) {
        return get_list_duration(env, list, nullable, [&array_out](int64_t val, bool is_nil) -> void {
            ArrowArrayAppendInt(array_out, val);
            if (is_nil) {
                ArrowArrayAppendNull(array_out, 1);
            }
        });
    } else {
        return get_list_duration(env, list, nullable, [&array_out](int64_t val, bool) -> void {
            ArrowArrayAppendInt(array_out, val);
        });
    }
}

// non-zero return value indicating errors
int adbc_column_to_adbc_field(ErlNifEnv *env, ERL_NIF_TERM adbc_buffer, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    array_out->release = NULL;
    schema_out->release = NULL;

    if (!enif_is_map(env, adbc_buffer)) {
        return kErrorBufferIsNotAMap;
    }

    ERL_NIF_TERM struct_name_term, name_term, type_term, nullable_term, metadata_term, data_term;
    if (!enif_get_map_value(env, adbc_buffer, kAtomStructKey, &struct_name_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_is_identical(struct_name_term, kAtomAdbcColumnModule)) {
        return kErrorBufferWrongStruct;
    }

    if (!enif_get_map_value(env, adbc_buffer, kAtomNameKey, &name_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_get_map_value(env, adbc_buffer, kAtomTypeKey, &type_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_get_map_value(env, adbc_buffer, kAtomNullableKey, &nullable_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_get_map_value(env, adbc_buffer, kAtomMetadataKey, &metadata_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_get_map_value(env, adbc_buffer, kAtomDataKey, &data_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_is_list(env, data_term)) {
        return kErrorBufferDataIsNotAList;
    }
    unsigned n_items = 0;
    if (!enif_get_list_length(env, data_term, &n_items)) {
        return kErrorBufferGetDataListLength;
    }

    std::string name;
    if (!enif_is_identical(name_term, kAtomNil)) {
        if (!erlang::nif::get(env, name_term, name)) {
            erlang::nif::get_atom(env, name_term, name);
        }
    }

    bool nullable = enif_is_identical(nullable_term, kAtomTrue);

    struct ArrowBuffer metadata_buffer{};
    NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderInit(&metadata_buffer, nullptr));
    if (enif_is_map(env, metadata_term)) {
        ERL_NIF_TERM metadata_key, metadata_value;
        ErlNifMapIterator iter;
        enif_map_iterator_create(env, metadata_term, &iter, ERL_NIF_MAP_ITERATOR_FIRST);
        while (enif_map_iterator_get_pair(env, &iter, &metadata_key, &metadata_value)) {
            ErlNifBinary key_bytes, value_bytes;
            struct ArrowStringView key_view{};
            struct ArrowStringView value_view{};
            if (enif_is_binary(env, metadata_key) && enif_inspect_binary(env, metadata_key, &key_bytes)) {
                key_view.data = (const char *)key_bytes.data;
                key_view.size_bytes = static_cast<int64_t>(key_bytes.size);
            } else {
                ArrowBufferReset(&metadata_buffer);
                enif_map_iterator_destroy(env, &iter);
                snprintf(error_out->message, sizeof(error_out->message), "cannot get metadata key");
                return kErrorBufferGetMetadataKey;
            }
            if (enif_is_binary(env, metadata_value) && enif_inspect_binary(env, metadata_value, &value_bytes)) {
                value_view.data = (const char *)value_bytes.data;
                value_view.size_bytes = static_cast<int64_t>(value_bytes.size);
            } else {
                ArrowBufferReset(&metadata_buffer);
                enif_map_iterator_destroy(env, &iter);
                enif_snprintf(error_out->message, sizeof(error_out->message), "cannot get metadata value for key: `%T`", metadata_key);
                return kErrorBufferGetMetadataValue;
            }

            NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderAppend(&metadata_buffer, key_view, value_view));
            enif_map_iterator_next(env, &iter);
        }
        enif_map_iterator_destroy(env, &iter);
    }

    ArrowSchemaInit(schema_out);
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema_out, name.c_str()));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetMetadata(schema_out, (const char*)metadata_buffer.data));
    ArrowBufferReset(&metadata_buffer);

    int ret = kErrorBufferUnknownType;
    if (enif_is_identical(type_term, kAdbcColumnTypeI8)) {
        ret = do_get_list_integer<int8_t>(env, data_term, nullable, NANOARROW_TYPE_INT8, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeI16)) {
        ret = do_get_list_integer<int16_t>(env, data_term, nullable, NANOARROW_TYPE_INT16, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeI32)) {
        ret = do_get_list_integer<int32_t>(env, data_term, nullable, NANOARROW_TYPE_INT32, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeI64)) {
        ret = do_get_list_integer<int64_t>(env, data_term, nullable, NANOARROW_TYPE_INT64, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeU8)) {
        ret = do_get_list_integer<uint8_t>(env, data_term, nullable, NANOARROW_TYPE_UINT8, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeU16)) {
        ret = do_get_list_integer<uint16_t>(env, data_term, nullable, NANOARROW_TYPE_UINT16, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeU32)) {
        ret = do_get_list_integer<uint32_t>(env, data_term, nullable, NANOARROW_TYPE_UINT32, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeU64)) {
        ret = do_get_list_integer<uint64_t>(env, data_term, nullable, NANOARROW_TYPE_UINT64, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeF32)) {
        ret = do_get_list_float(env, data_term, nullable, NANOARROW_TYPE_FLOAT, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeF64)) {
        ret = do_get_list_float(env, data_term, nullable, NANOARROW_TYPE_DOUBLE, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeString)) {
        ret = do_get_list_string(env, data_term, nullable, NANOARROW_TYPE_STRING, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeLargeString)) {
        ret = do_get_list_string(env, data_term, nullable, NANOARROW_TYPE_LARGE_STRING, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeBinary)) {
        ret = do_get_list_string(env, data_term, nullable, NANOARROW_TYPE_BINARY, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeLargeBinary)) {
        ret = do_get_list_string(env, data_term, nullable, NANOARROW_TYPE_LARGE_BINARY, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeDate32)) {
        ret = do_get_list_date(env, data_term, nullable, NANOARROW_TYPE_DATE32, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeDate64)) {
        ret = do_get_list_date(env, data_term, nullable, NANOARROW_TYPE_DATE64, array_out, schema_out, error_out);
    } else if (enif_is_identical(type_term, kAdbcColumnTypeBool)) {
        ret = do_get_list_boolean(env, data_term, nullable, NANOARROW_TYPE_BOOL, array_out, schema_out, error_out);
    } else if (enif_is_tuple(env, type_term)) {        
        if (enif_is_identical(type_term, kAdbcColumnTypeTime32Seconds)) {
            // NANOARROW_TYPE_TIME32
            ret = do_get_list_time(env, data_term, nullable, NANOARROW_TYPE_TIME32, NANOARROW_TIME_UNIT_SECOND, 1000000000, array_out, schema_out, error_out);
        } else if (enif_is_identical(type_term, kAdbcColumnTypeTime32Milliseconds)) {
            // NANOARROW_TYPE_TIME32
            ret = do_get_list_time(env, data_term, nullable, NANOARROW_TYPE_TIME32, NANOARROW_TIME_UNIT_MILLI, 1000000, array_out, schema_out, error_out);
        } else if (enif_is_identical(type_term, kAdbcColumnTypeTime64Microseconds)) {
            // NANOARROW_TYPE_TIME64
            ret = do_get_list_time(env, data_term, nullable, NANOARROW_TYPE_TIME64, NANOARROW_TIME_UNIT_MICRO, 1000, array_out, schema_out, error_out);
        } else if (enif_is_identical(type_term, kAdbcColumnTypeTime64Nanoseconds)) {
            // NANOARROW_TYPE_TIME64
            ret = do_get_list_time(env, data_term, nullable, NANOARROW_TYPE_TIME64, NANOARROW_TIME_UNIT_NANO, 1, array_out, schema_out, error_out);
        } else if (enif_is_identical(type_term, kAdbcColumnTypeDurationSeconds)) {
            // NANOARROW_TYPE_DURATION
            ret = do_get_list_duration(env, data_term, nullable, NANOARROW_TYPE_DURATION, NANOARROW_TIME_UNIT_SECOND, array_out, schema_out, error_out);
        } else if (enif_is_identical(type_term, kAdbcColumnTypeDurationMilliseconds)) {
            // NANOARROW_TYPE_DURATION
            ret = do_get_list_duration(env, data_term, nullable, NANOARROW_TYPE_DURATION, NANOARROW_TIME_UNIT_MILLI, array_out, schema_out, error_out);
        } else if (enif_is_identical(type_term, kAdbcColumnTypeDurationMicroseconds)) {
            // NANOARROW_TYPE_DURATION
            ret = do_get_list_duration(env, data_term, nullable, NANOARROW_TYPE_DURATION, NANOARROW_TIME_UNIT_MICRO, array_out, schema_out, error_out);
        } else if (enif_is_identical(type_term, kAdbcColumnTypeDurationNanoseconds)) {
            // NANOARROW_TYPE_DURATION
            ret = do_get_list_duration(env, data_term, nullable, NANOARROW_TYPE_DURATION, NANOARROW_TIME_UNIT_NANO, array_out, schema_out, error_out);
        } else {
            const ERL_NIF_TERM *tuple = nullptr;
            int arity;
            if (enif_get_tuple(env, type_term, &arity, &tuple)) {
                if (arity == 2) {
                    // NANOARROW_TYPE_FIXED_SIZE_BINARY
                    if (enif_is_identical(tuple[0], kAtomFixedSizeBinary)) {
                        int32_t fixed_size;
                        if (erlang::nif::get(env, tuple[1], &fixed_size)) {
                            ret = do_get_list_fixed_size_binary(env, data_term, nullable, NANOARROW_TYPE_FIXED_SIZE_BINARY, fixed_size, array_out, schema_out, error_out);
                        }
                    }
                } else if (arity == 3) {
                    // NANOARROW_TYPE_TIMESTAMP
                    if (enif_is_identical(tuple[0], kAtomTimestamp)) {
                        std::string timezone;
                        if (erlang::nif::get(env, tuple[2], timezone) && !timezone.empty()) {
                            if (enif_is_identical(tuple[1], kAtomSeconds)) {
                                ret = do_get_list_timestamp(env, data_term, nullable, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_SECOND, 1000000000, timezone.c_str(), array_out, schema_out, error_out);
                            } else if (enif_is_identical(tuple[1], kAtomMilliseconds)) {
                                ret = do_get_list_timestamp(env, data_term, nullable, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MILLI, 1000000, timezone.c_str(), array_out, schema_out, error_out);
                            } else if (enif_is_identical(tuple[1], kAtomMicroseconds)) {
                                ret = do_get_list_timestamp(env, data_term, nullable, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO, 1000, timezone.c_str(), array_out, schema_out, error_out);
                            } else if (enif_is_identical(tuple[1], kAtomNanoseconds)) {
                                ret = do_get_list_timestamp(env, data_term, nullable, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_NANO, 1, timezone.c_str(), array_out, schema_out, error_out);
                            }
                        }
                    }
                } else if (arity == 4) {
                    // NANOARROW_TYPE_DECIMAL128
                    // NANOARROW_TYPE_DECIMAL256
                    if (enif_is_identical(tuple[0], kAtomDecimal)) {
                        int bits = 0;
                        int precision = 0;
                        int scale = 0;
                        if (erlang::nif::get(env, tuple[1], &bits) && erlang::nif::get(env, tuple[2], &precision) && erlang::nif::get(env, tuple[3], &scale)) {
                            if (bits == 128) {
                                ret = do_get_list_decimal(env, data_term, nullable, NANOARROW_TYPE_DECIMAL128, bits, precision, scale, array_out, schema_out, error_out);
                            } else if (bits == 256) {
                                ret = do_get_list_decimal(env, data_term, nullable, NANOARROW_TYPE_DECIMAL256, bits, precision, scale, array_out, schema_out, error_out);
                            }
                        }
                    }
                }
            }
        }
    }

    if (ret != 0) {
        if (schema_out->release) schema_out->release(schema_out);
        if (array_out->release) array_out->release(array_out);

        if (ret == kErrorBufferUnknownType) {
            enif_snprintf(error_out->message, sizeof(error_out->message), "type `%T` not supported yet.", type_term);
        }
        return ret;
    }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(array_out, error_out));
    return 0;   
}

// non-zero return value indicating errors
int adbc_column_to_arrow_type_struct(ErlNifEnv *env, ERL_NIF_TERM values, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    array_out->release = NULL;
    schema_out->release = NULL;

    unsigned n_items = 0;
    if (!enif_get_list_length(env, values, &n_items)) {
        return 1;
    }

    ArrowSchemaInit(schema_out);
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema_out, n_items));
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromType(array_out, NANOARROW_TYPE_STRUCT));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAllocateChildren(array_out, static_cast<int64_t>(n_items)));
    array_out->length = 1;

    ERL_NIF_TERM head, tail;
    tail = values;
    int64_t processed = 0;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        auto schema_i = schema_out->children[processed];
        ArrowSchemaInit(schema_i);

        auto child_i = array_out->children[processed];

        ErlNifSInt64 i64;
        double f64;
        ErlNifBinary bytes;

        if (enif_get_int64(env, head, &i64)) {
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_i, NANOARROW_TYPE_INT64));
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema_i, ""));
            NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(child_i, schema_i, error_out));
            NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(child_i));
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(child_i, i64));
        } else if (enif_get_double(env, head, &f64)) {
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_i, NANOARROW_TYPE_DOUBLE));
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema_i, ""));
            NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(child_i, schema_i, error_out));
            NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(child_i));
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDouble(child_i, f64));
        } else if (enif_is_binary(env, head) && enif_inspect_binary(env, head, &bytes)) {
            auto type = NANOARROW_TYPE_STRING;
            if (bytes.size > INT32_MAX) {
                type = NANOARROW_TYPE_LARGE_STRING;
            }
            struct ArrowStringView view{};
            view.data = (const char*)(bytes.data);
            view.size_bytes = static_cast<int64_t>(bytes.size);
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_i, type));
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema_i, ""));
            NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(child_i, schema_i, error_out));
            NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(child_i));
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(child_i, view));
        } else if (enif_is_atom(env, head)) {
            int64_t val{};
            auto type = NANOARROW_TYPE_BOOL;
            if (enif_is_identical(head, kAtomTrue)) {
                val = 1;
            } else if (enif_is_identical(head, kAtomFalse)) {
                val = 0;
            } else if (enif_is_identical(head, kAtomNil)) {
                type = NANOARROW_TYPE_NA;
            } else {
                enif_snprintf(error_out->message, sizeof(error_out->message), "atom `:%T` is not supported yet.", head);
                return 1;
            }
            
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_i, type));
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema_i, ""));
            NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(child_i, schema_i, error_out));
            NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(child_i));
            if (type == NANOARROW_TYPE_BOOL) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(child_i, val));
            } else {
                // 1x Null
                val = 1;
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(child_i, val));
            }
        } else if (enif_is_map(env, head)) {
            int ret = adbc_column_to_adbc_field(env, head, child_i, schema_i, error_out);
            switch (ret)
            {
            case kErrorBufferIsNotAMap:
            case kErrorBufferWrongStruct:
                snprintf(error_out->message, sizeof(error_out->message), "Expected `%%Adbc.Column{}` or primitive data types.");
                return 1;
            case kErrorBufferGetMapValue:
                snprintf(error_out->message, sizeof(error_out->message), "Invalid `%%Adbc.Column{}`.");
                return 1;
            case kErrorBufferGetDataListLength:
            case kErrorBufferDataIsNotAList:
                snprintf(error_out->message, sizeof(error_out->message), "Expected the `data` field of `Adbc.Column` to be a list of values.");
                return 1;
            case kErrorBufferUnknownType:
            case kErrorBufferGetMetadataKey:
            case kErrorBufferGetMetadataValue:
                // error message is already set
                return 1;
            case kErrorExpectedCalendarISO:
                snprintf(error_out->message, sizeof(error_out->message), "Expected `Calendar.ISO`.");
                return 1;
            default:
                break;
            }
        } else {
            snprintf(error_out->message, sizeof(error_out->message), "type not supported yet.");
            return 1;
        }
        processed++;
    }
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(array_out, error_out));
    return !(processed == n_items);
}

#endif  // ADBC_COLUMN_HPP
