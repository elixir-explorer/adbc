#ifndef ADBC_COLUMN_HPP
#pragma once

#include <time.h>
#include <cstdbool>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <adbc.h>
#include <erl_nif.h>
#include <nanoarrow/nanoarrow.hpp>
#include "adbc_consts.h"
#include "adbc_half_float.hpp"
#include "nif_utils.hpp"

struct AdbcColumnType {
    int valid = 0;

    enum ArrowType arrow_type;

    // only valid if arrow_type is one of
    // - NANOARROW_TYPE_TIME32
    // - NANOARROW_TYPE_TIME64
    // - NANOARROW_TYPE_DURATION
    // - NANOARROW_TYPE_TIMESTAMP
    enum ArrowTimeUnit time_unit;

    // only valid if arrow_type is one of
    // - NANOARROW_TYPE_TIME32
    // - NANOARROW_TYPE_TIME64
    // - NANOARROW_TYPE_TIMESTAMP
    uint64_t unit;

    // only valid if arrow_type is NANOARROW_TYPE_TIMESTAMP
    std::string timezone;

    // only valid if arrow_type is NANOARROW_TYPE_FIXED_SIZE_*
    // if the value is `-1`, it means we need to infer the size from the data
    int32_t fixed_size;

    // only valid if arrow_type is NANOARROW_TYPE_DECIMAL128 or NANOARROW_TYPE_DECIMAL256
    int bits = 0;
    int precision = 0;
    int scale = 0;
};

struct AdbcColumnNifTerm {
    int is_nil;
    unsigned n_items;
    ERL_NIF_TERM struct_name_term, name_term, type_term, nullable_term, metadata_term, data_term;
    static int from_term(ErlNifEnv *env, ERL_NIF_TERM adbc_column, bool allow_nil, AdbcColumnNifTerm *out);
};

struct AdbcColumnType adbc_column_type_to_nanoarrow_type(ErlNifEnv *env, ERL_NIF_TERM type_term);
int adbc_column_to_adbc_field(ErlNifEnv *env, ERL_NIF_TERM adbc_column, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out, unsigned *n_items);
int adbc_column_to_adbc_field(ErlNifEnv *env, struct AdbcColumnNifTerm * column, bool allow_nil, bool skip_init, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out);
int must_be_adbc_column(ErlNifEnv *env,
    ERL_NIF_TERM adbc_column,
    ERL_NIF_TERM &struct_name_term,
    ERL_NIF_TERM &name_term,
    ERL_NIF_TERM &type_term,
    ERL_NIF_TERM &nullable_term,
    ERL_NIF_TERM &metadata_term,
    ERL_NIF_TERM &data_term,
    unsigned *n_items);

int AdbcColumnNifTerm::from_term(ErlNifEnv *env, ERL_NIF_TERM adbc_column, bool allow_nil, AdbcColumnNifTerm *out) {
    if (enif_is_identical(adbc_column, kAtomNil)) {
        if (allow_nil) {
            if (out) {
                out->is_nil = 1;
            }
            return 0;
        } else {
            return 1;
        }
    }

    ERL_NIF_TERM struct_name_term, name_term, type_term, nullable_term, metadata_term, data_term;
    unsigned n_items = 0;
    int ret = must_be_adbc_column(env, adbc_column, struct_name_term, name_term, type_term, nullable_term, metadata_term, data_term, &n_items);
    if (ret != 0) {
        return ret;
    }

    if (out) {
        out->is_nil = 0;
        out->n_items = n_items;
        out->struct_name_term = struct_name_term;
        out->name_term = name_term;
        out->type_term = type_term;
        out->nullable_term = nullable_term;
        out->metadata_term = metadata_term;
        out->data_term = data_term;
    }

    return 0;
}

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
int get_list_integer(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(Integer val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        int64_t val;
        if (!erlang::nif::get(env, head, &val)) {
            if (nullable && enif_is_identical(head, kAtomNil)) {
                NANOARROW_RETURN_NOT_OK(callback(0, true));
            } else {
                return 1;
            }
        }
        NANOARROW_RETURN_NOT_OK(callback((Integer)val, false));
    }
    return 0;
}

template <typename Integer, typename std::enable_if<
        std::is_integral<Integer>{} && !std::is_signed<Integer>{}, bool>::type = true>
int get_list_integer(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(Integer val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        uint64_t val;
        if (!erlang::nif::get(env, head, &val)) {
            if (nullable && enif_is_identical(head, kAtomNil)) {
                NANOARROW_RETURN_NOT_OK(callback(0, true));
            } else {
                return 1;
            }
        }
        NANOARROW_RETURN_NOT_OK(callback((Integer)val, false));
    }
    return 0;
}

template <typename T>
int do_get_list_integer(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, bool skip_init, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    nanoarrow::UniqueArray tmp;
    struct ArrowArray* write_array;
    if (!skip_init) {
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));

        NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
        NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));

        write_array = tmp.get();
    } else {
        write_array = array_out;
    }

    int ret;
    if (nullable) {
        ret = get_list_integer<T>(env, list, nullable, [&write_array](T val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(write_array, val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(write_array, 1));
            }
            return 0;
        });
    } else {
        ret = get_list_integer<T>(env, list, nullable, [&write_array](T val, bool) -> int {
            return ArrowArrayAppendInt(write_array, val);
        });
    }

    if (ret == 0) {
        if (!skip_init) {
            NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
            ArrowArrayMove(tmp.get(), array_out);
        }
    }
    return ret;
}

int get_list_float(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(double val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        double val;
        if (!erlang::nif::get(env, head, &val)) {
            if (nullable && enif_is_identical(head, kAtomNil)) {
                NANOARROW_RETURN_NOT_OK(callback(0, true));
            } else if (enif_is_identical(head, kAtomInfinity)) {
                NANOARROW_RETURN_NOT_OK(callback(std::numeric_limits<double>::infinity(), false));
            } else if (enif_is_identical(head, kAtomNegInfinity)) {
                NANOARROW_RETURN_NOT_OK(callback(-std::numeric_limits<double>::infinity(), false));
            } else if (enif_is_identical(head, kAtomNaN)) {
                NANOARROW_RETURN_NOT_OK(callback(std::numeric_limits<double>::quiet_NaN(), false));
            } else {
                return 1;
            }
        } else {
            NANOARROW_RETURN_NOT_OK(callback(val, false));
        }
    }
    return 0;
}

int do_get_list_half_float(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    struct ArrowArrayPrivateData* private_data = (struct ArrowArrayPrivateData*)tmp.get()->private_data;
    auto storage_type = private_data->storage_type;
    private_data->storage_type = NANOARROW_TYPE_UINT16;
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    int ret;
    if (nullable) {
        ret = get_list_float(env, list, nullable, [&tmp](double val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendUInt(tmp.get(), float_to_float16(val)));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_float(env, list, nullable, [&tmp](double val, bool) -> int {
            return ArrowArrayAppendUInt(tmp.get(), float_to_float16(val));
        });
    }

    if (ret == 0) {
        private_data->storage_type = storage_type;
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int do_get_list_float(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    int ret;
    if (nullable) {
        ret = get_list_float(env, list, nullable, [&tmp](double val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDouble(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_float(env, list, nullable, [&tmp](double val, bool) -> int {
            return ArrowArrayAppendDouble(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int get_list_decimal(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, int32_t bitwidth, int32_t precision, int32_t scale, const std::function<int(struct ArrowDecimal * val, bool is_nil)> &callback) {
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
            NANOARROW_RETURN_NOT_OK(callback(&val, false));
        } else if (nullable && enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(&val, true));
        } else {
            return 1;
        }
    }
    return 0;
}

int do_get_list_decimal(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, int32_t bitwidth, int32_t precision, int32_t scale, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDecimal(schema_out, nanoarrow_type, precision, scale));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    int ret;
    if (nullable) {
        ret = get_list_decimal(env, list, nullable, nanoarrow_type, bitwidth, precision, scale, [&tmp](struct ArrowDecimal * val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDecimal(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_decimal(env, list, nullable, nanoarrow_type,  bitwidth, precision, scale, [&tmp](struct ArrowDecimal * val, bool) -> int {
            return ArrowArrayAppendDecimal(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int get_list_string(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(struct ArrowStringView val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        ErlNifBinary bytes;
        struct ArrowStringView val{};
        if (enif_is_binary(env, head) && enif_inspect_binary(env, head, &bytes)) {
            val.data = (const char *)bytes.data;
            val.size_bytes = static_cast<int64_t>(bytes.size);
            NANOARROW_RETURN_NOT_OK(callback(val, false));
        } else if (nullable && enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(val, true));
        } else {
            return 1;
        }
    }
    return 0;
}

int do_get_list_string(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    int ret;
    if (nullable) {
        ret = get_list_string(env, list, nullable, [&tmp](struct ArrowStringView val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_string(env, list, nullable, [&tmp](struct ArrowStringView val, bool) -> int {
            return ArrowArrayAppendString(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int get_list_boolean(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(bool val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomTrue)) {
            NANOARROW_RETURN_NOT_OK(callback(true, false));
        } else if (enif_is_identical(head, kAtomFalse)) {
            NANOARROW_RETURN_NOT_OK(callback(false, false));
        } else if (enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(true, true));
        } else {
            return 1;
        }
    }
    return 0;
}

int do_get_list_boolean(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    int ret;
    if (nullable) {
        ret = get_list_boolean(env, list, nullable, [&tmp](bool val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_boolean(env, list, nullable, [&tmp](bool val, bool) -> int {
            return ArrowArrayAppendInt(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int get_list_fixed_size_binary(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(struct ArrowBufferView val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        ErlNifBinary bytes;
        struct ArrowBufferView val{};
        if (enif_is_binary(env, head) && enif_inspect_binary(env, head, &bytes)) {
            val.data.data = bytes.data;
            val.size_bytes = static_cast<int64_t>(bytes.size);
            NANOARROW_RETURN_NOT_OK(callback(val, false));
        } else if (nullable && enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(val, true));
        } else {
            return 1;
        }
    }
    return 0;
}

int do_get_list_fixed_size_binary(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, int32_t fixed_size, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeFixedSize(schema_out, nanoarrow_type, fixed_size));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    int ret;
    if (nullable) {
        ret = get_list_fixed_size_binary(env, list, nullable, [&tmp](struct ArrowBufferView val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_fixed_size_binary(env, list, nullable, [&tmp](struct ArrowBufferView val, bool) -> int {
            return ArrowArrayAppendBytes(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
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

int get_list_date(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int64_t(int64_t)> &normalize_ex_value, const std::function<int(int64_t val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(0, true));
        } else {
            int64_t val;
            if (erlang::nif::get(env, head, &val)) {
                NANOARROW_RETURN_NOT_OK(callback(val, false));
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
                NANOARROW_RETURN_NOT_OK(callback(normalize_ex_value(val), false));
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_date(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
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
    int ret;
    if (nullable) {
        ret = get_list_date(env, list, nullable, normalize_ex_value, [&tmp](int64_t val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_date(env, list, nullable, normalize_ex_value, [&tmp](int64_t val, bool) -> int {
            return ArrowArrayAppendInt(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int get_list_time(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int64_t(int64_t, uint64_t)> &normalize_ex_value, const std::function<int(int64_t val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(0, true));
        } else {
            int64_t val;
            if (erlang::nif::get(env, head, &val)) {
                NANOARROW_RETURN_NOT_OK(callback(val, false));
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
                NANOARROW_RETURN_NOT_OK(callback(normalize_ex_value(val, us), false));
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_time(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, enum ArrowTimeUnit time_unit, uint64_t unit, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out, nanoarrow_type, time_unit, NULL));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    auto normalize_ex_value = [=](int64_t val, uint64_t us) -> int64_t {
        if (time_unit == NANOARROW_TIME_UNIT_SECOND) {
            return val;
        }
        val = (val * 1000000 + us) * 1000 / unit;
        return val;
    };
    int ret;
    if (nullable) {
        ret = get_list_time(env, list, nullable, normalize_ex_value, [&tmp](int64_t val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_time(env, list, nullable, normalize_ex_value, [&tmp](int64_t val, bool) -> int {
            return ArrowArrayAppendInt(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int get_list_timestamp(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int64_t(int64_t, uint64_t)> &normalize_ex_value, const std::function<int(int64_t val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(0, true));
        } else {
            int64_t val;
            if (erlang::nif::get(env, head, &val)) {
                NANOARROW_RETURN_NOT_OK(callback(val, false));
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
                NANOARROW_RETURN_NOT_OK(callback(normalize_ex_value(val, us), false));
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_timestamp(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, enum ArrowTimeUnit time_unit, uint64_t unit, const char * timezone, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out, nanoarrow_type, time_unit, timezone));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    auto normalize_ex_value = [=](int64_t val, uint64_t us) -> int64_t {
        if (time_unit == NANOARROW_TIME_UNIT_SECOND) {
            return val;
        }
        val = (val * 1000000 + us) * 1000 / unit;
        return val;
    };
    int ret;
    if (nullable) {
        ret = get_list_timestamp(env, list, nullable, normalize_ex_value, [&tmp](int64_t val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_timestamp(env, list, nullable, normalize_ex_value, [&tmp](int64_t val, bool) -> int {
            return ArrowArrayAppendInt(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int get_list_duration(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(int64_t val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(0, true));
        } else {
            int64_t val;
            if (erlang::nif::get(env, head, &val)) {
                NANOARROW_RETURN_NOT_OK(callback(val, false));
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_duration(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, enum ArrowTimeUnit time_unit, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out, nanoarrow_type, time_unit, NULL));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
    int ret;
    if (nullable) {
        ret = get_list_duration(env, list, nullable, [&tmp](int64_t val, bool is_nil) -> int {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(tmp.get(), val));
            if (is_nil) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
            }
            return 0;
        });
    } else {
        ret = get_list_duration(env, list, nullable, [&tmp](int64_t val, bool) -> int {
            return ArrowArrayAppendInt(tmp.get(), val);
        });
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int get_list_interval_month(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(struct ArrowInterval * val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    struct ArrowInterval val{};
    val.type = NANOARROW_TYPE_INTERVAL_MONTHS;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(nullptr, true));
        } else {
            int32_t months;
            if (erlang::nif::get(env, head, &months)) {
                val.months = months;
                NANOARROW_RETURN_NOT_OK(callback(&val, false));
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int get_list_interval_day_time(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(struct ArrowInterval * val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    struct ArrowInterval val{};
    val.type = NANOARROW_TYPE_INTERVAL_DAY_TIME;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(nullptr, true));
        } else {
            int32_t days, milliseconds;
            const ERL_NIF_TERM *tuple = nullptr;
            int arity;
            if (enif_get_tuple(env, head, &arity, &tuple) && arity == 2) {
                if (!erlang::nif::get(env, tuple[0], &days) || !erlang::nif::get(env, tuple[1], &milliseconds)) {
                    return 1;
                }
                val.days = days;
                val.ms = milliseconds;
                NANOARROW_RETURN_NOT_OK(callback(&val, false));
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int get_list_duration_month_day_nano(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, const std::function<int(struct ArrowInterval * val, bool is_nil)> &callback) {
    ERL_NIF_TERM head, tail;
    tail = list;
    struct ArrowInterval val{};
    val.type = NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_is_identical(head, kAtomNil)) {
            NANOARROW_RETURN_NOT_OK(callback(nullptr, true));
        } else {
            int32_t months, days;
            int64_t nanoseconds;
            const ERL_NIF_TERM *tuple = nullptr;
            int arity;
            if (enif_get_tuple(env, head, &arity, &tuple) && arity == 3) {
                if (!erlang::nif::get(env, tuple[0], &months) ||
                    !erlang::nif::get(env, tuple[1], &days) ||
                    !erlang::nif::get(env, tuple[2], &nanoseconds)) {
                    return 1;
                }
                val.months = months;
                val.days = days;
                val.ns = nanoseconds;
                NANOARROW_RETURN_NOT_OK(callback(&val, false));
            } else {
                return 1;
            }
        }
    }
    return 0;
}

int do_get_list_interval(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, ArrowType nanoarrow_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, nanoarrow_type));

    nanoarrow::UniqueArray tmp;
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_out, error_out));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));

    auto appendNullableInterval = [&tmp](struct ArrowInterval * val, bool is_nil) -> int {
        NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInterval(tmp.get(), val));
        if (is_nil) {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(tmp.get(), 1));
        }
        return 0;
    };
    auto appendNonNullableInterval = [&tmp](struct ArrowInterval * val, bool) -> int {
        return ArrowArrayAppendInterval(tmp.get(), val);
    };

    int(*get_list_interval)(ErlNifEnv *, ERL_NIF_TERM, bool, const std::function<int(struct ArrowInterval *, bool)> &) = nullptr;

    if (nanoarrow_type == NANOARROW_TYPE_INTERVAL_MONTHS) {
        get_list_interval = get_list_interval_month;
    } else if (nanoarrow_type == NANOARROW_TYPE_INTERVAL_DAY_TIME) {
        get_list_interval = get_list_interval_day_time;
    } else if (nanoarrow_type == NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO) {
        get_list_interval = get_list_duration_month_day_nano;
    } else {
        return 1;
    }

    int ret;
    if (nullable) {
        ret = get_list_interval(env, list, nullable, appendNullableInterval);
    } else {
        ret = get_list_interval(env, list, nullable, appendNonNullableInterval);
    }

    if (ret == 0) {
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), error_out));
        ArrowArrayMove(tmp.get(), array_out);
    }
    return ret;
}

int do_get_list(ErlNifEnv *env, ERL_NIF_TERM list, bool nullable, struct AdbcColumnType * column_type, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    if (column_type == nullptr) {
        enif_snprintf(error_out->message, sizeof(error_out->message), "internal error: column_type is null in do_get_list:%d", __LINE__);
        return kErrorInternalError;
    }

    if (column_type->arrow_type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeFixedSize(schema_out, column_type->arrow_type, column_type->fixed_size));
    } else {
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, column_type->arrow_type));
    }

    unsigned n_items = 0;
    if (!enif_get_list_length(env, list, &n_items)) {
        return 1;
    }
    printf("n_items: %d\n", n_items);

    ERL_NIF_TERM head, tail;
    tail = list;
    std::vector<struct AdbcColumnNifTerm> items;
    struct AdbcColumnType list_item_type;
    ERL_NIF_TERM match_type;
    int found_item_type = 0;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        struct AdbcColumnNifTerm item;
        int ret = AdbcColumnNifTerm::from_term(env, head, true, &item);
        if (ret != 0) return ret;
        if (!item.is_nil) {
            struct AdbcColumnType item_column_type = adbc_column_type_to_nanoarrow_type(env, item.type_term);
            if (item_column_type.valid == 0) {
                enif_snprintf(error_out->message, sizeof(error_out->message), "unsupport type `%T` found in do_get_list:%d", item.type_term, __LINE__);
                return kErrorBufferUnknownType;
            } else {
                if (found_item_type) {
                    if (enif_is_identical(item.type_term, match_type) == 0) {
                        enif_snprintf(error_out->message, sizeof(error_out->message), "all items in the list must have the same type.");
                        return kErrorBufferUnknownType;
                    }
                } else {
                    found_item_type = 1;
                    match_type = item.type_term;
                    list_item_type = item_column_type;
                }
            }
        }
        items.emplace_back(item);
    }

    // if found_item_type is 0, it means all items were nil
    // hence we can infer that the item type is NANOARROW_TYPE_NA
    if (found_item_type == 0) {
        list_item_type.arrow_type = NANOARROW_TYPE_NA;
    }

    // set item type
    switch (list_item_type.arrow_type)
    {
    case NANOARROW_TYPE_NA:
    case NANOARROW_TYPE_BOOL:
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_UINT64:
    case NANOARROW_TYPE_HALF_FLOAT:
    case NANOARROW_TYPE_FLOAT:
    case NANOARROW_TYPE_DOUBLE:
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_DATE32:
    case NANOARROW_TYPE_DATE64:
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_INTERVAL_MONTHS:
    case NANOARROW_TYPE_INTERVAL_DAY_TIME:
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out->children[0], list_item_type.arrow_type));
        break;
    case NANOARROW_TYPE_TIMESTAMP:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out->children[0], list_item_type.arrow_type, list_item_type.time_unit, list_item_type.timezone.c_str()));
        break;
    case NANOARROW_TYPE_DURATION:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out->children[0], list_item_type.arrow_type, list_item_type.time_unit, NULL));
        break;
    case NANOARROW_TYPE_TIME32:
    case NANOARROW_TYPE_TIME64:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema_out->children[0], list_item_type.arrow_type, list_item_type.time_unit, NULL));
        break;
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeFixedSize(schema_out->children[0], list_item_type.arrow_type, list_item_type.fixed_size));
        break;
    case NANOARROW_TYPE_DECIMAL128:
    case NANOARROW_TYPE_DECIMAL256:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDecimal(schema_out->children[0], list_item_type.arrow_type, list_item_type.precision, list_item_type.scale));
        break;
    default:
        break;
    }

    // build the array
    // todo: handle nested types
    if (list_item_type.arrow_type == NANOARROW_TYPE_LIST || list_item_type.arrow_type == NANOARROW_TYPE_LARGE_LIST) {
        // NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromType(array_out, list_item_type.arrow_type));
        // NANOARROW_RETURN_NOT_OK(ArrowArrayAllocateChildren(array_out, 1));
        snprintf(error_out->message, sizeof(error_out->message), "nested types are not supported yet");
        return kErrorInternalError;
    } else {
        NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_out, schema_out, error_out));
        NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_out));
    }

    for (auto &item : items) {
        if (item.is_nil) {
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(array_out, 1));
        } else {
            struct ArrowSchema child_schema{};
            int ret = adbc_column_to_adbc_field(env, &item, true, true, array_out->children[0], &child_schema, error_out);
            if (ret != 0) {
                return ret;
            }
            NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(array_out));
        }
    }
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(array_out, error_out));

    return 0;
}

// non-zero return value indicates there was no metadata or an error
int build_metadata_from_nif(ErlNifEnv *env, ERL_NIF_TERM metadata_term, struct ArrowBuffer *metadata_buffer, struct ArrowError* error_out) {
    NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderInit(metadata_buffer, nullptr));
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
                ArrowBufferReset(metadata_buffer);
                enif_map_iterator_destroy(env, &iter);
                snprintf(error_out->message, sizeof(error_out->message), "cannot get metadata key");
                return kErrorBufferGetMetadataKey;
            }
            if (enif_is_binary(env, metadata_value) && enif_inspect_binary(env, metadata_value, &value_bytes)) {
                value_view.data = (const char *)value_bytes.data;
                value_view.size_bytes = static_cast<int64_t>(value_bytes.size);
            } else {
                ArrowBufferReset(metadata_buffer);
                enif_map_iterator_destroy(env, &iter);
                enif_snprintf(error_out->message, sizeof(error_out->message), "cannot get metadata value for key: `%T`", metadata_key);
                return kErrorBufferGetMetadataValue;
            }

            NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderAppend(metadata_buffer, key_view, value_view));
            enif_map_iterator_next(env, &iter);
        }
        enif_map_iterator_destroy(env, &iter);
    }
    return 0;
}

int must_be_adbc_column(ErlNifEnv *env,
    ERL_NIF_TERM adbc_column,
    ERL_NIF_TERM &struct_name_term,
    ERL_NIF_TERM &name_term,
    ERL_NIF_TERM &type_term,
    ERL_NIF_TERM &nullable_term,
    ERL_NIF_TERM &metadata_term,
    ERL_NIF_TERM &data_term,
    unsigned *n_items)
{
    if (!enif_is_map(env, adbc_column)) {
        return kErrorBufferIsNotAMap;
    }

    if (!enif_get_map_value(env, adbc_column, kAtomStructKey, &struct_name_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_is_identical(struct_name_term, kAtomAdbcColumnModule)) {
        return kErrorBufferWrongStruct;
    }

    if (!enif_get_map_value(env, adbc_column, kAtomNameKey, &name_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_get_map_value(env, adbc_column, kAtomTypeKey, &type_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_get_map_value(env, adbc_column, kAtomNullableKey, &nullable_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_get_map_value(env, adbc_column, kAtomMetadataKey, &metadata_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_get_map_value(env, adbc_column, kAtomDataKey, &data_term)) {
        return kErrorBufferGetMapValue;
    }
    if (!enif_is_list(env, data_term)) {
        return kErrorBufferDataIsNotAList;
    }

    if (n_items) {
        if (!enif_get_list_length(env, data_term, n_items)) {
            return kErrorBufferGetDataListLength;
        }
    }

    return 0;
}

struct AdbcColumnType adbc_column_type_to_nanoarrow_type(ErlNifEnv *env, ERL_NIF_TERM type_term) {
    struct AdbcColumnType ret{};
    ret.valid = 1;

    if (enif_is_identical(type_term, kAdbcColumnTypeBool)) {
        ret.arrow_type = NANOARROW_TYPE_BOOL;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeI8)) {
        ret.arrow_type = NANOARROW_TYPE_INT8;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeU8)) {
        ret.arrow_type = NANOARROW_TYPE_UINT8;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeI16)) {
        ret.arrow_type = NANOARROW_TYPE_INT16;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeU16)) {
        ret.arrow_type = NANOARROW_TYPE_UINT16;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeI32)) {
        ret.arrow_type = NANOARROW_TYPE_INT32;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeU32)) {
        ret.arrow_type = NANOARROW_TYPE_UINT32;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeI64)) {
        ret.arrow_type = NANOARROW_TYPE_INT64;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeU64)) {
        ret.arrow_type = NANOARROW_TYPE_UINT64;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeF16)) {
        ret.arrow_type = NANOARROW_TYPE_HALF_FLOAT;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeF32)) {
        ret.arrow_type = NANOARROW_TYPE_FLOAT;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeF64)) {
        ret.arrow_type = NANOARROW_TYPE_DOUBLE;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeBinary)) {
        ret.arrow_type = NANOARROW_TYPE_BINARY;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeLargeBinary)) {
        ret.arrow_type = NANOARROW_TYPE_LARGE_BINARY;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeString)) {
        ret.arrow_type = NANOARROW_TYPE_STRING;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeLargeString)) {
        ret.arrow_type = NANOARROW_TYPE_LARGE_STRING;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeDate32)) {
        ret.arrow_type = NANOARROW_TYPE_DATE32;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeDate64)) {
        ret.arrow_type = NANOARROW_TYPE_DATE64;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeList)) {
        ret.arrow_type = NANOARROW_TYPE_LIST;
    } else if (enif_is_identical(type_term, kAdbcColumnTypeLargeList)) {
        ret.arrow_type = NANOARROW_TYPE_LARGE_LIST;
    } else if (enif_is_tuple(env, type_term)) {
        if (enif_is_identical(type_term, kAdbcColumnTypeTime32Seconds)) {
            ret.arrow_type = NANOARROW_TYPE_TIME32;
            ret.time_unit = NANOARROW_TIME_UNIT_SECOND;
            ret.unit = 1000000000;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeTime32Milliseconds)) {
            ret.arrow_type = NANOARROW_TYPE_TIME32;
            ret.time_unit = NANOARROW_TIME_UNIT_MILLI;
            ret.unit = 1000000;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeTime64Microseconds)) {
            ret.arrow_type = NANOARROW_TYPE_TIME64;
            ret.time_unit = NANOARROW_TIME_UNIT_MICRO;
            ret.unit = 1000;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeTime64Nanoseconds)) {
            ret.arrow_type = NANOARROW_TYPE_TIME64;
            ret.time_unit = NANOARROW_TIME_UNIT_NANO;
            ret.unit = 1;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeDurationSeconds)) {
            ret.arrow_type = NANOARROW_TYPE_DURATION;
            ret.time_unit = NANOARROW_TIME_UNIT_SECOND;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeDurationMilliseconds)) {
            ret.arrow_type = NANOARROW_TYPE_DURATION;
            ret.time_unit = NANOARROW_TIME_UNIT_MILLI;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeDurationMicroseconds)) {
            ret.arrow_type = NANOARROW_TYPE_DURATION;
            ret.time_unit = NANOARROW_TIME_UNIT_MICRO;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeDurationNanoseconds)) {
            ret.arrow_type = NANOARROW_TYPE_DURATION;
            ret.time_unit = NANOARROW_TIME_UNIT_NANO;
        }  else if (enif_is_identical(type_term, kAdbcColumnTypeIntervalMonth)) {
            ret.arrow_type = NANOARROW_TYPE_INTERVAL_MONTHS;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeIntervalDayTime)) {
            ret.arrow_type = NANOARROW_TYPE_INTERVAL_DAY_TIME;
        } else if (enif_is_identical(type_term, kAdbcColumnTypeIntervalMonthDayNano)) {
            ret.arrow_type = NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
        } else {
            const ERL_NIF_TERM *tuple = nullptr;
            int arity;
            if (enif_get_tuple(env, type_term, &arity, &tuple)) {
                if (arity == 2) {
                    if (enif_is_identical(tuple[0], kAtomFixedSizeBinary)) {
                        int32_t fixed_size;
                        if (erlang::nif::get(env, tuple[1], &fixed_size)) {
                            ret.arrow_type = NANOARROW_TYPE_FIXED_SIZE_BINARY;
                            ret.fixed_size = fixed_size;
                        } else {
                            ret.valid = 0;
                        }
                    } else if (enif_is_identical(tuple[0], kAtomFixedSizeList)) {
                        int32_t fixed_size;
                        if (erlang::nif::get(env, tuple[1], &fixed_size)) {
                            ret.arrow_type = NANOARROW_TYPE_FIXED_SIZE_LIST;
                            ret.fixed_size = fixed_size;
                        } else {
                            ret.valid = 0;
                        }
                    } else {
                        ret.valid = 0;
                    }
                } else if (arity == 3) {
                    if (enif_is_identical(tuple[0], kAtomTimestamp)) {
                        ret.arrow_type = NANOARROW_TYPE_TIMESTAMP;
                        std::string timezone;
                        if (erlang::nif::get(env, tuple[2], timezone) && !timezone.empty()) {
                            ret.timezone = timezone;
                            if (enif_is_identical(tuple[1], kAtomSeconds)) {
                                ret.time_unit = NANOARROW_TIME_UNIT_SECOND;
                                ret.unit = 1000000000;
                            } else if (enif_is_identical(tuple[1], kAtomMilliseconds)) {
                                ret.time_unit = NANOARROW_TIME_UNIT_MILLI;
                                ret.unit = 1000000;
                            } else if (enif_is_identical(tuple[1], kAtomMicroseconds)) {
                                ret.time_unit = NANOARROW_TIME_UNIT_MICRO;
                                ret.unit = 1000;
                            } else if (enif_is_identical(tuple[1], kAtomNanoseconds)) {
                                ret.time_unit = NANOARROW_TIME_UNIT_NANO;
                                ret.unit = 1;
                            } else {
                                ret.valid = 0;
                            }
                        } else {
                            ret.valid = 0;
                        }
                    } else {
                        ret.valid = 0;
                    }
                } else if (arity == 4) {
                    if (enif_is_identical(tuple[0], kAtomDecimal)) {
                        int bits = 0;
                        int precision = 0;
                        int scale = 0;
                        if (erlang::nif::get(env, tuple[1], &bits) && erlang::nif::get(env, tuple[2], &precision) && erlang::nif::get(env, tuple[3], &scale)) {
                            ret.bits = bits;
                            ret.precision = precision;
                            ret.scale = scale;
                            if (bits == 128) {
                                ret.arrow_type = NANOARROW_TYPE_DECIMAL128;
                            } else if (bits == 256) {
                                ret.arrow_type = NANOARROW_TYPE_DECIMAL256;
                            } else {
                               ret.valid = 0;
                            }
                        } else {
                           ret.valid = 0;
                        }
                    } else {
                        ret.valid = 0;
                    }
                } else {
                    ret.valid = 0;
                }
            } else {
                ret.valid = 0;
            }
        }
    }

    return ret;
}

// non-zero return value indicating errors
int adbc_column_to_adbc_field(ErlNifEnv *env, struct AdbcColumnNifTerm * column, bool allow_nil, bool skip_init, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    if (column == nullptr) {
        enif_snprintf(error_out->message, sizeof(error_out->message), "internal error, AdbcColumnNifTerm is null");
        return kErrorInternalError;
    }

    std::string name;
    if (!enif_is_identical(column->name_term, kAtomNil)) {
        if (!erlang::nif::get(env, column->name_term, name)) {
            erlang::nif::get_atom(env, column->name_term, name);
        }
    }

    bool nullable = enif_is_identical(column->nullable_term, kAtomTrue);

    if (!skip_init) {
        ArrowSchemaInit(schema_out);
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema_out, name.c_str()));

        struct ArrowBuffer metadata_buffer{};
        int metadata_ret = build_metadata_from_nif(env, column->metadata_term, &metadata_buffer, error_out);
        if (metadata_ret) {
            if (schema_out->release) {
                schema_out->release(schema_out);
            }
            return metadata_ret;
        }
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetMetadata(schema_out, (const char*)metadata_buffer.data));
        ArrowBufferReset(&metadata_buffer);
    }

    // Data types can be found here:
    // https://arrow.apache.org/docs/format/CDataInterface.html
    struct AdbcColumnType column_type = adbc_column_type_to_nanoarrow_type(env, column->type_term);
    if (column_type.valid == 0) {
        enif_snprintf(error_out->message, sizeof(error_out->message), "unsupport type `%T` found in adbc_column_to_adbc_field:%d", column->type_term, __LINE__);
        return kErrorBufferUnknownType;
    }

    int ret = kErrorBufferUnknownType;
    ERL_NIF_TERM data_term = column->data_term;
    if (column_type.arrow_type == NANOARROW_TYPE_BOOL) {
        ret = do_get_list_boolean(env, data_term, nullable, column_type.arrow_type, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_INT8) {
        ret = do_get_list_integer<int8_t>(env, data_term, nullable, skip_init, NANOARROW_TYPE_INT8, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_UINT8) {
        ret = do_get_list_integer<uint8_t>(env, data_term, nullable, skip_init, NANOARROW_TYPE_UINT8, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_INT16) {
        ret = do_get_list_integer<int16_t>(env, data_term, nullable, skip_init, NANOARROW_TYPE_INT16, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_UINT16) {
        ret = do_get_list_integer<uint16_t>(env, data_term, nullable, skip_init, NANOARROW_TYPE_UINT16, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_INT32) {
        ret = do_get_list_integer<int32_t>(env, data_term, nullable, skip_init, NANOARROW_TYPE_INT32, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_UINT32) {
        ret = do_get_list_integer<uint32_t>(env, data_term, nullable, skip_init, NANOARROW_TYPE_UINT32, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_INT64) {
        ret = do_get_list_integer<int64_t>(env, data_term, nullable, skip_init, NANOARROW_TYPE_INT64, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_UINT64) {
        ret = do_get_list_integer<uint64_t>(env, data_term, nullable, skip_init, NANOARROW_TYPE_UINT64, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_HALF_FLOAT) {
        ret = do_get_list_half_float(env, data_term, nullable, NANOARROW_TYPE_HALF_FLOAT, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_FLOAT) {
        ret = do_get_list_float(env, data_term, nullable, NANOARROW_TYPE_FLOAT, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_DOUBLE) {
        ret = do_get_list_float(env, data_term, nullable, NANOARROW_TYPE_DOUBLE, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_BINARY) {
        ret = do_get_list_string(env, data_term, nullable, NANOARROW_TYPE_BINARY, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_LARGE_BINARY) {
        ret = do_get_list_string(env, data_term, nullable, NANOARROW_TYPE_LARGE_BINARY, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_STRING) {
        ret = do_get_list_string(env, data_term, nullable, NANOARROW_TYPE_STRING, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_LARGE_STRING) {
        ret = do_get_list_string(env, data_term, nullable, NANOARROW_TYPE_LARGE_STRING, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_DATE32) {
        ret = do_get_list_date(env, data_term, nullable, NANOARROW_TYPE_DATE32, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_DATE64) {
        ret = do_get_list_date(env, data_term, nullable, NANOARROW_TYPE_DATE64, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_LIST) {
        ret = do_get_list(env, data_term, nullable, &column_type, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_LARGE_LIST) {
        ret = do_get_list(env, data_term, nullable, &column_type, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
        ret = do_get_list(env, data_term, nullable, &column_type, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_TIME32 || column_type.arrow_type == NANOARROW_TYPE_TIME64) {
        ret = do_get_list_time(env, data_term, nullable, column_type.arrow_type, column_type.time_unit, column_type.unit, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_DURATION) {
        ret = do_get_list_duration(env, data_term, nullable, column_type.arrow_type, column_type.time_unit, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_TIMESTAMP) {
        ret = do_get_list_timestamp(env, data_term, nullable, column_type.arrow_type, column_type.time_unit, column_type.unit, column_type.timezone.c_str(), array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_INTERVAL_MONTHS) {
        ret = do_get_list_interval(env, data_term, nullable, column_type.arrow_type, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_INTERVAL_DAY_TIME) {
        ret = do_get_list_interval(env, data_term, nullable, column_type.arrow_type, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO) {
        ret = do_get_list_interval(env, data_term, nullable, column_type.arrow_type, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_FIXED_SIZE_BINARY) {
        ret = do_get_list_fixed_size_binary(env, data_term, nullable, column_type.arrow_type, column_type.fixed_size, array_out, schema_out, error_out);
    } else if (column_type.arrow_type == NANOARROW_TYPE_DECIMAL128 || column_type.arrow_type == NANOARROW_TYPE_DECIMAL256) {
        ret = do_get_list_decimal(env, data_term, nullable, column_type.arrow_type, column_type.bits, column_type.precision, column_type.scale, array_out, schema_out, error_out);
    }

    if (ret == kErrorBufferUnknownType) {
        enif_snprintf(error_out->message, sizeof(error_out->message), "unsupport type `%T` (arrow_type=%d) found in adbc_column_to_adbc_field:%d", column->type_term, column_type.arrow_type, __LINE__);
    }
    return ret;
}

int adbc_column_to_adbc_field(ErlNifEnv *env, ERL_NIF_TERM adbc_column, bool allow_nil, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out, unsigned *n_items) {
    struct AdbcColumnNifTerm column;
    int ret = AdbcColumnNifTerm::from_term(env, adbc_column, allow_nil, &column);
    if (ret != 0) {
        return ret;
    }

    ret = adbc_column_to_adbc_field(env, &column, allow_nil, false, array_out, schema_out, error_out);
    if (n_items) {
        *n_items = column.n_items;
    }
    return ret;
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
            unsigned n_items = 0;
            int ret = adbc_column_to_adbc_field(env, head, false, child_i, schema_i, error_out, &n_items);
            array_out->length = n_items;
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
            case kErrorInternalError:
                // error message is already set
                return 1;
            case kErrorExpectedCalendarISO:
                snprintf(error_out->message, sizeof(error_out->message), "Expected `Calendar.ISO`.");
                return 1;
            default:
                if (ret != 0) {
                    return ret;
                }
                break;
            }
        } else {
            enif_snprintf(error_out->message, sizeof(error_out->message), "unsupported parameter `%T` in adbc_column_to_arrow_type_struct:%d", head, __LINE__);
            return 1;
        }
        processed++;
    }
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuilding(array_out, NANOARROW_VALIDATION_LEVEL_FULL, error_out));
    return !(processed == n_items);
}

#endif  // ADBC_COLUMN_HPP
