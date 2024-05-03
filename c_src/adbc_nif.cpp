#include <erl_nif.h>
#include <cstdbool>
#include <cstdio>
#include <climits>
#include <map>
#include "nif_utils.hpp"
#include <adbc.h>
#include <nanoarrow/nanoarrow.h>
#include "adbc_nif_resource.hpp"

template<> ErlNifResourceType * NifRes<struct AdbcDatabase>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcConnection>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcStatement>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcError>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct ArrowArrayStream>::type = nullptr;

static ERL_NIF_TERM kAtomAdbcError;
static ERL_NIF_TERM kAtomNil;
static ERL_NIF_TERM kAtomTrue;
static ERL_NIF_TERM kAtomFalse;
static ERL_NIF_TERM kAtomEndOfSeries;

static ERL_NIF_TERM kAtomDateModule;
static ERL_NIF_TERM kAtomTimeModule;
static ERL_NIF_TERM kAtomNaiveDateTimeModule;
static ERL_NIF_TERM kAtomCalendarISO;
static ERL_NIF_TERM kAtomStructKey;
static ERL_NIF_TERM kAtomCalendarKey;
static ERL_NIF_TERM kAtomYearKey;
static ERL_NIF_TERM kAtomMonthKey;
static ERL_NIF_TERM kAtomDayKey;
static ERL_NIF_TERM kAtomHourKey;
static ERL_NIF_TERM kAtomMinuteKey;
static ERL_NIF_TERM kAtomSecondKey;
static ERL_NIF_TERM kAtomMicrosecondKey;

static ERL_NIF_TERM kAdbcBufferStructValue;
static ERL_NIF_TERM kAdbcBufferNameKey;
static ERL_NIF_TERM kAdbcBufferTypeKey;
static ERL_NIF_TERM kAdbcBufferNullableKey;
static ERL_NIF_TERM kAdbcBufferMetadataKey;
static ERL_NIF_TERM kAdbcBufferDataKey;
// static ERL_NIF_TERM kAdbcBufferPrivateKey;

static ERL_NIF_TERM kAdbcBufferTypeU8;
static ERL_NIF_TERM kAdbcBufferTypeU16;
static ERL_NIF_TERM kAdbcBufferTypeU32;
static ERL_NIF_TERM kAdbcBufferTypeU64;
static ERL_NIF_TERM kAdbcBufferTypeI8;
static ERL_NIF_TERM kAdbcBufferTypeI16;
static ERL_NIF_TERM kAdbcBufferTypeI32;
static ERL_NIF_TERM kAdbcBufferTypeI64;
static ERL_NIF_TERM kAdbcBufferTypeF32;
static ERL_NIF_TERM kAdbcBufferTypeF64;
static ERL_NIF_TERM kAdbcBufferTypeString;
static ERL_NIF_TERM kAdbcBufferTypeBinary;

static ERL_NIF_TERM nif_error_from_adbc_error(ErlNifEnv *env, struct AdbcError * adbc_error) {
    char const* message = (adbc_error->message == nullptr) ? "unknown error" : adbc_error->message;
    ERL_NIF_TERM nif_error = erlang::nif::error(env, enif_make_tuple4(env,
        kAtomAdbcError,
        erlang::nif::make_binary(env, message),
        enif_make_int(env, adbc_error->vendor_code),
        erlang::nif::make_binary(env, adbc_error->sqlstate, 5)
    ));

    if (adbc_error->release != nullptr) {
        adbc_error->release(adbc_error);
    }

    return nif_error;
}

template <typename T, typename M> static ERL_NIF_TERM values_from_buffer(ErlNifEnv *env, int64_t offset, int64_t count, const uint8_t * validity_bitmap, const T * value_buffer, const M& value_to_nif) {
    std::vector<ERL_NIF_TERM> values(count);
    if (validity_bitmap == nullptr) {
        for (int64_t i = offset; i < offset + count; i++) {
            values[i - offset] = value_to_nif(env, value_buffer[i]);
        }
    } else {
        for (int64_t i = offset; i < offset + count; i++) {
            uint8_t vbyte = validity_bitmap[i/8];
            if (vbyte & (1 << (i & 0b11111111))) {
                values[i - offset] = value_to_nif(env, value_buffer[i]);
            } else {
                values[i - offset] = kAtomNil;
            }
        }
    }

    return enif_make_list_from_array(env, values.data(), (unsigned)values.size());
}

template <typename T, typename M> static ERL_NIF_TERM values_from_buffer(ErlNifEnv *env, int64_t length, const uint8_t * validity_bitmap, const T * value_buffer, const M& value_to_nif) {
    return values_from_buffer(env, 0, length, validity_bitmap, value_buffer, value_to_nif);
}

template <typename M, typename OffsetT> static ERL_NIF_TERM strings_from_buffer(
    ErlNifEnv *env,
    int64_t element_offset,
    int64_t element_count,
    const uint8_t * validity_bitmap,
    const OffsetT * offsets_buffer,
    const uint8_t* value_buffer,
    const M& value_to_nif) {
    OffsetT offset = offsets_buffer[element_offset];
    std::vector<ERL_NIF_TERM> values(element_count);
    if (validity_bitmap == nullptr) {
        for (int64_t i = element_offset; i < element_offset + element_count; i++) {
            OffsetT end_index = offsets_buffer[i + 1];
            size_t nbytes = end_index - offset;
            if (nbytes == 0) {
                values[i - element_offset] = kAtomNil;
            } else {
                values[i - element_offset] = value_to_nif(env, value_buffer, offset, nbytes);
            }
            offset = end_index;
        }
    } else {
        for (int64_t i = element_offset; i < element_offset + element_count; i++) {
            uint8_t vbyte = validity_bitmap[i / 8];
            OffsetT end_index = offsets_buffer[i + 1];
            size_t nbytes = end_index - offset;
            if (nbytes > 0 && vbyte & (1 << (i & 0b11111111))) {
                values[i - element_offset] = value_to_nif(env, value_buffer, offset, nbytes);
            } else {
                values[i - element_offset] = kAtomNil;
            }
            offset = end_index;
        }
    }

    return enif_make_list_from_array(env, values.data(), (unsigned)values.size());
}

template <typename M, typename OffsetT> static ERL_NIF_TERM strings_from_buffer(
    ErlNifEnv *env,
    int64_t length,
    const uint8_t * validity_bitmap,
    const OffsetT * offsets_buffer,
    const uint8_t* value_buffer,
    const M& value_to_nif) {
    return strings_from_buffer(env, 0, length, validity_bitmap, offsets_buffer, value_buffer, value_to_nif);
}

static int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &error, bool *end_of_series = nullptr);
static int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, int64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &error, bool *end_of_series = nullptr);
static int get_arrow_array_children_as_list(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error);
static int get_arrow_array_children_as_list(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error);
static ERL_NIF_TERM get_arrow_array_map_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level);
static ERL_NIF_TERM get_arrow_array_map_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level);
static ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level);
static ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level);
static ERL_NIF_TERM get_arrow_array_dense_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level);
static ERL_NIF_TERM get_arrow_array_dense_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level);
static ERL_NIF_TERM get_arrow_array_sparse_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level);
static ERL_NIF_TERM get_arrow_array_sparse_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level);

int get_arrow_array_children_as_list(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error) {
    if (schema->n_children > 0 && schema->children == nullptr) {
        error = erlang::nif::error(env, "invalid ArrowSchema, schema->children == nullptr, however, schema->n_children > 0");
        return 1;
    }
    if (values->n_children > 0 && values->children == nullptr) {
        error =  erlang::nif::error(env, "invalid ArrowArray, values->children == nullptr, however, values->n_children > 0");
        return 1;
    }
    if (values->n_children != schema->n_children) {
        error =  erlang::nif::error(env, "invalid ArrowArray or ArrowSchema, values->n_children != schema->n_children");
        return 1;
    }
    if (offset < 0 || offset >= values->n_children) {
        error =  erlang::nif::error(env, "invalid offset value, offset < 0 || offset >= values->n_children");
        return 1;
    }
    if ((offset + count) > values->n_children) {
        error =  erlang::nif::error(env, "invalid offset or count value, (offset + count) > values->n_children");
        return 1;
    }

    constexpr int64_t bitmap_buffer_index = 0;
    const uint8_t * bitmap_buffer = (const uint8_t *)values->buffers[bitmap_buffer_index];
    children.resize(count);
    for (int64_t child_i = offset; child_i < offset + count; child_i++) {
        if (bitmap_buffer && values->null_count > 0) {
            uint8_t vbyte = bitmap_buffer[child_i / 8];
            if (!(vbyte & (1 << (child_i & 0b11111111)))) {
                children[child_i - offset] = kAtomNil;
                continue;
            }
        }
        struct ArrowSchema * child_schema = schema->children[child_i];
        struct ArrowArray * child_values = values->children[child_i];
        std::vector<ERL_NIF_TERM> childrens;
        if (arrow_array_to_nif_term(env, child_schema, child_values, level + 1, childrens, error) == 1) {
            return 1;
        }

        if (childrens.size() == 0) {
            children[child_i - offset] = childrens[0];
        } else {
            children[child_i - offset] = enif_make_tuple2(env, childrens[0], childrens[1]);
        }
    }

    return 0;
}

int get_arrow_array_children_as_list(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error) {
    return get_arrow_array_children_as_list(env, schema, values, 0, values->n_children, level, children, error);
}

ERL_NIF_TERM get_arrow_array_map_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level) {
    // From https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
    //
    //   As specified in the Arrow columnar format, the map type has a single child type named entries,
    //   itself a 2-child struct type of (key, value).

    ERL_NIF_TERM error{}, map_out{};
    if (schema->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowSchema (map), schema->children == nullptr");
    }
    if (schema->n_children != 1) {
        return erlang::nif::error(env, "invalid ArrowSchema (map), schema->n_children != 1");
    }
    if (values->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray (map), values->children == nullptr");
    }
    if (values->n_children != 1) {
        return erlang::nif::error(env, "invalid ArrowArray (map), values->n_children != 1");
    }

    struct ArrowSchema * entries_schema = schema->children[0];
    struct ArrowArray * entries_values = values->children[0];
    if (strncmp("entries", entries_schema->name, 7) != 0) {
        return erlang::nif::error(env, "invalid ArrowSchema (map), its single child is not named entries");
    }
    if (count == -1) count = entries_values->n_children;

    std::vector<ERL_NIF_TERM> nif_keys, nif_values;
    bool failed = false;
    for (int64_t child_i = offset; child_i < offset + count; child_i++) {
        struct ArrowSchema * entry_schema = entries_schema->children[child_i];
        struct ArrowArray * entry_values = entries_values->children[child_i];
        if (strncmp("key", entry_schema->name, 3) == 0) {
            if (get_arrow_array_children_as_list(env, entry_schema, entry_values, level + 1, nif_keys, error) == 1) {
                failed = true;
                break;
            }
        } else if (strncmp("value", entry_schema->name, 5) == 0 && entry_schema->n_children == 1) {
            struct ArrowSchema * item_schema = entry_schema->children[0];
            struct ArrowArray * item_values = entry_values->children[0];
            if (get_arrow_array_children_as_list(env, item_schema, item_values, level + 1, nif_values, error) == 1) {
                failed = true;
                break;
            }
        } else {
            failed = true;
        }
    }

    if (!failed) {
        if (nif_keys.size() != nif_values.size()) {
            return erlang::nif::error(env, "number of keys and values doesn't match");
        }

        if (!enif_make_map_from_arrays(env, nif_keys.data(), nif_values.data(), (unsigned)nif_keys.size(), &map_out)) {
            return erlang::nif::error(env, "map contains duplicated keys");
        } else {
            return map_out;
        }
    } else {
        return erlang::nif::error(env, "invalid map");
    }
}

ERL_NIF_TERM get_arrow_array_map_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level) {
    return get_arrow_array_map_children(env, schema, values, 0, -1, level);
}

ERL_NIF_TERM get_arrow_array_dense_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level) {
    ERL_NIF_TERM error{};
    if (schema->n_children > 0 && schema->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowSchema (dense union), schema->children == nullptr while schema->n_children > 0 ");
    }
    if (values->n_children > 0 && values->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray (dense union), values->children == nullptr while values->n_children > 0");
    }
    if (offset < 0 || offset >= values->length) {
        return erlang::nif::error(env, "invalid offset value when parsing ArrowArray (dense union), offset < 0 || offset >= values->length");
    }
    if (count == -1) count = values->length;
    if ((offset + count) > values->length) {
        return erlang::nif::error(env, "invalid offset or count value when parsing ArrowArray (dense union), (offset + count) > values->length");
    }
    if (values->n_buffers != 2) {
        return erlang::nif::error(env, "invalid ArrowArray (dense union), values->n_buffers != 2");
    }

    constexpr int64_t types_buffer_index = 0;
    constexpr int64_t offset_buffer_index = 1;
    const uint8_t * types_buffer = (const uint8_t *)values->buffers[types_buffer_index];
    const int32_t * offsets_buffer = (const int32_t *)values->buffers[offset_buffer_index];

    std::vector<ERL_NIF_TERM> elements(count);
    for (int64_t child_i = offset; child_i < offset + count; child_i++) {
        uint8_t child_type = types_buffer[child_i];
        int32_t child_offset = offsets_buffer[child_i];
        if (child_type >= schema->n_children || child_type >= values->n_children) {
            return erlang::nif::error(env, "invalid child type when parsing ArrowArray (dense union), child_type >= schema->n_children || child_type >= values->n_children");
        }
        struct ArrowSchema * field_schema = schema->children[child_type];
        struct ArrowArray * field_array = values->children[child_type];

        std::vector<ERL_NIF_TERM> union_element_name(1), union_element_value(1);
        std::vector<ERL_NIF_TERM> field_values;
        union_element_name[0] = erlang::nif::make_binary(env, field_schema->name);

        if (arrow_array_to_nif_term(env, field_schema, field_array, child_offset, 1, level + 1, field_values, error) == 1) {
            return error;
        }

        if (field_values.size() == 1) {
            union_element_value[0] = field_values[0];
        } else if (field_values.size() == 2) {
            union_element_value[0] = field_values[1];
        } else {
            return erlang::nif::error(env, "invalid dense union field value");
        }

        ERL_NIF_TERM element{};
        if (!enif_make_map_from_arrays(env, union_element_name.data(), union_element_value.data(), 1, &element)) {
            return erlang::nif::error(env, "failed to enif_make_map_from_arrays when parsing ArrowSchema (dense union)");
        }
        elements[child_i - offset] = element;
    }

    return enif_make_list_from_array(env, elements.data(), (unsigned)elements.size());
}

ERL_NIF_TERM get_arrow_array_dense_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level) {
    return get_arrow_array_dense_union_children(env, schema, values, 0, -1, level);
}

ERL_NIF_TERM get_arrow_array_sparse_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level) {
    ERL_NIF_TERM error{};
    if (schema->n_children > 0 && schema->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowSchema (sparse union), schema->children == nullptr while schema->n_children > 0 ");
    }
    if (values->n_children > 0 && values->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray (sparse union), values->children == nullptr while values->n_children > 0");
    }
    if (offset < 0 || offset >= values->length) {
        return erlang::nif::error(env, "invalid offset value when parsing ArrowArray (sparse union), offset < 0 || offset >= values->length");
    }
    if (count == -1) count = values->length;
    if ((offset + count) > values->length) {
        return erlang::nif::error(env, "invalid offset or count value when parsing ArrowArray (sparse union), (offset + count) > values->length");
    }
    if (values->n_buffers != 1) {
        return erlang::nif::error(env, "invalid ArrowArray (sparse union), values->n_buffers != 1");
    }

    constexpr int64_t types_buffer_index = 0;
    const uint8_t * types_buffer = (const uint8_t *)values->buffers[types_buffer_index];

    std::vector<ERL_NIF_TERM> elements(count);
    for (int64_t child_i = offset; child_i < offset + count; child_i++) {
        uint8_t child_type = types_buffer[child_i];
        if (child_type >= schema->n_children || child_type >= values->n_children) {
            return erlang::nif::error(env, "invalid child type when parsing ArrowArray (sparse union), child_type >= schema->n_children || child_type >= values->n_children");
        }
        struct ArrowSchema * field_schema = schema->children[child_type];
        struct ArrowArray * field_array = values->children[child_type];

        std::vector<ERL_NIF_TERM> union_element_name(1), union_element_value(1);
        std::vector<ERL_NIF_TERM> field_values;
        union_element_name[0] = erlang::nif::make_binary(env, field_schema->name);

        if (arrow_array_to_nif_term(env, field_schema, field_array, child_i, 1, level + 1, field_values, error) == 1) {
            return error;
        }

        if (field_values.size() == 1) {
            union_element_value[0] = field_values[0];
        } else if (field_values.size() == 2) {
            union_element_value[0] = field_values[1];
        } else {
            return erlang::nif::error(env, "invalid sparse union field value");
        }

        ERL_NIF_TERM element{};
        if (!enif_make_map_from_arrays(env, union_element_name.data(), union_element_value.data(), 1, &element)) {
            return erlang::nif::error(env, "failed to enif_make_map_from_arrays when parsing ArrowSchema (sparse union)");
        }
        elements[child_i - offset] = element;
    }

    return enif_make_list_from_array(env, elements.data(), (unsigned)elements.size());
}

ERL_NIF_TERM get_arrow_array_sparse_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level) {
    return get_arrow_array_sparse_union_children(env, schema, values, 0, -1, level);
}

ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level) {
    ERL_NIF_TERM error{};
    if (schema->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowSchema (list), schema->children == nullptr");
    }
    if (schema->n_children != 1) {
        return erlang::nif::error(env, "invalid ArrowSchema (list), schema->n_children != 1");
    }
    if (values->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray (list), values->children == nullptr");
    }
    if (values->n_children != 1) {
        return erlang::nif::error(env, "invalid ArrowArray (list), values->n_children != 1");
    }

    constexpr int64_t bitmap_buffer_index = 0;
    const uint8_t * bitmap_buffer = (const uint8_t *)values->buffers[bitmap_buffer_index];
    struct ArrowSchema * items_schema = schema->children[0];
    struct ArrowArray * items_values = values->children[0];
    if (strncmp("item", items_schema->name, 4) != 0) {
        return erlang::nif::error(env, "invalid ArrowSchema (list), its single child is not named item");
    }

    std::vector<ERL_NIF_TERM> children;
    if (items_values->n_children > 0) {
        if (offset < 0 || offset >= items_values->n_children) {
            return erlang::nif::error(env, "invalid offset for ArrowArray (list), offset < 0 || offset >= items_values->n_children");
        }
        if (count == -1) count = items_values->n_children;
        if ((offset + count) > items_values->n_children) {
            return erlang::nif::error(env, "invalid offset for ArrowArray (list), (offset + count) > items_values->n_children");
        }
        children.resize(count);
        for (int64_t child_i = offset; child_i < offset + count; child_i++) {
            if (bitmap_buffer && values->null_count > 0) {
                uint8_t vbyte = bitmap_buffer[child_i / 8];
                if (!(vbyte & (1 << (child_i & 0b11111111)))) {
                    children[child_i - offset] = kAtomNil;
                    continue;
                }
            }
            struct ArrowSchema * item_schema = items_schema->children[child_i];
            struct ArrowArray * item_values = items_values->children[child_i];

            std::vector<ERL_NIF_TERM> childrens;
            if (arrow_array_to_nif_term(env, item_schema, item_values, level + 1, childrens, error) == 1) {
                return error;
            }

            if (childrens.size() == 1) {
                children[child_i - offset] = childrens[0];
            } else {
                children[child_i - offset] = enif_make_tuple2(env, childrens[0], childrens[1]);
            }
        }
    } else {
        children.resize(1);
        std::vector<ERL_NIF_TERM> childrens;
        if (arrow_array_to_nif_term(env, items_schema, items_values, offset, count, level + 1, childrens, error) == 1) {
            return error;
        }

        if (childrens.size() == 1) {
            children[0] = childrens[0];
        } else {
            children[0] = childrens[1];
        }
    }
    return enif_make_list_from_array(env, children.data(), (unsigned)children.size());
}

ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level) {
    return get_arrow_array_list_children(env, schema, values, 0, -1, level);
}

int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, int64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &error, bool *end_of_series) {
    if (schema == nullptr) {
        error = erlang::nif::error(env, "invalid ArrowSchema (nullptr) when invoking next");
        return 1;
    }
    if (values == nullptr) {
        error = erlang::nif::error(env, "invalid ArrowArray (nullptr) when invoking next");
        return 1;
    }

    const char* format = schema->format ? schema->format : "";
    const char* name = schema->name ? schema->name : "";

    ERL_NIF_TERM current_term{}, children_term{};
    std::vector<ERL_NIF_TERM> children;

    constexpr int64_t bitmap_buffer_index = 0;
    int64_t data_buffer_index = 1;
    int64_t offset_buffer_index = 2;

    bool is_struct = false;
    size_t format_len = strlen(format);
    bool format_processed = true;
    if (format_len == 1) {
        if (format[0] == 'l') {
            // NANOARROW_TYPE_INT64
            using value_type = int64_t;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=l), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_int64
            );
        } else if (format[0] == 'c') {
            // NANOARROW_TYPE_INT8
            using value_type = int8_t;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=c), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_int64
            );
        } else if (format[0] == 's') {
            // NANOARROW_TYPE_INT16
            using value_type = int16_t;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=s), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_int64
            );
        } else if (format[0] == 'i') {
            // NANOARROW_TYPE_INT32
            using value_type = int32_t;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=i), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_int64
            );
        } else if (format[0] == 'L') {
            // NANOARROW_TYPE_UINT64
            using value_type = uint64_t;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=L), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_uint64
            );
        } else if (format[0] == 'C') {
            // NANOARROW_TYPE_UINT8
            using value_type = uint8_t;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=C), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_uint64
            );
        } else if (format[0] == 'S') {
            // NANOARROW_TYPE_UINT16
            using value_type = uint16_t;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=S), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_uint64
            );
        } else if (format[0] == 'I') {
            // NANOARROW_TYPE_UINT32
            using value_type = uint32_t;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=I), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_uint64
            );
        } else if (format[0] == 'f') {
            // NANOARROW_TYPE_FLOAT
            using value_type = float;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=f), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_double
            );
        } else if (format[0] == 'g') {
            // NANOARROW_TYPE_DOUBLE
            using value_type = double;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=g), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                enif_make_double
            );
        } else if (format[0] == 'b') {
            // NANOARROW_TYPE_BOOL
            using value_type = bool;
            if (count == -1) count = values->length;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=b), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                [](ErlNifEnv *env, bool val) -> ERL_NIF_TERM {
                    if (val) {
                        return kAtomTrue;
                    }
                    return kAtomFalse;
                }
            );
        } else if (format[0] == 'u' || format[0] == 'z') {
            // NANOARROW_TYPE_BINARY
            // NANOARROW_TYPE_STRING
            offset_buffer_index = 1;
            data_buffer_index = 2;
            if (count == -1) count = values->length;
            if (values->n_buffers != 3) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=u or format=z), values->n_buffers != 3");
                return 1;
            }
            current_term = strings_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const int32_t *)values->buffers[offset_buffer_index],
                (const uint8_t *)values->buffers[data_buffer_index],
                [](ErlNifEnv *env, const uint8_t * string_buffers, int32_t offset, size_t nbytes) -> ERL_NIF_TERM {
                    return erlang::nif::make_binary(env, (const char *)(string_buffers + offset), nbytes);
                }
            );
        } else if (format[0] == 'U' || format[0] == 'Z') {
            // NANOARROW_TYPE_LARGE_STRING
            // NANOARROW_TYPE_LARGE_BINARY
            offset_buffer_index = 1;
            data_buffer_index = 2;
            if (count == -1) count = values->length;
            if (values->n_buffers != 3) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=U or format=Z), values->n_buffers != 3");
                return 1;
            }
            current_term = strings_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const int64_t *)values->buffers[offset_buffer_index],
                (const uint8_t *)values->buffers[data_buffer_index],
                [](ErlNifEnv *env, const uint8_t * string_buffers, int64_t offset, size_t nbytes) -> ERL_NIF_TERM {
                    return erlang::nif::make_binary(env, (const char *)(string_buffers + offset), nbytes);
                }
            );
        } else {
            format_processed = false;
        }
    } else if (format_len == 2) {
        if (strncmp("+s", format, 2) == 0) {
            // NANOARROW_TYPE_STRUCT
            // only handle and return children if this is a struct
            is_struct = true;

            if (values->length > 0 || values->release != nullptr) {
                if (count == -1) count = values->n_children;
                if (get_arrow_array_children_as_list(env, schema, values, offset, count, level, children, error) == 1) {
                    return 1;
                }
                children_term = enif_make_list_from_array(env, children.data(), (unsigned)count);
            } else {
                if (end_of_series) {
                    *end_of_series = true;
                }
                children_term = kAtomEndOfSeries;
            }
        } else if (strncmp("+m", format, 2) == 0) {
            // NANOARROW_TYPE_MAP
            children_term = get_arrow_array_map_children(env, schema, values, offset, count, level);
        } else if (strncmp("+l", format, 2) == 0) {
            // NANOARROW_TYPE_LIST
            children_term = get_arrow_array_list_children(env, schema, values, offset, count, level);
        } else if (strncmp("+L", format, 2) == 0) {
            // NANOARROW_TYPE_LARGE_LIST
            children_term = get_arrow_array_list_children(env, schema, values, offset, count, level);
        } else {
            format_processed = false;
        }
    } else if (format_len >= 3) {
        if (strncmp("+w:", format, 3) == 0) {
            // NANOARROW_TYPE_FIXED_SIZE_LIST
            children_term = get_arrow_array_list_children(env, schema, values, offset, count, level);
        } else if (format_len > 3 && strncmp("w:", format, 2) == 0) {
            // NANOARROW_TYPE_FIXED_SIZE_BINARY
            if (get_arrow_array_children_as_list(env, schema, values, offset, count, level, children, error) == 1) {
                return 1;
            }
            children_term = enif_make_list_from_array(env, children.data(), (unsigned)count);
        } else if (format_len > 4 && (strncmp("+ud:", format, 4) == 0)) {
            // NANOARROW_TYPE_DENSE_UNION
            children_term = get_arrow_array_dense_union_children(env, schema, values, offset, count, level);
        } else if (format_len > 4 && (strncmp("+us:", format, 4) == 0)) {
            // NANOARROW_TYPE_SPARSE_UNION
            children_term = get_arrow_array_sparse_union_children(env, schema, values, offset, count, level);
        } else if (strncmp("td", format, 2) == 0) {
            char unit = format[2];

            if (unit == 'D' || unit == 'm') {
                // NANOARROW_TYPE_DATE32
                // NANOARROW_TYPE_DATE64
                ERL_NIF_TERM date_module = kAtomDateModule;
                ERL_NIF_TERM calendar_iso = kAtomCalendarISO;
                ERL_NIF_TERM keys[] = {
                    kAtomStructKey,
                    kAtomCalendarKey,
                    kAtomYearKey,
                    kAtomMonthKey,
                    kAtomDayKey,
                };

                auto convert = [unit, date_module, calendar_iso, &keys](ErlNifEnv *env, uint64_t val) -> ERL_NIF_TERM {
                    uint64_t seconds;
                    if (unit == 'D') {
                        seconds = val * 24 * 60 * 60; // days
                    } else {
                        seconds = val / 1000; // milliseconds
                    }
                    time_t t = (time_t)seconds;
                    tm* time = gmtime(&t);
                    ERL_NIF_TERM ex_date;
                    ERL_NIF_TERM values[] = {
                        date_module,
                        calendar_iso,
                        enif_make_int(env, time->tm_year + 1900),
                        enif_make_int(env, time->tm_mon + 1),
                        enif_make_int(env, time->tm_mday)
                    };
                    enif_make_map_from_arrays(env, keys, values, 5, &ex_date);
                    return ex_date;
                };
                if (unit == 'D') {
                    using value_type = uint32_t;
                    if (count == -1) count = values->length;
                    if (values->n_buffers != 2) {
                        error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=tdD), values->n_buffers != 2");
                        return 1;
                    }
                    current_term = values_from_buffer(
                        env,
                        offset,
                        count,
                        (const uint8_t *)values->buffers[bitmap_buffer_index],
                        (const value_type *)values->buffers[data_buffer_index],
                        convert
                    );
                } else {
                    using value_type = uint64_t;
                    if (count == -1) count = values->length;
                    if (values->n_buffers != 2) {
                        error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=tdm), values->n_buffers != 2");
                        return 1;
                    }
                    current_term = values_from_buffer(
                        env,
                        offset,
                        count,
                        (const uint8_t *)values->buffers[bitmap_buffer_index],
                        (const value_type *)values->buffers[data_buffer_index],
                        convert
                    );
                }
            } else {
              format_processed = false;
            }
        // time
        } else if (strncmp("tt", format, 2) == 0) {
            uint64_t unit;
            uint8_t us_precision;
            switch (format[2]) {
                case 's': // seconds
                    // NANOARROW_TYPE_TIME32
                    unit = 1000000000;
                    us_precision = 0;
                    break;
                case 'm': // milliseconds
                    // NANOARROW_TYPE_TIME32
                    unit = 1000000;
                    us_precision = 3;
                    break;
                case 'u': // microseconds
                    // NANOARROW_TYPE_TIME64
                    unit = 1000;
                    us_precision = 6;
                    break;
                case 'n': // nanoseconds
                    // NANOARROW_TYPE_TIME64
                    unit = 1;
                    us_precision = 6;
                    break;
                default:
                    format_processed = false;
            }

            if (format_processed) {
                using value_type = uint64_t;
                if (count == -1) count = values->length;
                if (values->n_buffers != 2) {
                    error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=tt), values->n_buffers != 2");
                    return 1;
                }

                ERL_NIF_TERM keys[] = {
                    kAtomStructKey,
                    kAtomCalendarKey,
                    kAtomHourKey,
                    kAtomMinuteKey,
                    kAtomSecondKey,
                    kAtomMicrosecondKey,
                };

                ERL_NIF_TERM time_module = kAtomTimeModule;
                ERL_NIF_TERM calendar_iso = kAtomCalendarISO;

                current_term = values_from_buffer(
                    env,
                    offset,
                    count,
                    (const uint8_t *)values->buffers[bitmap_buffer_index],
                    (const value_type *)values->buffers[data_buffer_index],
                    [unit, us_precision, time_module, calendar_iso, &keys](ErlNifEnv *env, uint64_t val) -> ERL_NIF_TERM {
                        // Elixir only supports microsecond precision
                        uint64_t us = val * unit / 1000;
                        time_t s = (time_t)(us / 1000000);
                        tm* time = gmtime(&s);
                        us = us % 1000000;

                        ERL_NIF_TERM ex_time;
                        ERL_NIF_TERM values[] = {
                            time_module,
                            calendar_iso,
                            enif_make_int(env, time->tm_hour),
                            enif_make_int(env, time->tm_min),
                            enif_make_int(env, time->tm_sec),
                            enif_make_tuple2(env, enif_make_int(env, us), enif_make_int(env, us_precision))
                        };
                        enif_make_map_from_arrays(env, keys, values, 6, &ex_time);
                        return ex_time;
                    }
              );
            }
        // timestamp
        } else if (strncmp("ts", format, 2) == 0) {
            // NANOARROW_TYPE_TIMESTAMP
            uint64_t unit;
            uint8_t us_precision;
            switch (format[2]) {
                case 's': // seconds
                    unit = 1000000000;
                    us_precision = 0;
                    break;
                case 'm': // milliseconds
                    unit = 1000000;
                    us_precision = 3;
                    break;
                case 'u': // microseconds
                    unit = 1000;
                    us_precision = 6;
                    break;
                case 'n': // nanoseconds
                    unit = 1;
                    us_precision = 6;
                    break;
                default:
                    format_processed = false;
            }

            if (format_len > 4) {
                // TODO: handle timezones (Snowflake always returns naive datetimes)
                // std::string timezone (&format[4]);
                format_processed = false;
            }

            if (format_processed) {
                using value_type = uint64_t;
                if (count == -1) count = values->length;
                if (values->n_buffers != 2) {
                    error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=ts), values->n_buffers != 2");
                    return 1;
                }

                ERL_NIF_TERM naive_dt_module = kAtomNaiveDateTimeModule;
                ERL_NIF_TERM calendar_iso = kAtomCalendarISO;

                ERL_NIF_TERM keys[] = {
                    kAtomStructKey,
                    kAtomCalendarKey,
                    kAtomYearKey,
                    kAtomMonthKey,
                    kAtomDayKey,
                    kAtomHourKey,
                    kAtomMinuteKey,
                    kAtomSecondKey,
                    kAtomMicrosecondKey,
                };

                current_term = values_from_buffer(
                    env,
                    offset,
                    count,
                    (const uint8_t *)values->buffers[bitmap_buffer_index],
                    (const value_type *)values->buffers[data_buffer_index],
                    [unit, us_precision, naive_dt_module, calendar_iso, &keys](ErlNifEnv *env, uint64_t val) -> ERL_NIF_TERM {
                        // Elixir only supports microsecond precision
                        uint64_t us = val * unit / 1000;
                        time_t t = (time_t)(us / 1000000);
                        tm* time = gmtime(&t);
                        us = us % 1000000;

                        ERL_NIF_TERM ex_dt;
                        ERL_NIF_TERM values[] = {
                            naive_dt_module,
                            calendar_iso,
                            enif_make_int(env, time->tm_year + 1900),
                            enif_make_int(env, time->tm_mon + 1),
                            enif_make_int(env, time->tm_mday),
                            enif_make_int(env, time->tm_hour),
                            enif_make_int(env, time->tm_min),
                            enif_make_int(env, time->tm_sec),
                            enif_make_tuple2(env, enif_make_int(env, us), enif_make_int(env, us_precision))
                        };

                        enif_make_map_from_arrays(env, keys, values, 9, &ex_dt);
                        return ex_dt;
                    }
                );
            }
        } else {
            format_processed = false;
        }
    } else {
        format_processed = false;
    }

    if (!format_processed) {
        char buf[256] = { '\0' };
        snprintf(buf, 255, "not yet implemented for format: `%s`", schema->format);
        error = erlang::nif::error(env, erlang::nif::make_binary(env, buf));
        return 1;
        // printf("not implemented for format: `%s`\r\n", schema->format);
        // printf("length: %lld\r\n", values->length);
        // printf("null_count: %lld\r\n", values->null_count);
        // printf("offset: %lld\r\n", values->offset);
        // printf("n_buffers: %lld\r\n", values->n_buffers);
        // printf("n_children: %lld\r\n", values->n_children);
        // printf("buffers: %p\r\n", values->buffers);
    }

    out_terms.clear();

    if (is_struct) {
        out_terms.emplace_back(children_term);
    } else {
        if (schema->children) {
            out_terms.emplace_back(erlang::nif::make_binary(env, name));
            out_terms.emplace_back(children_term);
        } else {
            out_terms.emplace_back(erlang::nif::make_binary(env, name));
            out_terms.emplace_back(current_term);
        }
    }

    return 0;
}

int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &error, bool *end_of_series) {
    return arrow_array_to_nif_term(env, schema, values, 0, -1, level, out_terms, error, end_of_series);
}

static ERL_NIF_TERM adbc_database_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM error{};
    auto database = res_type::allocate_resource(env, error);
    if (database == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcDatabaseNew(&database->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ERL_NIF_TERM ret = database->make_resource(env);
    return erlang::nif::ok(env, ret);
}

template <typename T, typename GetString, typename GetBytes, typename GetInt, typename GetDouble>
static ERL_NIF_TERM adbc_get_option(ErlNifEnv *env, const ERL_NIF_TERM argv[], GetString& get_string, GetBytes& get_bytes, GetInt& get_int, GetDouble& get_double) {
    using res_type = NifRes<T>;

    ERL_NIF_TERM error{};
    res_type * resource = nullptr;
    if ((resource = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    std::string type, key;
    if (!erlang::nif::get_atom(env, argv[1], type)) {
        return enif_make_badarg(env);
    }
    if (!erlang::nif::get(env, argv[2], key)) {
        return enif_make_badarg(env);
    }

    struct AdbcError adbc_error{};
    if (type == "string" || type == "binary") {
        int is_string = type == "string";
        uint8_t value[64] = {'\0'};
        constexpr size_t value_buffer_size = sizeof(value) / sizeof(value[0]);
        size_t value_len = value_buffer_size;
        AdbcStatusCode code;
        size_t elem_size = 0;
        if (is_string) {
            elem_size = sizeof(char);
            code = get_string(&resource->val, key.c_str(), (char *)value, &value_len, &adbc_error);
        } else {
            elem_size = sizeof(uint8_t);
            code = get_bytes(&resource->val, key.c_str(), value, &value_len, &adbc_error);
        }
        if (code != ADBC_STATUS_OK) {
            return nif_error_from_adbc_error(env, &adbc_error);
        }

        if (value_len > value_buffer_size) {
            uint8_t * out_value = (uint8_t *)enif_alloc(elem_size * (value_len + 1));
            memset(out_value, 0, elem_size * (value_len + 1));
            value_len += 1;
            if (is_string) {
                code = get_string(&resource->val, key.c_str(), (char *)out_value, &value_len, &adbc_error);
            } else {
                code = get_bytes(&resource->val, key.c_str(), out_value, &value_len, &adbc_error);
            }
            
            if (code != ADBC_STATUS_OK) {
                return nif_error_from_adbc_error(env, &adbc_error);
            }

            // minus 1 to remove the null terminator for strings
            ERL_NIF_TERM ret;
            ret = erlang::nif::make_binary(env, (const char *)out_value, value_len - (is_string ? 1 : 0));
            enif_free(out_value);
            return erlang::nif::ok(env, ret);
        } else {
            // minus 1 to remove the null terminator for strings
            return erlang::nif::ok(env, erlang::nif::make_binary(env, (const char *)value, value_len - (is_string ? 1 : 0)));
        }
    } else if (type == "integer") {
        int64_t value = 0;
        AdbcStatusCode code = get_int(&resource->val, key.c_str(), &value, &adbc_error);
        if (code != ADBC_STATUS_OK) {
            return nif_error_from_adbc_error(env, &adbc_error);
        }

        return erlang::nif::ok(env, erlang::nif::make(env, value));
    } else if (type == "float") {
        double value = 0;
        AdbcStatusCode code = get_double(&resource->val, key.c_str(), &value, &adbc_error);
        if (code != ADBC_STATUS_OK) {
            return nif_error_from_adbc_error(env, &adbc_error);
        }

        return erlang::nif::ok(env, erlang::nif::make(env, value));
    } else {
        return enif_make_badarg(env);
    }
}

template <typename T, typename SetString, typename SetBytes, typename SetInt, typename SetDouble>
static ERL_NIF_TERM adbc_set_option(ErlNifEnv *env, const ERL_NIF_TERM argv[], SetString& set_string, SetBytes &set_bytes, SetInt &set_int, SetDouble &set_double) {
    using res_type = NifRes<T>;

    ERL_NIF_TERM error{};
    res_type * resource = nullptr;
    if ((resource = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    std::string type, key;
    if (!erlang::nif::get_atom(env, argv[1], type)) {
        return enif_make_badarg(env);
    }
    if (!erlang::nif::get(env, argv[2], key)) {
        return enif_make_badarg(env);
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code;
    if (type == "string" || type == "binary") {
        if (type == "string") {
            std::string value;
            if (!erlang::nif::get(env, argv[3], value)) {
                return enif_make_badarg(env);
            }
            code = set_string(&resource->val, key.c_str(), value.c_str(), &adbc_error);
        } else {
            ErlNifBinary bin;
            ERL_NIF_TERM ret = enif_inspect_binary(env, argv[3], &bin);
            if (!ret) {
                return enif_make_badarg(env);
            }
            code = set_bytes(&resource->val, key.c_str(), bin.data, bin.size, &adbc_error);
        }
    } else if (type == "integer") {
        int64_t value;
        if (!erlang::nif::get(env, argv[3], &value)) {
            return enif_make_badarg(env);
        }
        code = set_int(&resource->val, key.c_str(), value, &adbc_error);
    } else if (type == "float") {
        double value;
        if (!erlang::nif::get(env, argv[3], &value)) {
            return enif_make_badarg(env);
        }
        code = set_double(&resource->val, key.c_str(), value, &adbc_error);
    } else {
        return enif_make_badarg(env);
    }

    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }
    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_database_get_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return adbc_get_option<struct AdbcDatabase>(
        env,
        argv,
        AdbcDatabaseGetOption,
        AdbcDatabaseGetOptionBytes,
        AdbcDatabaseGetOptionInt,
        AdbcDatabaseGetOptionDouble
    );
}

static ERL_NIF_TERM adbc_database_set_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return adbc_set_option<struct AdbcDatabase>(
        env,
        argv,
        AdbcDatabaseSetOption,
        AdbcDatabaseSetOptionBytes,
        AdbcDatabaseSetOptionInt,
        AdbcDatabaseSetOptionDouble
    );
}

static ERL_NIF_TERM adbc_database_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM error{};
    res_type * database = nullptr;
    if ((database = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcDatabaseInit(&database->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM error{};
    auto connection = res_type::allocate_resource(env, error);
    if (connection == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionNew(&connection->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ERL_NIF_TERM ret = connection->make_resource(env);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_connection_get_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return adbc_get_option<struct AdbcConnection>(
        env,
        argv,
        AdbcConnectionGetOption,
        AdbcConnectionGetOptionBytes,
        AdbcConnectionGetOptionInt,
        AdbcConnectionGetOptionDouble
    );
}

static ERL_NIF_TERM adbc_connection_set_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return adbc_set_option<struct AdbcConnection>(
        env,
        argv,
        AdbcConnectionSetOption,
        AdbcConnectionSetOptionBytes,
        AdbcConnectionSetOptionInt,
        AdbcConnectionSetOptionDouble
    );
}

static ERL_NIF_TERM adbc_connection_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using db_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    db_type * db = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }
    if ((db = db_type::get_resource(env, argv[1], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionInit(&connection->val, &db->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    connection->private_data = &db->val;
    enif_keep_resource(&db->val);
    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_get_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    std::vector<uint32_t> info_codes;
    if (!erlang::nif::get_list(env, argv[1], info_codes)) {
        return enif_make_badarg(env);
    }
    uint32_t* ptr = nullptr;
    size_t info_codes_length = info_codes.size();
    if (info_codes_length != 0) {
        ptr = info_codes.data();
    }

    auto array_stream = array_stream_type::allocate_resource(env, error);
    if (array_stream == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionGetInfo(&connection->val, ptr, info_codes_length, &array_stream->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ERL_NIF_TERM ret = array_stream->make_resource(env);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_connection_get_objects(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    int depth;
    std::string catalog, db_schema, table_name;
    std::vector<ErlNifBinary> table_type;
    std::string column_name;
    const char * catalog_p = nullptr;
    const char * db_schema_p = nullptr;
    const char * table_name_p = nullptr;
    const char * column_name_p = nullptr;

    if (!erlang::nif::get(env, argv[1], &depth)) {
        return enif_make_badarg(env);
    }
    if (!erlang::nif::get(env, argv[2], catalog)) {
        if (!erlang::nif::check_nil(env, argv[2])) {
            return enif_make_badarg(env);
        } else {
            catalog_p = catalog.c_str();
        }
    }
    if (!erlang::nif::get(env, argv[3], db_schema)) {
        if (!erlang::nif::check_nil(env, argv[3])) {
            return enif_make_badarg(env);
        } else {
            db_schema_p = db_schema.c_str();
        }
    }
    if (!erlang::nif::get(env, argv[4], table_name)) {
        if (!erlang::nif::check_nil(env, argv[4])) {
            return enif_make_badarg(env);
        } else {
            table_name_p = table_name.c_str();
        }
    }
    if (!erlang::nif::get_list(env, argv[5], table_type)) {
        if (!erlang::nif::check_nil(env, argv[5])) {
            return enif_make_badarg(env);
        }
    }
    if (!erlang::nif::get(env, argv[6], column_name)) {
        if (!erlang::nif::check_nil(env, argv[6])) {
            return enif_make_badarg(env);
        } else {
            column_name_p = column_name.c_str();
        }
    }

    std::vector<const char *> table_types;
    for (auto& tt : table_type) {
        char * t = (char *)enif_alloc(tt.size + 1);
        if (t == nullptr) {
            for (auto& at : table_types) {
                enif_free((void *)at);
            }
            return erlang::nif::error(env, "out of memory");
        }
        memcpy(t, tt.data, tt.size);
        t[tt.size] = '\0';
        table_types.emplace_back(t);
    }
    // Terminate the list with a NULL entry.
    table_types.emplace_back(nullptr);

    auto array_stream = array_stream_type::allocate_resource(env, error);
    if (array_stream == nullptr) {
        for (auto& at : table_types) {
            if (at) enif_free((void *)at);
        }
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionGetObjects(
        &connection->val,
        depth,
        catalog_p,
        db_schema_p,
        table_name_p,
        table_types.data(),
        column_name_p,
        &array_stream->val,
        &adbc_error);
    if (code != ADBC_STATUS_OK) {
        for (auto& at : table_types) {
            if (at) enif_free((void *)at);
        }
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ERL_NIF_TERM ret = array_stream->make_resource(env);
    for (auto& at : table_types) {
        if (at) enif_free((void *)at);
    }

    return enif_make_tuple2(env, erlang::nif::ok(env), ret);
}

static ERL_NIF_TERM adbc_connection_get_table_types(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    auto array_stream = array_stream_type::allocate_resource(env, error);
    if (array_stream == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionGetTableTypes(&connection->val, &array_stream->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ERL_NIF_TERM ret = array_stream->make_resource(env);

    return enif_make_tuple2(env, erlang::nif::ok(env), ret);
}

static ERL_NIF_TERM adbc_arrow_array_stream_get_pointer(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct ArrowArrayStream>;
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    return enif_make_uint64(env, reinterpret_cast<uint64_t>(&res->val));
}

static ERL_NIF_TERM adbc_arrow_array_stream_next(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct ArrowArrayStream>;
    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }
    if (res->val.get_next == nullptr) {
        return enif_make_badarg(env);
    }

    struct ArrowArray out{};
    int code = res->val.get_next(&res->val, &out);
    if (code != 0) {
        const char * reason = res->val.get_last_error(&res->val);
        return erlang::nif::error(env, reason ? reason : "unknown error");
    }

    if (res->private_data != nullptr) {
        enif_free(res->private_data);
        res->private_data = nullptr;
    }

    res->private_data = enif_alloc(sizeof(struct ArrowSchema));
    memset(res->private_data, 0, sizeof(struct ArrowSchema));
    code = res->val.get_schema(&res->val, (struct ArrowSchema *)res->private_data);
    if (code != 0) {
        const char * reason = res->val.get_last_error(&res->val);
        enif_free(res->private_data);
        res->private_data = nullptr;
        return erlang::nif::error(env, reason ? reason : "unknown error");
    }

    std::vector<ERL_NIF_TERM> out_terms;

    auto schema = (struct ArrowSchema*)res->private_data;
    bool end_of_series = false;
    if (arrow_array_to_nif_term(env, schema, &out, 0, out_terms, error, &end_of_series) == 1) {
        if (out.release) out.release(&out);
        return error;
    }

    if (out_terms.size() == 1) {
        ret = out_terms[0];
        if (end_of_series) {
            if (out.release) {
                out.release(&out);
            }
            return ret;
        }
    } else {
        ret = enif_make_tuple2(env, out_terms[0], out_terms[1]);
    }

    if (out.release) {
        out.release(&out);
        return enif_make_tuple3(env, erlang::nif::ok(env), ret, enif_make_int64(env, 1));
    } else {
        return enif_make_tuple3(env, erlang::nif::ok(env), ret, enif_make_int64(env, 0));
    }
}

static ERL_NIF_TERM adbc_arrow_array_stream_release(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct ArrowArrayStream>;
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    if (res->val.release) {
        res->val.release(&res->val);
        res->val.release = nullptr;
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_statement_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;
    using connection_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM error{};

    connection_type * connection = nullptr;
    if ((connection = connection_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    auto statement = res_type::allocate_resource(env, error);
    if (statement == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementNew(&connection->val, &statement->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    connection->private_data = &connection->val;
    enif_keep_resource(&connection->val);
    ERL_NIF_TERM ret = statement->make_resource(env);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_statement_get_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return adbc_get_option<struct AdbcStatement>(
        env,
        argv,
        AdbcStatementGetOption,
        AdbcStatementGetOptionBytes,
        AdbcStatementGetOptionInt,
        AdbcStatementGetOptionDouble
    );
}

static ERL_NIF_TERM adbc_statement_set_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    return adbc_set_option<struct AdbcStatement>(
        env,
        argv,
        AdbcStatementSetOption,
        AdbcStatementSetOptionBytes,
        AdbcStatementSetOptionInt,
        AdbcStatementSetOptionDouble
    );
}

static ERL_NIF_TERM adbc_statement_execute_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    auto array_stream = array_stream_type::allocate_resource(env, error);
    if (array_stream == nullptr) {
        return error;
    }

    int64_t rows_affected = 0;
    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementExecuteQuery(&statement->val, &array_stream->val, &rows_affected, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ERL_NIF_TERM ret = array_stream->make_resource(env);
    return enif_make_tuple3(env,
        erlang::nif::ok(env),
        ret,
        enif_make_int64(env, rows_affected)
    );
}

static ERL_NIF_TERM adbc_statement_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;

    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementPrepare(&statement->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_statement_set_sql_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;

    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    std::string query;
    if (!erlang::nif::get(env, argv[1], query)) {
        return enif_make_badarg(env);
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementSetSqlQuery(&statement->val, query.c_str(), &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    return erlang::nif::ok(env);
}

// non-zero return value indicating errors
int adbc_buffer_to_adbc_field(ErlNifEnv *env, ERL_NIF_TERM adbc_buffer, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    array_out->release = NULL;
    schema_out->release = NULL;

    size_t n_items = 0;
    if (!enif_is_map(env, adbc_buffer)) {
        return 1;
    }

    if (!enif_get_map_size(env, adbc_buffer, &n_items)) {
        return 2;
    }

    /*
    
    %{
        __struct__: "Elixir.Adbc.Buffer",
        name: String.t() | nil,
        type: atom(),
        nullable: boolean(),
        metadata: %{
            "key": "value"
        },
        data: [term()]
    }
    
    */

    ERL_NIF_TERM struct_name_term, name_term, type_term, nullable_term, metadata_term, data_term;
    if (!enif_get_map_value(env, adbc_buffer, kAdbcBufferStructKey, &struct_name_term)) {
        return 3;
    }
    if (!enif_is_identical(struct_name_term, kAdbcBufferStructValue)) {
        return 4;
    }

    if (!enif_get_map_value(env, adbc_buffer, kAdbcBufferNameKey, &name_term)) {
        return 3;
    }
    if (!enif_get_map_value(env, adbc_buffer, kAdbcBufferTypeKey, &type_term)) {
        return 3;
    }
    if (!enif_get_map_value(env, adbc_buffer, kAdbcBufferNullableKey, &nullable_term)) {
        return 3;
    }
    if (!enif_get_map_value(env, adbc_buffer, kAdbcBufferMetadataKey, &metadata_term)) {
        return 3;
    }
    if (!enif_get_map_value(env, adbc_buffer, kAdbcBufferDataKey, &data_term)) {
        return 3;
    }

    std::string name;
    if (!enif_is_identical(struct_name_term, kAdbcNifNil)) {
        if (!erlang::nif::get(env, name_term, name)) {
            erlang::nif::get_atom(env, name_term, name);
        }
    }
    printf("name: %s\r\n", name.c_str());

    if (enif_is_identical(type_term, kAdbcBufferTypeI8)) {
        return 5;
    }

    // ArrowSchemaInit(schema_out);
    // NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_out, NANOARROW_TYPE_LIST));
    // NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromType(array_out, NANOARROW_TYPE_LIST));
    // NANOARROW_RETURN_NOT_OK(ArrowArrayAllocateChildren(array_out, static_cast<int64_t>(n_items)));
    // array_out->length = 1;

    // ERL_NIF_TERM head, tail;
    // tail = list;

    // if (n_items > 1) {
    //     if (enif_get_list_cell(env, tail, &head, &tail)) {

    //     }
    // }

    // int64_t processed = 0;
    // while (enif_get_list_cell(env, tail, &head, &tail)) {
    //     auto schema_i = schema_out->children[processed];
    //     ArrowSchemaInit(schema_i);

    //     auto child_i = array_out->children[processed];
    //     ErlNifSInt64 i64;
    //     double f64;
    //     ErlNifBinary bytes;
    // }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(array_out, error_out));
    return 0;   
}

// non-zero return value indicating errors
int adbc_buffer_to_arrow_type_struct(ErlNifEnv *env, ERL_NIF_TERM values, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
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
        adbc_buffer_to_adbc_field(env, head, child_i, schema_i, error_out);

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
            auto type = NANOARROW_TYPE_BINARY;
            if (bytes.size > INT32_MAX) {
                type = NANOARROW_TYPE_LARGE_BINARY;
            }

            struct ArrowBufferView view{};
            view.data.data = bytes.data;
            view.size_bytes = static_cast<int64_t>(bytes.size);
            NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(child_i));
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(child_i, view));
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
        } else {
            snprintf(error_out->message, sizeof(error_out->message), "type not supported yet.");
            return 1;
        }
        processed++;
    }
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(array_out, error_out));
    return !(processed == n_items);
}

static ERL_NIF_TERM adbc_statement_bind(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    if (!enif_is_list(env, argv[1])) {
        return enif_make_badarg(env);
    }

    struct ArrowArray values{};
    struct ArrowSchema schema{};
    struct ArrowError arrow_error{};
    struct AdbcError adbc_error{};
    AdbcStatusCode code{};

    if (adbc_buffer_to_arrow_type_struct(env, argv[1], &values, &schema, &arrow_error)) {
        ret = erlang::nif::error(env, arrow_error.message);
        goto cleanup;
    }

    code = AdbcStatementBind(&statement->val, &values, &schema, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        goto cleanup;
    }
    ret = erlang::nif::ok(env);

cleanup:
    if (values.release) values.release(&values);
    if (schema.release) schema.release(&schema);
    return ret;
}

static ERL_NIF_TERM adbc_statement_bind_stream(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    array_stream_type * stream = nullptr;
    if ((stream = array_stream_type::get_resource(env, argv[1], error)) == nullptr) {
        return error;
    }
    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementBindStream(&statement->val, &stream->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    return erlang::nif::ok(env);
}

static int on_load(ErlNifEnv *env, void **, ERL_NIF_TERM) {
    ErlNifResourceType *rt;

    {
        using res_type = NifRes<struct AdbcDatabase>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResAdbcDatabase", destruct_adbc_database_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct AdbcConnection>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResAdbcConnection", destruct_adbc_connection_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct AdbcStatement>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResAdbcStatement", destruct_adbc_statement_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct AdbcError>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResAdbcError", destruct_adbc_error, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct ArrowArrayStream>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResArrowArrayStream", destruct_adbc_arrow_array_stream, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

<<<<<<< Updated upstream
    kAtomAdbcError = erlang::nif::atom(env, "adbc_error");
    kAtomNil = erlang::nif::atom(env, "nil");
    kAtomTrue = erlang::nif::atom(env, "true");
    kAtomFalse = erlang::nif::atom(env, "false");
    kAtomEndOfSeries = erlang::nif::atom(env, "end_of_series");
    kAtomDateModule = erlang::nif::atom(env, "Elixir.Date");
    kAtomCalendarISO = erlang::nif::atom(env, "Elixir.Calendar.ISO");
    kAtomTimeModule = erlang::nif::atom(env, "Elixir.Time");
    kAtomNaiveDateTimeModule = erlang::nif::atom(env, "Elixir.NaiveDateTime");
    kAtomStructKey = erlang::nif::atom(env, "__struct__");
    kAtomCalendarKey = erlang::nif::atom(env, "calendar");
    kAtomYearKey = erlang::nif::atom(env, "year");
    kAtomMonthKey = erlang::nif::atom(env, "month");
    kAtomDayKey = erlang::nif::atom(env, "day");
    kAtomHourKey = erlang::nif::atom(env, "hour");
    kAtomMinuteKey = erlang::nif::atom(env, "minute");
    kAtomSecondKey = erlang::nif::atom(env, "second");
    kAtomMicrosecondKey = erlang::nif::atom(env, "microsecond");

    kAdbcBufferStructValue = enif_make_atom(env, "Elixir.Adbc.Buffer");
    kAdbcBufferNameKey = enif_make_atom(env, "name");
    kAdbcBufferTypeKey = enif_make_atom(env, "type");
    kAdbcBufferNullableKey = enif_make_atom(env, "nullable");
    kAdbcBufferMetadataKey = enif_make_atom(env, "metadata");
    kAdbcBufferDataKey = enif_make_atom(env, "data");
    // kAdbcBufferPrivateKey = enif_make_atom(env, "__private__");

    kAdbcBufferTypeU8 = enif_make_atom(env, "u8");
    kAdbcBufferTypeU16 = enif_make_atom(env, "u16");
    kAdbcBufferTypeU32 = enif_make_atom(env, "u32");
    kAdbcBufferTypeU64 = enif_make_atom(env, "u64");
    kAdbcBufferTypeI8 = enif_make_atom(env, "i8");
    kAdbcBufferTypeI16 = enif_make_atom(env, "i16");
    kAdbcBufferTypeI32 = enif_make_atom(env, "i32");
    kAdbcBufferTypeI64 = enif_make_atom(env, "i64");
    kAdbcBufferTypeF32 = enif_make_atom(env, "f32");
    kAdbcBufferTypeF64 = enif_make_atom(env, "f64");
    kAdbcBufferTypeString = enif_make_atom(env, "string");
    kAdbcBufferTypeBinary = enif_make_atom(env, "binary");

    return 0;
}

static int on_reload(ErlNifEnv *, void **, ERL_NIF_TERM) {
    return 0;
}

static int on_upgrade(ErlNifEnv *, void **, void **, ERL_NIF_TERM) {
    return 0;
}

static ErlNifFunc nif_functions[] = {
    {"adbc_database_new", 0, adbc_database_new, 0},
    {"adbc_database_get_option", 3, adbc_database_get_option, 0},
    {"adbc_database_set_option", 4, adbc_database_set_option, 0},
    {"adbc_database_init", 1, adbc_database_init, 0},

    {"adbc_connection_new", 0, adbc_connection_new, 0},
    {"adbc_connection_get_option", 3, adbc_connection_get_option, 0},
    {"adbc_connection_set_option", 4, adbc_connection_set_option, 0},
    {"adbc_connection_init", 2, adbc_connection_init, 0},
    {"adbc_connection_get_info", 2, adbc_connection_get_info, 0},
    {"adbc_connection_get_objects", 7, adbc_connection_get_objects, 0},
    {"adbc_connection_get_table_types", 1, adbc_connection_get_table_types, 0},

    {"adbc_statement_new", 1, adbc_statement_new, 0},
    {"adbc_statement_get_option", 3, adbc_statement_get_option, 0},
    {"adbc_statement_set_option", 4, adbc_statement_set_option, 0},
    {"adbc_statement_execute_query", 1, adbc_statement_execute_query, 0},
    {"adbc_statement_prepare", 1, adbc_statement_prepare, 0},
    {"adbc_statement_set_sql_query", 2, adbc_statement_set_sql_query, 0},
    {"adbc_statement_bind", 2, adbc_statement_bind, 0},
    {"adbc_statement_bind_stream", 2, adbc_statement_bind_stream, 0},

    {"adbc_arrow_array_stream_get_pointer", 1, adbc_arrow_array_stream_get_pointer, 0},
    {"adbc_arrow_array_stream_next", 1, adbc_arrow_array_stream_next, 0},
    {"adbc_arrow_array_stream_release", 1, adbc_arrow_array_stream_release, 0}
};

ERL_NIF_INIT(Elixir.Adbc.Nif, nif_functions, on_load, on_reload, on_upgrade, NULL);
