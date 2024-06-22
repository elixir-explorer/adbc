#ifndef ADBC_ARROW_ARRAY_HPP
#define ADBC_ARROW_ARRAY_HPP
#pragma once

#include <stdio.h>
#include <cmath>
#include <cstdbool>
#include <cstdint>
#include <vector>
#include <adbc.h>
#include <erl_nif.h>
#include "adbc_half_float.hpp"
#include "adbc_arrow_metadata.hpp"

static int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &value_type, ERL_NIF_TERM &metadata, ERL_NIF_TERM &error, bool skip_dictionary_check = false);
static int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, int64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &value_type, ERL_NIF_TERM &metadata, ERL_NIF_TERM &error, bool skip_dictionary_check = false);
static int get_arrow_array_children_as_list(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error);
static int get_arrow_array_children_as_list(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error);
static int get_arrow_struct(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error);
static int get_arrow_struct(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error);
static ERL_NIF_TERM get_arrow_array_map_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level);
static ERL_NIF_TERM get_arrow_array_map_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level);
static ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, ArrowType list_type, unsigned n_items = 0);
static ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level, ArrowType list_type, unsigned n_items = 0);
static ERL_NIF_TERM get_arrow_array_dense_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level);
static ERL_NIF_TERM get_arrow_array_dense_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level);
static ERL_NIF_TERM get_arrow_array_sparse_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level);
static ERL_NIF_TERM get_arrow_array_sparse_union_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level);

template <typename M> static ERL_NIF_TERM bit_boolean_from_buffer(ErlNifEnv *env, int64_t offset, int64_t count, const uint8_t * value_buffer, const M& value_to_nif) {
    std::vector<ERL_NIF_TERM> values(count);
    for (int64_t i = offset; i < offset + count; i++) {
        uint8_t vbyte = value_buffer[i/8];
        values[i - offset] = value_to_nif(env, vbyte & (1 << (i % 8)));
    }
    return enif_make_list_from_array(env, values.data(), (unsigned)values.size());
}

static ERL_NIF_TERM boolean_values_from_buffer(ErlNifEnv *env, int64_t offset, int64_t count, const uint8_t * validity_bitmap, const bool * value_buffer) {
    std::vector<ERL_NIF_TERM> values(count);
    if (validity_bitmap == nullptr) {
        for (int64_t i = offset; i < offset + count; i++) {
            uint8_t dbyte = value_buffer[i/8];
            bool boolean_val = dbyte & (1 << (i % 8));
            values[i - offset] = boolean_val ? kAtomTrue : kAtomFalse;
        }
    } else {
        for (int64_t i = offset; i < offset + count; i++) {
            uint8_t vbyte = validity_bitmap[i/8];
            uint8_t mask = 1 << (i % 8);
            if (vbyte & mask) {
                uint8_t dbyte = value_buffer[i/8];
                bool boolean_val = dbyte & mask;
                values[i - offset] = boolean_val ? kAtomTrue : kAtomFalse;
            } else {
                values[i - offset] = kAtomNil;
            }
        }
    }

    return enif_make_list_from_array(env, values.data(), (unsigned)values.size());
}

static ERL_NIF_TERM boolean_values_from_buffer(ErlNifEnv *env, int64_t length, const uint8_t * validity_bitmap, const bool * value_buffer) {
    return boolean_values_from_buffer(env, 0, length, validity_bitmap, value_buffer);
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
            if (vbyte & (1 << (i % 8))) {
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
            if (nbytes > 0 && vbyte & (1 << (i % 8))) {
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

template <typename M>
static ERL_NIF_TERM fixed_size_binary_from_buffer(
    ErlNifEnv *env,
    int64_t element_offset,
    int64_t element_count,
    size_t element_bytes,
    const uint8_t * validity_bitmap,
    const uint8_t* value_buffer,
    const M& value_to_nif) {
    std::vector<ERL_NIF_TERM> values(element_count);
    if (validity_bitmap == nullptr) {
        for (int64_t i = element_offset; i < element_offset + element_count; i++) {
            values[i - element_offset] = value_to_nif(env, &value_buffer[element_bytes * i]);
        }
    } else {
        for (int64_t i = element_offset; i < element_offset + element_count; i++) {
            uint8_t vbyte = validity_bitmap[i / 8];
            if (vbyte & (1 << (i % 8))) {
                values[i - element_offset] = value_to_nif(env, &value_buffer[element_bytes * i]);
            } else {
                values[i - element_offset] = kAtomNil;
            }
        }
    }

    return enif_make_list_from_array(env, values.data(), (unsigned)values.size());
}

template <typename M> static ERL_NIF_TERM fixed_size_binary_from_buffer(
    ErlNifEnv *env,
    int64_t length,
    size_t element_bytes,
    const uint8_t * validity_bitmap,
    const uint8_t* value_buffer,
    const M& value_to_nif) {
    return fixed_size_binary_from_buffer(env, 0, length, element_bytes, validity_bitmap, value_buffer, value_to_nif);
}

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

    constexpr int64_t bitmap_buffer_index = 0;
    const uint8_t * bitmap_buffer = (const uint8_t *)values->buffers[bitmap_buffer_index];
    children.resize(values->n_children);
    for (int64_t child_i = 0; child_i < values->n_children; child_i++) {
        if (bitmap_buffer && ((schema->flags & ARROW_FLAG_NULLABLE) || (values->null_count > 0))) {
            uint8_t vbyte = bitmap_buffer[child_i / 8];
            if (!(vbyte & (1 << (child_i % 8)))) {
                children[child_i] = kAtomNil;
                continue;
            }
        }
        struct ArrowSchema * child_schema = schema->children[child_i];
        struct ArrowArray * child_values = values->children[child_i];
        std::vector<ERL_NIF_TERM> childrens;
        ERL_NIF_TERM child_type;
        ERL_NIF_TERM child_metadata;
        if (arrow_array_to_nif_term(env, child_schema, child_values, offset, count, level + 1, childrens, child_type, child_metadata, error) == 1) {
            return 1;
        }

        if (childrens.size() == 1) {
            children[child_i] = childrens[0];
        } else {
            bool nullable = (child_schema->flags & ARROW_FLAG_NULLABLE) || (child_values->null_count > 0);
            if (enif_is_identical(childrens[1], kAtomNil)) {
                children[child_i] = kAtomNil;
            } else {
                children[child_i] = make_adbc_column(env, schema, values, childrens[0], child_type, nullable, child_metadata, childrens[1]);
            }
        }
    }
    return 0;
}

int get_arrow_array_children_as_list(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error) {
    return get_arrow_array_children_as_list(env, schema, values, 0, -1, level, children, error);
}

int get_arrow_struct(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error) {
    if (schema->n_children > 0 && schema->children == nullptr) {
        error = erlang::nif::error(env, "invalid ArrowSchema, schema->children == nullptr while schema->n_children > 0");
        return 1;
    }
    if (values->n_children > 0 && values->children == nullptr) {
        error =  erlang::nif::error(env, "invalid ArrowArray, values->children == nullptr while values->n_children > 0");
        return 1;
    }
    if (values->n_children != schema->n_children) {
        error =  erlang::nif::error(env, "invalid ArrowArray or ArrowSchema, values->n_children != schema->n_children");
        return 1;
    }

    children.resize(values->n_children);
    for (int64_t child_i = 0; child_i < values->n_children; child_i++) {
        struct ArrowSchema * child_schema = schema->children[child_i];
        struct ArrowArray * child_values = values->children[child_i];
        std::vector<ERL_NIF_TERM> childrens;
        ERL_NIF_TERM child_type;
        ERL_NIF_TERM child_metadata;
        if (arrow_array_to_nif_term(env, child_schema, child_values, offset, count, level + 1, childrens, child_type, child_metadata, error) == 1) {
            return 1;
        }

        if (childrens.size() == 1) {
            children[child_i] = childrens[0];
        } else {
            bool nullable = (child_schema->flags & ARROW_FLAG_NULLABLE) || (child_values->null_count > 0);
            if (enif_is_identical(childrens[1], kAtomNil)) {
                children[child_i] = kAtomNil;
            } else {
                children[child_i] = make_adbc_column(env, child_schema, values, childrens[0], child_type, nullable, child_metadata, childrens[1]);
            }
        }
    }
    return 0;
}

int get_arrow_struct(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error) {
    return get_arrow_struct(env, schema, values, 0, -1, level, children, error);
}

int get_arrow_dictionary(ErlNifEnv *env, 
    struct ArrowSchema * index_schema, struct ArrowArray * index_array,
    struct ArrowSchema * value_schema, struct ArrowArray * value_array,
    int64_t offset, int64_t count, uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error) {
    std::vector<ERL_NIF_TERM> keys, values;
    ERL_NIF_TERM index_type, index_metadata;
    ERL_NIF_TERM value_type, value_metadata;
    if (arrow_array_to_nif_term(env, index_schema, index_array, offset, count, level + 1, keys, index_type, index_metadata, error, true) == 1) {
        return 1;
    }
    if (arrow_array_to_nif_term(env, value_schema, value_array, offset, count, level + 1, values, value_type, value_metadata, error, false) == 1) {
        return 1;
    }

    bool index_nullable = index_schema->flags & ARROW_FLAG_NULLABLE || index_array->null_count > 0;
    bool value_nullable = value_schema->flags & ARROW_FLAG_NULLABLE || value_array->null_count > 0;
    ERL_NIF_TERM data;
    ERL_NIF_TERM data_keys[] = {
        kAtomKey,
        kAtomValue
    };
    ERL_NIF_TERM data_values[] = {
        make_adbc_column(env, index_schema, index_array, keys[0], index_type, index_nullable, index_metadata, keys[1]),
        make_adbc_column(env, value_schema, value_array, values[0], value_type, value_nullable, value_metadata, values[1])
    };
    enif_make_map_from_arrays(env, data_keys, data_values, (unsigned)(sizeof(data_keys)/sizeof(data_keys[0])), &data);
    children.push_back(data);
    return 0;
}

int get_arrow_dictionary(ErlNifEnv *env, 
    struct ArrowSchema * index_schema, struct ArrowArray * index_array,
    struct ArrowSchema * value_schema, struct ArrowArray * value_array,
    uint64_t level, std::vector<ERL_NIF_TERM> &children, ERL_NIF_TERM &error) {
    return get_arrow_dictionary(env, index_schema, index_array, value_schema, value_array, 0, -1, level, children, error);
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
    if (strcmp("entries", entries_schema->name) != 0) {
        return erlang::nif::error(env, "invalid ArrowSchema (map), its single child is not named entries");
    }
    if (entries_schema->n_children != 2) {
        return erlang::nif::error(env, "invalid ArrowSchema (map), its entries n_children != 2");
    }

    struct ArrowSchema * key_schema, * value_schema;
    struct ArrowArray * key_values, * value_values;
    if (strcmp("key", entries_schema->children[0]->name) == 0 && strcmp("value", entries_schema->children[1]->name) == 0) {
        key_schema = entries_schema->children[0];
        key_values = entries_values->children[0];
        value_schema = entries_schema->children[1];
        value_values = entries_values->children[1];
    } else if (strcmp("key", entries_schema->children[1]->name) == 0 && strcmp("value", entries_schema->children[0]->name) == 0) {
        key_schema = entries_schema->children[1];
        key_values = entries_values->children[1];
        value_schema = entries_schema->children[0];
        value_values = entries_values->children[0];
    } else {
        return erlang::nif::error(env, "invalid map entries, key or value or both are missing");
    }

    std::vector<ERL_NIF_TERM> nif_keys, nif_values;
    ERL_NIF_TERM key_type, key_metadata;
    ERL_NIF_TERM value_type, value_metadata;
    if (arrow_array_to_nif_term(env, key_schema, key_values, offset, count, level + 1, nif_keys, key_type, key_metadata, error) == 1) {
        return erlang::nif::error(env, "failed to get map keys");
    }
    if (arrow_array_to_nif_term(env, value_schema, value_values, offset, count, level + 1, nif_values, value_type, value_metadata, error) == 1) {
        return erlang::nif::error(env, "failed to get map values");
    }
    
    ERL_NIF_TERM map_keys[] = {
        kAtomKey,
        kAtomValue
    };
    ERL_NIF_TERM map_values[] = {
        make_adbc_column(env, key_schema, key_values, nif_keys[0], key_type, key_schema->flags & ARROW_FLAG_NULLABLE, key_metadata, nif_keys[1]),
        make_adbc_column(env, value_schema, value_values, nif_values[0], value_type, value_schema->flags & ARROW_FLAG_NULLABLE, value_metadata, nif_values[1])
    };

    enif_make_map_from_arrays(env, map_keys, map_values, (unsigned)(sizeof(map_keys)/sizeof(map_keys[0])), &map_out);
    return map_out;
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

        ERL_NIF_TERM field_type;
        ERL_NIF_TERM field_metadata;
        if (arrow_array_to_nif_term(env, field_schema, field_array, child_offset, 1, level + 1, field_values, field_type, field_metadata, error) == 1) {
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

        ERL_NIF_TERM field_type;
        // todo: use field_metadata
        ERL_NIF_TERM field_metadata;
        if (arrow_array_to_nif_term(env, field_schema, field_array, child_i, 1, level + 1, field_values, field_type, field_metadata, error) == 1) {
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

ERL_NIF_TERM get_arrow_run_end_encoded(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level) {
    ERL_NIF_TERM error{};
    if (schema->n_children != 2 || values->n_children != 2) {
        return erlang::nif::error(env, "invalid ArrowSchema (run_end_encoded), schema->n_children != 2 || values->n_children != 2");
    }
    if (schema->children == nullptr || values->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray (run_end_encoded), schema->children == nullptr || values->children == nullptr");
    }
    if (strcmp("run_ends", schema->children[0]->name) != 0) {
        return erlang::nif::error(env, "invalid ArrowSchema (run_end_encoded), its first child is not named run_ends");
    }
    if (strcmp("values", schema->children[1]->name) != 0) {
        return erlang::nif::error(env, "invalid ArrowSchema (run_end_encoded), its second child is not named values");
    }

    std::vector<ERL_NIF_TERM> children(2);
    for (int64_t child_i = 0; child_i < 2; child_i++) {
        std::vector<ERL_NIF_TERM> childrens;
        ERL_NIF_TERM child_type;
        ERL_NIF_TERM child_metadata;
        if (arrow_array_to_nif_term(env, schema->children[child_i], values->children[child_i], 0, -1, level + 1, childrens, child_type, child_metadata, error) == 1) {
            return 1;
        }

        if (childrens.size() == 1) {
            children[child_i] = childrens[0];
        } else {
            bool nullable = child_i == 1 && ((schema->children[child_i]->flags & ARROW_FLAG_NULLABLE) || (values->children[child_i]->null_count > 0));
            if (enif_is_identical(childrens[1], kAtomNil)) {
                children[child_i] = kAtomNil;
            } else {
                children[child_i] = make_adbc_column(env, schema, values, childrens[0], child_type, nullable, child_metadata, childrens[1]);
            }
        }
    }
    
    ERL_NIF_TERM run_ends_keys[] = { kAtomRunEnds, kAtomValues };
    ERL_NIF_TERM run_ends_values[] = { children[0], children[1] };
    ERL_NIF_TERM run_ends_data;
    // only fail if there are duplicated keys
    // so we don't need to check the return value
    enif_make_map_from_arrays(env, run_ends_keys, run_ends_values, 2, &run_ends_data);
    return run_ends_data;
}

ERL_NIF_TERM get_arrow_run_end_encoded(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level) {
    return get_arrow_run_end_encoded(env, schema, values, 0, -1, level);
}

ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level, ArrowType list_type, unsigned n_items) {
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
    if (list_type != NANOARROW_TYPE_LIST && list_type != NANOARROW_TYPE_LARGE_LIST && list_type != NANOARROW_TYPE_FIXED_SIZE_LIST) {
        return erlang::nif::error(env, "invalid ArrowArray (list), internal error: unexpected list type");
    }

    constexpr int64_t bitmap_buffer_index = 0;
    const uint8_t * bitmap_buffer = (const uint8_t *)values->buffers[bitmap_buffer_index];
    struct ArrowSchema * items_schema = schema->children[0];
    struct ArrowArray * items_values = values->children[0];
    if (strcmp("item", items_schema->name) != 0) {
        return erlang::nif::error(env, "invalid ArrowSchema (list), its single child is not named item");
    }

    std::vector<ERL_NIF_TERM> children;
    if (list_type == NANOARROW_TYPE_LIST || list_type == NANOARROW_TYPE_LARGE_LIST) {
        constexpr int64_t offset_buffer_index = 1;
        const void * offsets_ptr = (const void *)values->buffers[offset_buffer_index];
        if (offsets_ptr == nullptr) return erlang::nif::error(env, "invalid ArrowArray (list), offsets == nullptr");
        if (count == -1) count = values->length;
        if (count > values->length) count = values->length - offset;
        bool items_nullable = (schema->flags & ARROW_FLAG_NULLABLE) || (values->null_count > 0);

        int has_error = 0;
        auto get_list_children_with_offsets = [&](auto offsets) -> void {
            for (int64_t i = offset; i < offset + count; i++) {
                if (bitmap_buffer && items_nullable) {
                    uint8_t vbyte = bitmap_buffer[i / 8];
                    if (!(vbyte & (1 << (i % 8)))) {
                        children.emplace_back(kAtomNil);
                        continue;
                    }
                }

                std::vector<ERL_NIF_TERM> childrens;
                ERL_NIF_TERM children_type;
                ERL_NIF_TERM children_metadata;
                if (arrow_array_to_nif_term(env, items_schema, items_values, offsets[i], offsets[i+1] - offsets[i], level + 1, childrens, children_type, children_metadata, error) == 1) {
                    has_error = 1;
                    return;
                }

                if (childrens.size() == 1) {
                    children.emplace_back(childrens[0]);
                } else {
                    bool children_nullable = (schema->flags & ARROW_FLAG_NULLABLE) || (values->null_count > 0);
                    if (enif_is_identical(childrens[1], kAtomNil)) {
                        children.emplace_back(kAtomNil);
                    } else {
                        children.emplace_back(make_adbc_column(env, schema, values, childrens[0], children_type, children_nullable, children_metadata, childrens[1]));
                    }
                }
            }
        };

        if (list_type == NANOARROW_TYPE_LIST) {
            get_list_children_with_offsets((const int32_t *)offsets_ptr);
            if (has_error) return error;
        } else if (list_type == NANOARROW_TYPE_LARGE_LIST) {
            get_list_children_with_offsets((const int64_t *)offsets_ptr);
            if (has_error) return error;
        }
    } else {
        // NANOARROW_TYPE_FIXED_SIZE_LIST
        if (count == -1) count = values->length;
        if (count > values->length) count = values->length - offset;
        bool items_nullable = (schema->flags & ARROW_FLAG_NULLABLE) || (values->null_count > 0);
        for (int64_t child_i = offset; child_i < offset + count; child_i++) {
            if (bitmap_buffer && items_nullable) {
                uint8_t vbyte = bitmap_buffer[child_i / 8];
                if (!(vbyte & (1 << (child_i % 8)))) {
                    children.emplace_back(kAtomNil);
                    continue;
                }
            }

            std::vector<ERL_NIF_TERM> childrens;
            ERL_NIF_TERM children_type;
            ERL_NIF_TERM children_metadata;
            if (arrow_array_to_nif_term(env, items_schema, items_values, child_i * n_items, n_items, level + 1, childrens, children_type, children_metadata, error)) {
                return error;
            }
            if (childrens.size() == 1) {
                children.emplace_back(childrens[0]);
            } else {
                bool children_nullable = (schema->flags & ARROW_FLAG_NULLABLE) || (values->null_count > 0);
                if (enif_is_identical(childrens[1], kAtomNil)) {
                    children.emplace_back(kAtomNil);
                } else {
                    children.emplace_back(make_adbc_column(env, schema, values, childrens[0], children_type, children_nullable, children_metadata, childrens[1]));
                }
            }
        }
    }
    return enif_make_list_from_array(env, children.data(), (unsigned)children.size());
}

ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, ArrowType list_type, unsigned n_items) {
    return get_arrow_array_list_children(env, schema, values, 0, -1, level, list_type, n_items);
}

ERL_NIF_TERM get_arrow_array_list_view(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, uint64_t level, ArrowType list_type) {
    ERL_NIF_TERM error{};
    if (schema->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowSchema (list view), schema->children == nullptr");
    }
    if (schema->n_children != 1) {
        return erlang::nif::error(env, "invalid ArrowSchema (list view), schema->n_children != 1");
    }
    if (values->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray (list view), values->children == nullptr");
    }
    if (values->n_children != 1) {
        return erlang::nif::error(env, "invalid ArrowArray (list view), values->n_children != 1");
    }
    if (list_type != NANOARROW_TYPE_LIST && list_type != NANOARROW_TYPE_LARGE_LIST) {
        return erlang::nif::error(env, "invalid ArrowArray (list view), internal error: unexpected list type");
    }

    constexpr int64_t bitmap_buffer_index = 0;
    constexpr int64_t offsets_buffer_index = 1;
    constexpr int64_t sizes_buffer_index = 2;
    const uint8_t * bitmap_buffer = (const uint8_t *)values->buffers[bitmap_buffer_index];
    const void * offsets_ptr = (const void *)values->buffers[offsets_buffer_index];
    const void * sizes_ptr = (const void *)values->buffers[sizes_buffer_index];
    bool items_nullable = (schema->flags & ARROW_FLAG_NULLABLE) || (values->null_count > 0);
    if (items_nullable && bitmap_buffer == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray (list view), items_nullable == true but bitmap_buffer == nullptr");
    }
    if (offsets_ptr == nullptr) return erlang::nif::error(env, "invalid ArrowArray (list view), offsets == nullptr");
    if (sizes_ptr == nullptr) return erlang::nif::error(env, "invalid ArrowArray (list view), sizes == nullptr");

    struct ArrowSchema * items_schema = schema->children[0];
    struct ArrowArray * items_values = values->children[0];
    if (strcmp("item", items_schema->name) != 0) {
        return erlang::nif::error(env, "invalid ArrowSchema (list), its single child is not named item");
    }
    if (count == -1) count = values->length;
    if (count > values->length) count = values->length - offset;

    auto bool_to_atom = [](ErlNifEnv *, bool b) -> ERL_NIF_TERM {
        return b ? kAtomTrue : kAtomFalse;
    };

    std::vector<ERL_NIF_TERM> childrens;
    ERL_NIF_TERM validity_term, offsets_term, sizes_term, values_term;
    ERL_NIF_TERM children_type;
    ERL_NIF_TERM children_metadata;
    // according to the Arrow spec, the bitmap buffer is not required for the child values
    // and this `buffer[0]` could be a random memory address, so we simply set it to nullptr here
    items_values->buffers[0] = nullptr;
    if (arrow_array_to_nif_term(env, items_schema, items_values, 0, -1, level + 1, childrens, children_type, children_metadata, error)) {
        return error;
    }
    items_values->buffers[0] = bitmap_buffer;

    if (childrens.size() == 1) {
        values_term = childrens[0];
    } else {
        if (enif_is_identical(childrens[1], kAtomNil)) {
            values_term = kAtomNil;
        } else {
            bool nullable = items_schema->flags & ARROW_FLAG_NULLABLE || items_values->null_count > 0;
            values_term = make_adbc_column(env, schema, values, childrens[0], children_type, nullable, children_metadata, childrens[1]);
        }
    }

    if (list_type == NANOARROW_TYPE_LIST) {
        validity_term = bit_boolean_from_buffer(env, offset, count, (const uint8_t *)bitmap_buffer, bool_to_atom);
        offsets_term = values_from_buffer(env, offset, count, NULL, (const int32_t *)offsets_ptr, enif_make_int);
        sizes_term = values_from_buffer(env, offset, count, NULL, (const int32_t *)sizes_ptr, enif_make_int);
    } else {
        validity_term = bit_boolean_from_buffer(env, offset, count, (const uint8_t *)bitmap_buffer, bool_to_atom);
        offsets_term = values_from_buffer(env, offset, count, NULL, (const int64_t *)offsets_ptr, enif_make_int64);
        sizes_term = values_from_buffer(env, offset, count, NULL, (const int64_t *)sizes_ptr, enif_make_int64);
    }
    ERL_NIF_TERM list_view_keys[] = { kAtomValidity, kAtomOffsets, kAtomSizes, kAtomValues };
    ERL_NIF_TERM list_view_values[] = { validity_term, offsets_term, sizes_term, values_term };
    ERL_NIF_TERM map_out;
    // only fail if there are duplicated keys
    // so we don't need to check the return value
    enif_make_map_from_arrays(env, list_view_keys, list_view_values, 4, &map_out);
    return map_out;
}

ERL_NIF_TERM get_arrow_array_list_view(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, ArrowType list_type) {
    return get_arrow_array_list_view(env, schema, values, 0, -1, level, list_type);
}

int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, int64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &term_type, ERL_NIF_TERM &arrow_metadata, ERL_NIF_TERM &error, bool skip_dictionary_check) {
    if (schema == nullptr) {
        error = erlang::nif::error(env, "invalid ArrowSchema (nullptr) when invoking next");
        return 1;
    }
    if (values == nullptr) {
        error = erlang::nif::error(env, "invalid ArrowArray (nullptr) when invoking next");
        return 1;
    }

    char err_msg_buf[256] = { '\0' };
    const char* format = schema->format ? schema->format : "";
    const char* name = schema->name ? schema->name : "";
    ERL_NIF_TERM current_term{}, children_term{};
    size_t format_len = strlen(format);

    term_type = kAtomNil;
    std::vector<ERL_NIF_TERM> children;

    constexpr int64_t bitmap_buffer_index = 0;
    int64_t data_buffer_index = 1;
    int64_t offset_buffer_index = 2;

    NANOARROW_RETURN_NOT_OK(arrow_metadata_to_nif_term(env, schema->metadata, &arrow_metadata));

    if (!skip_dictionary_check) {
        if (schema->dictionary != nullptr && values->dictionary != nullptr) {
            // NANOARROW_TYPE_DICTIONARY
            //
            // For dictionary-encoded arrays, the ArrowSchema.format string 
            // encodes the index type. The dictionary value type can be read 
            // from the ArrowSchema.dictionary structure.
            //
            // The same holds for ArrowArray structure: while the parent 
            // structure points to the index data, the ArrowArray.dictionary 
            // points to the dictionary values array.
            term_type = kAdbcColumnTypeDictionary;

            if (get_arrow_dictionary(env, schema, values, schema->dictionary, values->dictionary, offset, count, level, children, error) == 1) {
                return 1;
            }
            out_terms.emplace_back(erlang::nif::make_binary(env, name));
            out_terms.emplace_back(children[0]);
            return 0;
        }
        if (schema->dictionary != nullptr && values->dictionary == nullptr) {
            error = erlang::nif::error(env, "invalid ArrowArray (dictionary), values->dictionary == nullptr");
            return 1;
        }
        if (schema->dictionary == nullptr && values->dictionary != nullptr) {
            error = erlang::nif::error(env, "invalid ArrowArray (dictionary), schema->dictionary == nullptr");
            return 1;
        }
    }
    
    bool is_struct = false;
    bool format_processed = true;
    if (format_len == 1) {
        if (format[0] == 'n') {
            term_type = kAtomNil;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
            std::vector<ERL_NIF_TERM> nils(count);
            for (int64_t i = offset; i < offset + count; i++) {
                nils.push_back(kAtomNil);
            }
            current_term = kAtomNil;
        } else if (format[0] == 'l') {
            // NANOARROW_TYPE_INT64
            using value_type = int64_t;
            term_type = kAdbcColumnTypeS64;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            term_type = kAdbcColumnTypeS8;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            term_type = kAdbcColumnTypeS16;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            term_type = kAdbcColumnTypeS32;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            term_type = kAdbcColumnTypeU64;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            term_type = kAdbcColumnTypeU8;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            term_type = kAdbcColumnTypeU16;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            term_type = kAdbcColumnTypeU32;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
        } else if (format[0] == 'e') {
            // NANOARROW_TYPE_HALF_FLOAT
            using value_type = uint16_t;
            term_type = kAdbcColumnTypeF16;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=e), values->n_buffers != 2");
                return 1;
            }
            current_term = values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index],
                [](ErlNifEnv *env, const uint16_t u16) -> ERL_NIF_TERM {
                    float val = float16_to_float(u16);
                    if (std::isnan(val)) {
                        return kAtomNaN;
                    } else if (std::isinf(val)) {
                        if (val > 0) {
                            return kAtomInfinity;
                        } else {
                            return kAtomNegInfinity;
                        }
                    } else {
                        return enif_make_double(env, val);
                    }
                }
            );
        } else if (format[0] == 'f') {
            // NANOARROW_TYPE_FLOAT
            using value_type = float;
            term_type = kAdbcColumnTypeF32;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
                [](ErlNifEnv *env, double val) -> ERL_NIF_TERM {
                    if (std::isnan(val)) {
                        return kAtomNaN;
                    } else if (std::isinf(val)) {
                        if (val > 0) {
                            return kAtomInfinity;
                        } else {
                            return kAtomNegInfinity;
                        }
                    } else {
                        return enif_make_double(env, val);
                    }
                }
            );
        } else if (format[0] == 'g') {
            // NANOARROW_TYPE_DOUBLE
            using value_type = double;
            term_type = kAdbcColumnTypeF64;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
                [](ErlNifEnv *env, double val) -> ERL_NIF_TERM {
                    if (std::isnan(val)) {
                        return kAtomNaN;
                    } else if (std::isinf(val)) {
                        if (val > 0) {
                            return kAtomInfinity;
                        } else {
                            return kAtomNegInfinity;
                        }
                    } else {
                        return enif_make_double(env, val);
                    }
                }
            );
        } else if (format[0] == 'b') {
            // NANOARROW_TYPE_BOOL
            using value_type = bool;
            term_type = kAdbcColumnTypeBool;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
            if (values->n_buffers != 2) {
                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=b), values->n_buffers != 2");
                return 1;
            }
            current_term = boolean_values_from_buffer(
                env,
                offset,
                count,
                (const uint8_t *)values->buffers[bitmap_buffer_index],
                (const value_type *)values->buffers[data_buffer_index]
            );
        } else if (format[0] == 'u' || format[0] == 'z') {
            // NANOARROW_TYPE_BINARY
            // NANOARROW_TYPE_STRING
            if (format[0] == 'z') {
                term_type = kAdbcColumnTypeBinary;
            } else {
                term_type = kAdbcColumnTypeString;
            }
            offset_buffer_index = 1;
            data_buffer_index = 2;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            if (format[0] == 'Z') {
                term_type = kAdbcColumnTypeLargeBinary;
            } else {
                term_type = kAdbcColumnTypeLargeString;
            }
            offset_buffer_index = 1;
            data_buffer_index = 2;
            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
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
            is_struct = true;
            term_type = kAdbcColumnTypeStruct;

            if (count == -1) count = values->length;
            if (count > values->length) count = values->length - offset;
            if (get_arrow_struct(env, schema, values, offset, count, level, children, error) == 1) {
                return 1;
            }
            children_term = enif_make_list_from_array(env, children.data(), (unsigned)children.size());
        } else if (strncmp("+r", format, 2) == 0) {
            // NANOARROW_TYPE_RUN_END_ENCODED (maybe in nanoarrow v0.6.0)
            // https://github.com/apache/arrow-nanoarrow/pull/507
            term_type = kAdbcColumnTypeRunEndEncoded;
            children_term = get_arrow_run_end_encoded(env, schema, values, offset, count, level);
        } else if (strncmp("+m", format, 2) == 0) {
            // NANOARROW_TYPE_MAP
            term_type = kAdbcColumnTypeMap;
            children_term = get_arrow_array_map_children(env, schema, values, offset, count, level);
        } else if (strncmp("+l", format, 2) == 0) {
            // NANOARROW_TYPE_LIST
            term_type = kAdbcColumnTypeList;
            children_term = get_arrow_array_list_children(env, schema, values, offset, count, level, NANOARROW_TYPE_LIST);
        } else if (strncmp("+L", format, 2) == 0) {
            // NANOARROW_TYPE_LARGE_LIST
            term_type = kAdbcColumnTypeLargeList;
            children_term = get_arrow_array_list_children(env, schema, values, offset, count, level, NANOARROW_TYPE_LARGE_LIST);
        } else {
            format_processed = false;
        }
    } else if (format_len >= 3) {
        // handle all formats that start with `t` (temporal types)
        if (format[0] == 't') {
            // formats for timestamp can be 3 or more
            // lets handle timestamps after this `if` block
            // (tdX, ttX, tDX and tiX are in this block)
            if (format_len == 3) {
                if (format[1] == 'd') {
                    // possible format strings:
                    // tdD - date32 [days]
                    // tdm - date64 [milliseconds]
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
                            term_type = kAdbcColumnTypeDate32;
                            if (count == -1) count = values->length;
                            if (count > values->length) count = values->length - offset;
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
                            term_type = kAdbcColumnTypeDate64;
                            if (count == -1) count = values->length;
                            if (count > values->length) count = values->length - offset;
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
                } else if (format[1] == 't') {
                    // possible format strings:
                    // tts - time32 [seconds]
                    // ttm - time32 [milliseconds]
                    // ttu - time64 [microseconds]
                    // ttn - time64 [nanoseconds]
                    uint64_t unit;
                    uint8_t us_precision;
                    switch (format[2]) {
                        case 's': // seconds
                            // NANOARROW_TYPE_TIME32
                            unit = 1000000000;
                            us_precision = 0;
                            term_type = kAdbcColumnTypeTime32Seconds;
                            break;
                        case 'm': // milliseconds
                            // NANOARROW_TYPE_TIME32
                            unit = 1000000;
                            us_precision = 3;
                            term_type = kAdbcColumnTypeTime32Milliseconds;
                            break;
                        case 'u': // microseconds
                            // NANOARROW_TYPE_TIME64
                            unit = 1000;
                            us_precision = 6;
                            term_type = kAdbcColumnTypeTime64Microseconds;
                            break;
                        case 'n': // nanoseconds
                            // NANOARROW_TYPE_TIME64
                            unit = 1;
                            us_precision = 6;
                            term_type = kAdbcColumnTypeTime64Nanoseconds;
                            break;
                        default:
                            format_processed = false;
                    }

                    if (format_processed) {
                        using value_type = uint64_t;
                        if (count == -1) count = values->length;
                        if (count > values->length) count = values->length - offset;
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
                } else if (format[1] == 'D') {
                    // possible format strings:
                    // tDs - duration [seconds]
                    // tDm - duration [milliseconds]
                    // tDu - duration [microseconds]
                    // tDn - duration [nanoseconds]

                    // NANOARROW_TYPE_DURATION
                    switch (format[2]) {
                        case 's': // seconds
                            term_type = kAdbcColumnTypeDurationSeconds;
                            break;
                        case 'm': // milliseconds
                            term_type = kAdbcColumnTypeDurationMilliseconds;
                            break;
                        case 'u': // microseconds
                            term_type = kAdbcColumnTypeDurationMicroseconds;
                            break;
                        case 'n': // nanoseconds
                            term_type = kAdbcColumnTypeDurationNanoseconds;
                            break;
                        default:
                            format_processed = false;
                    }

                    if (format_processed) {
                        using value_type = int64_t;
                        if (count == -1) count = values->length;
                        if (count > values->length) count = values->length - offset;
                        if (values->n_buffers != 2) {
                            error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=tD), values->n_buffers != 2");
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
                    }
                } else if (format[1] == 'i') {
                    // possible format strings:
                    // tiM - interval [months]
                    // tiD - interval [days, time]
                    // tin - interval [month, day, nanoseconds]

                    // NANOARROW_TYPE_INTERVAL
                    switch (format[2]) {
                        case 'M': // months
                            term_type = kAdbcColumnTypeIntervalMonth;
                            break;
                        case 'D': // days, time
                            term_type = kAdbcColumnTypeIntervalDayTime;
                            break;
                        case 'n': // month, day, nanoseconds
                            term_type = kAdbcColumnTypeIntervalMonthDayNano;
                            break;
                        default:
                            format_processed = false;
                    }

                    if (format_processed) {
                        if (format[2] == 'M') {
                            using value_type = int32_t;
                            if (count == -1) count = values->length;
                            if (count > values->length) count = values->length - offset;
                            if (values->n_buffers != 2) {
                                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=tiM), values->n_buffers != 2");
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
                        } else if (format[2] == 'D') {
                            using value_type = int64_t;
                            if (count == -1) count = values->length;
                            if (count > values->length) count = values->length - offset;
                            if (values->n_buffers != 2) {
                                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=tiD), values->n_buffers != 2");
                                return 1;
                            }

                            current_term = values_from_buffer(
                                env,
                                offset,
                                count,
                                (const uint8_t *)values->buffers[bitmap_buffer_index],
                                (const value_type *)values->buffers[data_buffer_index],
                                [](ErlNifEnv *env, int64_t val) -> ERL_NIF_TERM {
                                    int32_t days = val & 0xFFFFFFFF;
                                    int32_t time = val >> 32;
                                    return enif_make_tuple2(env, enif_make_int(env, days), enif_make_int(env, time));
                                }
                            );
                        } else {
                            using value_type = struct {
                                int64_t data[2];
                            };
                            if (count == -1) count = values->length;
                            if (count > values->length) count = values->length - offset;
                            if (values->n_buffers != 2) {
                                error = erlang::nif::error(env, "invalid n_buffers value for ArrowArray (format=tin), values->n_buffers != 2");
                                return 1;
                            }

                            current_term = values_from_buffer(
                                env,
                                offset,
                                count,
                                (const uint8_t *)values->buffers[bitmap_buffer_index],
                                (const value_type *)values->buffers[data_buffer_index],
                                [](ErlNifEnv *env, value_type val) -> ERL_NIF_TERM {
                                    int32_t months = val.data[0] & 0xFFFFFFFF;
                                    int32_t days = val.data[0] >> 32;
                                    return enif_make_tuple3(env, enif_make_int64(env, months), enif_make_int64(env, days), enif_make_int64(env, val.data[1]));
                                }
                            );
                        }
                    }
                } else {
                    format_processed = false;
                }
            } else if (format_len >= 4 && format[1] == 's' && format[3] == ':') {
                // according to the arrow spec:
                //   The timezone string is appended as-is after the colon character :, 
                //   without any quotes. If the timezone is empty, the colon : must still be included.
                // so the format length for timestamps must be >= 4
            
                // possible format strings:
                // tss: - timestamp [seconds]
                // tsm: - timestamp [milliseconds]
                // tsu: - timestamp [microseconds]
                // tsn: - timestamp [nanoseconds]
                //
                // if there're any timezone infomation
                // it should be in the format like `tsu:timezone`

                // NANOARROW_TYPE_TIMESTAMP
                uint64_t unit;
                uint8_t us_precision;
                ERL_NIF_TERM term_unit;
                ERL_NIF_TERM term_timezone = kAtomNil;
                switch (format[2]) {
                    case 's': // seconds
                        unit = 1000000000;
                        us_precision = 0;
                        term_unit = kAtomSeconds;
                        break;
                    case 'm': // milliseconds
                        unit = 1000000;
                        us_precision = 3;
                        term_unit = kAtomMilliseconds;
                        break;
                    case 'u': // microseconds
                        unit = 1000;
                        us_precision = 6;
                        term_unit = kAtomMicroseconds;
                        break;
                    case 'n': // nanoseconds
                        unit = 1;
                        us_precision = 6;
                        term_unit = kAtomNanoseconds;
                        break;
                    default:
                        format_processed = false;
                }

                if (format_processed) {
                    if (format_len > 4 && format[3] == ':') {
                        std::string timezone(&format[4]);
                        term_timezone = erlang::nif::make_binary(env, timezone);
                    }
                    term_type = enif_make_tuple3(env, kAtomTimestamp, term_unit, term_timezone);
                    
                    using value_type = int64_t;
                    if (count == -1) count = values->length;
                    if (count > values->length) count = values->length - offset;
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
                        [unit, us_precision, naive_dt_module, calendar_iso, &keys](ErlNifEnv *env, int64_t val) -> ERL_NIF_TERM {
                            // Elixir only supports microsecond precision
                            int64_t us = val * unit / 1000;
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
            if (format_len == 3 && strncmp("+vl", format, 3) == 0) {
                // NANOARROW_TYPE_LIST(VIEW)
                term_type = kAdbcColumnTypeListView;
                children_term = get_arrow_array_list_view(env, schema, values, offset, count, level, NANOARROW_TYPE_LIST);
            } else if (format_len == 3 && strncmp("+vL", format, 3) == 0) {
                // NANOARROW_TYPE_LARGE_LIST(VIEW)
                term_type = kAdbcColumnTypeLargeListView;
                children_term = get_arrow_array_list_view(env, schema, values, offset, count, level, NANOARROW_TYPE_LARGE_LIST);
            } else if (strncmp("+w:", format, 3) == 0) {
                // NANOARROW_TYPE_FIXED_SIZE_LIST
                unsigned n_items = 0;
                for (size_t i = 3; i < format_len; i++) {
                    n_items = n_items * 10 + (format[i] - '0');
                }
                term_type = kAdbcColumnTypeFixedSizeList(n_items);
                children_term = get_arrow_array_list_children(env, schema, values, offset, count, level, NANOARROW_TYPE_FIXED_SIZE_LIST, n_items);
            } else if (strncmp("w:", format, 2) == 0) {
                // NANOARROW_TYPE_FIXED_SIZE_BINARY
                if (count == -1) count = values->length;
                if (count > values->length) count = values->length - offset;
                if (values->n_buffers != 2) {
                    snprintf(err_msg_buf, 255, "invalid n_buffers value for ArrowArray (format=%s), values->n_buffers != 2", schema->format);
                    error = erlang::nif::error(env, erlang::nif::make_binary(env, err_msg_buf));
                    return 1;
                }
                size_t nbytes = 0;
                for (size_t i = 2; i < format_len; i++) {
                    nbytes = nbytes * 10 + (format[i] - '0');
                }
                term_type = kAdbcColumnTypeFixedSizeBinary(nbytes);
                current_term = fixed_size_binary_from_buffer(
                    env,
                    offset,
                    count,
                    nbytes,
                    (const uint8_t *)values->buffers[bitmap_buffer_index],
                    (const uint8_t *)values->buffers[data_buffer_index],
                    [&](ErlNifEnv *env, const uint8_t * val) -> ERL_NIF_TERM {
                        return erlang::nif::make_binary(env, (const char *)val, nbytes);
                    }
                );
            } else if (format_len > 4 && (strncmp("+ud:", format, 4) == 0)) {
                // NANOARROW_TYPE_DENSE_UNION
                term_type = kAdbcColumnTypeDenseUnion;
                children_term = get_arrow_array_dense_union_children(env, schema, values, offset, count, level);
            } else if (format_len > 4 && (strncmp("+us:", format, 4) == 0)) {
                // NANOARROW_TYPE_SPARSE_UNION
                term_type = kAdbcColumnTypeSparseUnion;
                children_term = get_arrow_array_sparse_union_children(env, schema, values, offset, count, level);
            } else if (strncmp("d:", format, 2) == 0) {
                // NANOARROW_TYPE_DECIMAL128
                // NANOARROW_TYPE_DECIMAL256
                //
                // format should match `d:P,S[,N]`
                // where P is precision, S is scale, N is bits
                // N is optional and defaults to 128
                int precision = 0;
                int scale = 0;
                int bits = 128;
                int * d[3] = {&precision, &scale, &bits};
                int index = 0;
                for (size_t i = 2; i < format_len; i++) {
                    if (format[i] == ',') {
                        if (index < 2) {
                            index++;
                        } else {
                            format_processed = false;
                            break;
                        }
                        continue;
                    }

                    *d[index] = *d[index] * 10 + (format[i] - '0');
                }

                if (format_processed) {
                    term_type = kAdbcColumnTypeDecimal(bits, precision, scale);
                    if (count == -1) count = values->length;
                    if (count > values->length) count = values->length - offset;
                    if (values->n_buffers != 2) {
                        snprintf(err_msg_buf, 255, "invalid n_buffers value for ArrowArray (format=%s), values->n_buffers != 2", schema->format);
                        error = erlang::nif::error(env, erlang::nif::make_binary(env, err_msg_buf));
                        return 1;
                    }
                    current_term = fixed_size_binary_from_buffer(
                        env,
                        offset,
                        count,
                        bits / 8,
                        (const uint8_t *)values->buffers[bitmap_buffer_index],
                        (const uint8_t *)values->buffers[data_buffer_index],
                        [&](ErlNifEnv *env, const uint8_t * val) -> ERL_NIF_TERM {
                            return erlang::nif::make_binary(env, (const char *)val, bits / 8);
                        }
                    );
                }
            } else {
                format_processed = false;
            }
        }
    } else {
        format_processed = false;
    }

    if (!format_processed) {
        snprintf(err_msg_buf, sizeof(err_msg_buf)/sizeof(err_msg_buf[0]), "not yet implemented for format: `%s`", schema->format);
        error = erlang::nif::error(env, erlang::nif::make_binary(env, err_msg_buf));
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
        if (level > 0) {
            out_terms.emplace_back(erlang::nif::make_binary(env, name));
        }
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

int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &out_type, ERL_NIF_TERM &metadata, ERL_NIF_TERM &error, bool skip_dictionary_check) {
    return arrow_array_to_nif_term(env, schema, values, 0, -1, level, out_terms, out_type, metadata, error, skip_dictionary_check);
}

#endif  // ADBC_ARROW_ARRAY_HPP
