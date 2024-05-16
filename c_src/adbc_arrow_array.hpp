#ifndef ADBC_ARROW_ARRAY_HPP
#pragma once

#include <stdio.h>
#include <cstdbool>
#include <cstdint>
#include <vector>
#include <adbc.h>
#include <erl_nif.h>

static int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &value_type, ERL_NIF_TERM &metadata, ERL_NIF_TERM &error, bool *end_of_series = nullptr);
static int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, int64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &value_type, ERL_NIF_TERM &metadata, ERL_NIF_TERM &error, bool *end_of_series = nullptr);
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
        int64_t index = 0;
        for (int64_t i = element_offset; i < element_offset + element_count; i++) {
            uint8_t vbyte = validity_bitmap[i / 8];
            if (vbyte & (1 << (i % 8))) {
                values[i - element_offset] = value_to_nif(env, &value_buffer[element_bytes * index]);
                index++;
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
    if (offset < 0 || (offset != 0 && offset >= values->n_children)) {
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
        if (bitmap_buffer && ((schema->flags & ARROW_FLAG_NULLABLE) || (values->null_count > 0))) {
            uint8_t vbyte = bitmap_buffer[child_i / 8];
            if (!(vbyte & (1 << (child_i % 8)))) {
                children[child_i - offset] = kAtomNil;
                continue;
            }
        }
        struct ArrowSchema * child_schema = schema->children[child_i];
        struct ArrowArray * child_values = values->children[child_i];
        std::vector<ERL_NIF_TERM> childrens;
        ERL_NIF_TERM child_type;
        ERL_NIF_TERM child_metadata;
        if (arrow_array_to_nif_term(env, child_schema, child_values, level + 1, childrens, child_type, child_metadata, error) == 1) {
            return 1;
        }

        if (childrens.size() == 1) {
            children[child_i - offset] = childrens[0];
        } else {
            bool nullable = (child_schema->flags & ARROW_FLAG_NULLABLE) || (child_values->null_count > 0);
            children[child_i - offset] = make_adbc_column(env, childrens[0], child_type, nullable, child_metadata, childrens[1]);
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
        bool items_nullable = (items_schema->flags & ARROW_FLAG_NULLABLE) || (items_values->null_count > 0);
        for (int64_t child_i = offset; child_i < offset + count; child_i++) {
            if (bitmap_buffer && items_nullable) {
                uint8_t vbyte = bitmap_buffer[child_i / 8];
                if (!(vbyte & (1 << (child_i % 8)))) {
                    children[child_i - offset] = kAtomNil;
                    continue;
                }
            }
            struct ArrowSchema * item_schema = items_schema->children[child_i];
            struct ArrowArray * item_values = items_values->children[child_i];

            std::vector<ERL_NIF_TERM> childrens;
            ERL_NIF_TERM item_type;
            ERL_NIF_TERM item_metadata;
            if (arrow_array_to_nif_term(env, item_schema, item_values, level + 1, childrens, item_type, item_metadata, error) == 1) {
                return error;
            }

            if (childrens.size() == 1) {
                children[child_i - offset] = childrens[0];
            } else {
                bool children_nullable = (item_schema->flags & ARROW_FLAG_NULLABLE) || (item_values->null_count > 0);
                children[child_i - offset] = make_adbc_column(env, childrens[0], item_type, children_nullable, item_metadata, childrens[1]);
            }
        }
        return enif_make_list_from_array(env, children.data(), (unsigned)children.size());
    } else {
        children.resize(1);
        std::vector<ERL_NIF_TERM> childrens;
        ERL_NIF_TERM children_type;
        ERL_NIF_TERM children_metadata;
        if (arrow_array_to_nif_term(env, items_schema, items_values, offset, count, level + 1, childrens, children_type, children_metadata, error) == 1) {
            return error;
        }

        if (childrens.size() == 1) {
            children[0] = childrens[0];
            return enif_make_list_from_array(env, children.data(), (unsigned)children.size());
        } else {
            bool children_nullable = (schema->flags & ARROW_FLAG_NULLABLE) || (values->null_count > 0);
            ERL_NIF_TERM column[1];
            column[0] = make_adbc_column(env, childrens[0], children_type, children_nullable, children_metadata, childrens[1]);
            return enif_make_list_from_array(env, column, 1);
        }
    }
}

ERL_NIF_TERM get_arrow_array_list_children(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level) {
    return get_arrow_array_list_children(env, schema, values, 0, -1, level);
}

int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, int64_t offset, int64_t count, int64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &term_type, ERL_NIF_TERM &arrow_metadata, ERL_NIF_TERM &error, bool *end_of_series) {
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
    term_type = kAtomNil;
    arrow_metadata = kAtomNil;

    ERL_NIF_TERM current_term{}, children_term{};
    std::vector<ERL_NIF_TERM> children;

    constexpr int64_t bitmap_buffer_index = 0;
    int64_t data_buffer_index = 1;
    int64_t offset_buffer_index = 2;

    std::vector<ERL_NIF_TERM> metadata_keys, metadata_values;
    if (schema->metadata) {
        struct ArrowMetadataReader metadata_reader{};
        struct ArrowStringView key;
        struct ArrowStringView value;
        if (ArrowMetadataReaderInit(&metadata_reader, schema->metadata) == NANOARROW_OK) {
            while (ArrowMetadataReaderRead(&metadata_reader, &key, &value) == NANOARROW_OK) {
                // printf("key: %.*s, value: %.*s\n", (int)key.size_bytes, key.data, (int)value.size_bytes, value.data);
                metadata_keys.push_back(erlang::nif::make_binary(env, key.data, (size_t)key.size_bytes));
                metadata_values.push_back(erlang::nif::make_binary(env, value.data, (size_t)key.size_bytes));
            }
            if (metadata_keys.size() > 0) {
                enif_make_map_from_arrays(env, metadata_keys.data(), metadata_values.data(), (unsigned)metadata_keys.size(), &arrow_metadata);
            }
        }
    }

    bool is_struct = false;
    size_t format_len = strlen(format);
    bool format_processed = true;
    if (format_len == 1) {
        if (format[0] == 'l') {
            // NANOARROW_TYPE_INT64
            using value_type = int64_t;
            term_type = kAdbcColumnTypeI64;
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
            term_type = kAdbcColumnTypeI8;
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
            term_type = kAdbcColumnTypeI16;
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
            term_type = kAdbcColumnTypeI32;
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
            term_type = kAdbcColumnTypeU64;
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
            term_type = kAdbcColumnTypeU8;
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
            term_type = kAdbcColumnTypeU16;
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
            term_type = kAdbcColumnTypeU32;
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
            term_type = kAdbcColumnTypeF32;
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
            term_type = kAdbcColumnTypeF64;
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
            term_type = kAdbcColumnTypeBool;
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
            if (format[0] == 'z') {
                term_type = kAdbcColumnTypeBinary;
            } else {
                term_type = kAdbcColumnTypeString;
            }
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
            if (format[0] == 'Z') {
                term_type = kAdbcColumnTypeLargeBinary;
            } else {
                term_type = kAdbcColumnTypeLargeString;
            }
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
            term_type = kAdbcColumnTypeStruct;

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
            term_type = kAdbcColumnTypeMap;
            children_term = get_arrow_array_map_children(env, schema, values, offset, count, level);
        } else if (strncmp("+l", format, 2) == 0) {
            // NANOARROW_TYPE_LIST
            term_type = kAdbcColumnTypeList;
            children_term = get_arrow_array_list_children(env, schema, values, offset, count, level);
        } else if (strncmp("+L", format, 2) == 0) {
            // NANOARROW_TYPE_LARGE_LIST
            term_type = kAdbcColumnTypeLargeList;
            children_term = get_arrow_array_list_children(env, schema, values, offset, count, level);
        } else {
            format_processed = false;
        }
    } else if (format_len >= 3) {
        if (strncmp("+w:", format, 3) == 0) {
            // NANOARROW_TYPE_FIXED_SIZE_LIST
            term_type = kAdbcColumnTypeFixedSizeList;
            children_term = get_arrow_array_list_children(env, schema, values, offset, count, level);
        } else if (format_len > 3 && strncmp("w:", format, 2) == 0) {
            // NANOARROW_TYPE_FIXED_SIZE_BINARY
            if (get_arrow_array_children_as_list(env, schema, values, offset, count, level, children, error) == 1) {
                return 1;
            }
            term_type = kAdbcColumnTypeFixedSizeBinary;
            children_term = enif_make_list_from_array(env, children.data(), (unsigned)count);
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
                    term_type = kAdbcColumnTypeDate32;
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
                    term_type = kAdbcColumnTypeDate64;
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
                if (format_len > 4) {
                    std::string timezone(&format[4]);
                    term_timezone = erlang::nif::make_binary(env, timezone);
                }
                term_type = enif_make_tuple3(env, kAtomTimestamp, term_unit, term_timezone);
                
                using value_type = int64_t;
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
        } else if (strncmp("tD", format, 2) == 0) {
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
        } else {
            format_processed = false;
        }
    } else {
        format_processed = false;
    }

    if (!format_processed) {
        snprintf(err_msg_buf, 255, "not yet implemented for format: `%s`", schema->format);
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

int arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level, std::vector<ERL_NIF_TERM> &out_terms, ERL_NIF_TERM &out_type, ERL_NIF_TERM &metadata, ERL_NIF_TERM &error, bool *end_of_series) {
    return arrow_array_to_nif_term(env, schema, values, 0, -1, level, out_terms, out_type, metadata, error, end_of_series);
}

#endif  // ADBC_ARROW_ARRAY_HPP
