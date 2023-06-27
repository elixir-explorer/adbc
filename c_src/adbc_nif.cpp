#include <erl_nif.h>
#include <cstdbool>
#include <cstdio>
#include <climits>
#include <map>
#include "nif_utils.hpp"
#include <adbc.h>
#include <nanoarrow/nanoarrow.h>
#include "adbc_nif_resource.hpp"

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
#endif

template<> ErlNifResourceType * NifRes<struct AdbcDatabase>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcConnection>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcStatement>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcError>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct ArrowArrayStream>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct ArrowSchema>::type = nullptr;

static ERL_NIF_TERM nif_error_from_adbc_error(ErlNifEnv *env, struct AdbcError * adbc_error) {
    char const* message = (adbc_error->message == nullptr) ? "unknown error" : adbc_error->message;
    ERL_NIF_TERM nif_error = erlang::nif::error(env, enif_make_tuple4(env,
        erlang::nif::atom(env, "adbc_error"),
        erlang::nif::make_binary(env, message),
        enif_make_int(env, adbc_error->vendor_code),
        erlang::nif::make_binary(env, adbc_error->sqlstate, 5)
    ));

    if (adbc_error->release != nullptr) {
        adbc_error->release(adbc_error);
    }

    return nif_error;
}

static ERL_NIF_TERM arrow_schema_metadata_to_nif_term(ErlNifEnv *env, const char* metadata) {
    if (metadata == nullptr) return erlang::nif::atom(env, "nil");
    
    std::map<std::string, std::string> metadata_map;

    int64_t bytes = 0;
    uint8_t * ptr = (uint8_t *)metadata;
    int32_t n_pairs = *(int32_t *)ptr; ptr += sizeof(int32_t);
    int32_t key_bytes = 0, val_bytes = 0;

    for (int32_t pair_i = 0; pair_i < n_pairs; pair_i++) {
        key_bytes = *(int32_t *)ptr; ptr += sizeof(int32_t);
        std::string key{(char *)ptr, (size_t)key_bytes}; ptr += key_bytes;

        val_bytes = *(int32_t *)ptr; ptr += sizeof(int32_t);
        std::string val{(char *)ptr, (size_t)val_bytes}; ptr += val_bytes;
        
        metadata_map[key] = val;
    }

    ERL_NIF_TERM out{};
    erlang::nif::make(env, metadata_map, out, true);
    return out;
}

static ERL_NIF_TERM arrow_schema_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema) {
    if (schema == nullptr) {
        return erlang::nif::error(env, "invalid schema, schema == nullptr");
    }

    const char* format = schema->format ? schema->format : "";
    const char* name = schema->name ? schema->name : "";

    ERL_NIF_TERM children_term{};
    std::vector<ERL_NIF_TERM> children(schema->n_children < 0 ? 0 : schema->n_children);
    if (schema->n_children > 0 && schema->children == nullptr) {
        return erlang::nif::error(env, "invalid schema, schema->children == nullptr, however, schema->n_children > 0");
    }
    if (schema->n_children > 0) {
        for (int64_t child_i = 0; child_i < schema->n_children; child_i++) {
            struct ArrowSchema * child = schema->children[child_i];
            children[child_i] = arrow_schema_to_nif_term(env, child);
        }
    }
    children_term = enif_make_list_from_array(env, children.data(), (unsigned)schema->n_children);

    return enif_make_tuple6(env,
        erlang::nif::make_binary(env, format),
        erlang::nif::make_binary(env, name),
        arrow_schema_metadata_to_nif_term(env, schema->metadata),
        enif_make_int64(env, schema->flags),
        enif_make_int64(env, schema->n_children),
        children_term
    );
}

template <typename T, typename M> static ERL_NIF_TERM values_from_buffer(ErlNifEnv *env, int64_t length, const uint8_t * validity_bitmap, const T * value_buffer, const M& value_to_nif) {
    std::vector<ERL_NIF_TERM> values(length);
    if (validity_bitmap == nullptr) {
        for (int64_t i = 0; i < length; i++) {
            values[i] = value_to_nif(env, value_buffer[i]);
        }
    } else {
        for (int64_t i = 0; i < length; i++) {
            uint8_t vbyte = validity_bitmap[i/8];
            if (vbyte & (1 << (i & 0b11111111))) {
                values[i] = value_to_nif(env, value_buffer[i]);
            } else {
                values[i] = erlang::nif::atom(env, "nil");
            }
        }
    }

    return enif_make_list_from_array(env, values.data(), (unsigned)values.size());
}

template <typename M> static ERL_NIF_TERM strings_from_buffer(
    ErlNifEnv *env,
    int64_t length, 
    const uint8_t * validity_bitmap, 
    const int32_t * offsets_buffer,
    const uint8_t* value_buffer,
    const M& value_to_nif) {
    int64_t start_index = validity_bitmap == nullptr ? 1 : 2;

    int32_t offset = offsets_buffer[0];
    std::vector<ERL_NIF_TERM> values(length);
    if (validity_bitmap == nullptr) {
        for (int64_t i = 0; i < length; i++) {
            int32_t end_index = offsets_buffer[i + 1];
            size_t nbytes = end_index - offset;
            if (nbytes == 0) {
                values[i] = erlang::nif::atom(env, "nil");
            } else {
                values[i] = value_to_nif(env, value_buffer, offset, nbytes);
            }
            offset = end_index;
        }
    } else {
        for (int64_t i = 0; i < length; i++) {
            uint8_t vbyte = validity_bitmap[i / 8];
            int32_t end_index = offsets_buffer[i + 1];
            size_t nbytes = end_index - offset;
            if (nbytes > 0 && vbyte & (1 << (i & 0b11111111))) {
                values[i] = value_to_nif(env, value_buffer, offset, nbytes);
            } else {
                values[i] = erlang::nif::atom(env, "nil");
            }
            offset = end_index;
        }
    }

    return enif_make_list_from_array(env, values.data(), (unsigned)values.size());
}

static ERL_NIF_TERM arrow_array_to_nif_term(ErlNifEnv *env, struct ArrowSchema * schema, struct ArrowArray * values, uint64_t level) {
    if (schema == nullptr) {
        return erlang::nif::error(env, "invalid ArrowSchema (nullptr) when invoking next");
    }
    if (values == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray (nullptr) when invoking next");
    }

    const char* format = schema->format ? schema->format : "";
    const char* name = schema->name ? schema->name : "";

    ERL_NIF_TERM children_term{};
    if (schema->n_children > 0 && schema->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowSchema, schema->children == nullptr, however, schema->n_children > 0");
    }
    if (values->n_children > 0 && values->children == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArray, values->children == nullptr, however, values->n_children > 0");
    }
    if (values->n_children != schema->n_children) {
        return erlang::nif::error(env, "invalid ArrowArray or ArrowSchema, values->n_children != schema->n_children");
    }

    std::vector<ERL_NIF_TERM> children(schema->n_children < 0 ? 0 : schema->n_children);
    if (schema->n_children > 0) {
        for (int64_t child_i = 0; child_i < schema->n_children; child_i++) {
            struct ArrowSchema * child_schema = schema->children[child_i];
            struct ArrowArray * child_values = values->children[child_i];
            children[child_i] = arrow_array_to_nif_term(env, child_schema, child_values, level + 1);
        }
    }
    children_term = enif_make_list_from_array(env, children.data(), (unsigned)schema->n_children);

    ERL_NIF_TERM current_term{};

    bool has_validity_bitmap = values->null_count != 0 && values->null_count != -1;
    const uint8_t * bitmap_buffer = nullptr;
    if (has_validity_bitmap && values->n_buffers >= 2) {
        bitmap_buffer = (const uint8_t *)values->buffers[0];
    }
    const int32_t * offsets_buffer = nullptr;
    if (values->n_buffers >= 3) {
        offsets_buffer = (const int32_t *)values->buffers[1];
    }

    bool is_struct = false;
    if (strncmp("l", format, 1) == 0) {
        using value_type = int64_t;
        current_term = values_from_buffer(
            env,
            values->length, 
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_int64
        );
    } else if (strncmp("c", format, 1) == 0) {
        using value_type = int8_t;
        current_term = values_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_int64
        );
    } else if (strncmp("s", format, 1) == 0) {
        using value_type = int16_t;
        current_term = values_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_int64
        );
    } else if (strncmp("i", format, 1) == 0) {
        using value_type = int32_t;
        current_term = values_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_int64
        );
    } else if (strncmp("L", format, 1) == 0) {
        using value_type = uint64_t;
        current_term = values_from_buffer(
            env,
            values->length, 
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_uint64
        );
    } else if (strncmp("C", format, 1) == 0) {
        using value_type = uint8_t;
        current_term = values_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_uint64
        );
    } else if (strncmp("S", format, 1) == 0) {
        using value_type = uint16_t;
        current_term = values_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_uint64
        );
    } else if (strncmp("I", format, 1) == 0) {
        using value_type = uint32_t;
        current_term = values_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_uint64
        );
    } else if (strncmp("f", format, 1) == 0) {
        using value_type = float;
        current_term = values_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_double
        );
    } else if (strncmp("g", format, 1) == 0) {
        using value_type = double;
        current_term = values_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            (const value_type *)values->buffers[1],
            enif_make_double
        );
    } else if (strncmp("u", format, 1) == 0) {
        int64_t length = values->length;
        current_term = strings_from_buffer(
            env,
            values->length,
            bitmap_buffer,
            offsets_buffer,
            (const uint8_t *)values->buffers[2],
            [](ErlNifEnv *env, const uint8_t * string_buffers, int32_t offset, size_t nbytes) -> ERL_NIF_TERM {
                return erlang::nif::make_binary(env, (const char *)(string_buffers + offset), nbytes);
            }
        );
    } else if (strncmp("+s", format, 1) == 0) {
        // only handle and return children if this is a struct
        is_struct = true;
    } else {
        char buf[256] = { '\0' };
        snprintf(buf, 256, "not implemented for format: `%s`", schema->format);
        children_term = erlang::nif::error(env, buf);
        // printf("not implemented for format: `%s`\r\n", schema->format);
        // printf("length: %lld\r\n", values->length);
        // printf("null_count: %lld\r\n", values->null_count);
        // printf("offset: %lld\r\n", values->offset);
        // printf("n_buffers: %lld\r\n", values->n_buffers);
        // printf("n_children: %lld\r\n", values->n_children);
        // printf("buffers: %p\r\n", values->buffers);
    }

    if (is_struct) {
        return children_term;
    } else {
        if (schema->children) {
            return enif_make_tuple2(env,
                erlang::nif::make_binary(env, name),
                children_term
            );
        } else {
            return enif_make_tuple2(env,
                erlang::nif::make_binary(env, name),
                current_term
            );
        }
    }
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

static ERL_NIF_TERM adbc_database_set_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * database = nullptr;
    if ((database = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    std::string key, value;
    if (!erlang::nif::get(env, argv[1], key)) {
        return enif_make_badarg(env);
    }
    if (!erlang::nif::get(env, argv[2], value)) {
        return enif_make_badarg(env);
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcDatabaseSetOption(&database->val, key.c_str(), value.c_str(), &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_database_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM ret{};
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

static ERL_NIF_TERM adbc_database_release(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * database = nullptr;
    if ((database = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcDatabaseRelease(&database->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        if (code == ADBC_STATUS_INVALID_STATE) {
            return erlang::nif::error(env, "invalid state");
        } else {
            return nif_error_from_adbc_error(env, &adbc_error);
        }
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

static ERL_NIF_TERM adbc_connection_set_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * connection = res_type::get_resource(env, argv[0], error);
    if (connection == nullptr) {
        return error;
    }

    std::string key, value;
    if (!erlang::nif::get(env, argv[1], key)) {
        return enif_make_badarg(env);
    }
    if (!erlang::nif::get(env, argv[2], value)) {
        return enif_make_badarg(env);
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionSetOption(&connection->val, key.c_str(), value.c_str(), &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using db_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM ret{};
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

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_release(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionRelease(&connection->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        if (code == ADBC_STATUS_INVALID_STATE) {
            return erlang::nif::error(env, "invalid state");
        } else {
            return nif_error_from_adbc_error(env, &adbc_error);
        }
    }

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
    return enif_make_tuple2(env, erlang::nif::ok(env), ret);
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

static ERL_NIF_TERM adbc_connection_get_table_schema(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using schema_type = NifRes<struct ArrowSchema>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM nif_schema{};
    ERL_NIF_TERM error{};
    res_type * connection = res_type::get_resource(env, argv[0], error);
    if (connection == nullptr) {
        return error;
    }

    std::string catalog, db_schema, table_name;
    const char * catalog_p = nullptr;
    const char * db_schema_p = nullptr;
    const char * table_name_p = nullptr;

    if (!erlang::nif::get(env, argv[1], catalog)) {
        if (!erlang::nif::check_nil(env, argv[1])) {
            return enif_make_badarg(env);
        } else {
            catalog_p = catalog.c_str();
        }
    }
    if (!erlang::nif::get(env, argv[2], db_schema)) {
        if (!erlang::nif::check_nil(env, argv[2])) {
            return enif_make_badarg(env);
        } else {
            db_schema_p = db_schema.c_str();
        }
    }
    if (!erlang::nif::get(env, argv[3], table_name)) {
        return enif_make_badarg(env);
    }
    table_name_p = table_name.c_str();

    auto schema = schema_type::allocate_resource(env, error);
    if (schema  == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionGetTableSchema(
        &connection->val,
        catalog_p,
        db_schema_p,
        table_name_p,
        &schema->val,
        &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = schema->make_resource(env);
    nif_schema = arrow_schema_to_nif_term(env, &schema->val);
    return enif_make_tuple3(env, 
        erlang::nif::ok(env),
        ret,
        nif_schema
    );
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

static ERL_NIF_TERM adbc_connection_commit(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionCommit(&connection->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_rollback(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM  error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionRollback(&connection->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    return erlang::nif::ok(env);
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

static ERL_NIF_TERM adbc_arrow_array_stream_get_schema(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct ArrowArrayStream>;
    using schema_type = NifRes<struct ArrowSchema>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM nif_schema{};
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    auto schema = schema_type::allocate_resource(env, error);
    if (schema == nullptr) {
        return error;
    }
    if (res->val.get_schema == nullptr) {
        return erlang::nif::error(env, "invalid ArrowArrayStream.");
    }

    return erlang::nif::ok(env, ret);
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

    if (res->private_data == nullptr) {
        res->private_data = enif_alloc(sizeof(struct ArrowSchema));
        memset(res->private_data, 0, sizeof(struct ArrowSchema));
        int code = res->val.get_schema(&res->val, (struct ArrowSchema *)res->private_data);
        if (code != 0) {
            const char * reason = res->val.get_last_error(&res->val);
            enif_free(res->private_data);
            res->private_data = nullptr;
            return erlang::nif::error(env, reason ? reason : "unknown error");
        }
    }

    auto schema = (struct ArrowSchema*)res->private_data;
    ret = arrow_array_to_nif_term(env, schema, &out, 0);

    if (out.release) {
        out.release(&out);
    }

    return erlang::nif::ok(env, ret);
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

    ERL_NIF_TERM ret = statement->make_resource(env);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_statement_release(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementRelease(&statement->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        if (code == ADBC_STATUS_INVALID_STATE) {
            return erlang::nif::error(env, "invalid state");
        } else {
            return nif_error_from_adbc_error(env, &adbc_error);
        }
    }

    return erlang::nif::ok(env);
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

    ERL_NIF_TERM ret{};
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

    ERL_NIF_TERM ret{};
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
int elixir_to_arrow_type_struct(ErlNifEnv *env, ERL_NIF_TERM values, struct ArrowArray* array_out, struct ArrowSchema* schema_out, struct ArrowError* error_out) {
    array_out->release = NULL;
    schema_out->release = NULL;

    unsigned n_items = 0;
    if (!enif_get_list_length(env, values, &n_items)) {
        return 1;
    }

    ArrowSchemaInit(schema_out);
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema_out, n_items));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema_out, ""));

    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromType(array_out, NANOARROW_TYPE_STRUCT));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAllocateChildren(array_out, static_cast<int64_t>(n_items)));

    array_out->length = 1;
    array_out->null_count = -1;

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
            auto type = NANOARROW_TYPE_BINARY;
            if (bytes.size > INT32_MAX) {
                type = NANOARROW_TYPE_LARGE_BINARY;
            }

            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema_i, type));
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema_i, ""));
            NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(child_i, schema_i, error_out));

            struct ArrowBufferView view{};
            view.data.data = bytes.data;
            view.size_bytes = static_cast<int64_t>(bytes.size);
            NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(child_i));
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(child_i, view));
        } else if (enif_is_atom(env, head)) {
            std::string atom_val;
            int64_t val{};
            if (erlang::nif::get_atom(env, head, atom_val)) {
                auto type = NANOARROW_TYPE_BOOL;
                if (atom_val == "true" || atom_val == "TRUE") {
                    val = 1;
                } else if (atom_val == "false" || atom_val == "FALSE") {
                    val = 0;
                } else if (atom_val == "nil") {
                    type = NANOARROW_TYPE_NA;
                } else {
                    snprintf(error_out->message, sizeof(error_out->message), "atom `:%s` is not supported yet.", atom_val.c_str());
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
                snprintf(error_out->message, sizeof(error_out->message), "failed to get atom");
                return 1;
            }
        } else {
            snprintf(error_out->message, sizeof(error_out->message), "type not supported yet.");
            return 1;
        }
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(child_i, error_out));
        processed++;
    }

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

    if (elixir_to_arrow_type_struct(env, argv[1], &values, &schema, &arrow_error)) {
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

    ERL_NIF_TERM ret{};
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
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResAdbcDatabase", res_type::destruct_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct AdbcConnection>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResAdbcConnection", res_type::destruct_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct AdbcStatement>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResAdbcStatement", res_type::destruct_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct AdbcError>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResAdbcError", res_type::destruct_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct ArrowArrayStream>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResArrowArrayStream", res_type::destruct_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

    {
        using res_type = NifRes<struct ArrowSchema>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResArrowSchema", res_type::destruct_resource, ERL_NIF_RT_CREATE, NULL);
        if (!rt) return -1;
        res_type::type = rt;
    }

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
    {"adbc_database_set_option", 3, adbc_database_set_option, 0},
    {"adbc_database_init", 1, adbc_database_init, 0},
    {"adbc_database_release", 1, adbc_database_release, 0},

    {"adbc_connection_new", 0, adbc_connection_new, 0},
    {"adbc_connection_set_option", 3, adbc_connection_set_option, 0},
    {"adbc_connection_init", 2, adbc_connection_init, 0},
    {"adbc_connection_release", 1, adbc_connection_release, 0},
    {"adbc_connection_get_info", 2, adbc_connection_get_info, 0},
    {"adbc_connection_get_objects", 7, adbc_connection_get_objects, 0},
    {"adbc_connection_get_table_schema", 4, adbc_connection_get_table_schema, 0},
    {"adbc_connection_get_table_types", 1, adbc_connection_get_table_types, 0},
    {"adbc_connection_commit", 1, adbc_connection_commit, 0},
    {"adbc_connection_rollback", 1, adbc_connection_rollback, 0},

    {"adbc_statement_new", 1, adbc_statement_new, 0},
    {"adbc_statement_release", 1, adbc_statement_release, 0},
    {"adbc_statement_execute_query", 1, adbc_statement_execute_query, 0},
    {"adbc_statement_prepare", 1, adbc_statement_prepare, 0},
    {"adbc_statement_set_sql_query", 2, adbc_statement_set_sql_query, 0},
    {"adbc_statement_bind", 2, adbc_statement_bind, 0},
    {"adbc_statement_bind_stream", 2, adbc_statement_bind_stream, 0},

    {"adbc_arrow_array_stream_get_pointer", 1, adbc_arrow_array_stream_get_pointer, 0},
    {"adbc_arrow_array_stream_get_schema", 1, adbc_arrow_array_stream_get_schema, 0},
    {"adbc_arrow_array_stream_next", 1, adbc_arrow_array_stream_next, 0},
    {"adbc_arrow_array_stream_release", 1, adbc_arrow_array_stream_release, 0}
};

ERL_NIF_INIT(Elixir.Adbc.Nif, nif_functions, on_load, on_reload, on_upgrade, NULL);

#if defined(__GNUC__)
#pragma GCC visibility push(default)
#endif
