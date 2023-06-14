#include <erl_nif.h>
#include <cstdbool>
#include <cstdio>
#include <climits>
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
template<> ErlNifResourceType * NifRes<struct ArrowArray>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct ArrowSchema>::type = nullptr;

static ERL_NIF_TERM nif_error_from_adbc_error(ErlNifEnv *env, struct AdbcError * adbc_error) {
    char const* message = (adbc_error->message == nullptr) ? "unknown error" : adbc_error->message;
    ERL_NIF_TERM nif_error = erlang::nif::error(env, enif_make_tuple3(env,
        erlang::nif::make_binary(env, message),
        enif_make_int(env, adbc_error->vendor_code),
        erlang::nif::make_binary(env, adbc_error->sqlstate, 5)
    ));

    if (adbc_error->release != nullptr) {
        adbc_error->release(adbc_error);
    }

    return nif_error;
}

// static int elixir_to_arrow_type_struct(ErlNifEnv *env, ERL_NIF_TERM argv, struct ArrowArray* array_out, struct ArrowSchema* schema_out);

static ERL_NIF_TERM adbc_database_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM error{};
    res_type * database = nullptr;
    if ((database = res_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcDatabaseNew(&database->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_release_resource(database);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ERL_NIF_TERM ret = enif_make_resource(env, database);
    enif_release_resource(database);
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

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionNew(&connection->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_release_resource(connection);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, connection);
    enif_release_resource(connection);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_connection_set_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
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

    ERL_NIF_TERM ret{};
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

    array_stream_type * array_stream = nullptr;
    if ((array_stream = array_stream_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionGetInfo(&connection->val, ptr, info_codes_length, &array_stream->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_release_resource(array_stream);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, array_stream);
    enif_release_resource(array_stream);
    return enif_make_tuple3(env,
        erlang::nif::ok(env),
        ret,
        enif_make_uint64(env, reinterpret_cast<uint64_t>(&array_stream->val))
    );
}

static ERL_NIF_TERM adbc_connection_get_objects(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM ret{};
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

    array_stream_type * array_stream = nullptr;
    if ((array_stream = array_stream_type::allocate_resource(env, error)) == nullptr) {
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
        enif_release_resource(array_stream);
        for (auto& at : table_types) {
            if (at) enif_free((void *)at);
        }
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, array_stream);
    enif_release_resource(array_stream);
    for (auto& at : table_types) {
        if (at) enif_free((void *)at);
    }

    return enif_make_tuple3(env,
        erlang::nif::ok(env),
        ret,
        enif_make_uint64(env, reinterpret_cast<uint64_t>(&array_stream->val))
    );
}

static ERL_NIF_TERM adbc_connection_get_table_schema(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using schema_type = NifRes<struct ArrowSchema>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
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

    schema_type * schema = nullptr;
    if ((schema = schema_type::allocate_resource(env, error)) == nullptr) {
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
        enif_release_resource(schema);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, schema);
    enif_release_resource(schema);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_connection_get_table_types(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    array_stream_type * array_stream = nullptr;
    if ((array_stream = array_stream_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionGetTableTypes(&connection->val, &array_stream->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_release_resource(array_stream);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, array_stream);
    enif_release_resource(array_stream);
    
    return enif_make_tuple3(env,
        erlang::nif::ok(env),
        ret,
        enif_make_uint64(env, reinterpret_cast<uint64_t>(&array_stream->val))
    );
}

static ERL_NIF_TERM adbc_connection_read_partition(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    ErlNifBinary serialized_partition;
    size_t serialized_length;

    if (!enif_inspect_binary(env, argv[1], &serialized_partition)) {
        return enif_make_badarg(env);
    }
    if (!erlang::nif::get(env, argv[2], &serialized_length)) {
        return enif_make_badarg(env);
    }
    if (serialized_length > serialized_partition.size) {
        return enif_make_badarg(env);
    }

    array_stream_type * array_stream = nullptr;
    if ((array_stream = array_stream_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcConnectionReadPartition(
        &connection->val,
        (const uint8_t *)serialized_partition.data,
        serialized_length,
        &array_stream->val,
        &adbc_error
    );
    if (code != ADBC_STATUS_OK) {
        enif_release_resource(array_stream);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, array_stream);
    enif_release_resource(array_stream);
    
    return enif_make_tuple3(env,
        erlang::nif::ok(env),
        ret,
        enif_make_uint64(env, reinterpret_cast<uint64_t>(&array_stream->val))
    );
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

static ERL_NIF_TERM adbc_statement_get_pointer(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    return enif_make_uint64(env, reinterpret_cast<uint64_t>(&res->val));
}

static ERL_NIF_TERM adbc_arrow_schema_get_pointer(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct ArrowSchema>;
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    return enif_make_uint64(env, reinterpret_cast<uint64_t>(&res->val));
}

static ERL_NIF_TERM adbc_arrow_array_get_pointer(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct ArrowArray>;
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    return enif_make_uint64(env, reinterpret_cast<uint64_t>(&res->val));
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

static ERL_NIF_TERM adbc_arrow_array_stream_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct ArrowArrayStream>;
    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    ret = enif_make_resource(env, res);
    enif_release_resource(res);

    return enif_make_tuple3(env, 
        erlang::nif::ok(env), 
        ret, 
        enif_make_uint64(env, reinterpret_cast<uint64_t>(&res->val))
    );
}

static ERL_NIF_TERM adbc_arrow_array_stream_reset(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct ArrowArrayStream>;
    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    if (res->val.release) {
        res->val.release(&res->val);
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_error_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcError>;
    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    ret = enif_make_resource(env, res);
    enif_release_resource(res);

    return enif_make_tuple3(env, 
        erlang::nif::ok(env), 
        ret, 
        enif_make_uint64(env, reinterpret_cast<uint64_t>(&res->val))
    );
}

static ERL_NIF_TERM adbc_error_reset(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcError>;
    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    if (res->val.release) {
        res->val.release(&res->val);
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_error_to_term(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcError>;
    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * res = nullptr;
    if ((res = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    if (res->val.message == nullptr) {
        return erlang::nif::error(env, "error hasn't been set");
    }

    ret = nif_error_from_adbc_error(env, &res->val);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_statement_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;
    using connection_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    connection_type * connection = nullptr;
    if ((connection = connection_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    res_type * statement = nullptr;
    if ((statement = res_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementNew(&connection->val, &statement->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_release_resource(statement);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, statement);
    enif_release_resource(statement);
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

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    array_stream_type * array_stream = nullptr;
    if ((array_stream = array_stream_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    int64_t rows_affected = 0;
    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementExecuteQuery(&statement->val, &array_stream->val, &rows_affected, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_release_resource(array_stream);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, array_stream);
    enif_release_resource(array_stream);
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

static ERL_NIF_TERM adbc_statement_set_substrait_plan(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }
    
    ErlNifBinary plan;
    size_t length;

    if (!enif_inspect_binary(env, argv[1], &plan)) {
        return enif_make_badarg(env);
    }
    if (!erlang::nif::get(env, argv[2], &length)) {
        return enif_make_badarg(env);
    }
    if (length > plan.size) {
        return enif_make_badarg(env);
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementSetSubstraitPlan(&statement->val, (const uint8_t *)plan.data, length, &adbc_error);
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

    // TODO: only 1 row for now
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

static ERL_NIF_TERM adbc_statement_get_parameter_schema(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcStatement>;
    using schema_type = NifRes<struct ArrowSchema>;

    ERL_NIF_TERM ret{};
    ERL_NIF_TERM error{};

    res_type * statement = nullptr;
    if ((statement = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }
    
    schema_type * schema = nullptr;
    if ((schema = schema_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error{};
    AdbcStatusCode code = AdbcStatementGetParameterSchema(&statement->val, &schema->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_release_resource(schema);
        return nif_error_from_adbc_error(env, &adbc_error);
    }

    ret = enif_make_resource(env, schema);
    enif_release_resource(schema);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_get_all_function_pointers(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ERL_NIF_TERM ret{};
    std::map<std::string, uint64_t> fptr = {
        {"AdbcDatabaseNew", reinterpret_cast<uint64_t>(&AdbcDatabaseNew)},
        {"AdbcDatabaseSetOption", reinterpret_cast<uint64_t>(&AdbcDatabaseSetOption)},
        {"AdbcDatabaseInit", reinterpret_cast<uint64_t>(&AdbcDatabaseInit)},
        {"AdbcDatabaseRelease", reinterpret_cast<uint64_t>(&AdbcDatabaseRelease)},
        
        {"AdbcConnectionNew", reinterpret_cast<uint64_t>(&AdbcConnectionNew)},
        {"AdbcConnectionSetOption", reinterpret_cast<uint64_t>(&AdbcConnectionSetOption)},
        {"AdbcConnectionInit", reinterpret_cast<uint64_t>(&AdbcConnectionInit)},
        {"AdbcConnectionRelease", reinterpret_cast<uint64_t>(&AdbcConnectionRelease)},
        {"AdbcConnectionGetInfo", reinterpret_cast<uint64_t>(&AdbcConnectionGetInfo)},
        {"AdbcConnectionGetObjects", reinterpret_cast<uint64_t>(&AdbcConnectionGetObjects)},
        {"AdbcConnectionGetTableSchema", reinterpret_cast<uint64_t>(&AdbcConnectionGetTableSchema)},
        {"AdbcConnectionGetTableTypes", reinterpret_cast<uint64_t>(&AdbcConnectionGetTableTypes)},
        {"AdbcConnectionReadPartition", reinterpret_cast<uint64_t>(&AdbcConnectionReadPartition)},
        {"AdbcConnectionCommit", reinterpret_cast<uint64_t>(&AdbcConnectionCommit)},
        {"AdbcConnectionRollback", reinterpret_cast<uint64_t>(&AdbcConnectionRollback)},

        {"AdbcStatementNew", reinterpret_cast<uint64_t>(&AdbcStatementNew)},
        {"AdbcStatementRelease", reinterpret_cast<uint64_t>(&AdbcStatementRelease)},
        {"AdbcStatementExecuteQuery", reinterpret_cast<uint64_t>(&AdbcStatementExecuteQuery)},
        {"AdbcStatementPrepare", reinterpret_cast<uint64_t>(&AdbcStatementPrepare)},
        {"AdbcStatementSetSqlQuery", reinterpret_cast<uint64_t>(&AdbcStatementSetSqlQuery)},
        {"AdbcStatementSetSubstraitPlan", reinterpret_cast<uint64_t>(&AdbcStatementSetSubstraitPlan)},
        {"AdbcStatementBind", reinterpret_cast<uint64_t>(&AdbcStatementBind)},
        {"AdbcStatementBindStream", reinterpret_cast<uint64_t>(&AdbcStatementBindStream)},
        {"AdbcStatementGetParameterSchema", reinterpret_cast<uint64_t>(&AdbcStatementGetParameterSchema)},
        {"AdbcStatementSetOption", reinterpret_cast<uint64_t>(&AdbcStatementSetOption)},
        {"AdbcStatementExecutePartitions", reinterpret_cast<uint64_t>(&AdbcStatementExecutePartitions)}
    };
    erlang::nif::make(env, fptr, ret, false);
    return ret;
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
        using res_type = NifRes<struct ArrowArray>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResArrowArray", res_type::destruct_resource, ERL_NIF_RT_CREATE, NULL);
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
    {"adbc_connection_read_partition", 3, adbc_connection_read_partition, 0},
    {"adbc_connection_commit", 1, adbc_connection_commit, 0},
    {"adbc_connection_rollback", 1, adbc_connection_rollback, 0},

    {"adbc_statement_get_pointer", 1, adbc_statement_get_pointer, 0},
    {"adbc_statement_new", 1, adbc_statement_new, 0},
    {"adbc_statement_release", 1, adbc_statement_release, 0},
    {"adbc_statement_execute_query", 1, adbc_statement_execute_query, 0},
    {"adbc_statement_prepare", 1, adbc_statement_prepare, 0},
    {"adbc_statement_set_sql_query", 2, adbc_statement_set_sql_query, 0},
    {"adbc_statement_set_substrait_plan", 3, adbc_statement_set_substrait_plan, 0},
    {"adbc_statement_bind", 2, adbc_statement_bind, 0},
    {"adbc_statement_bind_stream", 2, adbc_statement_bind_stream, 0},
    {"adbc_statement_get_parameter_schema", 1, adbc_statement_get_parameter_schema, 0},

    {"adbc_arrow_schema_get_pointer", 1, adbc_arrow_schema_get_pointer, 0},
    {"adbc_arrow_array_get_pointer", 1, adbc_arrow_array_get_pointer, 0},
    {"adbc_arrow_array_stream_get_pointer", 1, adbc_arrow_array_stream_get_pointer, 0},

    {"adbc_error_new", 0, adbc_error_new, 0},
    {"adbc_error_reset", 1, adbc_error_reset, 0},
    {"adbc_error_to_term", 1, adbc_error_to_term, 0},

    {"adbc_get_all_function_pointers", 0, adbc_get_all_function_pointers, 0}
};

ERL_NIF_INIT(Elixir.Adbc.Nif, nif_functions, on_load, on_reload, on_upgrade, NULL);

#if defined(__GNUC__)
#pragma GCC visibility push(default)
#endif
