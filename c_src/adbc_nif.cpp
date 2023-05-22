#include <erl_nif.h>
#include <stdbool.h>
#include <stdio.h>
#include <climits>
#include "nif_utils.hpp"
#include <adbc.h>
#include "adbc_nif_resource.hpp"

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
#endif

template<> ErlNifResourceType * NifRes<struct AdbcDatabase>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcConnection>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct ArrowArrayStream>::type = nullptr;

static ERL_NIF_TERM nif_error_from_adbc_error(ErlNifEnv *env, struct AdbcError * error) {
    return erlang::nif::error(env, enif_make_tuple3(env,
        erlang::nif::make_binary(env, error->message),
        enif_make_int(env, error->vendor_code),
        erlang::nif::make_binary(env, error->sqlstate, 5)
    ));
}

static ERL_NIF_TERM adbc_database_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM ret, error;
    res_type * database = nullptr;
    if ((database = res_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    database->val = (res_type::val_type_p)enif_alloc(sizeof(res_type::val_type));
    memset(database->val, 0, sizeof(res_type::val_type));
    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcDatabaseNew(database->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_free(database->val);
        enif_release_resource(database);
        ret = nif_error_from_adbc_error(env, &adbc_error);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    ret = enif_make_resource(env, database);
    enif_release_resource(database);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_database_set_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM ret, error;
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

    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcDatabaseSetOption(database->val, key.c_str(), value.c_str(), &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_database_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;
    
    ERL_NIF_TERM ret, error;
    res_type * database = nullptr;
    if ((database = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcDatabaseInit(database->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_database_release(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM ret, error;
    res_type * database = nullptr;
    if ((database = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    if (database->val == nullptr) {
        return enif_make_badarg(env);
    }

    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcDatabaseRelease(database->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    enif_free(database->val);
    database->val = nullptr;
    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret, error;
    res_type * connection = nullptr;
    if ((connection = res_type::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    connection->val = (res_type::val_type_p)enif_alloc(sizeof(res_type::val_type));
    memset(connection->val, 0, sizeof(res_type::val_type));
    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcConnectionNew(connection->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        enif_free(connection->val);
        enif_release_resource(connection);
        ret = nif_error_from_adbc_error(env, &adbc_error);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    ret = enif_make_resource(env, connection);
    enif_release_resource(connection);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_connection_set_option(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret, error;
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

    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcConnectionSetOption(connection->val, key.c_str(), value.c_str(), &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using db_type = NifRes<struct AdbcDatabase>;

    ERL_NIF_TERM ret, error;
    res_type * connection = nullptr;
    db_type * db = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }
    if ((db = db_type::get_resource(env, argv[1], error)) == nullptr) {
        return error;
    }

    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcConnectionInit(connection->val, db->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_release(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;

    ERL_NIF_TERM ret, error;
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }

    if (connection->val == nullptr) {
        return enif_make_badarg(env);
    }

    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcConnectionRelease(connection->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    enif_free(connection->val);
    connection->val = nullptr;
    return erlang::nif::ok(env);
}

static ERL_NIF_TERM adbc_connection_get_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM ret, error;
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }
    if (connection->val == nullptr) {
        return enif_make_badarg(env);
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
    array_stream->val = (array_stream_type::val_type_p)enif_alloc(sizeof(array_stream_type::val_type));
    memset(array_stream->val, 0, sizeof(array_stream_type::val_type));

    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcConnectionGetInfo(connection->val, ptr, info_codes_length, array_stream->val, &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        enif_free(array_stream->val);
        enif_release_resource(array_stream);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        return ret;
    }

    ret = enif_make_resource(env, array_stream);
    enif_release_resource(array_stream);
    return erlang::nif::ok(env, ret);
}

static ERL_NIF_TERM adbc_connection_get_objects(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    using res_type = NifRes<struct AdbcConnection>;
    using array_stream_type = NifRes<struct ArrowArrayStream>;

    ERL_NIF_TERM ret, error;
    res_type * connection = nullptr;
    if ((connection = res_type::get_resource(env, argv[0], error)) == nullptr) {
        return error;
    }
    if (connection->val == nullptr) {
        return enif_make_badarg(env);
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
    array_stream->val = (array_stream_type::val_type_p)enif_alloc(sizeof(array_stream_type::val_type));
    memset(array_stream->val, 0, sizeof(array_stream_type::val_type));

    struct AdbcError adbc_error;
    AdbcStatusCode code = AdbcConnectionGetObjects(
        connection->val, 
        depth,
        catalog_p,
        db_schema_p,
        table_name_p,
        table_types.data(),
        column_name_p,
        array_stream->val,
        &adbc_error);
    if (code != ADBC_STATUS_OK) {
        ret = nif_error_from_adbc_error(env, &adbc_error);
        enif_free(array_stream->val);
        enif_release_resource(array_stream);
        if (adbc_error.release != nullptr) {
            adbc_error.release(&adbc_error);
        }
        for (auto& at : table_types) {
            if (at) enif_free((void *)at);
        }
        return ret;
    }

    ret = enif_make_resource(env, array_stream);
    enif_release_resource(array_stream);
    for (auto& at : table_types) {
        if (at) enif_free((void *)at);
    }
    return erlang::nif::ok(env, ret);
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
        using res_type = NifRes<struct ArrowArrayStream>;
        rt = enif_open_resource_type(env, "Elixir.Adbc.Nif", "NifResArrowArrayStream", res_type::destruct_resource, ERL_NIF_RT_CREATE, NULL);
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
    {"adbc_connection_get_objects", 7, adbc_connection_get_objects, 0}
};

ERL_NIF_INIT(Elixir.Adbc.Nif, nif_functions, on_load, on_reload, on_upgrade, NULL);

#if defined(__GNUC__)
#pragma GCC visibility push(default)
#endif
