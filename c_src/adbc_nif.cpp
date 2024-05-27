#include <cstdbool>
#include <cstdio>
#include <climits>
#include <adbc.h>
#include <erl_nif.h>
#include <nanoarrow/nanoarrow.h>
#include "adbc_nif_resource.hpp"
#include "nif_utils.hpp"
#include "adbc_consts.h"
#include "adbc_column.hpp"
#include "adbc_arrow_array.hpp"

template<> ErlNifResourceType * NifRes<struct AdbcDatabase>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcConnection>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcStatement>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct AdbcError>::type = nullptr;
template<> ErlNifResourceType * NifRes<struct ArrowArrayStream>::type = nullptr;

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
    ERL_NIF_TERM out_type;
    ERL_NIF_TERM out_metadata;
    if (arrow_array_to_nif_term(env, schema, &out, 0, out_terms, out_type, out_metadata, error, &end_of_series) == 1) {
        if (out.release) out.release(&out);
        return error;
    }

    if (out_terms.size() == 1) {
        ret = out_terms[0];
        if (end_of_series) {
            if (out.release) {
                out.release(&out);
            }
            return kAtomEndOfSeries;
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

    if (adbc_column_to_arrow_type_struct(env, argv[1], &values, &schema, &arrow_error)) {
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

    kAtomAdbcError = erlang::nif::atom(env, "adbc_error");
    kAtomNil = erlang::nif::atom(env, "nil");
    kAtomTrue = erlang::nif::atom(env, "true");
    kAtomFalse = erlang::nif::atom(env, "false");
    kAtomInfinity = erlang::nif::atom(env, "infinity");
    kAtomNegInfinity = erlang::nif::atom(env, "neg_infinity");
    kAtomNaN = erlang::nif::atom(env, "nan");
    kAtomEndOfSeries = erlang::nif::atom(env, "end_of_series");
    kAtomStructKey = erlang::nif::atom(env, "__struct__");

    kAtomDecimal = erlang::nif::atom(env, "decimal");
    kAtomFixedSizeBinary = erlang::nif::atom(env, "fixed_size_binary");
    kAtomFixedSizeList = erlang::nif::atom(env, "fixed_size_list");
    kAtomTime32 = erlang::nif::atom(env, "time32");
    kAtomTime64 = erlang::nif::atom(env, "time64");
    kAtomTimestamp = erlang::nif::atom(env, "timestamp");
    kAtomDuration = erlang::nif::atom(env, "duration");
    kAtomInterval = erlang::nif::atom(env, "interval");
    kAtomSeconds = erlang::nif::atom(env, "seconds");
    kAtomMilliseconds = erlang::nif::atom(env, "milliseconds");
    kAtomMicroseconds = erlang::nif::atom(env, "microseconds");
    kAtomNanoseconds = erlang::nif::atom(env, "nanoseconds");
    kAtomMonth = erlang::nif::atom(env, "month");
    kAtomDayTime = erlang::nif::atom(env, "day_time");
    kAtomMonthDayNano = erlang::nif::atom(env, "month_day_nano");
    
    kAtomCalendarKey = erlang::nif::atom(env, "calendar");
    kAtomCalendarISO = erlang::nif::atom(env, "Elixir.Calendar.ISO");

    kAtomDateModule = erlang::nif::atom(env, "Elixir.Date");
    kAtomYearKey = erlang::nif::atom(env, "year");
    kAtomMonthKey = erlang::nif::atom(env, "month");
    kAtomDayKey = erlang::nif::atom(env, "day");

    kAtomNaiveDateTimeModule = erlang::nif::atom(env, "Elixir.NaiveDateTime");
    kAtomTimeModule = erlang::nif::atom(env, "Elixir.Time");
    kAtomHourKey = erlang::nif::atom(env, "hour");
    kAtomMinuteKey = erlang::nif::atom(env, "minute");
    kAtomSecondKey = erlang::nif::atom(env, "second");
    kAtomMicrosecondKey = erlang::nif::atom(env, "microsecond");

    kAtomAdbcColumnModule = erlang::nif::atom(env, "Elixir.Adbc.Column");
    kAtomNameKey = erlang::nif::atom(env, "name");
    kAtomTypeKey = erlang::nif::atom(env, "type");
    kAtomNullableKey = erlang::nif::atom(env, "nullable");
    kAtomMetadataKey = erlang::nif::atom(env, "metadata");
    kAtomDataKey = erlang::nif::atom(env, "data");

    kAdbcColumnTypeBool = erlang::nif::atom(env, "boolean");
    kAdbcColumnTypeI8 = erlang::nif::atom(env, "i8");
    kAdbcColumnTypeU8 = erlang::nif::atom(env, "u8");
    kAdbcColumnTypeI16 = erlang::nif::atom(env, "i16");
    kAdbcColumnTypeU16 = erlang::nif::atom(env, "u16");
    kAdbcColumnTypeI32 = erlang::nif::atom(env, "i32");
    kAdbcColumnTypeU32 = erlang::nif::atom(env, "u32");
    kAdbcColumnTypeI64 = erlang::nif::atom(env, "i64");
    kAdbcColumnTypeU64 = erlang::nif::atom(env, "u64");
    kAdbcColumnTypeF16 = erlang::nif::atom(env, "f16");
    kAdbcColumnTypeF32 = erlang::nif::atom(env, "f32");
    kAdbcColumnTypeF64 = erlang::nif::atom(env, "f64");
    kAdbcColumnTypeBinary = erlang::nif::atom(env, "binary");
    kAdbcColumnTypeLargeBinary = erlang::nif::atom(env, "large_binary");
    kAdbcColumnTypeString = erlang::nif::atom(env, "string");
    kAdbcColumnTypeLargeString = erlang::nif::atom(env, "large_string");
    kAdbcColumnTypeDate32 = erlang::nif::atom(env, "date32");
    kAdbcColumnTypeDate64 = erlang::nif::atom(env, "date64");
    kAdbcColumnTypeList = erlang::nif::atom(env, "list");
    kAdbcColumnTypeLargeList = erlang::nif::atom(env, "large_list");
    kAdbcColumnTypeStruct = erlang::nif::atom(env, "struct");
    kAdbcColumnTypeMap = erlang::nif::atom(env, "map");
    kAdbcColumnTypeDenseUnion = erlang::nif::atom(env, "dense_union");
    kAdbcColumnTypeSparseUnion = erlang::nif::atom(env, "sparse_union");

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
