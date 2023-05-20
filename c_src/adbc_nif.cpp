#include <erl_nif.h>
#include <stdbool.h>
#include <stdio.h>
#include <climits>
#include "nif_utils.hpp"
#include <adbc.h>

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
#endif

struct NifResAdbcDatabase {
    struct AdbcDatabase* val;

    static ErlNifResourceType * type;
    static NifResAdbcDatabase * allocate_resource(ErlNifEnv * env, ERL_NIF_TERM &error) {
        NifResAdbcDatabase * res = (NifResAdbcDatabase *)enif_alloc_resource(NifResAdbcDatabase::type, sizeof(NifResAdbcDatabase));
        if (res == nullptr) {
            error = erlang::nif::error(env, "cannot allocate NifResAdbcDatabase resource");
            return res;
        }
        return res;
    }

    static NifResAdbcDatabase * get_resource(ErlNifEnv * env, ERL_NIF_TERM term, ERL_NIF_TERM &error) {
        NifResAdbcDatabase * self_res = nullptr;
        if (!enif_get_resource(env, term, NifResAdbcDatabase::type, (void **)&self_res) || self_res == nullptr || self_res->val == nullptr) {
            error = erlang::nif::error(env, "cannot access NifResAdbcDatabase resource");
        }
        return self_res;
    }

    static void destruct_resource(ErlNifEnv *env, void *args) {
        auto res = (NifResAdbcDatabase *)args;
        if (res) {
            if (res->val) {
                enif_free(res->val);
                res->val = nullptr;
            }
        }
    }
};

ErlNifResourceType * NifResAdbcDatabase::type = nullptr;
static ERL_NIF_TERM nif_error_from_adbc_error(ErlNifEnv *env, struct AdbcError * error) {
    return erlang::nif::error(env, enif_make_tuple3(env,
        erlang::nif::make_binary(env, error->message),
        enif_make_int(env, error->vendor_code),
        erlang::nif::make_binary(env, error->sqlstate, 5)
    ));
}

static ERL_NIF_TERM adbc_database_new(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ERL_NIF_TERM ret, error;
    NifResAdbcDatabase * database = nullptr;
    if ((database = NifResAdbcDatabase::allocate_resource(env, error)) == nullptr) {
        return error;
    }

    database->val = (struct AdbcDatabase *)enif_alloc(sizeof(struct AdbcDatabase));
    memset(database->val, 0, sizeof(struct AdbcDatabase));
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
    ERL_NIF_TERM ret, error;
    NifResAdbcDatabase * database = nullptr;
    if ((database = NifResAdbcDatabase::get_resource(env, argv[0], error)) == nullptr) {
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
    ERL_NIF_TERM ret, error;
    NifResAdbcDatabase * database = nullptr;
    if ((database = NifResAdbcDatabase::get_resource(env, argv[0], error)) == nullptr) {
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
    ERL_NIF_TERM ret, error;
    NifResAdbcDatabase * database = nullptr;
    if ((database = NifResAdbcDatabase::get_resource(env, argv[0], error)) == nullptr) {
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

static int on_load(ErlNifEnv *env, void **, ERL_NIF_TERM) {
    ErlNifResourceType *rt;

    rt = enif_open_resource_type(env, "Elixir.ADBC.Nif", "NifResAdbcDatabase", NifResAdbcDatabase::destruct_resource, ERL_NIF_RT_CREATE, NULL);
    if (!rt) return -1;
    NifResAdbcDatabase::type = rt;

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
    {"adbc_database_release", 1, adbc_database_release, 0}
};

ERL_NIF_INIT(Elixir.Adbc.Nif, nif_functions, on_load, on_reload, on_upgrade, NULL);

#if defined(__GNUC__)
#pragma GCC visibility push(default)
#endif
