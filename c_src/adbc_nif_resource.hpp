#ifndef ADBC_NIF_RESOURCE_HPP
#define ADBC_NIF_RESOURCE_HPP

#include <erl_nif.h>
#include <atomic>
#include <adbc.h>

template <typename T>
struct NifRes {
    using val_type_p = T *;
    using val_type = T;
    using res_type = NifRes<T>;

    val_type val;

    static ErlNifResourceType * type;
    static res_type * allocate_resource(ErlNifEnv * env, ERL_NIF_TERM &error) {
        res_type * res = (res_type *)enif_alloc_resource(res_type::type, sizeof(res_type));
        if (res == nullptr) {
            error = erlang::nif::error(env, "cannot allocate Nif resource");
            return res;
        }
        memset(&res->val, 0, sizeof(val_type));
        return res;
    }

    static res_type * get_resource(ErlNifEnv * env, ERL_NIF_TERM term, ERL_NIF_TERM &error) {
        res_type * self_res = nullptr;
        if (!enif_get_resource(env, term, res_type::type, reinterpret_cast<void **>(&self_res)) || self_res == nullptr) {
            error = erlang::nif::error(env, "cannot access Nif resource");
        }

        return self_res;
    }

    static void destruct_resource(ErlNifEnv *env, void *args);
};

template <typename T>
void NifRes<T>::destruct_resource(ErlNifEnv *env, void *args) {
}

template <>
void NifRes<struct AdbcError>::destruct_resource(ErlNifEnv *env, void *args) {
    auto res = (NifRes<struct AdbcError> *)args;
    if (res) {
        if (res->val.release) {
            res->val.release(&res->val);
        }
    }
}

template <>
void NifRes<struct ArrowArrayStream>::destruct_resource(ErlNifEnv *env, void *args) {
    auto res = (NifRes<struct ArrowArrayStream> *)args;
    if (res) {
        if (res->val.release) {
            res->val.release(&res->val);
        }
    }
}

#endif  /* ADBC_NIF_RESOURCE_HPP */
