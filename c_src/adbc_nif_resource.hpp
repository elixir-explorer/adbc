#ifndef ADBC_NIF_RESOURCE_HPP
#define ADBC_NIF_RESOURCE_HPP

#pragma once

#include <erl_nif.h>
#include <adbc.h>

template <typename T>
struct NifRes {
    using val_type_p = T *;
    using val_type = typename std::remove_pointer<val_type_p>::type;
    using res_type = NifRes<T>;

    val_type_p val;

    static ErlNifResourceType * type;
    static res_type * allocate_resource(ErlNifEnv * env, ERL_NIF_TERM &error) {
        res_type * res = (res_type *)enif_alloc_resource(res_type::type, sizeof(res_type));
        if (res == nullptr) {
            error = erlang::nif::error(env, "cannot allocate Nif resource");
            return res;
        }
        return res;
    }

    static res_type * get_resource(ErlNifEnv * env, ERL_NIF_TERM term, ERL_NIF_TERM &error) {
        res_type * self_res = nullptr;
        if (!enif_get_resource(env, term, res_type::type, (void **)&self_res) || self_res == nullptr || self_res->val == nullptr) {
            error = erlang::nif::error(env, "cannot access Nif resource");
        }
        return self_res;
    }

    static void destruct_resource(ErlNifEnv *env, void *args);
};

template <typename T>
void NifRes<T>::destruct_resource(ErlNifEnv *env, void *args) {
    auto res = (NifRes<T> *)args;
    if (res) {
        if (res->val) {
            enif_free(res->val);
            res->val = nullptr;
        }
    }
}

template <>
void NifRes<struct AdbcError>::destruct_resource(ErlNifEnv *env, void *args) {
    auto res = (NifRes<struct AdbcError> *)args;
    if (res) {
        if (res->val) {
            auto adbc_error = (struct AdbcError *)res->val;
            if (adbc_error->release) {
                adbc_error->release(adbc_error);
            }
            enif_free(res->val);
            res->val = nullptr;
        }
    }
}

#endif  /* ADBC_NIF_RESOURCE_HPP */
