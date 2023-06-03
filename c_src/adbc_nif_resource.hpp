#ifndef ADBC_NIF_RESOURCE_HPP
#define ADBC_NIF_RESOURCE_HPP

#pragma once

#include <erl_nif.h>
#include <atomic>
#include <adbc.h>

template <typename T>
struct NifRes {
    using val_type_p = T *;
    using val_type = T;
    using res_type = NifRes<T>;

    val_type val;
    std::atomic_bool released{false};

    static ErlNifResourceType * type;
    static res_type * allocate_resource(ErlNifEnv * env, ERL_NIF_TERM &error) {
        res_type * res = (res_type *)enif_alloc_resource(res_type::type, sizeof(res_type));
        if (res == nullptr) {
            error = erlang::nif::error(env, "cannot allocate Nif resource");
            return res;
        }
        memset(&res->val, 0, sizeof(val_type));
        res->released = false;
        return res;
    }

    static res_type * get_resource(ErlNifEnv * env, ERL_NIF_TERM term, ERL_NIF_TERM &error) {
        res_type * self_res = nullptr;
        if (!enif_get_resource(env, term, res_type::type, (void **)&self_res) || self_res == nullptr) {
            error = erlang::nif::error(env, "cannot access Nif resource");
        }

        if (self_res->released) {
            self_res = nullptr;
            error = enif_make_badarg(env);
        }

        return self_res;
    }

    static void destruct_resource(ErlNifEnv *env, void *args);
};

template <typename T>
void NifRes<T>::destruct_resource(ErlNifEnv *env, void *args) {
    auto res = (NifRes<T> *)args;
    res->released = true;
    return;
}

template <>
void NifRes<struct AdbcError>::destruct_resource(ErlNifEnv *env, void *args) {
    auto res = (NifRes<struct AdbcError> *)args;
    if (res) {
        if (!res->released && res->val.release) {
            res->val.release(&res->val);
        }
        res->released = true;
    }
}

template <>
void NifRes<struct ArrowArrayStream>::destruct_resource(ErlNifEnv *env, void *args) {
    auto res = (NifRes<struct ArrowArrayStream> *)args;
    if (res) {
        if (!res->released && res->val.release) {
            res->val.release(&res->val);
        }
        res->released = true;
    }
}

#endif  /* ADBC_NIF_RESOURCE_HPP */
