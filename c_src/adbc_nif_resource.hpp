#ifndef ADBC_NIF_RESOURCE_HPP
#define ADBC_NIF_RESOURCE_HPP

#include <erl_nif.h>
#include <atomic>
#include <memory>
#include <type_traits>
#include <adbc.h>

// Only for debugging:
#include <cstdio>

template<typename T> struct NoOpDeleter;

template <typename...>
using void_t = void;

// Check for the existence of member variable 'release' using SFINAE
template <typename T, typename = void>
struct release_guard : std::false_type {};

template <typename T>
struct release_guard<T, void_t<decltype(T::release)>> : std::true_type {};

/// A NifRes<T> wraps a `T` to allow it to be shared between C++ and Erlang while managing its lifetime properly.
///
/// Because the in-memory representation of a `*NifRes<T>` is the same as a `*T`,
/// the result from `my_nif_res->get_resource()` can be passed to a NIF
/// written in other languages as well (as long as they know the memory representation of T).
///
/// Lifetime is managed automatically by:
/// - Returning an owned pointer from `allocate_resource` and `clone_ref`. Behind the scenes, these calls manipulate Erlang's refcount of the resource object.
/// - Whenever one of these owned pointers leaves the scope, the refcount is decremented.
/// - Separately, a `destruct_resource` callback is provided to Erlang which will be called by the GC when there are no references (from either Erlang or C++ side) remaining.
template <typename T>
struct NifRes {
    using val_type_p = T *;
    using val_type = T;
    using res_type = NifRes<T>;

    val_type val;

    static ErlNifResourceType * type;

    /// Creates a new NifRes<T> using `enif_alloc_resource`, returning it as an owned pointer.
    /// When this owned pointer leaves the scope, `enif_release_resource` is automatically called.
    static auto allocate_resource(ErlNifEnv * env, ERL_NIF_TERM &error) -> std::unique_ptr<NifRes<T>, NoOpDeleter<res_type>> {
        std::unique_ptr<NifRes<T>, NoOpDeleter<res_type>> res{static_cast<res_type *>(enif_alloc_resource(res_type::type, sizeof(res_type)))};
        if (res == nullptr) {
            error = erlang::nif::error(env, "cannot allocate Nif resource\n");
            return res;
        }
        memset(&res->val, 0, sizeof(val_type));

        printf("%p: Allocating resource (and setting refcount to 1)\n", &*res);

        return res;
    }

    /// Given a `term` that should be a NifRes<T>,
    /// Obtain a pointer to the contained `val`
    /// which is guaranteed to be valid for at least the lifetime of `env`.
    ///
    /// In the case something which is not a NifRes<T> is passed,
    /// `nullptr` is returned and the output-parameter `error` filled.
    static res_type * get_resource(ErlNifEnv * env, ERL_NIF_TERM term, ERL_NIF_TERM &error) {
        res_type * self_res = nullptr;
        if (!enif_get_resource(env, term, res_type::type, reinterpret_cast<void **>(&self_res)) || self_res == nullptr) {
            error = erlang::nif::error(env, "cannot access Nif resource");
        }

        return self_res;
    }

    /// Creates another reference to the same underlying NifRes to be used in Erlang.
    /// (uses `enif_make_resource`)
    ERL_NIF_TERM make_resource(ErlNifEnv *env) {
        return enif_make_resource(env, this);
    }

    /// Creates another reference to the same underlying NifRes to be used in C++.
    /// (Increments the reference count on the C++ side).
    auto clone_ref() const -> std::unique_ptr<NifRes<T>, NoOpDeleter<res_type>> {
        printf("%p: Incrementing resource refcount\n", &*this);
        enif_keep_resource(this);
        return std::unique_ptr<NifRes<T>>{this};
    }

    // Called whenever a _single_ reference to the resource goes out of scope.
    // Decrements the reference count on the C++ side.
    ~NifRes() {
      printf("%p: Decrementing resource count\n", this);
      enif_release_resource(this);
    }

    // Callback which is called by the Erlang GC when the _last_ reference to the NifRes goes out of scope.
    // If there is special cleanup that should happen for a particular child-class,
    // create a template specialization for it.
    template <typename R = T>
    static auto destruct_resource(ErlNifEnv *env, void *args) -> typename std::enable_if<release_guard<R>::value, void>::type {
        auto res = (NifRes<T> *)args;
        printf("%p: Destructing resource (release %s)\n", res, typeid(R).name());
        if (res) {
            if (res->val.release) {
                res->val.release(&res->val);
            }
        }
    }    

    template <typename R = T>
    static auto destruct_resource(ErlNifEnv *env, void *args) -> typename std::enable_if<!release_guard<R>::value, void>::type {
        printf("%p: Destructing resource\n", args);
    }
};

// Used to construct a unique_ptr wrapping memory that is managed remotely.
// The value in this memory *does* need to be destructed
// but *should not* be deallocated using `delete`.
template<typename T>
struct NoOpDeleter {
  void operator()(T* val) {
    // Do destruct
    val->~T();
    // Do not delete; we do not own the memory
  }
};

#endif  /* ADBC_NIF_RESOURCE_HPP */
