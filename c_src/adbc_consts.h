#ifndef ADBC_CONSTS_H
#pragma once

#include <erl_nif.h>

// Atoms
static ERL_NIF_TERM kAtomAdbcError;
static ERL_NIF_TERM kAtomNil;
static ERL_NIF_TERM kAtomTrue;
static ERL_NIF_TERM kAtomFalse;
static ERL_NIF_TERM kAtomEndOfSeries;
static ERL_NIF_TERM kAtomStructKey;
static ERL_NIF_TERM kAtomTime32;
static ERL_NIF_TERM kAtomTime64;
static ERL_NIF_TERM kAtomDuration;
static ERL_NIF_TERM kAtomSeconds;
static ERL_NIF_TERM kAtomMilliseconds;
static ERL_NIF_TERM kAtomMicroseconds;
static ERL_NIF_TERM kAtomNanoseconds;
static ERL_NIF_TERM kAtomDecimal;
static ERL_NIF_TERM kAtomTimestamp;
static ERL_NIF_TERM kAtomFixedSizeBinary;

static ERL_NIF_TERM kAtomCalendarKey;
static ERL_NIF_TERM kAtomCalendarISO;

static ERL_NIF_TERM kAtomDateModule;
static ERL_NIF_TERM kAtomYearKey;
static ERL_NIF_TERM kAtomMonthKey;
static ERL_NIF_TERM kAtomDayKey;

static ERL_NIF_TERM kAtomNaiveDateTimeModule;
static ERL_NIF_TERM kAtomTimeModule;
static ERL_NIF_TERM kAtomHourKey;
static ERL_NIF_TERM kAtomMinuteKey;
static ERL_NIF_TERM kAtomSecondKey;
static ERL_NIF_TERM kAtomMicrosecondKey;

static ERL_NIF_TERM kAtomAdbcColumnModule;
static ERL_NIF_TERM kAtomNameKey;
static ERL_NIF_TERM kAtomTypeKey;
static ERL_NIF_TERM kAtomNullableKey;
static ERL_NIF_TERM kAtomMetadataKey;
static ERL_NIF_TERM kAtomDataKey;

static ERL_NIF_TERM kAdbcColumnTypeU8;
static ERL_NIF_TERM kAdbcColumnTypeU16;
static ERL_NIF_TERM kAdbcColumnTypeU32;
static ERL_NIF_TERM kAdbcColumnTypeU64;
static ERL_NIF_TERM kAdbcColumnTypeI8;
static ERL_NIF_TERM kAdbcColumnTypeI16;
static ERL_NIF_TERM kAdbcColumnTypeI32;
static ERL_NIF_TERM kAdbcColumnTypeI64;
static ERL_NIF_TERM kAdbcColumnTypeF32;
static ERL_NIF_TERM kAdbcColumnTypeF64;
static ERL_NIF_TERM kAdbcColumnTypeStruct;
static ERL_NIF_TERM kAdbcColumnTypeMap;
static ERL_NIF_TERM kAdbcColumnTypeList;
static ERL_NIF_TERM kAdbcColumnTypeLargeList;
static ERL_NIF_TERM kAdbcColumnTypeFixedSizeList;
static ERL_NIF_TERM kAdbcColumnTypeString;
static ERL_NIF_TERM kAdbcColumnTypeLargeString;
static ERL_NIF_TERM kAdbcColumnTypeBinary;
static ERL_NIF_TERM kAdbcColumnTypeLargeBinary;
static ERL_NIF_TERM kAdbcColumnTypeDenseUnion;
static ERL_NIF_TERM kAdbcColumnTypeSparseUnion;
static ERL_NIF_TERM kAdbcColumnTypeDate32;
static ERL_NIF_TERM kAdbcColumnTypeDate64;
static ERL_NIF_TERM kAdbcColumnTypeBool;

// tuples cannot be made in advance
#define kAdbcColumnTypeTime32Seconds enif_make_tuple2(env, kAtomTime32, kAtomSeconds)
#define kAdbcColumnTypeTime32Milliseconds enif_make_tuple2(env, kAtomTime32, kAtomMilliseconds)
#define kAdbcColumnTypeTime64Microseconds enif_make_tuple2(env, kAtomTime64, kAtomMicroseconds)
#define kAdbcColumnTypeTime64Nanoseconds enif_make_tuple2(env, kAtomTime64, kAtomNanoseconds)
#define kAdbcColumnTypeDurationSeconds enif_make_tuple2(env, kAtomDuration, kAtomSeconds)
#define kAdbcColumnTypeDurationMilliseconds enif_make_tuple2(env, kAtomDuration, kAtomMilliseconds)
#define kAdbcColumnTypeDurationMicroseconds enif_make_tuple2(env, kAtomDuration, kAtomMicroseconds)
#define kAdbcColumnTypeDurationNanoseconds enif_make_tuple2(env, kAtomDuration, kAtomNanoseconds)
#define kAdbcColumnTypeDecimal(bitwidth, precision, scale) enif_make_tuple4(env, kAtomDecimal, enif_make_int(env, bitwidth), enif_make_int(env, precision), enif_make_int(env, scale))
#define kAdbcColumnTypeFixedSizeBinary(nbytes) enif_make_tuple2(env, kAtomFixedSizeBinary, enif_make_int64(env, nbytes))

// error codes
constexpr int kErrorBufferIsNotAMap = 1;
constexpr int kErrorBufferGetDataListLength = 2;
constexpr int kErrorBufferGetMapValue = 3;
constexpr int kErrorBufferWrongStruct = 4;
constexpr int kErrorBufferDataIsNotAList = 5;
constexpr int kErrorBufferUnknownType = 6;
constexpr int kErrorBufferGetMetadataKey = 7;
constexpr int kErrorBufferGetMetadataValue = 8;
constexpr int kErrorExpectedCalendarISO = 9;

#endif  // ADBC_CONSTS_H
