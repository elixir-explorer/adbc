#ifndef ADBC_HALF_FLOAT_HPP
#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>

// Function to convert float16 (IEEE 754 half-precision) to float
float float16_to_float(uint16_t value) {
    static_assert(std::numeric_limits<float>::is_iec559, "IEEE 754 required");
    
    uint16_t sign = (value >> 15) & 0x1;
    uint16_t exp = (value >> 10) & 0x1F;
    uint16_t mant = value & 0x3FF;
    
    uint32_t fbits;
    
    if (exp == 0) {
        if (mant == 0) {
            // Zero
            fbits = sign << 31;
        } else {
            // Denormalized number
            exp = 127 - 15 + 1;
            while ((mant & 0x400) == 0) {
                mant <<= 1;
                exp--;
            }
            mant &= 0x3FF;
            fbits = (sign << 31) | (exp << 23) | (mant << 13);
        }
    } else if (exp == 0x1F) {
        // Infinity or NaN
        fbits = (sign << 31) | 0x7F800000 | (mant << 13);
    } else {
        // Normalized number
        exp = exp - 15 + 127;
        fbits = (sign << 31) | (exp << 23) | (mant << 13);
    }
    
    float result;
    memcpy(&result, &fbits, sizeof(result));
    return result;
}

// Function to convert float to float16 (IEEE 754 half-precision)
uint16_t float_to_float16(float value) {
    static_assert(std::numeric_limits<float>::is_iec559, "IEEE 754 required");

    uint32_t fbits;
    memcpy(&fbits, &value, sizeof(fbits));
    
    uint16_t sign = (fbits >> 16) & 0x8000;   // sign bit
    int16_t exp = ((fbits >> 23) & 0xFF) - 127 + 15; // exponent
    uint32_t mant = fbits & 0x007FFFFF;  // mantissa
    
    if (exp <= 0) {
        if (exp < -10) {
            // Too small to be represented as a normalized float16
            return sign;
        }
        // Denormalized half-precision
        mant = (mant | 0x00800000) >> (1 - exp);
        return sign | (mant >> 13);
    } else if (exp == 0xFF - (127 - 15)) {
        if (mant == 0) {
            // Infinity
            return sign | 0x7C00;
        } else {
            // NaN
            return sign | 0x7C00 | (mant >> 13);
        }
    } else if (exp > 30) {
        // Overflow to infinity
        return sign | 0x7C00;
    }
    
    // Normalized half-precision
    return sign | (exp << 10) | (mant >> 13);
}

#endif  // ADBC_HALF_FLOAT_HPP
