#pragma once

#include <attribute.h>

#include <cassert>
#include <cstdint>
#include <iostream>

template <typename T>
inline uint16_t AlingDTOffset() {
    return alignof(T) > 4 ? alignof(T) : 4;
}
template <>
uint16_t AlingDTOffset<char>() = delete;
template <>
uint16_t AlingDTOffset<char*>() = delete;

static inline uint16_t AlingDTOffset(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return AlingDTOffset<int32_t>();
    case DataType::INT64:   return AlingDTOffset<int64_t>();
    case DataType::FP64:    return AlingDTOffset<double>();
    case DataType::VARCHAR: assert(false);
    }
    return 42; // unreachable - cpp sucks
}

static inline uint8_t SizeDT(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return 4;
    case DataType::INT64:   return 8;
    case DataType::FP64:    return 8;
    case DataType::VARCHAR: assert(false);
    }
    return 42; // unreachable - cpp sucks
}

static inline uint8_t Sizeof(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return 4;
    case DataType::INT64:   return 8;
    case DataType::FP64:    return 8;
    case DataType::VARCHAR: assert(false);
    }
    return 42; // unreachable - cpp sucks
}

// Debug area:

static const char* dt_strs[4] = {
    "i32",
    "i64",
    "fp64",
    "string",
};

static const char* DtToString(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return dt_strs[0];
    case DataType::INT64:   return dt_strs[1];
    case DataType::FP64:    return dt_strs[2];
    case DataType::VARCHAR: return dt_strs[3];
    }
    return nullptr; // unreachable - cpp sucks
}

// Assumes that if data_type == Varchar that value has already been changed to be null terminated
static void PrintVal(void* value, DataType data_type) {
    if (value == nullptr) {
        std::cout << "Null\t";
        return;
    }
    switch (data_type) {
    case DataType::INT32:   std::cout << *reinterpret_cast<int32_t*>(value) << "\t"; break;
    case DataType::INT64:   std::cout << *reinterpret_cast<int64_t*>(value) << "\t"; break;
    case DataType::FP64:    std::cout << *reinterpret_cast<double*>(value) << "\t"; break;
    case DataType::VARCHAR: std::cout << reinterpret_cast<char*>(value) << "\t"; break;
    }
}

