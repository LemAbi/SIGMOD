#pragma once

#include "columnar.h"

#include <cstdint>
#include <unordered_map>
#include <vector>

inline void ProbeAndCollectStr(std::unordered_map<std::string, std::vector<size_t>>* tbl,
    SensibleColumnarTable*                                                           tbl_l,
    SensibleColumnarTable*                                                           tbl_r,
    SensibleColumnarTable*                                                           results,
    bool                                             hashed_is_left,
    size_t                                           curr_id,
    const std::vector<std::tuple<size_t, DataType>>* output_attrs,
    void*                                            page,
    uint16_t*                                        current_non_null,
    uint16_t*                                        current_str_begin,
    uint16_t                                         str_base_offset) {
    uint8_t*  u8_p    = reinterpret_cast<uint8_t*>(page);
    uint16_t* u16_p   = reinterpret_cast<uint16_t*>(page);
    void*     str     = &u8_p[*current_str_begin];
    uint16_t  str_end = u16_p[*current_non_null + 2] + str_base_offset;
    uint16_t  str_len = str_end - *current_str_begin;

    // TODO: I'm sure one of the 20 constructors could do this without this copy
    char* tmp_str = (char*)malloc(str_len + 1);
    memcpy(tmp_str, str, str_len);
    tmp_str[str_len] = '\0';
    std::string key(tmp_str);

    if (auto itr = tbl->find(key); itr != tbl->end()) {
        std::vector<size_t>& matches   = itr->second;
        size_t               match_cnt = matches.size();
        for (size_t k = 0; k < match_cnt; k += 1) {
            CollectRecord(tbl_l,
                tbl_r,
                results,
                hashed_is_left ? matches[k] : curr_id,
                hashed_is_left ? curr_id : matches[k],
                output_attrs);
        }
    }
    free(tmp_str);

    *current_str_begin  = str_end;
    *current_non_null  += 1;
}

template <typename T>
inline void ProbeAndCollect(std::unordered_map<T, std::vector<size_t>>* tbl,
    T&                                                                  key,
    SensibleColumnarTable*                                              tbl_l,
    SensibleColumnarTable*                                              tbl_r,
    SensibleColumnarTable*                                              results,
    bool                                                                hashed_is_left,
    size_t                                                              curr_id,
    const std::vector<std::tuple<size_t, DataType>>*                    output_attrs) {
    if (auto itr = tbl->find(key); itr != tbl->end()) {
        std::vector<size_t>& matches   = itr->second;
        size_t               match_cnt = matches.size();
        for (size_t k = 0; k < match_cnt; k += 1) {
            CollectRecord(tbl_l,
                tbl_r,
                results,
                hashed_is_left ? matches[k] : curr_id,
                hashed_is_left ? curr_id : matches[k],
                output_attrs);
        }
    }
}

template <typename T, uint16_t batch_size>
inline void BatchProbe(T*                            data,
    size_t                                           start_id,
    size_t                                           start_non_null_id,
    SensibleColumnarTable*                           tbl_l,
    SensibleColumnarTable*                           tbl_r,
    SensibleColumnarTable*                           result,
    std::unordered_map<T, std::vector<size_t>>*      hash_tbl,
    const std::vector<std::tuple<size_t, DataType>>* output_attrs,
    bool                                             hashed_is_left) {
    size_t global_id   = start_id;
    size_t non_null_id = start_non_null_id;
    for (size_t i = 0; i < batch_size; i += 1) {
        T& key = data[non_null_id++];
        ProbeAndCollect(hash_tbl,
            key,
            tbl_l,
            tbl_r,
            result,
            hashed_is_left,
            global_id++,
            output_attrs);
    }
}

template <uint16_t batch_size>
inline void BatchProbeStr(size_t                          start_id,
    SensibleColumnarTable*                                tbl_l,
    SensibleColumnarTable*                                tbl_r,
    SensibleColumnarTable*                                result,
    std::unordered_map<std::string, std::vector<size_t>>* hash_tbl,
    const std::vector<std::tuple<size_t, DataType>>*      output_attrs,
    bool                                                  hashed_is_left,
    void*                                                 page,
    uint16_t*                                             curr_str_begin,
    uint16_t*                                             non_null_id,
    uint16_t                                              str_base_offset) {
    size_t global_id = start_id;
    for (size_t i = 0; i < batch_size; i += 1) {
        ProbeAndCollectStr(hash_tbl,
            tbl_l,
            tbl_r,
            result,
            hashed_is_left,
            global_id++,
            output_attrs,
            page,
            non_null_id,
            curr_str_begin,
            str_base_offset);
    }
}

template <typename T>
inline uint16_t NonBatchProbe(T*                     data,
    size_t                                           start_id,
    size_t                                           start_non_null_id,
    uint16_t                                         to_test,
    SensibleColumnarTable*                           tbl_l,
    SensibleColumnarTable*                           tbl_r,
    SensibleColumnarTable*                           result,
    std::unordered_map<T, std::vector<size_t>>*      hash_tbl,
    const std::vector<std::tuple<size_t, DataType>>* output_attrs,
    bool                                             hashed_is_left,
    uint8_t*                                         bitmap) {
    size_t non_null_id = start_non_null_id;
    for (size_t id = 0; id < to_test; id += 1) {
        uint16_t byte_id = (id & ~bottom_three_bits_mask) >> 3;
        uint16_t bit_id  = id & bottom_three_bits_mask;
        if ((bitmap[byte_id] & (1 << bit_id)) != 0) {
            size_t global_id = start_id + id;
            T&     key       = data[non_null_id++];
            ProbeAndCollect(hash_tbl,
                key,
                tbl_l,
                tbl_r,
                result,
                hashed_is_left,
                id,
                output_attrs);
        }
    }
    return non_null_id;
}

inline void NonBatchProbeStr(size_t                       start_id,
    uint16_t                                              to_test,
    SensibleColumnarTable*                                tbl_l,
    SensibleColumnarTable*                                tbl_r,
    SensibleColumnarTable*                                result,
    std::unordered_map<std::string, std::vector<size_t>>* hash_tbl,
    const std::vector<std::tuple<size_t, DataType>>*      output_attrs,
    bool                                                  hashed_is_left,
    uint8_t*                                              bitmap,
    void*                                                 page,
    uint16_t*                                             curr_str_begin,
    uint16_t*                                             non_null_id,
    uint16_t                                              str_base_offset) {
    for (size_t id = 0; id < to_test; id += 1) {
        uint16_t byte_id = (id & ~bottom_three_bits_mask) >> 3;
        uint16_t bit_id  = id & bottom_three_bits_mask;
        if ((bitmap[byte_id] & (1 << bit_id)) != 0) {
            size_t global_id = start_id + id;
            ProbeAndCollectStr(hash_tbl,
                tbl_l,
                tbl_r,
                result,
                hashed_is_left,
                global_id,
                output_attrs,
                page,
                non_null_id,
                curr_str_begin,
                str_base_offset);
        }
    }
}

template <typename T, uint16_t batch_size>
void ProbePage(SensibleColumnarTable*                tbl_l,
    SensibleColumnarTable*                           tbl_r,
    SensibleColumnarTable*                           result,
    std::unordered_map<T, std::vector<size_t>>*      hash_tbl,
    size_t                                           col_id_of_non_hashed,
    Page*                                            page,
    PageDescriptor*                                  page_info,
    size_t                                           rows_in_prev_pages,
    const std::vector<std::tuple<size_t, DataType>>* output_attrs,
    bool                                             hashed_is_left) {
    assert(batch_size % 8 == 0);

    constexpr uint16_t     bytes_per_batch = batch_size / 8;
    RegularPageDescriptor* regular_info    = &page_info->regular;
    size_t                 non_null_cnt    = regular_info->non_null_in_page;
    size_t                 total_cnt       = regular_info->rows_in_page;
    T*                     data            = DataBegin<T>(page);

    size_t non_null_id = 0;
    size_t id          = rows_in_prev_pages;

    if (total_cnt == non_null_cnt) {
        for (size_t i = 0; i < non_null_cnt; i += 1) {
            T& key = data[non_null_id++];
            ProbeAndCollect(hash_tbl,
                key,
                tbl_l,
                tbl_r,
                result,
                hashed_is_left,
                id,
                output_attrs);
            id++;
        }
    } else {
        size_t   batch_cnt = non_null_cnt / batch_size;
        size_t   remaining = non_null_cnt % batch_size;
        uint8_t* bitmap    = BitMapBegin(page, regular_info);

        uint16_t cur_bitmap_offset = 0;
        for (size_t i = 0; i < batch_cnt; i += 1) {
            bool all_non_null;
            if constexpr (batch_size == 8) {
                all_non_null = bitmap[cur_bitmap_offset] == u8_max;
            } else if constexpr (batch_size == 64) {
                all_non_null =
                    *reinterpret_cast<uint64_t*>(&bitmap[cur_bitmap_offset]) == u64_max;
            } else {
                all_non_null = false;
            }

            if (all_non_null) {
                BatchProbe<T, batch_size>(data,
                    id,
                    non_null_id,
                    tbl_l,
                    tbl_r,
                    result,
                    hash_tbl,
                    output_attrs,
                    hashed_is_left);
                id                += batch_size;
                non_null_id       += batch_size;
                cur_bitmap_offset += bytes_per_batch;
            } else {
                non_null_id        = NonBatchProbe<T>(data,
                    id,
                    non_null_id,
                    batch_size,
                    tbl_l,
                    tbl_r,
                    result,
                    hash_tbl,
                    output_attrs,
                    hashed_is_left,
                    &bitmap[cur_bitmap_offset]);
                id                += batch_size;
                cur_bitmap_offset += bytes_per_batch;
            }
        }

        non_null_id = NonBatchProbe<T>(data,
            id,
            non_null_id,
            remaining,
            tbl_l,
            tbl_r,
            result,
            hash_tbl,
            output_attrs,
            hashed_is_left,
            &bitmap[cur_bitmap_offset]);
    }
}

// **IMPORTANT** Currently this can only handle regular str pages
// -> Either weed big ones out before dispatching to other threads or use page ids instead
// (single threaded path only uses this for the regular strs so no worries there)
template <uint16_t batch_size>
void ProbePageStr(SensibleColumnarTable*                  tbl_l,
    SensibleColumnarTable*                                tbl_r,
    SensibleColumnarTable*                                result,
    std::unordered_map<std::string, std::vector<size_t>>* hash_tbl,
    size_t                                                col_id_of_non_hashed,
    Page*                                                 page,
    PageDescriptor*                                       page_info,
    size_t                                                rows_in_prev_pages,
    const std::vector<std::tuple<size_t, DataType>>*      output_attrs,
    bool                                                  hashed_is_left) {
    assert(batch_size % 8 == 0);

    constexpr uint16_t     bytes_per_batch = batch_size / 8;
    RegularPageDescriptor* regular_info    = &page_info->regular;
    size_t                 non_null_cnt    = regular_info->non_null_in_page;
    size_t                 total_cnt       = regular_info->rows_in_page;
    uint16_t               str_base_offset = regular_info->non_null_in_page * 2 + 4;

    uint16_t non_null_id    = 0;
    size_t   id             = rows_in_prev_pages;
    uint16_t curr_str_begin = str_base_offset;
    if (total_cnt == non_null_cnt) {
        for (size_t i = 0; i < non_null_cnt; i += 1) {
            ProbeAndCollectStr(hash_tbl,
                tbl_l,
                tbl_r,
                result,
                hashed_is_left,
                id,
                output_attrs,
                page,
                &non_null_id,
                &curr_str_begin,
                str_base_offset);
            id++;
        }
    } else {
        size_t   batch_cnt = non_null_cnt / batch_size;
        size_t   remaining = non_null_cnt % batch_size;
        uint8_t* bitmap    = BitMapBegin(page, regular_info);

        uint16_t cur_bitmap_offset = 0;
        for (size_t i = 0; i < batch_cnt; i += 1) {
            bool all_non_null;
            if constexpr (batch_size == 8) {
                all_non_null = bitmap[cur_bitmap_offset] == u8_max;
            } else if constexpr (batch_size == 64) {
                all_non_null =
                    *reinterpret_cast<uint64_t*>(&bitmap[cur_bitmap_offset]) == u64_max;
            } else {
                all_non_null = false;
            }

            if (all_non_null) {
                BatchProbeStr<batch_size>(id,
                    tbl_l,
                    tbl_r,
                    result,
                    hash_tbl,
                    output_attrs,
                    hashed_is_left,
                    page,
                    &curr_str_begin,
                    &non_null_id,
                    str_base_offset);
                id                += batch_size;
                cur_bitmap_offset += bytes_per_batch;
            } else {
                NonBatchProbeStr(id,
                    batch_size,
                    tbl_l,
                    tbl_r,
                    result,
                    hash_tbl,
                    output_attrs,
                    hashed_is_left,
                    &bitmap[cur_bitmap_offset],
                    page,
                    &curr_str_begin,
                    &non_null_id,
                    str_base_offset);
                id                += batch_size;
                cur_bitmap_offset += bytes_per_batch;
            }
        }

        NonBatchProbeStr(id,
            remaining,
            tbl_l,
            tbl_r,
            result,
            hash_tbl,
            output_attrs,
            hashed_is_left,
            &bitmap[cur_bitmap_offset],
            page,
            &curr_str_begin,
            &non_null_id,
            str_base_offset);
    }
}
