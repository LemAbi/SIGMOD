#include <attribute.h>
#include <plan.h>
#include <table.h>

// TODO: Remove tray before submission
// #include "../build/_deps/tracy-src/public/tracy/Tracy.hpp"
#include "columnar.h"
#include "thread_pool.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace Contest {
static constexpr uint8_t u8_max = 0xffu;

SensibleColumnarTable execute_impl(const Plan& plan, size_t node_idx);

template <typename T>
inline void
InsertToHashmap(std::unordered_map<T, std::vector<size_t>>& tbl, T& key, size_t id) {
    if (auto itr = tbl.find(key); itr == tbl.end()) {
        tbl.emplace(key, std::vector<size_t>(1, id));
    } else {
        itr->second.push_back(id);
    }
}

inline void InsertStrToHashmap(std::unordered_map<std::string, std::vector<size_t>>& tbl,
    size_t                                                                           id,
    void*                                                                            page,
    uint16_t* current_str_begin,
    uint16_t* current_non_null_id,
    uint16_t  str_base_offset) {
    uint16_t* u16_p   = reinterpret_cast<uint16_t*>(page);
    uint8_t*  u8_p    = reinterpret_cast<uint8_t*>(page);
    void*     str     = &u8_p[*current_str_begin];
    uint16_t  str_end = u16_p[*current_non_null_id + 2] + str_base_offset;
    uint16_t  str_len = str_end - *current_str_begin;

    // TODO: I'm sure one of the 20 constructors could do this without this copy
    char* tmp_str = (char*)malloc(str_len + 1);
    memcpy(tmp_str, str, str_len);
    tmp_str[str_len] = '\0';
    std::string key(tmp_str);
    free(tmp_str);
    InsertToHashmap<std::string>(tbl, key, id);

    *current_non_null_id += 1;
    *current_str_begin    = str_end;
}

template <typename T>
void BuildHashTbl(SensibleColumnarTable&        input_tbl,
    size_t                                      col_id,
    std::unordered_map<T, std::vector<size_t>>& tbl) {
    // ZoneScoped;
    SensibleColumn& clm_to_hash         = input_tbl.columns[col_id];
    size_t          page_cnt            = clm_to_hash.pages.size();
    size_t          items_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*                 cur_page     = clm_to_hash.pages[i].page;
        PageDescriptor        page_info_   = clm_to_hash.pages[i].page_info;
        RegularPageDescriptor page_info    = clm_to_hash.pages[i].page_info.regular;
        T*                    data         = DataBegin<T>(cur_page);
        uint8_t*              bitmap_begin = page_info_.BitMapBegin(cur_page);
        size_t                id           = items_in_prev_pages;

        uint16_t processed     = 0;
        uint16_t cur_bitmap_id = 0;
        if (page_info.non_null_in_page == page_info.rows_in_page) {
            for (size_t j = 0; j < page_info.rows_in_page; j += 1) {
                InsertToHashmap<T>(tbl, data[processed++], id);
                id++;
            }
        } else {
            while (processed < page_info.non_null_in_page) {
                // NOTE: testing at larger than byte granularity could be faster overall
                // (e.g. 512/256bit)
                if (bitmap_begin[cur_bitmap_id] == u8_max
                    && (page_info.non_null_in_page - processed >= 8)) {
                    // Full byte not null
                    for (size_t i = 0; i < 8; i += 1) {
                        InsertToHashmap<T>(tbl, data[processed++], id);
                        id++;
                    }
                } else {
                    // Some not null
                    uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                    for (uint8_t intra_bitmap_id  = 0; intra_bitmap_id < 8;
                        intra_bitmap_id          += 1) {
                        if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                            InsertToHashmap<T>(tbl, data[processed++], id);
                        }
                        id++;
                        if (processed >= page_info.non_null_in_page) {
                            break;
                        }
                    }
                }
                cur_bitmap_id += 1;
            }
        }
        items_in_prev_pages += page_info.rows_in_page;
    }
}

void BuildHashTblStr(SensibleColumnarTable&               input_tbl,
    size_t                                                col_id,
    std::unordered_map<std::string, std::vector<size_t>>& tbl) {
    // ZoneScoped;
    SensibleColumn& clm_to_hash         = input_tbl.columns[col_id];
    size_t          page_cnt            = clm_to_hash.pages.size();
    size_t          items_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*          cur_page   = clm_to_hash.pages[i].page;
        PageDescriptor page_info_ = clm_to_hash.pages[i].page_info;
        size_t         id         = items_in_prev_pages;

        switch (page_info_.type) {
        case PageType::Regular: {
            uint8_t*              bitmap_begin      = page_info_.BitMapBegin(cur_page);
            RegularPageDescriptor page_info         = page_info_.regular;
            uint16_t              processed         = 0;
            uint16_t              cur_bitmap_id     = 0;
            uint16_t              str_base_offset   = page_info.non_null_in_page * 2 + 4;
            uint16_t              current_str_begin = str_base_offset;
            if (page_info.rows_in_page == page_info.non_null_in_page) {
                for (size_t j = 0; j < page_info.rows_in_page; j += 1) {
                    InsertStrToHashmap(tbl,
                        id,
                        cur_page,
                        &current_str_begin,
                        &processed,
                        str_base_offset);
                    id++;
                }
            } else {
                while (processed < page_info.non_null_in_page) {
                    // NOTE: testing at larger than byte granularity could be faster overall
                    // (e.g. 512/256bit)
                    if (bitmap_begin[cur_bitmap_id] == u8_max
                        && (page_info.non_null_in_page - processed >= 8)) {
                        // Full byte not null
                        for (size_t i = 0; i < 8; i += 1) {
                            InsertStrToHashmap(tbl,
                                id,
                                cur_page,
                                &current_str_begin,
                                &processed,
                                str_base_offset);
                            id++;
                        }
                    } else {
                        // Some not null
                        uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                        for (uint8_t intra_bitmap_id  = 0; intra_bitmap_id < 8;
                            intra_bitmap_id          += 1) {
                            if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                                InsertStrToHashmap(tbl,
                                    id,
                                    cur_page,
                                    &current_str_begin,
                                    &processed,
                                    str_base_offset);
                            }
                            id++;
                            if (processed >= page_info.non_null_in_page) {
                                break;
                            }
                        }
                    }
                    cur_bitmap_id += 1;
                }
            }
            items_in_prev_pages += page_info.rows_in_page;
            break;
        };
        case PageType::LargeStrFirst: {
            char*       long_str = ConcatLargeString(i, clm_to_hash);
            std::string key(long_str);
            InsertToHashmap<std::string>(tbl, key, id);
            free(long_str);
            items_in_prev_pages += 1;
            break;
        };
        case PageType::LargeStrSubsequent: break; // These have already been processed above
        }
    }
}

// TODO: this is not particularly efficient. We could instead collect all ids and then collect
// the attr later column-wise
void CollectRecord(SensibleColumnarTable&            tbl_l,
    SensibleColumnarTable&                           tbl_r,
    SensibleColumnarTable&                           results,
    size_t                                           record_id_l,
    size_t                                           record_id_r,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    size_t attr_cnt   = output_attrs.size();
    size_t attr_l_cnt = tbl_l.columns.size();
    for (size_t i = 0; i < attr_cnt; i += 1) {
        size_t col_id   = std::get<0>(output_attrs[i]);
        size_t use_left = col_id < attr_l_cnt;

        SensibleColumnarTable& tbl_to_use       = use_left ? tbl_l : tbl_r;
        size_t                 col_id_to_use    = use_left ? col_id : col_id - attr_l_cnt;
        size_t                 record_id_to_use = use_left ? record_id_l : record_id_r;

        bool   is_large_str                    = false;
        size_t page_id_of_large_str_or_str_len = 0;
        void*  attr                            = GetValueClmn(record_id_to_use,
            tbl_to_use.columns[col_id_to_use],
            &is_large_str,
            &page_id_of_large_str_or_str_len);

        if (!is_large_str) {
            if (attr == nullptr) {
                AppendNull(results.columns[i]);
            } else {
            }
            if (tbl_to_use.columns[col_id_to_use].type == DataType::VARCHAR) {
                AppendStr(attr, page_id_of_large_str_or_str_len, results.columns[i]);
            } else {
                AppendAttr(attr, results.columns[i]);
            }
        } else {
            // TODO: this copy could be elided see comment in execute_hash_join()
            results.columns[i].AddPageCopy(
                tbl_to_use.columns[col_id_to_use].pages[page_id_of_large_str_or_str_len].page);

            for (size_t j = page_id_of_large_str_or_str_len + 1;
                j < tbl_to_use.columns[col_id_to_use].pages.size();
                j += 1) {
                uint16_t* u16_p = reinterpret_cast<uint16_t*>(
                    tbl_to_use.columns[col_id_to_use].pages[j].page);
                if (u16_p[0] == is_subsequent_big_str_page) {
                    // TODO: this copy could be elided see comment in execute_hash_join()
                    results.columns[i].AddPageCopy(
                        tbl_to_use.columns[col_id_to_use].pages[j].page);
                } else {
                    break;
                }
            }
        }
    }
    results.num_rows++;
}

template <typename T>
void ValidateJoinMatch(size_t l_col_id,
    size_t                    r_col_id,
    size_t                    l_row_id,
    size_t                    r_row_id,
    SensibleColumnarTable&    tbl_l,
    SensibleColumnarTable&    tbl_r,
    T                         key) {
    bool   is_large_str = false;
    size_t dummy_id     = 0;
    T* val_l = (T*)GetValueClmn(l_row_id, tbl_l.columns[l_col_id], &is_large_str, &dummy_id);
    T* val_r = (T*)GetValueClmn(r_row_id, tbl_r.columns[r_col_id], &is_large_str, &dummy_id);
    if (*val_l != key || *val_r != key) {
        std::cout << "Match Validation failed. key: " << key << " l " << *val_l << " r "
                  << *val_r << std::endl;
        std::abort();
    }
}

template <typename T>
inline void ProbeAndCollect(std::unordered_map<T, std::vector<size_t>>& tbl,
    T&                                                                  key,
    SensibleColumnarTable&                                              tbl_l,
    SensibleColumnarTable&                                              tbl_r,
    SensibleColumnarTable&                                              results,
    bool                                                                hashed_is_left,
    size_t                                                              curr_id,
    const std::vector<std::tuple<size_t, DataType>>&                    output_attrs,
    size_t                                                              l_col_id,
    size_t                                                              r_col_id) {
    if (auto itr = tbl.find(key); itr != tbl.end()) {
        std::vector<size_t>& matches   = itr->second;
        size_t               match_cnt = matches.size();
        for (size_t k = 0; k < match_cnt; k += 1) {
            if constexpr (!std::is_same<T, std::string>()) {
                ValidateJoinMatch<T>(l_col_id,
                    r_col_id,
                    hashed_is_left ? matches[k] : curr_id,
                    hashed_is_left ? curr_id : matches[k],
                    tbl_l,
                    tbl_r,
                    key);
            }
            CollectRecord(tbl_l,
                tbl_r,
                results,
                hashed_is_left ? matches[k] : curr_id,
                hashed_is_left ? curr_id : matches[k],
                output_attrs);
        }
    }
}

inline void ProbeAndCollectStr(std::unordered_map<std::string, std::vector<size_t>>& tbl,
    SensibleColumnarTable&                                                           tbl_l,
    SensibleColumnarTable&                                                           tbl_r,
    SensibleColumnarTable&                                                           results,
    bool                                             hashed_is_left,
    size_t                                           curr_id,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
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

    if (auto itr = tbl.find(key); itr != tbl.end()) {
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
void Probe(SensibleColumnarTable&               tbl_l,
    SensibleColumnarTable&                      tbl_r,
    size_t                                      col_id_of_non_hashed_in,
    size_t                                      l_col_id,
    size_t                                      r_col_id,
    std::unordered_map<T, std::vector<size_t>>& tbl,
    // TODO: when para -> use one result table for each and then "merge" which is just
    // collecting the pages per column in one result table i.e. no need to actually cpy
    // smth between pages
    SensibleColumnarTable&                           results,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    bool                                             hashed_is_left) {
    // ZoneScoped;
    SensibleColumnarTable& non_hashed_tbl = hashed_is_left ? tbl_r : tbl_l;
    SensibleColumn&        clm_to_check   = non_hashed_tbl.columns[col_id_of_non_hashed_in];

    size_t page_cnt            = clm_to_check.pages.size();
    size_t items_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*                  cur_page     = clm_to_check.pages[i].page;
        PageDescriptor&        page_info_   = clm_to_check.pages[i].page_info;
        RegularPageDescriptor& page_info    = page_info_.regular;
        T*                     data         = DataBegin<T>(cur_page);
        uint8_t*               bitmap_begin = page_info_.BitMapBegin(cur_page);
        size_t                 curr_id      = items_in_prev_pages;

        uint16_t curr_non_null_id = 0;
        uint16_t cur_bitmap_id    = 0;
        if (page_info.rows_in_page == page_info.non_null_in_page) {
            for (size_t j = 0; j < page_info.rows_in_page; j += 1) {
                T& key = data[curr_non_null_id++];
                ProbeAndCollect(tbl,
                    key,
                    tbl_l,
                    tbl_r,
                    results,
                    hashed_is_left,
                    curr_id,
                    output_attrs,
                    l_col_id,
                    r_col_id);
                curr_id++;
            }
        } else {
            while (curr_non_null_id < page_info.non_null_in_page) {
                if (bitmap_begin[cur_bitmap_id] == u8_max
                    && (page_info.non_null_in_page - curr_non_null_id
                        >= 8)) { // Full byte not null
                    for (size_t j = 0; j < 8; j += 1) {
                        T& key = data[curr_non_null_id++];
                        ProbeAndCollect(tbl,
                            key,
                            tbl_l,
                            tbl_r,
                            results,
                            hashed_is_left,
                            curr_id,
                            output_attrs,
                            l_col_id,
                            r_col_id);
                        curr_id++;
                    }
                } else {
                    // Some not null
                    uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                    for (uint8_t intra_bitmap_id  = 0; intra_bitmap_id < 8;
                        intra_bitmap_id          += 1) {
                        if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                            T& key = data[curr_non_null_id++];
                            ProbeAndCollect(tbl,
                                key,
                                tbl_l,
                                tbl_r,
                                results,
                                hashed_is_left,
                                curr_id,
                                output_attrs,
                                l_col_id,
                                r_col_id);
                        }
                        curr_id++;
                        if (curr_non_null_id >= page_info.non_null_in_page) {
                            break;
                        }
                    }
                }
                cur_bitmap_id += 1;
            }
        }
        items_in_prev_pages += page_info.rows_in_page;
    }
}

void ProbeStr(SensibleColumnarTable&                      tbl_l,
    SensibleColumnarTable&                                tbl_r,
    size_t                                                col_id_of_non_hashed_in,
    std::unordered_map<std::string, std::vector<size_t>>& tbl,
    // TODO: when para -> use one result table for each and then "merge" which is just
    // collecting the pages per column in one result table i.e. no need to actually cpy
    // smth between pages
    SensibleColumnarTable&                           results,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    bool                                             hashed_is_left) {
    // ZoneScoped;
    SensibleColumnarTable& non_hashed_tbl = hashed_is_left ? tbl_r : tbl_l;

    SensibleColumn& clm_to_check = non_hashed_tbl.columns[col_id_of_non_hashed_in];
    size_t          page_cnt     = clm_to_check.pages.size();
    size_t          items_handled_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*           cur_page   = clm_to_check.pages[i].page;
        PageDescriptor& page_info_ = clm_to_check.pages[i].page_info;
        size_t          curr_id    = items_handled_in_prev_pages;

        switch (page_info_.type) {
        case PageType::Regular: {
            uint8_t*               bitmap_begin      = page_info_.BitMapBegin(cur_page);
            RegularPageDescriptor& page_info         = page_info_.regular;
            uint16_t               curr_non_null_id  = 0;
            uint16_t               cur_bitmap_id     = 0;
            uint16_t               str_base_offset   = page_info.non_null_in_page * 2 + 4;
            uint16_t               current_str_begin = str_base_offset;
            if (page_info.rows_in_page == page_info.non_null_in_page) {
                for (size_t j = 0; j < page_info.rows_in_page; j += 1) {
                    ProbeAndCollectStr(tbl,
                        tbl_l,
                        tbl_r,
                        results,
                        hashed_is_left,
                        curr_id,
                        output_attrs,
                        cur_page,
                        &curr_non_null_id,
                        &current_str_begin,
                        str_base_offset);
                    curr_id++;
                }
            } else {
                while (curr_non_null_id < page_info.non_null_in_page) {
                    if (bitmap_begin[cur_bitmap_id] == u8_max
                        && (page_info.non_null_in_page - curr_non_null_id >= 8)) {
                        // Full byte not null
                        for (size_t j = 0; j < 8; j += 1) {
                            ProbeAndCollectStr(tbl,
                                tbl_l,
                                tbl_r,
                                results,
                                hashed_is_left,
                                curr_id,
                                output_attrs,
                                cur_page,
                                &curr_non_null_id,
                                &current_str_begin,
                                str_base_offset);
                            curr_id++;
                        }
                    } else {
                        // Some not null
                        uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                        for (uint8_t intra_bitmap_id  = 0; intra_bitmap_id < 8;
                            intra_bitmap_id          += 1) {
                            if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                                ProbeAndCollectStr(tbl,
                                    tbl_l,
                                    tbl_r,
                                    results,
                                    hashed_is_left,
                                    curr_id,
                                    output_attrs,
                                    cur_page,
                                    &curr_non_null_id,
                                    &current_str_begin,
                                    str_base_offset);
                            }
                            curr_id++;
                            if (curr_non_null_id >= page_info.non_null_in_page) {
                                break;
                            }
                        }
                    }
                    cur_bitmap_id += 1;
                }
            }
            items_handled_in_prev_pages += page_info.rows_in_page;
            break;
        };
        case PageType::LargeStrFirst: {
            char*       long_str = ConcatLargeString(i, clm_to_check);
            std::string key(long_str);
            ProbeAndCollect(tbl,
                key,
                tbl_l,
                tbl_r,
                results,
                hashed_is_left,
                curr_id,
                output_attrs,
                0,
                0);
            free(long_str);
            items_handled_in_prev_pages += 1;
            break;
        };
        case PageType::LargeStrSubsequent: break; // These have already been processed above
        }
    }
}

struct JoinAlgorithm {
    bool                                             build_left;
    SensibleColumnarTable&                           left;
    SensibleColumnarTable&                           right;
    SensibleColumnarTable&                           results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto run() {
        // ZoneScoped;
        if constexpr (std::is_same<T, char*>()) {
            std::unordered_map<std::string, std::vector<size_t>> hash_table;
            if (build_left) {
                BuildHashTblStr(left, left_col, hash_table);
                ProbeStr(left, right, right_col, hash_table, results, output_attrs, true);
            } else {
                BuildHashTblStr(right, right_col, hash_table);
                ProbeStr(left, right, left_col, hash_table, results, output_attrs, false);
            }
        } else {
            std::unordered_map<T, std::vector<size_t>> hash_table;
            if (build_left) {
                BuildHashTbl<T>(left, left_col, hash_table);
                Probe<T>(left,
                    right,
                    right_col,
                    left_col,
                    right_col,
                    hash_table,
                    results,
                    output_attrs,
                    true);
            } else {
                BuildHashTbl<T>(right, right_col, hash_table);
                Probe<T>(left,
                    right,
                    left_col,
                    left_col,
                    right_col,
                    hash_table,
                    results,
                    output_attrs,
                    false);
            }
        }
    }
};

SensibleColumnarTable execute_hash_join(const Plan&  plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    // ZoneScoped;
    auto  left_idx    = join.left;
    auto  right_idx   = join.right;
    auto& left_node   = plan.nodes[left_idx];
    auto& right_node  = plan.nodes[right_idx];
    auto& left_types  = left_node.output_attrs;
    auto& right_types = right_node.output_attrs;
    auto  left        = execute_impl(plan, left_idx);
    auto  right       = execute_impl(plan, right_idx);

    for (size_t i = 0; i < output_attrs.size(); i += 1) {
        size_t   src_id          = std::get<0>(output_attrs[i]);
        DataType result_col_type = std::get<1>(output_attrs[i]);
        DataType tbl_col_type    = src_id < left.columns.size()
                                     ? left.columns[src_id].type
                                     : right.columns[src_id - left.columns.size()].type;
        // assert(result_col_type == tbl_col_type);
        if (result_col_type != tbl_col_type) {
            std::abort();
        }
    }

    SensibleColumnarTable results;
    results.num_rows = 0;
    for (size_t i = 0; i < output_attrs.size(); i += 1) {
        results.columns.emplace_back(std::get<1>(output_attrs[i]));
    }
    if (left.columns.size() == 0 || right.columns.size() == 0 || output_attrs.size() == 0) {
        return results;
    }

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs};
    if (join.build_left) {
        switch (std::get<1>(left_types[join.left_attr])) {
        case DataType::INT32:   join_algorithm.run<int32_t>(); break;
        case DataType::INT64:   join_algorithm.run<int64_t>(); break;
        case DataType::FP64:    join_algorithm.run<double>(); break;
        case DataType::VARCHAR: join_algorithm.run<char*>(); break;
        }
    } else {
        switch (std::get<1>(right_types[join.right_attr])) {
        case DataType::INT32:   join_algorithm.run<int32_t>(); break;
        case DataType::INT64:   join_algorithm.run<int64_t>(); break;
        case DataType::FP64:    join_algorithm.run<double>(); break;
        case DataType::VARCHAR: join_algorithm.run<char*>(); break;
        }
    }

    // TODO: left and right intermediates go out of scope here. If we want to do non-owning
    // pages e.g. for large strings we would need to take ownership here.
    // Input pages are handled in exectue() in the very end before returning.

    return results;
}

SensibleColumnarTable execute_scan(const Plan&       plan,
    const ScanNode&                                  scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    // ZoneScoped;
    auto   table_id   = scan.base_table_id;
    auto&  input      = plan.inputs[table_id];
    size_t record_cnt = input.num_rows;
    size_t column_cnt = output_attrs.size();
    for (size_t i = 0; i < output_attrs.size(); i += 1) {
        // assert(std::get<0>(output_attrs[i]) < input.columns.size());
        if (std::get<0>(output_attrs[i]) >= input.columns.size()) {
			std::abort();
		}
    }

    SensibleColumnarTable result;
    result.num_rows = record_cnt;
    result.columns.reserve(column_cnt);
    for (size_t i = 0; i < output_attrs.size(); i += 1) {
        size_t select_col_id = std::get<0>(output_attrs[i]);
        result.columns.emplace_back(std::get<1>(output_attrs[i]));
    }
    if (record_cnt == 0) {
        return result;
    }

    for (size_t i = 0; i < output_attrs.size(); i += 1) {
        size_t select_col_id = std::get<0>(output_attrs[i]);
        size_t page_cnt      = input.columns[select_col_id].pages.size();
        // assert(std::get<1>(output_attrs[i]) == input.columns[select_col_id].type);
        if (std::get<1>(output_attrs[i]) != input.columns[select_col_id].type) {
            std::abort();
        }
        for (size_t j = 0; j < page_cnt; j += 1) {
            result.columns[i].AddInputPage(input.columns[select_col_id].pages[j]);
        }
    }
    return result;
}

SensibleColumnarTable execute_impl(const Plan& plan, size_t node_idx) {
    // ZoneScoped;
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan, value, node.output_attrs);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
    // ZoneScoped;
    auto          ret = execute_impl(plan, plan.root);
    ColumnarTable result;
    result.num_rows   = ret.num_rows;
    size_t column_cnt = ret.columns.size();
    result.columns.reserve(column_cnt);
    for (size_t i = 0; i < column_cnt; i += 1) {
        result.columns.push_back(Column(ret.columns[i].type));

        size_t pages_to_move = ret.columns[i].pages.size();
        for (size_t j = 0; j < pages_to_move; j += 1) {
            switch (ret.columns[i].pages[j].ownership) {
            case PageOwnerShip::InputPage: {
                // Sadly we can not elide this copy as we have to return a ColumnarTable not a
                // SensibleColumnarTable and thus can not conditionally elide the free of a
                // page. If we would just pass the pointer we would double free the page once in
                // ~Plan() once in ~ColumnarTable()
                Page* page = new Page;
                memcpy(page, ret.columns[i].pages[j].page, PAGE_SIZE);
                result.columns[i].pages.push_back(page);
            } break;
            case PageOwnerShip::Owning: {
                result.columns[i].pages.push_back(ret.columns[i].pages[j].page);
                ret.columns[i].pages[j].ownership = PageOwnerShip::NonOwning;
            } break;
            case PageOwnerShip::NonOwning: // For now illegal
                assert(false);
                std::abort();
                break;
                // case PageOwnerShip::NonOwning:
                //     Page* page = new Page;
                //     std::memcpy(page, ret.columns[i].pages[j], PAGE_SIZE);
                //     result.columns[i].pages.push_back(page);
                //     ret.columns[i].owns_pages[j] = PageOwnerShip::Owning;
                //     break;
            }
        }
    }
    return result;
}

void* build_context() {
    // ZoneScoped;
    // return new ExecContext();
    return nullptr;
}

void destroy_context(void* context) {
    // ZoneScoped;
    // ExecContext* ctx = static_cast<ExecContext*>(context);
    // delete ctx;
}

} // namespace Contest
