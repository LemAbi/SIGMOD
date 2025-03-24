#include <atomic>
#include <attribute.h>
#include <plan.h>
#include <table.h>

// TODO: Remove tray before submission
// #include "../build/_deps/tracy-src/public/tracy/Tracy.hpp"
#include "columnar.h"
#include "probe.h"
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
static constexpr uint64_t PARALLEL_PROBE_THRESHOLD             = 2;
static constexpr uint64_t PARALLEL_PROBE_MAX_PAGES_PER_TASKLET = 64;

SensibleColumnarTable execute_impl(const Plan& plan, size_t node_idx, void* ctx);

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
        PageDescriptor        page_info    = clm_to_hash.pages[i].page_info;
        RegularPageDescriptor regular_info = clm_to_hash.pages[i].page_info.regular;
        T*                    data         = DataBegin<T>(cur_page);
        uint8_t*              bitmap_begin = BitMapBegin(cur_page, &regular_info);
        size_t                id           = items_in_prev_pages;

        uint16_t processed     = 0;
        uint16_t cur_bitmap_id = 0;
        if (regular_info.non_null_in_page == regular_info.rows_in_page) {
            for (size_t j = 0; j < regular_info.rows_in_page; j += 1) {
                InsertToHashmap<T>(tbl, data[processed++], id);
                id++;
            }
        } else {
            while (processed < regular_info.non_null_in_page) {
                // NOTE: testing at larger than byte granularity could be faster overall
                // (e.g. 512/256bit)
                if (bitmap_begin[cur_bitmap_id] == u8_max
                    && (regular_info.non_null_in_page - processed >= 8)) {
                    // Full byte not null
                    for (size_t k = 0; k < 8; k += 1) {
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
                        if (processed >= regular_info.non_null_in_page) {
                            break;
                        }
                    }
                }
                cur_bitmap_id += 1;
            }
        }
        items_in_prev_pages += regular_info.rows_in_page;
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
        Page*          cur_page  = clm_to_hash.pages[i].page;
        PageDescriptor page_info = clm_to_hash.pages[i].page_info;
        size_t         id        = items_in_prev_pages;

        switch (page_info.type) {
        case PageType::Regular: {
            RegularPageDescriptor regular_info      = page_info.regular;
            uint8_t*              bitmap_begin      = BitMapBegin(cur_page, &regular_info);
            uint16_t              processed         = 0;
            uint16_t              cur_bitmap_id     = 0;
            uint16_t              str_base_offset   = regular_info.non_null_in_page * 2 + 4;
            uint16_t              current_str_begin = str_base_offset;
            if (regular_info.rows_in_page == regular_info.non_null_in_page) {
                for (size_t j = 0; j < regular_info.rows_in_page; j += 1) {
                    InsertStrToHashmap(tbl,
                        id,
                        cur_page,
                        &current_str_begin,
                        &processed,
                        str_base_offset);
                    id++;
                }
            } else {
                while (processed < regular_info.non_null_in_page) {
                    // NOTE: testing at larger than byte granularity could be faster overall
                    // (e.g. 512/256bit)
                    if (bitmap_begin[cur_bitmap_id] == u8_max
                        && (regular_info.non_null_in_page - processed >= 8)) {
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
                            if (processed >= regular_info.non_null_in_page) {
                                break;
                            }
                        }
                    }
                    cur_bitmap_id += 1;
                }
            }
            items_in_prev_pages += regular_info.rows_in_page;
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
    if (val_l == nullptr || val_r == nullptr) {
        std::cout << "Match Validation failed. key: " << key << std::endl;
        std::abort();
    }
    if (*val_l != key || *val_r != key) {
        std::cout << "Match Validation failed. key: " << key << " l " << *val_l << " r "
                  << *val_r << std::endl;
        std::abort();
    }
}

template <typename T>
void SingleThreadedProbe(SensibleColumnarTable&      tbl_l,
    SensibleColumnarTable&                           tbl_r,
    size_t                                           col_id_of_non_hashed,
    size_t                                           l_col_id,
    size_t                                           r_col_id,
    std::unordered_map<T, std::vector<size_t>>&      tbl,
    SensibleColumnarTable&                           results,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    bool                                             hashed_is_left) {
    SensibleColumnarTable& non_hashed_tbl = hashed_is_left ? tbl_r : tbl_l;
    SensibleColumn&        clm_to_check   = non_hashed_tbl.columns[col_id_of_non_hashed];

    size_t page_cnt           = clm_to_check.pages.size();
    size_t rows_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*                  cur_page     = clm_to_check.pages[i].page;
        PageDescriptor&        page_info    = clm_to_check.pages[i].page_info;
        RegularPageDescriptor& regular_info = page_info.regular;

        ProbePage<T, PROBEBATCH_SIZE>(&tbl_l,
            &tbl_r,
            &results,
            &tbl,
            col_id_of_non_hashed,
            cur_page,
            &page_info,
            rows_in_prev_pages,
            &output_attrs,
            hashed_is_left);

        rows_in_prev_pages += regular_info.rows_in_page;
    }
}

// TODO: **IMPORTANT** **If** we join on str -> This must either weed out big str pages before
// or we must switch to using page_ids instead. This is currently addressed by not using the
// parallel path for joins of strings
void ParallelProbe(SensibleColumnarTable&            tbl_l,
    SensibleColumnarTable&                           tbl_r,
    size_t                                           col_id_of_non_hashed,
    size_t                                           l_col_id,
    size_t                                           r_col_id,
    void*                                            tbl,
    SensibleColumnarTable&                           results,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    bool                                             hashed_is_left,
    ExecContext*                                     ctx) {
    SensibleColumnarTable& non_hashed_tbl = hashed_is_left ? tbl_r : tbl_l;
    SensibleColumn&        clm_to_check   = non_hashed_tbl.columns[col_id_of_non_hashed];
    size_t                 page_cnt       = clm_to_check.pages.size();
    uint32_t thread_cnt = ctx->worker.size() + 1; // this thread will also participate
    std::atomic<uint64_t> finished_tasklets = 0;

    // Prepare one results tbl for each worker including this thread (the one for this thread
    // could be elided but w/e for now)
    std::vector<SensibleColumnarTable> worker_results;
    for (size_t i = 0; i < thread_cnt; i += 1) {
        worker_results.push_back(SensibleColumnarTable());
        for (size_t j = 0; j < output_attrs.size(); j += 1) {
            worker_results.back().columns.emplace_back(std::get<1>(output_attrs[j]));
        }
    }

    ctx->work_access_lck.lock();

    // Prep Tasklets
    size_t   rows_in_prev_pages = 0;
    size_t   curr_page_id       = 0;
    uint64_t tasklets_enqd      = 0;
    while (curr_page_id < page_cnt) {
        size_t remaining_pages  = page_cnt - curr_page_id;
        size_t pages_per_thread = std::min(PARALLEL_PROBE_MAX_PAGES_PER_TASKLET,
            ((static_cast<size_t>(
                 static_cast<double>(remaining_pages) / static_cast<double>(thread_cnt)))
                + 1));

        for (size_t t_id = 0; t_id < thread_cnt; t_id += 1) {
            size_t pages_for_this_thread = std::min(remaining_pages, pages_per_thread);
            if (pages_for_this_thread == 0) {
                break;
            }

            WorkItem tasklet;
            tasklet.work_type                       = WorkItemType::Join_Probe;
            tasklet.work_info.output_attrs          = &output_attrs;
            tasklet.work_info.result                = &worker_results;
            tasklet.work_info.in_left               = &tbl_l;
            tasklet.work_info.in_right              = &tbl_r;
            tasklet.work_info.hash_tbl              = tbl;
            tasklet.work_info.tasklet_done_ctr      = &finished_tasklets;
            tasklet.work_info.join_col_id_left      = l_col_id;
            tasklet.work_info.join_col_id_right     = r_col_id;
            tasklet.work_info.hashed_in_is_left_tbl = hashed_is_left;

            tasklet.work_info.our_pages = {};
            tasklet.work_info.our_pages.reserve(pages_for_this_thread);
            for (size_t i = 0; i < pages_for_this_thread; i += 1) {
                Page*                  cur_page  = clm_to_check.pages[curr_page_id].page;
                PageDescriptor*        page_info = &clm_to_check.pages[curr_page_id].page_info;
                RegularPageDescriptor& regular_info = page_info->regular;

                // TODO: see note at fn top -> switch over type and page type and for varchar +
                // big str page -> handle it here ourselfes and skip page
                tasklet.work_info.our_pages.push_back(
                    {cur_page, page_info, rows_in_prev_pages});

                rows_in_prev_pages += regular_info.rows_in_page;
                curr_page_id++;
            }
            remaining_pages -= pages_for_this_thread;

            ctx->work_stack.push_back(std::move(tasklet));
            tasklets_enqd++;
        }
    }

    // We have enqd all the tasks but still hold the lock
    // -> barrier to ensure input tbls are synchronized + inc item count and unlock so other
    // threads can start work
    std::atomic_fetch_add_explicit(&ctx->work_item_count,
        tasklets_enqd,
        std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    ctx->work_access_lck.unlock();

    // There is no reason this thread should sit idly by
    while (std::atomic_load_explicit(&finished_tasklets, std::memory_order_relaxed)
           < tasklets_enqd) {
        ctx->work_access_lck.lock();
        if (ctx->work_stack.size() > 0) {
            WorkItem allocated_work = ctx->work_stack[ctx->work_stack.size() - 1];
            ctx->work_stack.pop_back();
            std::atomic_fetch_sub_explicit(&ctx->work_item_count, 1, std::memory_order_relaxed);
            ctx->work_access_lck.unlock();

            ExecuteWork(allocated_work, thread_cnt - 1);
        } else {
            ctx->work_access_lck.unlock();
        }
    }

    // Sync output tbl results
    std::atomic_thread_fence(std::memory_order_seq_cst);

    // Gather results together
    for (size_t i = 0; i < thread_cnt; i += 1) {
        results.num_rows += worker_results[i].num_rows;
    }

    for (size_t c_id = 0; c_id < results.columns.size(); c_id += 1) {
        size_t total_page_cnt = 0;
        for (size_t i = 0; i < thread_cnt; i += 1) {
            total_page_cnt += worker_results[i].columns[c_id].pages.size();
        }
        results.columns[c_id].pages.reserve(total_page_cnt);

        for (size_t i = 0; i < thread_cnt; i += 1) {
            size_t pages_from_this_thread = worker_results[i].columns[c_id].pages.size();
            for (size_t p_id = 0; p_id < pages_from_this_thread; p_id += 1) {
                results.columns[c_id].pages.push_back(
                    worker_results[i].columns[c_id].pages[p_id]);
            }
            worker_results[i].columns[c_id].pages.clear();
        }
    }
}

template <typename T>
void Probe(SensibleColumnarTable&                    tbl_l,
    SensibleColumnarTable&                           tbl_r,
    size_t                                           col_id_of_non_hashed,
    size_t                                           l_col_id,
    size_t                                           r_col_id,
    std::unordered_map<T, std::vector<size_t>>&      tbl,
    SensibleColumnarTable&                           results,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    bool                                             hashed_is_left,
    void*                                            ctx) {
    // ZoneScoped;
    SensibleColumnarTable& non_hashed_tbl = hashed_is_left ? tbl_r : tbl_l;
    // TODO: switch to parallel probe also for str path - disabled here since we currently dont
    // handle big str pages correctly - sec comment on paraprobe fn
    if (non_hashed_tbl.columns[col_id_of_non_hashed].pages.size() < PARALLEL_PROBE_THRESHOLD
        || tbl_l.columns[l_col_id].type == DataType::VARCHAR) {
        SingleThreadedProbe<T>(tbl_l,
            tbl_r,
            col_id_of_non_hashed,
            l_col_id,
            r_col_id,
            tbl,
            results,
            output_attrs,
            hashed_is_left);
    } else {
        ParallelProbe(tbl_l,
            tbl_r,
            col_id_of_non_hashed,
            l_col_id,
            r_col_id,
            &tbl,
            results,
            output_attrs,
            hashed_is_left,
            static_cast<ExecContext*>(ctx));
    }
}

void ProbeStr(SensibleColumnarTable&                      tbl_l,
    SensibleColumnarTable&                                tbl_r,
    size_t                                                col_id_of_non_hashed,
    std::unordered_map<std::string, std::vector<size_t>>& tbl,
    SensibleColumnarTable&                                results,
    const std::vector<std::tuple<size_t, DataType>>&      output_attrs,
    bool                                                  hashed_is_left) {
    // ZoneScoped;
    SensibleColumnarTable& non_hashed_tbl = hashed_is_left ? tbl_r : tbl_l;

    SensibleColumn& clm_to_check       = non_hashed_tbl.columns[col_id_of_non_hashed];
    size_t          page_cnt           = clm_to_check.pages.size();
    size_t          rows_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) {
        Page*           cur_page  = clm_to_check.pages[i].page;
        PageDescriptor& page_info = clm_to_check.pages[i].page_info;
        size_t          curr_id   = rows_in_prev_pages;
        switch (page_info.type) {
        case PageType::Regular: {
            ProbePageStr<PROBEBATCH_SIZE>(&tbl_l,
                &tbl_r,
                &results,
                &tbl,
                col_id_of_non_hashed,
                cur_page,
                &page_info,
                rows_in_prev_pages,
                &output_attrs,
                hashed_is_left);
            rows_in_prev_pages += page_info.regular.rows_in_page;
            break;
        };
        case PageType::LargeStrFirst: {
            char*       long_str = ConcatLargeString(i, clm_to_check);
            std::string key(long_str);
            ProbeAndCollect(&tbl,
                key,
                &tbl_l,
                &tbl_r,
                &results,
                hashed_is_left,
                curr_id,
                &output_attrs);
            free(long_str);
            rows_in_prev_pages += page_info.regular.rows_in_page;
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
    void*                                            ctx;

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
                    true,
                    ctx);
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
                    false,
                    ctx);
            }
        }
    }
};

SensibleColumnarTable execute_hash_join(const Plan&  plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    void*                                            ctx) {
    // ZoneScoped;
    auto  left_idx    = join.left;
    auto  right_idx   = join.right;
    auto& left_node   = plan.nodes[left_idx];
    auto& right_node  = plan.nodes[right_idx];
    auto& left_types  = left_node.output_attrs;
    auto& right_types = right_node.output_attrs;
    auto  left        = execute_impl(plan, left_idx, ctx);
    auto  right       = execute_impl(plan, right_idx, ctx);

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
        .output_attrs                        = output_attrs,
        .ctx                                 = ctx};
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

    SensibleColumnarTable result;
    result.num_rows = record_cnt;
    result.columns.reserve(column_cnt);
    for (size_t i = 0; i < output_attrs.size(); i += 1) {
        result.columns.emplace_back(std::get<1>(output_attrs[i]));
    }

    for (size_t i = 0; i < output_attrs.size(); i += 1) {
        if (std::get<0>(output_attrs[i]) >= input.columns.size()) {
            return result;
        }
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

SensibleColumnarTable execute_impl(const Plan& plan, size_t node_idx, void* ctx) {
    // ZoneScoped;
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan, value, node.output_attrs, ctx);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

ColumnarTable execute(const Plan& plan, void* context) {
    // ZoneScoped;
    auto          ret = execute_impl(plan, plan.root, context);
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
    return new ExecContext();
    return nullptr;
}

void destroy_context(void* context) {
    // ZoneScoped;
    ExecContext* ctx = static_cast<ExecContext*>(context);
    delete ctx;
}

} // namespace Contest
