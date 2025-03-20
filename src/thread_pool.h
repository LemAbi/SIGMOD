#pragma once

// #include "../build/_deps/tracy-src/public/tracy/Tracy.hpp"

#include "attribute.h"
#include "columnar.h"
#include "probe.h"

#include <atomic>
#include <mutex>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

enum class WorkItemType {
    Join_Probe,
    Join_Build,
};

struct ScanInfo {
    uint64_t               start_id;
    uint64_t               item_count;
    std::atomic<uint32_t>* completion_ctr;
};

struct ProbePageInfo {
    Page*           page;
    PageDescriptor* page_info;
    size_t          rows_in_prev_pages;
};

struct JoinProbeInfo {
    std::vector<ProbePageInfo>                       our_pages;
    SensibleColumnarTable*                           in_left;
    SensibleColumnarTable*                           in_right;
    std::vector<SensibleColumnarTable>*              result;
    void*                                            hash_tbl;
    size_t                                           join_col_id_left;
    size_t                                           join_col_id_right;
    const std::vector<std::tuple<size_t, DataType>>* output_attrs;
    std::atomic<uint64_t>*                           tasklet_done_ctr;
    bool                                             hashed_in_is_left_tbl;
};

template <typename T>
void ExecuteProbeTasklet(JoinProbeInfo& probe_info, uint32_t thread_id) {
    bool                   hash_is_left = probe_info.hashed_in_is_left_tbl;
    SensibleColumnarTable* tbl_l        = probe_info.in_left;
    SensibleColumnarTable* tbl_r        = probe_info.in_right;
    SensibleColumnarTable* result       = &((*probe_info.result)[thread_id]);
    const std::vector<std::tuple<size_t, DataType>>* output_attrs = probe_info.output_attrs;
    size_t                                           col_id_l     = probe_info.join_col_id_left;
    size_t                                           col_id_r = probe_info.join_col_id_right;
    size_t                 col_id_of_non_hashed = hash_is_left ? col_id_r : col_id_l;
    SensibleColumnarTable* non_hashed_tbl       = hash_is_left ? tbl_r : tbl_l;
    SensibleColumn&        clm_to_check         = non_hashed_tbl->columns[col_id_of_non_hashed];
    std::unordered_map<T, std::vector<size_t>>* tbl =
        static_cast<std::unordered_map<T, std::vector<size_t>>*>(probe_info.hash_tbl);
    size_t page_cnt = probe_info.our_pages.size();

    for (size_t i = 0; i < page_cnt; i += 1) {
        Page*           page               = probe_info.our_pages[i].page;
        PageDescriptor* page_info          = probe_info.our_pages[i].page_info;
        size_t          rows_in_prev_pages = probe_info.our_pages[i].rows_in_prev_pages;
        ProbePage<T, PROBEBATCH_SIZE>(tbl_l,
            tbl_r,
            result,
            tbl,
            col_id_of_non_hashed,
            page,
            page_info,
            rows_in_prev_pages,
            output_attrs,
            hash_is_left);
    }

    std::atomic_fetch_add_explicit(probe_info.tasklet_done_ctr, 1, std::memory_order_relaxed);
}

static void ExecuteProbeTaskletStr(JoinProbeInfo& probe_info, uint32_t thread_id) {
    bool                   hash_is_left = probe_info.hashed_in_is_left_tbl;
    SensibleColumnarTable* tbl_l        = probe_info.in_left;
    SensibleColumnarTable* tbl_r        = probe_info.in_right;
    SensibleColumnarTable* result       = &(*probe_info.result)[thread_id];
    const std::vector<std::tuple<size_t, DataType>>* output_attrs = probe_info.output_attrs;
    size_t                                           col_id_l     = probe_info.join_col_id_left;
    size_t                                           col_id_r = probe_info.join_col_id_right;
    size_t                 col_id_of_non_hashed = hash_is_left ? col_id_r : col_id_l;
    SensibleColumnarTable* non_hashed_tbl       = hash_is_left ? tbl_r : tbl_l;
    SensibleColumn&        clm_to_check         = non_hashed_tbl->columns[col_id_of_non_hashed];
    std::unordered_map<std::string, std::vector<size_t>>* tbl =
        static_cast<std::unordered_map<std::string, std::vector<size_t>>*>(probe_info.hash_tbl);
    size_t page_cnt = probe_info.our_pages.size();

    for (size_t i = 0; i < page_cnt; i += 1) {
        Page*           page               = probe_info.our_pages[i].page;
        PageDescriptor* page_info          = probe_info.our_pages[i].page_info;
        size_t          rows_in_prev_pages = probe_info.our_pages[i].rows_in_prev_pages;
        ProbePageStr<PROBEBATCH_SIZE>(tbl_l,
            tbl_r,
            result,
            tbl,
            col_id_of_non_hashed,
            page,
            page_info,
            rows_in_prev_pages,
            output_attrs,
            hash_is_left);
    }

    std::atomic_fetch_add_explicit(probe_info.tasklet_done_ctr, 1, std::memory_order_relaxed);
}

struct JoinBuildInfo {};

// union WorkItemInfo {
//     JoinProbeInfo probe;
//     JoinBuildInfo build;
// };

// Remove union here for now because c++ is a piece of shit
struct WorkItem {
    JoinProbeInfo work_info;
    WorkItemType  work_type;
};

static void ExecuteProbe(uint32_t thread_id, JoinProbeInfo& info) {
    // ZoneScoped;
    switch (info.in_left->columns[info.join_col_id_left].type) {
    case DataType::INT32:   ExecuteProbeTasklet<int32_t>(info, thread_id); break;
    case DataType::INT64:   ExecuteProbeTasklet<int64_t>(info, thread_id); break;
    case DataType::FP64:    ExecuteProbeTasklet<double>(info, thread_id); break;
    case DataType::VARCHAR: ExecuteProbeTaskletStr(info, thread_id); break;
    }
}

static void ExecuteBuild(uint32_t thead_id) {
    // ZoneScoped;
}

static void ExecuteWork(WorkItem& work, uint32_t thread_id) {
    switch (work.work_type) {
    case WorkItemType::Join_Probe: ExecuteProbe(thread_id, work.work_info); break;
    case WorkItemType::Join_Build: ExecuteBuild(thread_id); break;
    }
}

struct ExecContext;
static void WorkerEventLoop(uint32_t thread_id, ExecContext* ctx);

struct ExecContext {
    std::vector<std::thread> worker;
    std::vector<WorkItem>    work_stack;
    std::mutex               work_access_lck;
    std::atomic<uint64_t>    work_item_count;
    std::atomic<bool>        shutdown_requested;

    ExecContext()
    : worker()
    , work_stack()
    , work_access_lck()
    , work_item_count(0)
    , shutdown_requested(false) {
        unsigned int worker_cnt = std::thread::hardware_concurrency() - 1;
        worker.reserve(worker_cnt);
        for (uint32_t i = 0; i < worker_cnt; i += 1) {
            worker.push_back(std::move(std::thread(WorkerEventLoop, i, this)));
        }
    }

    ~ExecContext() {
        std::atomic_store_explicit(&shutdown_requested, true, std::memory_order_relaxed);
        for (uint32_t i = 0; i < worker.size(); i += 1) {
            worker[i].join();
        }
    }
};

static void WorkerEventLoop(uint32_t thread_id, ExecContext* ctx) {
    // ZoneScoped;
    std::mutex*            q_lck           = &ctx->work_access_lck;
    std::atomic<uint64_t>* work_count      = &ctx->work_item_count;
    std::atomic<bool>*     should_shutdown = &ctx->shutdown_requested;

    while (
        !std::atomic_load_explicit(should_shutdown, std::memory_order::memory_order_relaxed)) {
        // No need to check the shutdown flag every time hence the for loop
        for (size_t i = 0; i < 1000; i += 1) {
            uint32_t possibly_available_items =
                std::atomic_load_explicit(work_count, std::memory_order_relaxed);
            if (possibly_available_items != 0) {
                if (q_lck->try_lock()) {
                    if (!ctx->work_stack.empty()) {
                        WorkItem allocated_work = ctx->work_stack.back();
                        ctx->work_stack.pop_back();
                        std::atomic_fetch_sub_explicit(work_count,
                            1,
                            std::memory_order_relaxed);
                        q_lck->unlock();
                        ExecuteWork(allocated_work, thread_id);
                    } else {
                        q_lck->unlock();
                    }
                }
            }
        }
    }
}
