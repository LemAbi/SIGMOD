#include "../build/_deps/tracy-src/public/tracy/Tracy.hpp"

#include <atomic>
#include <thread>
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

struct JoinProbeInfo {};

struct JoinBuildInfo {};

union WorkItemInfo {
    JoinProbeInfo probe;
    JoinBuildInfo build;
};

struct WorkItem {
    WorkItemType work_type;
    WorkItemInfo work_info;

    void Execute() {
        switch (work_type) {
        case WorkItemType::Join_Probe: ExecuteProbe(); break;
        case WorkItemType::Join_Build: ExecuteBuild(); break;
        }
    }

    void ExecuteProbe() { ZoneScoped; }

    void ExecuteBuild() { ZoneScoped; }
};

struct ExecContext;
static void WorkerEventLoop(uint32_t thread_id, ExecContext* ctx);

struct ExecContext {
    std::vector<std::thread> worker;
    std::vector<WorkItem>    work_stack;
    std::mutex               work_access_lck;
    std::atomic<uint32_t>    work_item_count;
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
    ZoneScoped;
    std::mutex*            q_lck           = &ctx->work_access_lck;
    std::atomic<uint32_t>* work_count      = &ctx->work_item_count;
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
                        allocated_work.Execute();
                    } else {
                        q_lck->unlock();
                    }
                }
            }
        }
    }
}

