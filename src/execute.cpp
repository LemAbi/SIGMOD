#include <plan.h>
#include <table.h>

#include "../build/_deps/tracy-src/public/tracy/Tracy.hpp"

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

namespace Contest {

using ExecuteResult = std::vector<std::vector<Data>>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

struct JoinAlgorithm {
    bool                                             build_left;
    ExecuteResult&                                   left;
    ExecuteResult&                                   right;
    ExecuteResult&                                   results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto run() {
        ZoneScoped;
        namespace views = ranges::views;
        std::unordered_map<T, std::vector<size_t>> hash_table;
        if (build_left) {
            for (auto&& [idx, record]: left | views::enumerate) {
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                                hash_table.emplace(key, std::vector<size_t>(1, idx));
                            } else {
                                itr->second.push_back(idx);
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    record[left_col]);
            }
            for (auto& right_record: right) {
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                                for (auto left_idx: itr->second) {
                                    auto&             left_record = left[left_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    right_record[right_col]);
            }
        } else {
            for (auto&& [idx, record]: right | views::enumerate) {
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                                hash_table.emplace(key, std::vector<size_t>(1, idx));
                            } else {
                                itr->second.push_back(idx);
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    record[right_col]);
            }
            for (auto& left_record: left) {
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                                for (auto right_idx: itr->second) {
                                    auto&             right_record = right[right_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    left_record[left_col]);
            }
        }
    }
};

ExecuteResult execute_hash_join(const Plan&          plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    ZoneScoped;
    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto                           left        = execute_impl(plan, left_idx);
    auto                           right       = execute_impl(plan, right_idx);
    std::vector<std::vector<Data>> results;

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
        case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
        }
    } else {
        switch (std::get<1>(right_types[join.right_attr])) {
        case DataType::INT32:   join_algorithm.run<int32_t>(); break;
        case DataType::INT64:   join_algorithm.run<int64_t>(); break;
        case DataType::FP64:    join_algorithm.run<double>(); break;
        case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
        }
    }

    return results;
}

ExecuteResult execute_scan(const Plan&               plan,
    const ScanNode&                                  scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    ZoneScoped;
    auto                            table_id = scan.base_table_id;
    auto&                           input    = plan.inputs[table_id];
    auto                            table    = Table::from_columnar(input);
    std::vector<std::vector<Data>>  results;
    std::vector<std::vector<Data>>& input_data  = table.table();
    size_t                          collumn_cnt = output_attrs.size();
    size_t                          record_cnt  = input_data.size();

    // NOTE: the output type here is really dumb... This could all be a single
    // piece of memory and also thus a single allocation, but no let's use
    // a vector of vector....
    // Also transposing the table for no reason is insane
    // TODO: check if we can replace ExecuteResult everywhere with one piece of memory and also
    // flip the table orientation back to column oriented
    results.resize(record_cnt);
    for (size_t i = 0; i < record_cnt; i += 1) {
        results[i].resize(collumn_cnt);
    }
    for (size_t i = 0; i < record_cnt; i += 1) {
        for (size_t j = 0; j < collumn_cnt; j += 1) {
            size_t column_id = std::get<0>(output_attrs[j]);
            results[i][j]    = input_data[i][column_id];
        }
    }
    return results;
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
    ZoneScoped;
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
    ZoneScoped;
    namespace views = ranges::views;
    auto ret        = execute_impl(plan, plan.root);
    auto ret_types  = plan.nodes[plan.root].output_attrs
                   | views::transform([](const auto& v) { return std::get<1>(v); })
                   | ranges::to<std::vector<DataType>>();
    Table table{std::move(ret), std::move(ret_types)};
    return table.to_columnar();
}

enum class WorkItemType {
    Scan,
    Join,
};

struct ScanInfo {
    uint64_t               start_id;
    uint64_t               item_count;
    std::atomic<uint32_t>* completion_ctr;
};

struct JoinInfo {};

union WorkItemInfo {
    ScanInfo scan;
    JoinInfo join;
};

struct WorkItem {
    WorkItemType work_type;
    WorkItemInfo work_info;
};

void ExecuteJoinTasklet(JoinInfo* info) {
    ZoneScoped;
}

void ExecuteScanTasklet(ScanInfo* info) {
    ZoneScoped;
}

void ExecuteWorkItem(WorkItem* work) {
    ZoneScoped;
    switch (work->work_type) {
    case WorkItemType::Join: ExecuteJoinTasklet(&work->work_info.join); break;
    case WorkItemType::Scan: ExecuteScanTasklet(&work->work_info.scan); break;
    }
}

struct ExecContext;
void WorkerEventLoop(uint32_t thread_id, ExecContext* ctx);

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

void WorkerEventLoop(uint32_t thread_id, ExecContext* ctx) {
    ZoneScoped;
    std::mutex*            q_lck           = &ctx->work_access_lck;
    std::atomic<uint32_t>* work_count      = &ctx->work_item_count;
    std::atomic<bool>*     should_shutdown = &ctx->shutdown_requested;

    while (
        !std::atomic_load_explicit(should_shutdown, std::memory_order::memory_order_relaxed)) {
        // No need to check the shutdown flag every time -> this loop
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
                        ExecuteWorkItem(&allocated_work);
                    } else {
                        q_lck->unlock();
                    }
                }
            }
        }
    }
}

void* build_context() {
    ZoneScoped;
    return new ExecContext();
}

void destroy_context(void* context) {
    ZoneScoped;
    ExecContext* ctx = static_cast<ExecContext*>(context);
    delete ctx;
}

} // namespace Contest
