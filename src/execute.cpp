#include <attribute.h>
#include <plan.h>
#include <table.h>

#include "../build/_deps/tracy-src/public/tracy/Tracy.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <iostream>

namespace Contest {

struct SensibleColumn {
    DataType           type;
    std::vector<Page*> pages;
    bool               owns_pages;

    void AddPage() {
        owns_pages         = true;
        Page*     page     = new Page;
        uint16_t* page_u16 = reinterpret_cast<uint16_t*>(page);
        page_u16[0]        = 0;
        page_u16[1]        = 0;
        pages.push_back(page);
    }

    SensibleColumn(DataType data_type, bool owns_pages)
    : type(data_type)
    , pages()
    , owns_pages(owns_pages) {}

    ~SensibleColumn() {
        if (owns_pages) {
            for (size_t i = 0; i < pages.size(); i += 1) {
                delete pages[i];
            }
        }
    }
};

struct SensibleColumnarTable {
    size_t                      num_rows = 0;
    std::vector<SensibleColumn> columns;
};

SensibleColumnarTable execute_impl(const Plan& plan, size_t node_idx);

struct PageDescriptor {
    uint16_t rows_in_page;
    uint16_t non_null_in_page;
    uint16_t bitmap_size;

    template <typename T>
    uint16_t AlingOffset() {
        return alignof(T) > 4 ? alignof(T) : 4;
    }

    template <typename T>
    T* DataBegin(Page* page) {
        return reinterpret_cast<T*>(&(reinterpret_cast<uint8_t*>(page)[AlingOffset<T>()]));
    }

    void* DataBegin(Page* page, DataType data_type) {
        switch (data_type) {
        case DataType::INT32:   return DataBegin<int32_t>(page);
        case DataType::INT64:   return DataBegin<int64_t>(page);
        case DataType::FP64:    return DataBegin<double>(page);
        case DataType::VARCHAR: return DataBegin<std::string>(page);
        }
        return nullptr; // unreachable - cpp sucks
    }

    uint8_t* BitMapBegin(Page* page) {
        return &(reinterpret_cast<uint8_t*>(page)[PAGE_SIZE - bitmap_size]);
    }
};

PageDescriptor ParsePage(Page* page) {
    PageDescriptor result;
    uint16_t*      page_u16 = reinterpret_cast<uint16_t*>(page);
    result.rows_in_page     = page_u16[0];
    result.non_null_in_page = page_u16[1];
    result.bitmap_size      = (result.rows_in_page + 7) / 8;
    return result;
}

template <typename T>
void AppendValue(T* value, SensibleColumn& clm, PageDescriptor& page_info) {
    if (clm.pages.size() == 0) {
        clm.AddPage();
    }

    Page*    page       = clm.pages.back();
    uint16_t bytes_used = page_info.AlingOffset<T>() + page_info.bitmap_size
                        + (page_info.non_null_in_page * sizeof(T));
    uint16_t bottom_three_bits_mask = 7;
    bool     will_need_new_bitmap_byte =
        ((page_info.rows_in_page - page_info.non_null_in_page) & bottom_three_bits_mask) == 0;
    uint16_t bytes_required =
        will_need_new_bitmap_byte ? bytes_used + sizeof(T) + 1 : bytes_used + sizeof(T);

    if (bytes_required > PAGE_SIZE) {
        clm.AddPage();
        page = clm.pages.back();
    }

    page_info.DataBegin<T>(page)[page_info.non_null_in_page] = *value;
    page_info.non_null_in_page++;
    page_info.rows_in_page++;

    uint8_t* bitmap_start = page_info.BitMapBegin(page);
    if (will_need_new_bitmap_byte) {
        // We need to move the old bitmap one byte to the right
        uint8_t* new_bitmap_start = &bitmap_start[-1];
        // aliasing -> no memcpy
        for (size_t i = 0; i < page_info.bitmap_size; i += 1) {
            new_bitmap_start[i] = bitmap_start[i];
        }
        new_bitmap_start[page_info.bitmap_size] = 0;
        page_info.bitmap_size++;
    }
    uint16_t byte_id       = page_info.rows_in_page / 8;
    uint8_t  bit_id        = page_info.rows_in_page & bottom_three_bits_mask;
    bitmap_start[byte_id] |= (1 << bit_id);
}

template <typename T>
void AppendNull(SensibleColumn& clm, PageDescriptor& page_info) {
    if (clm.pages.size() == 0) {
        clm.AddPage();
    }

    Page*    page       = clm.pages.back();
    uint16_t bytes_used = page_info.bitmap_size + (page_info.non_null_in_page * sizeof(T))
                        + page_info.AlingOffset<T>();
    uint16_t bottom_three_bits_mask = 7;
    bool     will_need_new_bitmap_byte =
        ((page_info.rows_in_page - page_info.non_null_in_page) & bottom_three_bits_mask) == 0;
    uint16_t bytes_required = will_need_new_bitmap_byte ? bytes_used + 1 : bytes_used;

    if (bytes_required > PAGE_SIZE) {
        clm.AddPage();
        page = clm.pages.back();
    }

    page_info.rows_in_page++;

    uint8_t* bitmap_start = page_info.BitMapBegin(page);
    if (will_need_new_bitmap_byte) {
        // We need to move the old bitmap one byte to the right
        uint8_t* new_bitmap_start = &bitmap_start[-1];
        // aliasing -> no memcpy
        for (size_t i = 0; i < page_info.bitmap_size; i += 1) {
            new_bitmap_start[i] = bitmap_start[i];
        }
        new_bitmap_start[page_info.bitmap_size] = 0;
        page_info.bitmap_size++;
    }
    uint16_t byte_id       = page_info.rows_in_page / 8;
    uint8_t  bit_id        = page_info.rows_in_page & bottom_three_bits_mask;
    bitmap_start[byte_id] |= (1 << bit_id);
}

// Sigh*
void AppendAttr(void* value, SensibleColumn& clm) {
    if (clm.pages.size() == 0) {
        clm.AddPage();
    }
    PageDescriptor page_info = ParsePage(clm.pages.back());
    if (value == nullptr) {
        switch (clm.type) {
        case DataType::INT32:
            AppendValue<int32_t>(reinterpret_cast<int32_t*>(value), clm, page_info);
            break;
        case DataType::INT64:
            AppendValue<int64_t>(reinterpret_cast<int64_t*>(value), clm, page_info);
            break;
        case DataType::FP64:
            AppendValue<double>(reinterpret_cast<double*>(value), clm, page_info);
            break;
        case DataType::VARCHAR:
            AppendValue<std::string>(reinterpret_cast<std::string*>(value), clm, page_info);
            break;
        }
    } else {
        switch (clm.type) {
        case DataType::INT32:   AppendNull<int32_t>(clm, page_info); break;
        case DataType::INT64:   AppendNull<int64_t>(clm, page_info); break;
        case DataType::FP64:    AppendNull<double>(clm, page_info); break;
        case DataType::VARCHAR: AppendNull<std::string>(clm, page_info); break;
        }
    }
}

template <typename T>
void BuildHashTbl(SensibleColumnarTable&        input_tbl,
    size_t                                      col_id,
    std::unordered_map<T, std::vector<size_t>>& tbl) {
    ZoneScoped;
    SensibleColumn& clm_to_hash = input_tbl.columns[col_id];
    size_t          page_cnt    = clm_to_hash.pages.size();
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*          cur_page     = clm_to_hash.pages[i];
        PageDescriptor page_info    = ParsePage(cur_page);
        T*             data         = page_info.DataBegin<T>(cur_page);
        uint8_t*       bitmap_begin = page_info.BitMapBegin(cur_page);

        uint16_t processed     = 0;
        uint16_t id            = 0;
        uint16_t cur_bitmap_id = 0;
        while (processed < page_info.non_null_in_page) {
            // NOTE: testing at larger than byte granularity could be faster overall (e.g.
            // 512/256bit)
            if (bitmap_begin[cur_bitmap_id] == 0xff) {
                // Full byte not null
                for (size_t i = 0; i < 8; i += 1) {
                    T& key = data[processed++];
                    if (auto itr = tbl.find(key); itr == tbl.end()) {
                        tbl.emplace(key, std::vector<size_t>(1, id));
                    } else {
                        itr->second.push_back(id);
                    }
                    id++;
                }
            } else {
                // Some not null
                uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                for (uint8_t intra_bitmap_id = 0; intra_bitmap_id < 8; intra_bitmap_id += 1) {
                    if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                        T& key = data[processed++];
                        if (auto itr = tbl.find(key); itr == tbl.end()) {
                            tbl.emplace(key, std::vector<size_t>(1, id));
                        } else {
                            itr->second.push_back(id);
                        }
                    }
                    id++;
                }
            }
            cur_bitmap_id += 1;
        }
    }
}

uint8_t Sizeof(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return 4;
    case DataType::INT64:   return 8;
    case DataType::FP64:    return 8;
    case DataType::VARCHAR: return sizeof(std::string);
    }
    return 42; // unreachable - cpp sucks
}

void PrintVal(void* value, DataType data_type) {
    switch (data_type) {
		case DataType::INT32:  std::cout << *reinterpret_cast<uint32_t*>(value); break;
		case DataType::INT64:  std::cout << *reinterpret_cast<uint64_t*>(value); break;
		case DataType::FP64:  std::cout << *reinterpret_cast<double*>(value); break;
		case DataType::VARCHAR:  std::cout << *reinterpret_cast<std::string*>(value); break;
    }
}

void* GetValueClmnPage(size_t page_record_id,
    Page*                     page,
    PageDescriptor&           page_info,
    DataType                  data_type) {
    if (page_info.rows_in_page == page_info.non_null_in_page) {
        return &reinterpret_cast<uint8_t*>(
            page_info.DataBegin(page, data_type))[Sizeof(data_type) * page_record_id];
    } else {
        size_t   current_non_null = 0;
        size_t   current_checked  = 0;
        uint8_t* bitmap           = page_info.BitMapBegin(page);
        while (current_checked < page_record_id) {
            // NOTE: popcnt would be nice here but c++ sucks :) (i.e. it is C++ >= 20)
            uint16_t byte_id = current_checked / 8;
            uint8_t  bit_id  = current_checked % 8;
            if ((bitmap[byte_id] & (1 << bit_id)) != 0) {
                current_non_null++;
            }
            current_checked++;
        }
        return &reinterpret_cast<uint8_t*>(
            page_info.DataBegin(page, data_type))[Sizeof(data_type) * current_non_null];
    }
}

void* GetValueClmn(size_t record_id, SensibleColumn& clm) {
    size_t page_cnt = clm.pages.size();
    size_t row_cnt  = 0;
    for (size_t i = 0; i < page_cnt; i += 1) {
        PageDescriptor page_info    = ParsePage(clm.pages[i]);
        size_t         next_row_cnt = row_cnt + page_info.rows_in_page;
        if (record_id < next_row_cnt) {
            return GetValueClmnPage(i - row_cnt, clm.pages[i], page_info, clm.type);
        }
        row_cnt = next_row_cnt;
    }
    return nullptr; // unreachable
}

void PrintTbl(SensibleColumnarTable& tbl) {
    size_t num_clmns = tbl.columns.size();
    for (size_t i = 0; i < tbl.num_rows; i += 1) {
        for (size_t j = 0; j < num_clmns; j += 1) {
            void* value = GetValueClmn(i, tbl.columns[j]);
            if (value == nullptr) {
                std::cout << "Null\t";
            } else {
                switch (tbl.columns[j].type) {
                case DataType::INT32:
                    std::cout << *reinterpret_cast<int32_t*>(value) << "\t";
                    break;
                case DataType::INT64:
                    std::cout << *reinterpret_cast<int64_t*>(value) << "\t";
                    break;
                case DataType::FP64:
                    std::cout << *reinterpret_cast<double*>(value) << "\t";
                    break;
                case DataType::VARCHAR:
                    std::cout << *reinterpret_cast<std::string*>(value) << "\t";
                    break;
                }
            }
        }
        std::cout << "\n";
    }
}

// TODO: this is not particularly efficient
void CollectRecord(SensibleColumnarTable&            tbl_0,
    SensibleColumnarTable&                           tbl_1,
    SensibleColumnarTable&                           results,
    size_t                                           record_id,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    size_t attr_cnt   = output_attrs.size();
    size_t attr_l_cnt = tbl_0.columns.size();
    for (size_t i = 0; i < attr_cnt; i += 1) {
        size_t col_id = std::get<0>(output_attrs[i]);
        void*  attr;
        if (col_id < attr_l_cnt) {
            attr = GetValueClmn(record_id, tbl_0.columns[col_id]);
        } else {
            attr = GetValueClmn(record_id, tbl_1.columns[col_id - attr_l_cnt]);
        }
        if (attr == nullptr) {
            AppendAttr(attr, results.columns[i]);
        }
    }
}

template <typename T>
void Probe(SensibleColumnarTable&               non_hashed_in,
    SensibleColumnarTable&                      hashed_in,
    size_t                                      col_id_of_non_hashed_in,
    std::unordered_map<T, std::vector<size_t>>& tbl,
    // TODO: when para -> use one result table for each and then "merge" which is just
    // collecting the pages per column in one result table i.e. no need to actually cpy smth
    // between pages
    SensibleColumnarTable&                           results,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    ZoneScoped;

    SensibleColumn& clm_to_check = non_hashed_in.columns[col_id_of_non_hashed_in];
    size_t          page_cnt     = clm_to_check.pages.size();
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*          cur_page     = clm_to_check.pages[i];
        PageDescriptor page_info    = ParsePage(cur_page);
        T*             data         = page_info.DataBegin<T>(cur_page);
        uint8_t*       bitmap_begin = page_info.BitMapBegin(cur_page);

        uint16_t curr_non_null_id = 0;
        uint16_t curr_id          = 0;
        uint16_t cur_bitmap_id    = 0;
        while (curr_non_null_id < page_info.non_null_in_page) {
            if (bitmap_begin[cur_bitmap_id] == 0xff) {
                // Full byte not null
                for (size_t i = 0; i < 8; i += 1) {
                    T& key = data[curr_non_null_id++];
                    if (auto itr = tbl.find(key); itr != tbl.end()) {
                        CollectRecord(non_hashed_in, hashed_in, results, curr_id, output_attrs);
                    }
                    curr_id++;
                }
            } else {
                // Some not null
                uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                for (uint8_t intra_bitmap_id = 0; intra_bitmap_id < 8; intra_bitmap_id += 1) {
                    if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                        T& key = data[curr_non_null_id++];
                        if (auto itr = tbl.find(key); itr != tbl.end()) {
                            CollectRecord(non_hashed_in,
                                hashed_in,
                                results,
                                curr_id,
                                output_attrs);
                        }
                    }
                    curr_id++;
                }
            }
            cur_bitmap_id += 1;
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
        ZoneScoped;
        std::unordered_map<T, std::vector<size_t>> hash_table;
        if (build_left) {
            BuildHashTbl(left, left_col, hash_table);
            Probe(right, left, right_col, hash_table, results, output_attrs);
        } else {
            BuildHashTbl(right, right_col, hash_table);
            Probe(left, right, left_col, hash_table, results, output_attrs);
        }
    }
};

SensibleColumnarTable execute_hash_join(const Plan&  plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    ZoneScoped;
    auto                  left_idx    = join.left;
    auto                  right_idx   = join.right;
    auto&                 left_node   = plan.nodes[left_idx];
    auto&                 right_node  = plan.nodes[right_idx];
    auto&                 left_types  = left_node.output_attrs;
    auto&                 right_types = right_node.output_attrs;
    auto                  left        = execute_impl(plan, left_idx);
    auto                  right       = execute_impl(plan, right_idx);
    SensibleColumnarTable results;
    results.num_rows = 0;
    for (size_t i = 0; i < output_attrs.size(); i += 1) {
        results.columns.emplace_back(std::get<1>(output_attrs[i]), true);
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

SensibleColumnarTable execute_scan(const Plan&       plan,
    const ScanNode&                                  scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    ZoneScoped;

    auto   table_id   = scan.base_table_id;
    auto&  input      = plan.inputs[table_id];
    size_t column_cnt = output_attrs.size();
    size_t record_cnt = input.num_rows;

    SensibleColumnarTable result;
    result.num_rows = record_cnt;
    result.columns.reserve(column_cnt);
    for (size_t i = 0; i < column_cnt; i += 1) {
        size_t select_col_id = std::get<0>(output_attrs[i]);
        result.columns.emplace_back(input.columns[select_col_id].type, false);

        size_t page_cnt = input.columns[select_col_id].pages.size();
        for (size_t j = 0; j < page_cnt; j += 1) {
            result.columns[i].pages.push_back(input.columns[select_col_id].pages[j]);
        }
    }

    return result;
}

SensibleColumnarTable execute_impl(const Plan& plan, size_t node_idx) {
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
    auto          ret = execute_impl(plan, plan.root);
    ColumnarTable result;
    result.num_rows   = ret.num_rows;
    size_t column_cnt = ret.columns.size();
    result.columns.reserve(column_cnt);
    for (size_t i = 0; i < column_cnt; i += 1) {
        result.columns.push_back(Column(ret.columns[i].type));
        ret.columns[i].owns_pages = false;

        size_t pages_to_move = ret.columns[i].pages.size();
        for (size_t j = 0; j < pages_to_move; j += 1) {
            result.columns[i].pages.push_back(ret.columns[i].pages[j]);
        }
    }
    return result;
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
