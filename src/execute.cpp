#include <attribute.h>
#include <plan.h>
#include <table.h>

// TODO: Remove tray before submission
#include "../build/_deps/tracy-src/public/tracy/Tracy.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace Contest {

void PrintVal(void* value, DataType data_type);

template <typename T>
uint16_t AlingDTOffset() {
    return alignof(T) > 4 ? alignof(T) : 4;
}

uint16_t AlingDTOffset(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return AlingDTOffset<int32_t>();
    case DataType::INT64:   return AlingDTOffset<int64_t>();
    case DataType::FP64:    return AlingDTOffset<double>();
    case DataType::VARCHAR: std::abort();
    }
    return 42; // unreachable - cpp sucks
}

uint8_t SizeDT(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return 4;
    case DataType::INT64:   return 8;
    case DataType::FP64:    return 8;
    case DataType::VARCHAR: std::abort();
    }
    return 42; // unreachable - cpp sucks
}

template <typename T>
T* DataBegin(Page* page) {
    if constexpr (std::is_same<T, char>()) {
        std::abort();
    } else {
        return reinterpret_cast<T*>(&(reinterpret_cast<uint8_t*>(page)[AlingDTOffset<T>()]));
    }
}

char* GetStr(Page* page, uint16_t non_null_id) {
    uint16_t offset =
        non_null_id == 0 ? 4 : reinterpret_cast<uint16_t*>(page)[2 + non_null_id - 1];
    return &(reinterpret_cast<char*>(page)[offset]);
}

void* DataBegin(Page* page, DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return DataBegin<int32_t>(page);
    case DataType::INT64:   return DataBegin<int64_t>(page);
    case DataType::FP64:    return DataBegin<double>(page);
    case DataType::VARCHAR: std::abort();
    }
    return nullptr; // unreachable - cpp sucks
}

struct PageDescriptor {
    uint16_t rows_in_page;
    uint16_t non_null_in_page;
    uint16_t bitmap_size;
    uint8_t  curr_free_slots_in_last_bitmap_byte;
    uint16_t curr_next_data_begin_offset;

    uint8_t* BitMapBegin(Page* page) {
        return &(reinterpret_cast<uint8_t*>(page)[PAGE_SIZE - bitmap_size]);
    }
};

PageDescriptor ParsePage(Page* page, DataType data_type) {
    PageDescriptor result;
    uint16_t*      page_u16                    = reinterpret_cast<uint16_t*>(page);
    result.rows_in_page                        = page_u16[0];
    result.non_null_in_page                    = page_u16[1];
    result.bitmap_size                         = (result.rows_in_page + 7) / 8;
    result.curr_free_slots_in_last_bitmap_byte = (result.rows_in_page + 7) % 8;
    if (data_type != DataType::VARCHAR) {
        result.curr_next_data_begin_offset =
            AlingDTOffset(data_type) + (result.non_null_in_page * SizeDT(data_type));
    } else {
        result.curr_next_data_begin_offset = page_u16[2 + (result.non_null_in_page - 1)];
    }
    return result;
}

struct SensibleColumn {
    DataType           type;
    std::vector<Page*> pages;
    // Used when filling in new pages to avoid re-calculating a bunch of shit
    std::vector<PageDescriptor> page_meta;
    std::vector<bool>           owns_pages;

    void AddPage() {
        Page*     page                                  = new Page;
        uint16_t* page_u16                              = reinterpret_cast<uint16_t*>(page);
        page_u16[0]                                     = 0;
        page_u16[1]                                     = 0;
        reinterpret_cast<uint8_t*>(page)[PAGE_SIZE - 1] = 0; // Clear bitmap byte
        pages.push_back(page);
        uint16_t begin_offset = type == DataType::VARCHAR ? 4 : AlingDTOffset(type);
        page_meta.push_back({0, 0, 1, 8, begin_offset});
        owns_pages.push_back(true);
    }

    SensibleColumn(DataType data_type)
    : type(data_type)
    , pages()
    , page_meta()
    , owns_pages() {}

    ~SensibleColumn() {
        for (size_t i = 0; i < pages.size(); i += 1) {
            if (owns_pages[i]) {
                delete pages[i];
            }
        }
    }
};

void* GetValueClmn(size_t record_id,
    SensibleColumn&       clm,
    bool*                 is_large_str,
    size_t*               page_id_of_large_str_or_str_len);

struct SensibleColumnarTable {
    size_t                      num_rows = 0;
    std::vector<SensibleColumn> columns;
};

SensibleColumnarTable execute_impl(const Plan& plan, size_t node_idx);

uint16_t bottom_three_bits_mask = 7;

template <typename T>
void AppendValue(T* value, SensibleColumn& clm) {
    if (clm.pages.size() == 0) {
        clm.AddPage();
    }
    PageDescriptor& page_info = clm.page_meta.back();
    Page*           page      = clm.pages.back();

    uint16_t bytes_used = page_info.curr_next_data_begin_offset + page_info.bitmap_size;
    bool     will_need_new_bitmap_byte = page_info.curr_free_slots_in_last_bitmap_byte == 0;
    uint16_t new_bytes_for_bitmap      = will_need_new_bitmap_byte ? 1 : 0;
    uint16_t bytes_required            = bytes_used + sizeof(T) + new_bytes_for_bitmap;

    if (bytes_required > PAGE_SIZE) {
        clm.AddPage();
        page                      = clm.pages.back();
        page_info                 = clm.page_meta.back();
        will_need_new_bitmap_byte = false;
    }

    void* current_start =
        &(reinterpret_cast<uint8_t*>(page)[page_info.curr_next_data_begin_offset]);
    reinterpret_cast<T*>(current_start)[0] = *value;
    uint16_t* page_u16                     = reinterpret_cast<uint16_t*>(page);
    page_u16[0]++;
    page_u16[1]++;

    uint8_t* bitmap_start = page_info.BitMapBegin(page);
    if (will_need_new_bitmap_byte) {
        // We need to move the old bitmap one byte to the right
        uint8_t* new_bitmap_start = &bitmap_start[-1];
        // aliasing -> no memcpy
        for (size_t i = 0; i < page_info.bitmap_size; i += 1) {
            new_bitmap_start[i] = bitmap_start[i];
        }
        new_bitmap_start[page_info.bitmap_size] = 0; // clear new last byte
        page_info.bitmap_size++;
        page_info.curr_free_slots_in_last_bitmap_byte = 8;
        bitmap_start                                  = new_bitmap_start;
    }
    uint16_t byte_id       = (page_info.rows_in_page & (~bottom_three_bits_mask)) >> 3;
    uint8_t  bit_id        = page_info.rows_in_page & bottom_three_bits_mask;
    bitmap_start[byte_id] |= (1 << bit_id);

    page_info.curr_next_data_begin_offset += sizeof(T);
    page_info.non_null_in_page++;
    page_info.rows_in_page++;
    page_info.curr_free_slots_in_last_bitmap_byte -= 1;
}

void AppendLargStr(char* value, size_t space_for_value, SensibleColumn& clm) {
    PageDescriptor& page_info = clm.page_meta.back();
    Page*           page      = clm.pages.back();

    if (page_info.rows_in_page != 0) {
        clm.AddPage();
        page      = clm.pages.back();
        page_info = clm.page_meta.back();
    }
    uint16_t* page_u16     = reinterpret_cast<uint16_t*>(page);
    uint8_t*  page_u8      = reinterpret_cast<uint8_t*>(page);
    page_info.rows_in_page = 0xffffu;
    page_u16[0]            = 0xffffu;

    size_t consumed_bytes = 0;
    while (true) {
        uint16_t bytes_for_this_page = (space_for_value - consumed_bytes) > (PAGE_SIZE - 7)
                                         ? (PAGE_SIZE - 7)
                                         : space_for_value - consumed_bytes;
        page_u16[1]                  = bytes_for_this_page;
        page_u8[PAGE_SIZE - 1]       = 0b1u;
        memcpy(&page_u8[4], &value[consumed_bytes], bytes_for_this_page);

        consumed_bytes += bytes_for_this_page;
        if (consumed_bytes < space_for_value) {
            clm.AddPage();
            page                   = clm.pages.back();
            page_info              = clm.page_meta.back();
            page_u16               = reinterpret_cast<uint16_t*>(page);
            page_u8                = reinterpret_cast<uint8_t*>(page);
            page_info.rows_in_page = 0xfffeu;
            page_u16[0]            = 0xfffeu;
        } else {
            break;
        }
    }
}

void AppendStr(void* value, size_t str_len, SensibleColumn& clm) {
    if (clm.pages.size() == 0) {
        clm.AddPage();
    }

    if (str_len >= (PAGE_SIZE - 7)) {
        AppendLargStr((char*)value, str_len, clm);
        return;
    }

    PageDescriptor& page_info  = clm.page_meta.back();
    Page*           page       = clm.pages.back();
    uint16_t        bytes_used = page_info.curr_next_data_begin_offset + page_info.bitmap_size;
    bool     will_need_new_bitmap_byte = page_info.curr_free_slots_in_last_bitmap_byte == 0;
    uint16_t new_bytes_for_bitmap      = will_need_new_bitmap_byte ? 1 : 0;
    uint16_t bytes_required =
        bytes_used + str_len + new_bytes_for_bitmap + 2; // 2 for offset array slot

    if (bytes_required > PAGE_SIZE) {
        clm.AddPage();
        page                      = clm.pages.back();
        page_info                 = clm.page_meta.back();
        will_need_new_bitmap_byte = false;
    }

    uint16_t* page_u16  = reinterpret_cast<uint16_t*>(page);
    char*     page_char = reinterpret_cast<char*>(page);

    uint16_t prev_offset =
        page_info.non_null_in_page == 0 ? 0 : page_u16[2 + (page_info.non_null_in_page - 1)];
    // TODO: this sucks...
    // shift existing strs out of the way for new offset array entry
    // aliasing -> no memcpy
    uint16_t old_start = (page_info.rows_in_page + 2) * 2;
    uint16_t old_end   = page_info.curr_next_data_begin_offset;
    uint16_t to_move   = old_end - old_start;
    uint16_t moved     = 0;
    char*    src       = &(page_char[old_end - 1]);
    while (moved < to_move) {
        src[2] = src[0];
        src--;
        moved++;
    }
    page_info.curr_next_data_begin_offset += 2;

    page_u16[2 + page_info.non_null_in_page] = str_len + prev_offset;
    memcpy(&(page_char[page_info.curr_next_data_begin_offset]), value, str_len);

    page_u16[0]++;
    page_u16[1]++;

    uint8_t* bitmap_start = page_info.BitMapBegin(page);
    if (will_need_new_bitmap_byte) {
        // We need to move the old bitmap one byte to the right
        uint8_t* new_bitmap_start = &bitmap_start[-1];
        // aliasing -> no memcpy
        for (size_t i = 0; i < page_info.bitmap_size; i += 1) {
            new_bitmap_start[i] = bitmap_start[i];
        }
        new_bitmap_start[page_info.bitmap_size] = 0; // clear new last byte
        page_info.bitmap_size++;
        page_info.curr_free_slots_in_last_bitmap_byte = 8;
        bitmap_start                                  = new_bitmap_start;
    }
    uint16_t byte_id       = (page_info.rows_in_page & (~bottom_three_bits_mask)) >> 3;
    uint8_t  bit_id        = page_info.rows_in_page & bottom_three_bits_mask;
    bitmap_start[byte_id] |= (1 << bit_id);

    page_info.non_null_in_page++;
    page_info.rows_in_page++;
    page_info.curr_next_data_begin_offset         += str_len;
    page_info.curr_free_slots_in_last_bitmap_byte -= 1;
}

void AppendNull(SensibleColumn& clm) {
    if (clm.pages.size() == 0) {
        clm.AddPage();
    }
    PageDescriptor& page_info = clm.page_meta.back();
    Page*           page      = clm.pages.back();

    uint16_t bytes_used = page_info.curr_next_data_begin_offset + page_info.bitmap_size;
    bool     will_need_new_bitmap_byte = page_info.curr_free_slots_in_last_bitmap_byte == 0;
    uint16_t new_bytes_for_bitmap      = will_need_new_bitmap_byte ? 1 : 0;
    uint16_t bytes_required            = bytes_used + new_bytes_for_bitmap;

    if (bytes_required > PAGE_SIZE) {
        clm.AddPage();
        page = clm.pages.back();
    }

    uint16_t* page_u16 = reinterpret_cast<uint16_t*>(page);
    page_u16[0]++;
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
        page_info.curr_free_slots_in_last_bitmap_byte = 8;
    }
    uint16_t byte_id       = page_info.rows_in_page / 8;
    uint8_t  bit_id        = page_info.rows_in_page & bottom_three_bits_mask;
    bitmap_start[byte_id] &= ~(1 << bit_id);
    page_info.curr_free_slots_in_last_bitmap_byte -= 1;
}

// Sigh*
void AppendAttr(void* value, SensibleColumn& clm) {
    if (value != nullptr) {
        switch (clm.type) {
        case DataType::INT32:
            AppendValue<int32_t>(reinterpret_cast<int32_t*>(value), clm);
            break;
        case DataType::INT64:
            AppendValue<int64_t>(reinterpret_cast<int64_t*>(value), clm);
            break;
        case DataType::FP64:    AppendValue<double>(reinterpret_cast<double*>(value), clm); break;
        case DataType::VARCHAR: std::abort(); break;
        }
    } else {
        AppendNull(clm);
    }
}

template <typename T>
inline void
InsertToHashmap(std::unordered_map<T, std::vector<size_t>>& tbl, T& key, size_t id) {
    if (auto itr = tbl.find(key); itr == tbl.end()) {
        tbl.emplace(key, std::vector<size_t>(1, id));
    } else {
        itr->second.push_back(id);
    }
}

template <typename T>
void BuildHashTbl(SensibleColumnarTable&        input_tbl,
    size_t                                      col_id,
    std::unordered_map<T, std::vector<size_t>>& tbl) {
    ZoneScoped;
    SensibleColumn& clm_to_hash         = input_tbl.columns[col_id];
    size_t          page_cnt            = clm_to_hash.pages.size();
    size_t          items_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*          cur_page     = clm_to_hash.pages[i];
        PageDescriptor page_info    = clm_to_hash.page_meta[i];
        T*             data         = DataBegin<T>(cur_page);
        uint8_t*       bitmap_begin = page_info.BitMapBegin(cur_page);
        size_t         id           = items_in_prev_pages;

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
                if (bitmap_begin[cur_bitmap_id] == (uint8_t)0xff) {
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
                    }
                }
                cur_bitmap_id += 1;
            }
        }
        items_in_prev_pages += page_info.rows_in_page;
    }
}

template <char*>
void BuildHashTbl(SensibleColumnarTable&                  input_tbl,
    size_t                                                col_id,
    std::unordered_map<std::string, std::vector<size_t>>& tbl) {
    ZoneScoped;
    SensibleColumn& clm_to_hash         = input_tbl.columns[col_id];
    size_t          page_cnt            = clm_to_hash.pages.size();
    size_t          items_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*          cur_page     = clm_to_hash.pages[i];
        PageDescriptor page_info    = clm_to_hash.page_meta[i];
        char*          data         = DataBegin<char>(cur_page);
        uint16_t*      u16_p        = reinterpret_cast<uint16_t*>(cur_page);
        uint8_t*       bitmap_begin = page_info.BitMapBegin(cur_page);
        size_t         id           = items_in_prev_pages;

        uint16_t processed                = 0;
        uint16_t cur_bitmap_id            = 0;
        uint16_t str_base_offset          = page_info.non_null_in_page * 2 + 4;
        uint16_t current_str_begin_offset = str_base_offset;
        if (page_info.rows_in_page == page_info.non_null_in_page) {
            for (size_t j = 0; j < page_info.rows_in_page; j += 1) {
                char*    str             = &data[current_str_begin_offset];
                uint16_t current_str_len = u16_p[processed];

                // This is a quick workaround to make the non null terminated string in
                // the pages work with the hashmap, by using std::string which we
                // construct from a null terminated char* that we create here
                // temporarily
                char* tmp_str = (char*)malloc(current_str_len + 1);
                memcpy(tmp_str, str, current_str_len);
                tmp_str[current_str_len] = 0;
                std::string key(tmp_str);
                free(tmp_str);

                InsertToHashmap<std::string>(tbl, key, id);

                current_str_begin_offset += current_str_len;
                processed++;
                id++;
            }
        } else {
            while (processed < page_info.non_null_in_page) {
                // NOTE: testing at larger than byte granularity could be faster overall
                // (e.g. 512/256bit)
                if (bitmap_begin[cur_bitmap_id] == (uint8_t)0xffu) {
                    // Full byte not null
                    for (size_t i = 0; i < 8; i += 1) {
                        char*    str             = &data[current_str_begin_offset];
                        uint16_t current_str_len = u16_p[processed];

                        // This is a quick workaround to make the non null terminated string in
                        // the pages work with the hashmap, by using std::string which we
                        // construct from a null terminated char* that we create here
                        // temporarily
                        char* tmp_str = (char*)malloc(current_str_len + 1);
                        memcpy(tmp_str, str, current_str_len);
                        tmp_str[current_str_len] = 0;
                        std::string key(tmp_str);
                        free(tmp_str);

                        InsertToHashmap<std::string>(tbl, key, id);

                        current_str_begin_offset += current_str_len;
                        processed++;
                        id++;
                    }
                } else {
                    // Some not null
                    uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                    for (uint8_t intra_bitmap_id  = 0; intra_bitmap_id < 8;
                        intra_bitmap_id          += 1) {
                        if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                            char*    str             = &data[current_str_begin_offset];
                            uint16_t current_str_len = u16_p[processed];

                            // This is a quick workaround to make the non null terminated string
                            // in the pages work with the hashmap, by using std::string which we
                            // construct from a null terminated char* that we create here
                            // temporarily
                            char* tmp_str = (char*)malloc(current_str_len + 1);
                            memcpy(tmp_str, str, current_str_len);
                            tmp_str[current_str_len] = 0;
                            std::string key(tmp_str);
                            free(tmp_str);

                            InsertToHashmap<std::string>(tbl, key, id);

                            current_str_begin_offset += current_str_len;
                            processed++;
                        }
                        id++;
                    }
                }
                cur_bitmap_id += 1;
            }
        }
        items_in_prev_pages += page_info.rows_in_page;
    }
}

uint8_t Sizeof(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return 4;
    case DataType::INT64:   return 8;
    case DataType::FP64:    return 8;
    case DataType::VARCHAR: std::abort();
    }
    return 42; // unreachable - cpp sucks
}

const char* dt_strs[4] = {
    "i32",
    "i64",
    "fp64",
    "string",
};

const char* DtToString(DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return dt_strs[0];
    case DataType::INT64:   return dt_strs[1];
    case DataType::FP64:    return dt_strs[2];
    case DataType::VARCHAR: return dt_strs[3];
    }
    return nullptr; // unreachable - cpp sucks
}

// TODO: this is not great
void* GetValueClmnPage(size_t page_record_id,
    Page*                     page,
    PageDescriptor&           page_info,
    DataType                  data_type,
    bool*                     is_large_str,
    size_t*                   str_len) {
    if (data_type == DataType::VARCHAR && page_info.rows_in_page == 0xffffu) {
        *is_large_str = true;
        return page;
    }
    *is_large_str = false;

    uint16_t non_null_id;
    if (page_info.rows_in_page == page_info.non_null_in_page) {
        non_null_id = page_record_id;
    } else {
        size_t   current_non_null = 0;
        size_t   current_checked  = 0;
        uint8_t* bitmap           = page_info.BitMapBegin(page);
        while (current_checked < page_record_id) {
            // NOTE: popcnt would be nice here but c++ sucks :) (i.e. it is C++ >= 20)
            // TODO: could still do blocked testing here
            uint16_t byte_id = current_checked / 8;
            uint8_t  bit_id  = current_checked % 8;
            if ((bitmap[byte_id] & (1 << bit_id)) != 0) {
                current_non_null++;
            }
            current_checked++;
        }
        non_null_id = current_non_null;
    }

    if (data_type != DataType::VARCHAR) {
        return &reinterpret_cast<uint8_t*>(
            DataBegin(page, data_type))[Sizeof(data_type) * non_null_id];
    }

    uint16_t* u16_p       = reinterpret_cast<uint16_t*>(page);
    uint16_t  base_offset = (page_info.non_null_in_page * 2) + 4;
    uint16_t  start =
        non_null_id == 0 ? base_offset : base_offset + u16_p[2 + (non_null_id - 1)];
    uint16_t end = base_offset + u16_p[2 + non_null_id];
    *str_len     = end - start;
    char* u8_p   = reinterpret_cast<char*>(page);
    return &u8_p[start];
}

void* GetValueClmn(size_t record_id,
    SensibleColumn&       clm,
    bool*                 is_large_str,
    size_t*               page_id_of_large_str_or_str_len) {
    size_t page_cnt = clm.pages.size();
    size_t row_cnt  = 0;
    for (size_t i = 0; i < page_cnt; i += 1) {
        PageDescriptor page_info = clm.page_meta[i];
        size_t         rows_in_page;
        if (clm.type == DataType::VARCHAR) {
            if (page_info.rows_in_page == 0xffffu) {
                rows_in_page = 1;
            } else if (page_info.rows_in_page == 0xfffeu) {
                rows_in_page = 0;
            } else {
                rows_in_page = page_info.rows_in_page;
            }
        } else {
            rows_in_page = page_info.rows_in_page;
        }
        size_t next_row_cnt = row_cnt + page_info.rows_in_page;
        if (record_id < next_row_cnt) {
            void* result = GetValueClmnPage(record_id - row_cnt,
                clm.pages[i],
                page_info,
                clm.type,
                is_large_str,
                page_id_of_large_str_or_str_len);
            if (*is_large_str) {
                *page_id_of_large_str_or_str_len = i;
            }
            return result;
        }
        row_cnt = next_row_cnt;
    }
    return nullptr; // unreachable
}

void PrintVal(void* value, DataType data_type) {
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

char* SpliceLargeString(size_t start_page_id, SensibleColumn& clm) {
    size_t total_len = PAGE_SIZE - 7;
    for (size_t i = start_page_id + 1; i < clm.pages.size(); i += 1) {
        uint16_t* u16_p = reinterpret_cast<uint16_t*>(clm.pages[i]);
        if (u16_p[0] == 0xfffeu) {
            total_len += u16_p[1];
        } else {
            break;
        }
    }
    char* result = reinterpret_cast<char*>(malloc(total_len + 1));

    memcpy(result, &(reinterpret_cast<char*>(clm.pages[start_page_id])[4]), PAGE_SIZE - 7);
    size_t copied = PAGE_SIZE - 7;
    for (size_t i = start_page_id + 1; i < clm.pages.size(); i += 1) {
        uint16_t* u16_p = reinterpret_cast<uint16_t*>(clm.pages[i]);
        if (u16_p[0] == 0xfffeu) {
            memcpy(result, &(reinterpret_cast<char*>(clm.pages[start_page_id])[4]), u16_p[1]);
            copied += u16_p[1];
        } else {
            break;
        }
    }
    result[total_len] = 0;
    return result;
}

void PrintRow(SensibleColumnarTable& tbl, size_t row_id) {
    size_t num_clmns = tbl.columns.size();
    for (size_t j = 0; j < num_clmns; j += 1) {
        bool   is_large_str                 = false;
        size_t page_id_large_str_or_str_len = 0;
        void*  value =
            GetValueClmn(row_id, tbl.columns[j], &is_large_str, &page_id_large_str_or_str_len);
        if (tbl.columns[j].type != DataType::VARCHAR) {
            PrintVal(value, tbl.columns[j].type);
        } else {
            if (is_large_str) {
                value = SpliceLargeString(page_id_large_str_or_str_len, tbl.columns[j]);
                PrintVal(value, tbl.columns[j].type);
                free(value);
            } else {
                char* tmp_str = (char*)malloc(page_id_large_str_or_str_len + 1);
                memcpy(tmp_str, value, page_id_large_str_or_str_len);
                tmp_str[page_id_large_str_or_str_len] = 0;
                std::cout << tmp_str << "\t";
                free(tmp_str);
            }
        }
    }
    std::cout << "\n";
}

void PrintTbl(SensibleColumnarTable& tbl, int64_t max_row_print) {
    size_t num_clmns         = tbl.columns.size();
    size_t num_rows_to_print = max_row_print < 0            ? tbl.num_rows
                             : max_row_print > tbl.num_rows ? tbl.num_rows
                                                            : max_row_print;
    for (size_t i = 0; i < num_rows_to_print; i += 1) {
        PrintRow(tbl, i);
    }
}

// TODO: this is not particularly efficient
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
            // BUG: ? - check lifetime of intermediate tbls
            results.columns[i].pages.push_back(
                tbl_to_use.columns[col_id_to_use].pages[page_id_of_large_str_or_str_len]);
            results.columns[i].page_meta.push_back(
                tbl_to_use.columns[col_id_to_use].page_meta[page_id_of_large_str_or_str_len]);
            results.columns[i].owns_pages.push_back(false);

            for (size_t j = page_id_of_large_str_or_str_len + 1;
                j < tbl_to_use.columns[col_id_to_use].pages.size();
                j += 1) {
                uint16_t* u16_p =
                    reinterpret_cast<uint16_t*>(tbl_to_use.columns[col_id_to_use].pages[j]);
                if (u16_p[0] == 0xfffeu) {
                    results.columns[i].pages.push_back(
                        tbl_to_use.columns[col_id_to_use].pages[j]);
                    results.columns[i].page_meta.push_back(
                        tbl_to_use.columns[col_id_to_use].page_meta[j]);
                    results.columns[i].owns_pages.push_back(false);
                } else {
                    break;
                }
            }
        }
    }
    results.num_rows++;
}

template <typename T>
void Probe(SensibleColumnarTable&               tbl_l,
    SensibleColumnarTable&                      tbl_r,
    size_t                                      col_id_of_non_hashed_in,
    std::unordered_map<T, std::vector<size_t>>& tbl,
    // TODO: when para -> use one result table for each and then "merge" which is just
    // collecting the pages per column in one result table i.e. no need to actually cpy
    // smth between pages
    SensibleColumnarTable&                           results,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    bool                                             hashed_is_left) {
    ZoneScoped;

    SensibleColumnarTable& non_hashed_tbl = hashed_is_left ? tbl_r : tbl_l;

    SensibleColumn& clm_to_check        = non_hashed_tbl.columns[col_id_of_non_hashed_in];
    size_t          page_cnt            = clm_to_check.pages.size();
    size_t          items_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*           cur_page     = clm_to_check.pages[i];
        PageDescriptor& page_info    = clm_to_check.page_meta[i];
        T*              data         = DataBegin<T>(cur_page);
        uint8_t*        bitmap_begin = page_info.BitMapBegin(cur_page);
        size_t          curr_id      = items_in_prev_pages;

        uint16_t curr_non_null_id = 0;
        uint16_t cur_bitmap_id    = 0;
        if (page_info.rows_in_page == page_info.non_null_in_page) {
            for (size_t j = 0; j < page_info.rows_in_page; j += 1) {
                T& key = data[curr_non_null_id++];
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
                curr_id++;
            }
        } else {
            while (curr_non_null_id < page_info.non_null_in_page) {
                if (bitmap_begin[cur_bitmap_id] == 0xffu) { // Full byte not null
                    for (size_t j = 0; j < 8; j += 1) {
                        T& key = data[curr_non_null_id++];
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
                        curr_id++;
                    }
                } else {
                    // Some not null
                    uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                    for (uint8_t intra_bitmap_id  = 0; intra_bitmap_id < 8;
                        intra_bitmap_id          += 1) {
                        if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                            T& key = data[curr_non_null_id++];
                            if (auto itr = tbl.find(key); itr != tbl.end()) {
                                std::vector<size_t>& matches   = itr->second;
                                size_t               match_cnt = matches.size();
                                for (size_t j = 0; j < match_cnt; j += 1) {
                                    CollectRecord(tbl_l,
                                        tbl_r,
                                        results,
                                        hashed_is_left ? matches[j] : curr_id,
                                        hashed_is_left ? curr_id : matches[j],
                                        output_attrs);
                                }
                            }
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

template <char*>
void Probe(SensibleColumnarTable&                         tbl_l,
    SensibleColumnarTable&                                tbl_r,
    size_t                                                col_id_of_non_hashed_in,
    std::unordered_map<std::string, std::vector<size_t>>& tbl,
    // TODO: when para -> use one result table for each and then "merge" which is just
    // collecting the pages per column in one result table i.e. no need to actually cpy
    // smth between pages
    SensibleColumnarTable&                           results,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    bool                                             hashed_is_left) {
    ZoneScoped;
    SensibleColumnarTable& non_hashed_tbl = hashed_is_left ? tbl_r : tbl_l;

    SensibleColumn& clm_to_check = non_hashed_tbl.columns[col_id_of_non_hashed_in];
    size_t          page_cnt     = clm_to_check.pages.size();
    size_t          items_handled_in_prev_pages = 0;
    for (size_t i = 0; i < page_cnt; i += 1) { // TODO: do the parallelisation here
        Page*           cur_page     = clm_to_check.pages[i];
        PageDescriptor& page_info    = clm_to_check.page_meta[i];
        char*           data         = DataBegin<char>(cur_page);
        uint16_t*       u16_p        = reinterpret_cast<uint16_t*>(cur_page);
        uint8_t*        bitmap_begin = page_info.BitMapBegin(cur_page);
        size_t          curr_id      = items_handled_in_prev_pages;

        uint16_t curr_non_null_id         = 0;
        uint16_t cur_bitmap_id            = 0;
        uint16_t str_base_offset          = page_info.non_null_in_page * 2 + 4;
        uint16_t current_str_begin_offset = str_base_offset;
        if (page_info.rows_in_page == page_info.non_null_in_page) {
            for (size_t j = 0; j < page_info.rows_in_page; j += 1) {
                char*    str             = &data[current_str_begin_offset];
                uint16_t current_str_len = u16_p[curr_id + 2];

                // This is a quick workaround to make the non null terminated string in
                // the pages work with the hashmap, by using std::string which we
                // construct from a null terminated char* that we create here
                // temporarily
                char* tmp_str = (char*)malloc(current_str_len + 1);
                memcpy(tmp_str, str, current_str_len);
                tmp_str[current_str_len] = 0;

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

                current_str_begin_offset += current_str_len;
                curr_non_null_id++;
                curr_id++;
            }
        } else {
            while (curr_non_null_id < page_info.non_null_in_page) {
                if (bitmap_begin[cur_bitmap_id] == 0xffu) {
                    // Full byte not null
                    for (size_t j = 0; j < 8; j += 1) {
                        char*    str             = &data[current_str_begin_offset];
                        uint16_t current_str_len = u16_p[curr_id + 2];

                        // This is a quick workaround to make the non null terminated string in
                        // the pages work with the hashmap, by using std::string which we
                        // construct from a null terminated char* that we create here
                        // temporarily
                        char* tmp_str = (char*)malloc(current_str_len + 1);
                        memcpy(tmp_str, str, current_str_len);
                        tmp_str[current_str_len] = 0;

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

                        current_str_begin_offset += current_str_len;
                        curr_non_null_id++;
                        curr_id++;
                    }
                } else {
                    // Some not null
                    uint8_t current_byte = bitmap_begin[cur_bitmap_id];
                    for (uint8_t intra_bitmap_id  = 0; intra_bitmap_id < 8;
                        intra_bitmap_id          += 1) {
                        if ((current_byte & (1 << intra_bitmap_id)) != 0) {
                            char*    str             = &data[current_str_begin_offset];
                            uint16_t current_str_len = u16_p[curr_id + 2];

                            // This is a quick workaround to make the non null terminated string
                            // in the pages work with the hashmap, by using std::string which we
                            // construct from a null terminated char* that we create here
                            // temporarily
                            char* tmp_str = (char*)malloc(current_str_len + 1);
                            memcpy(tmp_str, str, current_str_len);
                            tmp_str[current_str_len] = 0;

                            std::string key(tmp_str);
                            if (auto itr = tbl.find(key); itr != tbl.end()) {
                                std::vector<size_t>& matches   = itr->second;
                                size_t               match_cnt = matches.size();
                                for (size_t j = 0; j < match_cnt; j += 1) {
                                    CollectRecord(tbl_l,
                                        tbl_r,
                                        results,
                                        hashed_is_left ? matches[j] : curr_id,
                                        hashed_is_left ? curr_id : matches[j],
                                        output_attrs);
                                }
                            }
                            free(tmp_str);

                            current_str_begin_offset += current_str_len;
                            curr_non_null_id++;
                            if (curr_non_null_id >= page_info.non_null_in_page) {
                                break;
                            }
                        }
                        curr_id++;
                    }
                }
                cur_bitmap_id += 1;
            }
        }
        items_handled_in_prev_pages += page_info.rows_in_page;
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
            Probe(left, right, right_col, hash_table, results, output_attrs, true);
        } else {
            BuildHashTbl(right, right_col, hash_table);
            Probe(left, right, left_col, hash_table, results, output_attrs, false);
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
        results.columns.emplace_back(std::get<1>(output_attrs[i]));
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
        result.columns.emplace_back(input.columns[select_col_id].type);

        size_t page_cnt = input.columns[select_col_id].pages.size();
        for (size_t j = 0; j < page_cnt; j += 1) {
            // NOTE: just taking the ptrs here should be fine since plans.inputs should live
            // during the whole query
            result.columns[i].pages.push_back(input.columns[select_col_id].pages[j]);
            result.columns[i].page_meta.push_back(
                ParsePage(result.columns[i].pages.back(), result.columns[i].type));
            result.columns[i].owns_pages.push_back(false);
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

        size_t pages_to_move = ret.columns[i].pages.size();
        for (size_t j = 0; j < pages_to_move; j += 1) {
            // // BUG: ? -  check lifetimes - maybe need cpy here
            // result.columns[i].pages.push_back(ret.columns[i].pages[j]);
            // ret.columns[i].owns_pages[j] = false;
            Page* page = new Page;
            std::memcpy(page, ret.columns[i].pages[j], PAGE_SIZE);
            result.columns[i].pages.push_back(page);
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
