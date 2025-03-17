#pragma once

#include <attribute.h>
#include <plan.h>

#include "data_type_util.h"

#include <cstdint>
#include <iostream>

static uint16_t bottom_three_bits_mask = 0b111;

template <typename T>
inline T* DataBegin(Page* page) {
    return reinterpret_cast<T*>(&(reinterpret_cast<uint8_t*>(page)[AlingDTOffset<T>()]));
}

template <>
inline char* DataBegin(Page* page) = delete;
template <>
inline char** DataBegin(Page* page) = delete;

static inline char* DataStrBegin(Page* page) {
    return &(reinterpret_cast<char*>(page)[4]);
}

static inline char* GetStr(Page* page, uint16_t non_null_id) {
    uint16_t offset =
        non_null_id == 0 ? 4 : reinterpret_cast<uint16_t*>(page)[2 + non_null_id - 1];
    return &(reinterpret_cast<char*>(page)[offset]);
}

static inline void* DataBegin(Page* page, DataType data_type) {
    switch (data_type) {
    case DataType::INT32:   return DataBegin<int32_t>(page);
    case DataType::INT64:   return DataBegin<int64_t>(page);
    case DataType::FP64:    return DataBegin<double>(page);
    case DataType::VARCHAR: assert(false);
    }
    return nullptr; // unreachable - cpp sucks
}

struct PageDescriptor {
    uint16_t rows_in_page;
    uint16_t non_null_in_page;
    uint16_t bitmap_size;
    uint8_t  curr_free_slots_in_last_bitmap_byte;
    uint16_t curr_next_data_begin_offset;

    inline uint8_t* BitMapBegin(Page* page) {
        return &(reinterpret_cast<uint8_t*>(page)[PAGE_SIZE - bitmap_size]);
    }
};

static PageDescriptor ParsePage(Page* page, DataType data_type) {
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

struct SensibleColumnarTable {
    size_t                      num_rows = 0;
    std::vector<SensibleColumn> columns;
};

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

template <>
void AppendValue<char>(char* value, SensibleColumn& clm) = delete;
template <>
void AppendValue<char*>(char** value, SensibleColumn& clm) = delete;

static void AppendLargStr(char* value, size_t space_for_value, SensibleColumn& clm) {
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

static void AppendStr(void* value, size_t str_len, SensibleColumn& clm) {
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

static void AppendNull(SensibleColumn& clm) {
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

static void AppendAttr(void* value, SensibleColumn& clm) {
    if (value != nullptr) {
        switch (clm.type) {
        case DataType::INT32:
            AppendValue<int32_t>(reinterpret_cast<int32_t*>(value), clm);
            break;
        case DataType::INT64:
            AppendValue<int64_t>(reinterpret_cast<int64_t*>(value), clm);
            break;
        case DataType::FP64:    AppendValue<double>(reinterpret_cast<double*>(value), clm); break;
        case DataType::VARCHAR: assert(false); break;
        }
    } else {
        AppendNull(clm);
    }
}

// TODO: this is not great
static void* GetValueClmnPage(size_t page_record_id,
    Page*                            page,
    PageDescriptor&                  page_info,
    DataType                         data_type,
    bool*                            is_large_str,
    size_t*                          str_len) {
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

static void* GetValueClmn(size_t record_id,
    SensibleColumn&              clm,
    bool*                        is_large_str,
    size_t*                      page_id_of_large_str_or_str_len) {
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

// Debug area:

static char* ConcatLargeString(size_t start_page_id, SensibleColumn& clm) {
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

static void PrintRow(SensibleColumnarTable& tbl, size_t row_id) {
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
                value = ConcatLargeString(page_id_large_str_or_str_len, tbl.columns[j]);
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

static void PrintTbl(SensibleColumnarTable& tbl, int64_t max_row_print) {
    size_t num_clmns         = tbl.columns.size();
    size_t num_rows_to_print = max_row_print < 0            ? tbl.num_rows
                             : max_row_print > tbl.num_rows ? tbl.num_rows
                                                            : max_row_print;
    for (size_t i = 0; i < num_rows_to_print; i += 1) {
        PrintRow(tbl, i);
    }
}
