#pragma once

#include <attribute.h>
#include <plan.h>

#include "data_type_util.h"
// #include "../build/_deps/tracy-src/public/tracy/Tracy.hpp"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <vector>

static constexpr uint16_t bottom_three_bits_mask     = 0b111u;
static constexpr uint16_t is_first_big_str_page      = 0xffffu;
static constexpr uint16_t is_subsequent_big_str_page = 0xfffeu;
static constexpr uint16_t PROBEBATCH_SIZE            = 64;
static constexpr uint8_t  u8_max                     = std::numeric_limits<uint8_t>::max();
static constexpr uint64_t u64_max                    = std::numeric_limits<uint64_t>::max();

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

enum class PageType {
    Regular,
    LargeStrFirst,
    LargeStrSubsequent,
};

struct RegularPageDescriptor {
    uint16_t rows_in_page;
    uint16_t non_null_in_page;
    uint16_t bitmap_size;
    uint16_t curr_next_data_begin_offset;
    uint8_t  curr_free_slots_in_last_bitmap_byte;
};

struct LargeStrPageDescriptor {
    uint16_t str_len;
};

struct PageDescriptor {
    union {
        RegularPageDescriptor  regular;
        LargeStrPageDescriptor large_str;
    };

    PageType type;
};

static inline uint8_t* BitMapBegin(Page* page, RegularPageDescriptor* page_info) {
    return &(reinterpret_cast<uint8_t*>(page)[PAGE_SIZE - page_info->bitmap_size]);
}

static PageDescriptor ParsePage(Page* page, DataType data_type) {
    PageDescriptor result;
    uint16_t*      page_u16 = reinterpret_cast<uint16_t*>(page);

    switch (page_u16[0]) {
    case is_first_big_str_page:
        result.type      = PageType::LargeStrFirst;
        result.large_str = {page_u16[1]};
        return result;
    case is_subsequent_big_str_page:
        result.type      = PageType::LargeStrSubsequent;
        result.large_str = {page_u16[1]};
        return result;
    }

    result.type                     = PageType::Regular;
    result.regular.rows_in_page     = page_u16[0];
    result.regular.non_null_in_page = page_u16[1];
    result.regular.bitmap_size =
        ((result.regular.rows_in_page + 7) & ~bottom_three_bits_mask) >> 3;
    result.regular.curr_free_slots_in_last_bitmap_byte =
        (result.regular.rows_in_page + 7) & bottom_three_bits_mask;
    if (data_type != DataType::VARCHAR) {
        result.regular.curr_next_data_begin_offset =
            AlingDTOffset(data_type) + (result.regular.non_null_in_page * SizeDT(data_type));
    } else {
        result.regular.curr_next_data_begin_offset =
            page_u16[2 + (result.regular.non_null_in_page - 1)];
    }
    return result;
}

enum class PageOwnerShip {
    InputPage,
    Owning,
    NonOwning,
};

struct TrackedPage {
    Page* page;
    // Used when filling in new pages to avoid re-calculating a bunch of shit
    PageDescriptor page_info;
    PageOwnerShip  ownership;
};

struct SensibleColumn {
    DataType                 type;
    std::vector<TrackedPage> pages;

    void AddEmptyRegularPage() {
        // ZoneScoped;
        Page*     page         = new Page;
        uint16_t* page_u16     = reinterpret_cast<uint16_t*>(page);
        uint8_t*  page_u8      = reinterpret_cast<uint8_t*>(page);
        page_u8[PAGE_SIZE - 1] = 0u; // Clear bitmap byte
        page_u16[0]            = 0;
        page_u16[1]            = 0;
        uint16_t begin_offset  = type == DataType::VARCHAR ? 4 : AlingDTOffset(type);
        pages.push_back({
            page,
            {{0, 0, 1, begin_offset, 8}, PageType::Regular},
            PageOwnerShip::Owning
        });
    }

    void AddLargeStrPage(uint16_t str_len, bool is_first) {
        // ZoneScoped;
        Page*     page     = new Page;
        uint16_t* page_u16 = reinterpret_cast<uint16_t*>(page);
        uint8_t*  page_u8  = reinterpret_cast<uint8_t*>(page);
        page_u16[0]        = is_first ? is_first_big_str_page : is_subsequent_big_str_page;
        page_u16[1]        = str_len;
        pages.push_back({
            page,
            {{str_len}, is_first ? PageType::LargeStrFirst : PageType::LargeStrSubsequent},
            PageOwnerShip::Owning
        });
    }

    void AddPageCopy(Page* in_page) {
        // ZoneScoped;
        Page* page = new Page;
        memcpy(page, in_page, PAGE_SIZE);
        pages.push_back({page, ParsePage(in_page, type), PageOwnerShip::Owning});
    }

    void AddInputPage(Page* input_page) {
        // ZoneScoped;
        pages.push_back({input_page, ParsePage(input_page, type), PageOwnerShip::InputPage});
    }

    SensibleColumn(DataType data_type)
    : type(data_type)
    , pages() {}

    ~SensibleColumn() {
        for (size_t i = 0; i < pages.size(); i += 1) {
            if (pages[i].ownership == PageOwnerShip::Owning) {
                delete pages[i].page;
            }
        }
    }
};

struct SensibleColumnarTable {
    size_t                      num_rows = 0;
    std::vector<SensibleColumn> columns;
};

static void AddByteToBitmap(uint8_t** bitmap, RegularPageDescriptor* page_info) {
    // ZoneScoped;
    uint8_t* old_bitmap_start = *bitmap;
    // We need to move the old bitmap one byte to the right
    uint8_t* new_bitmap_start = &old_bitmap_start[-1];
    // aliasing -> no memcpy
    for (size_t i = 0; i < page_info->bitmap_size; i += 1) {
        new_bitmap_start[i] = old_bitmap_start[i];
    }
    new_bitmap_start[page_info->bitmap_size] = 0; // clear new last byte
    page_info->bitmap_size++;
    page_info->curr_free_slots_in_last_bitmap_byte = 8;
    *bitmap                                        = new_bitmap_start;
}

template <typename T>
void AppendValue(T* value, SensibleColumn& clm) {
    // ZoneScoped;
    if (clm.pages.size() == 0) {
        clm.AddEmptyRegularPage();
    }
    TrackedPage* page = &clm.pages.back();
    if (page->page_info.type != PageType::Regular) {
        clm.AddEmptyRegularPage();
        page = &clm.pages.back();
    }

    uint16_t bytes_used = page->page_info.regular.curr_next_data_begin_offset
                        + page->page_info.regular.bitmap_size;
    bool will_need_new_bitmap_byte =
        page->page_info.regular.curr_free_slots_in_last_bitmap_byte == 0;
    uint16_t new_bytes_for_bitmap = will_need_new_bitmap_byte ? 1 : 0;
    uint16_t bytes_required       = bytes_used + sizeof(T) + new_bytes_for_bitmap;

    if (bytes_required > PAGE_SIZE) {
        clm.AddEmptyRegularPage();
        page                      = &clm.pages.back();
        will_need_new_bitmap_byte = false;
    }

    void* current_start                    = &(reinterpret_cast<uint8_t*>(
        page->page)[page->page_info.regular.curr_next_data_begin_offset]);
    reinterpret_cast<T*>(current_start)[0] = *value;
    uint16_t* page_u16                     = reinterpret_cast<uint16_t*>(page->page);
    page_u16[0]++;
    page_u16[1]++;

    uint8_t* bitmap_start = BitMapBegin(page->page, &page->page_info.regular);
    if (will_need_new_bitmap_byte) {
        AddByteToBitmap(&bitmap_start, &page->page_info.regular);
    }

    uint16_t byte_id = (page->page_info.regular.rows_in_page & (~bottom_three_bits_mask)) >> 3;
    uint8_t  bit_id  = page->page_info.regular.rows_in_page & bottom_three_bits_mask;
    bitmap_start[byte_id] |= (1 << bit_id);

    page->page_info.regular.curr_next_data_begin_offset += sizeof(T);
    page->page_info.regular.non_null_in_page++;
    page->page_info.regular.rows_in_page++;
    page->page_info.regular.curr_free_slots_in_last_bitmap_byte -= 1;
}

template <>
void AppendValue<char>(char* value, SensibleColumn& clm) = delete;
template <>
void AppendValue<char*>(char** value, SensibleColumn& clm) = delete;

static void AppendLargStr(char* value, size_t large_str_len, SensibleColumn& clm) {
    // ZoneScoped;
    bool   is_first       = true;
    size_t consumed_bytes = 0;
    while (consumed_bytes < large_str_len) {
        uint16_t bytes_for_this_page = (large_str_len - consumed_bytes) > MAX_PAGE_BIG_STR_SIZE
                                         ? MAX_PAGE_BIG_STR_SIZE
                                         : large_str_len - consumed_bytes;
        clm.AddLargeStrPage(bytes_for_this_page, is_first);
        is_first = false;

        TrackedPage* page    = &clm.pages.back();
        uint8_t*     page_u8 = reinterpret_cast<uint8_t*>(page->page);
        memcpy(&page_u8[4], &value[consumed_bytes], bytes_for_this_page);

        consumed_bytes += bytes_for_this_page;
    }
}

static void AppendStr(void* value, size_t str_len, SensibleColumn& clm) {
    // ZoneScoped;
    if (str_len > (MAX_PAGE_REGULAR_STR_SIZE)) {
        AppendLargStr((char*)value, str_len, clm);
        return;
    }
    if (clm.pages.size() == 0) {
        clm.AddEmptyRegularPage();
    }
    TrackedPage* page = &clm.pages.back();
    if (page->page_info.type != PageType::Regular) {
        clm.AddEmptyRegularPage();
        page = &clm.pages.back();
    }

    uint16_t bytes_used = page->page_info.regular.curr_next_data_begin_offset
                        + page->page_info.regular.bitmap_size;
    bool will_need_new_bitmap_byte =
        page->page_info.regular.curr_free_slots_in_last_bitmap_byte == 0;
    uint16_t new_bytes_for_bitmap = will_need_new_bitmap_byte ? 1 : 0;
    uint16_t bytes_required =
        bytes_used + str_len + new_bytes_for_bitmap + 2; // 2 for offset array slot

    if (bytes_required > PAGE_SIZE) {
        clm.AddEmptyRegularPage();
        page                      = &clm.pages.back();
        will_need_new_bitmap_byte = false;
    }

    uint16_t* page_u16  = reinterpret_cast<uint16_t*>(page->page);
    char*     page_char = reinterpret_cast<char*>(page->page);

    uint16_t prev_offset = page->page_info.regular.non_null_in_page == 0
                             ? 0
                             : page_u16[2 + (page->page_info.regular.non_null_in_page - 1)];
    // TODO: this sucks...
    // shift existing strs out of the way for new offset array entry
    // aliasing -> no memcpy
    uint16_t old_start = (page->page_info.regular.rows_in_page + 2) * 2;
    uint16_t old_end   = page->page_info.regular.curr_next_data_begin_offset;
    uint16_t to_move   = old_end - old_start;
    uint16_t moved     = 0;
    char*    src       = &(page_char[old_end - 1]);
    while (moved < to_move) {
        src[2] = src[0];
        src--;
        moved++;
    }
    page->page_info.regular.curr_next_data_begin_offset += 2;

    page_u16[2 + page->page_info.regular.non_null_in_page] = str_len + prev_offset;
    memcpy(&(page_char[page->page_info.regular.curr_next_data_begin_offset]), value, str_len);

    page_u16[0]++;
    page_u16[1]++;

    uint8_t* bitmap_start = BitMapBegin(page->page, &page->page_info.regular);
    if (will_need_new_bitmap_byte) {
        AddByteToBitmap(&bitmap_start, &page->page_info.regular);
    }

    uint16_t byte_id = (page->page_info.regular.rows_in_page & (~bottom_three_bits_mask)) >> 3;
    uint8_t  bit_id  = page->page_info.regular.rows_in_page & bottom_three_bits_mask;
    bitmap_start[byte_id] |= (1 << bit_id);

    page->page_info.regular.non_null_in_page++;
    page->page_info.regular.rows_in_page++;
    page->page_info.regular.curr_next_data_begin_offset         += str_len;
    page->page_info.regular.curr_free_slots_in_last_bitmap_byte -= 1;
}

static void AppendNull(SensibleColumn& clm) {
    // ZoneScoped;
    if (clm.pages.size() == 0) {
        clm.AddEmptyRegularPage();
    }
    TrackedPage* page = &clm.pages.back();
    if (page->page_info.type != PageType::Regular) {
        clm.AddEmptyRegularPage();
        page = &clm.pages.back();
    }

    uint16_t bytes_used = page->page_info.regular.curr_next_data_begin_offset
                        + page->page_info.regular.bitmap_size;
    bool will_need_new_bitmap_byte =
        page->page_info.regular.curr_free_slots_in_last_bitmap_byte == 0;
    uint16_t new_bytes_for_bitmap = will_need_new_bitmap_byte ? 1 : 0;
    uint16_t bytes_required       = bytes_used + new_bytes_for_bitmap;

    if (bytes_required > PAGE_SIZE) {
        clm.AddEmptyRegularPage();
        page = &clm.pages.back();
    }

    uint16_t* page_u16 = reinterpret_cast<uint16_t*>(page->page);
    page_u16[0]++;
    page->page_info.regular.rows_in_page++;

    uint8_t* bitmap_start = BitMapBegin(page->page, &page->page_info.regular);
    if (will_need_new_bitmap_byte) {
        AddByteToBitmap(&bitmap_start, &page->page_info.regular);
    }

    uint16_t byte_id = (page->page_info.regular.rows_in_page & ~bottom_three_bits_mask) >> 3;
    uint8_t  bit_id  = page->page_info.regular.rows_in_page & bottom_three_bits_mask;
    bitmap_start[byte_id] &= ~(1 << bit_id);

    page->page_info.regular.curr_free_slots_in_last_bitmap_byte -= 1;
}

static void AppendAttr(void* value, SensibleColumn& clm) {
    if (value != nullptr) {
        switch (clm.type) {
        case DataType::INT32:   AppendValue<int32_t>(static_cast<int32_t*>(value), clm); break;
        case DataType::INT64:   AppendValue<int64_t>(static_cast<int64_t*>(value), clm); break;
        case DataType::FP64:    AppendValue<double>(static_cast<double*>(value), clm); break;
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
    // ZoneScoped;
    if (data_type == DataType::VARCHAR && page_info.type == PageType::LargeStrFirst) {
        *is_large_str = true;
        return page;
    }
    *is_large_str = false;
    assert(page_info.type == PageType::Regular);

    uint16_t non_null_id;
    if (page_info.regular.rows_in_page == page_info.regular.non_null_in_page) {
        non_null_id = page_record_id;
    } else {
        uint8_t* bitmap = BitMapBegin(page, &page_info);

        uint16_t byte_id = (page_record_id & ~bottom_three_bits_mask) >> 3;
        uint8_t  bit_id  = page_record_id & bottom_three_bits_mask;
        if ((bitmap[byte_id] & (1 << bit_id)) == 0) {
            return nullptr;
        }

        size_t current_non_null = 0;
        size_t current_checked  = 0;
        while (current_checked < page_record_id) {
            // NOTE: popcnt would be nice here but c++ sucks :) (i.e. it is C++ >= 20)
            // TODO: could still do blocked testing here
            byte_id = (current_checked & ~bottom_three_bits_mask) >> 3;
            bit_id  = current_checked & bottom_three_bits_mask;
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
    uint16_t  base_offset = (page_info.regular.non_null_in_page * 2) + 4;
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
    // ZoneScoped;
    size_t page_cnt = clm.pages.size();
    size_t row_cnt  = 0;
    for (size_t i = 0; i < page_cnt; i += 1) {
        TrackedPage&   page      = clm.pages[i];
        PageDescriptor page_info = page.page_info;
        size_t         rows_in_page;
        switch (page_info.type) {
        case PageType::Regular: {
            rows_in_page = page_info.regular.rows_in_page;
            break;
        };
        case PageType::LargeStrFirst: {
            rows_in_page = 1;
            break;
        };
        case PageType::LargeStrSubsequent: {
            continue;
        };
        }
        size_t next_row_cnt = row_cnt + rows_in_page;
        if (record_id < next_row_cnt) {
            void* result = GetValueClmnPage(record_id - row_cnt,
                page.page,
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
    std::abort(); // unreachable
    // return nullptr; // unreachable
}

// TODO: this is not particularly efficient. We could instead collect all ids and then collect
// the attr later column-wise
static void CollectRecord(SensibleColumnarTable*            tbl_l,
    SensibleColumnarTable*                           tbl_r,
    SensibleColumnarTable*                           results,
    size_t                                           record_id_l,
    size_t                                           record_id_r,
    const std::vector<std::tuple<size_t, DataType>>* output_attrs) {
    size_t attr_cnt   = output_attrs->size();
    size_t attr_l_cnt = tbl_l->columns.size();
    for (size_t i = 0; i < attr_cnt; i += 1) {
        size_t col_id   = std::get<0>((*output_attrs)[i]);
        size_t use_left = col_id < attr_l_cnt;

        SensibleColumnarTable* tbl_to_use       = use_left ? tbl_l : tbl_r;
        size_t                 col_id_to_use    = use_left ? col_id : col_id - attr_l_cnt;
        size_t                 record_id_to_use = use_left ? record_id_l : record_id_r;

        bool   is_large_str                    = false;
        size_t page_id_of_large_str_or_str_len = 0;
        void*  attr                            = GetValueClmn(record_id_to_use,
            tbl_to_use->columns[col_id_to_use],
            &is_large_str,
            &page_id_of_large_str_or_str_len);

        if (attr != nullptr) {
            if (!is_large_str) {
                if (tbl_to_use->columns[col_id_to_use].type == DataType::VARCHAR) {
                    AppendStr(attr, page_id_of_large_str_or_str_len, results->columns[i]);
                } else {
                    AppendAttr(attr, results->columns[i]);
                }
            } else {
                // TODO: this copy could be elided see comment in execute_hash_join()
                results->columns[i].AddPageCopy(tbl_to_use->columns[col_id_to_use]
                        .pages[page_id_of_large_str_or_str_len]
                        .page);

                for (size_t j = page_id_of_large_str_or_str_len + 1;
                    j < tbl_to_use->columns[col_id_to_use].pages.size();
                    j += 1) {
                    uint16_t* u16_p = reinterpret_cast<uint16_t*>(
                        tbl_to_use->columns[col_id_to_use].pages[j].page);
                    if (u16_p[0] == is_subsequent_big_str_page) {
                        // TODO: this copy could be elided see comment in execute_hash_join()
                        results->columns[i].AddPageCopy(
                            tbl_to_use->columns[col_id_to_use].pages[j].page);
                    } else {
                        break;
                    }
                }
            }
        } else {
            AppendNull(results->columns[i]);
        }
    }
    results->num_rows++;
}


// Debug area:

static char* ConcatLargeString(size_t start_page_id, SensibleColumn& clm) {
    // ZoneScoped;
    size_t initial_len = reinterpret_cast<uint16_t*>(clm.pages[start_page_id].page)[1];
    size_t total_len   = initial_len;
    for (size_t i = start_page_id + 1; i < clm.pages.size(); i += 1) {
        uint16_t* u16_p = reinterpret_cast<uint16_t*>(clm.pages[i].page);
        if (u16_p[0] == is_subsequent_big_str_page) {
            total_len += u16_p[1];
        } else {
            break;
        }
    }
    char* result = reinterpret_cast<char*>(malloc(total_len + 1));

    memcpy(result, &(reinterpret_cast<char*>(clm.pages[start_page_id].page)[4]), initial_len);
    size_t copied = initial_len;
    for (size_t i = start_page_id + 1; i < clm.pages.size(); i += 1) {
        uint16_t* u16_p = reinterpret_cast<uint16_t*>(clm.pages[i].page);
        if (u16_p[0] == is_subsequent_big_str_page) {
            memcpy(result,
                &(reinterpret_cast<char*>(clm.pages[start_page_id].page)[4]),
                u16_p[1]);
            copied += u16_p[1];
        } else {
            break;
        }
    }
    result[total_len] = '\0';
    return result;
}

static void PrintRow(SensibleColumnarTable& tbl, size_t row_id) {
    size_t num_clmns = tbl.columns.size();
    std::cout << "Row id: " << row_id << " ";
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
                if (value == nullptr) {
                    std::cout << "Null\t";
                } else {
                    char* tmp_str = (char*)malloc(page_id_large_str_or_str_len + 1);
                    memcpy(tmp_str, value, page_id_large_str_or_str_len);
                    tmp_str[page_id_large_str_or_str_len] = '\0';
                    std::cout << tmp_str << "\t";
                    free(tmp_str);
                }
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

static void PrintClmStats(SensibleColumn& clm) {
    const char* type_name = DtToString(clm.type);
    std::cout << "Clm Stats:\nType: " << type_name << "\nNum Pages: " << clm.pages.size();
    if (clm.type == DataType::VARCHAR) {
        size_t big_str_cnt      = 0;
        size_t big_str_cnt_page = 0;
        for (size_t i = 0; i < clm.pages.size(); i += 1) {
            if (clm.pages[i].page_info.type == PageType::LargeStrFirst) {
                big_str_cnt++;
                big_str_cnt_page++;
            }
            if (clm.pages[i].page_info.type == PageType::LargeStrSubsequent) {
                big_str_cnt_page++;
            }
        }
        std::cout << " Num Big strs: " << big_str_cnt
                  << " Num pages for big str: " << big_str_cnt_page;
    }
    std::cout << "\n";
}

static void PrintTblStats(SensibleColumnarTable& tbl) {
    std::cout << "Table Stats:\n\nNum Clms: " << tbl.columns.size()
              << "\nNum Rows: " << tbl.num_rows << std::endl;
    for (size_t i = 0; i < tbl.columns.size(); i += 1) {
        PrintClmStats(tbl.columns[i]);
    }
    std::cout << std::endl;
}

static void PrintOutAttr(const std::vector<std::tuple<size_t, DataType>>& attrs) {
    for (size_t i = 0; i < attrs.size(); i += 1) {
        std::cout << "Attribute i: " << i << " src_id: " << std::get<0>(attrs[i])
                  << " DataType: " << DtToString(std::get<1>(attrs[i])) << std::endl;
    }
}
