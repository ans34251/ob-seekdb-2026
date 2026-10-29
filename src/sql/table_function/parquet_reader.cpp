/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/table_function/parquet_reader.h"

#include <cstdio>
#include <cstring>
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
// Parquet physical types (parquet.thrift enum Type).
enum ParquetType { PQ_BOOLEAN = 0, PQ_INT32 = 1, PQ_INT64 = 2, PQ_INT96 = 3,
                   PQ_FLOAT = 4, PQ_DOUBLE = 5, PQ_BYTE_ARRAY = 6, PQ_FIXED = 7 };
enum ParquetCodec { PQ_UNCOMPRESSED = 0, PQ_SNAPPY = 1, PQ_GZIP = 2, PQ_LZO = 3,
                    PQ_BROTLI = 4, PQ_LZ4 = 5, PQ_ZSTD = 6 };
enum ParquetPageType { PQ_DATA_PAGE = 0, PQ_INDEX_PAGE = 1, PQ_DICT_PAGE = 2, PQ_DATA_PAGE_V2 = 3 };
enum ParquetRepetition { PQ_REQUIRED = 0, PQ_OPTIONAL = 1, PQ_REPEATED = 2 };
// Thrift compact protocol type ids.
enum ThriftType { TT_STOP = 0, TT_BOOL_TRUE = 1, TT_BOOL_FALSE = 2, TT_BYTE = 3, TT_I16 = 4,
                  TT_I32 = 5, TT_I64 = 6, TT_DOUBLE = 7, TT_BINARY = 8, TT_LIST = 9, TT_SET = 10,
                  TT_MAP = 11, TT_STRUCT = 12 };

// Minimal Thrift compact-protocol reader: varints, zigzag ints, binaries, lists,
// structs.  Exactly what the Parquet footer and page headers need.
class ThriftCompactReader
{
public:
  ThriftCompactReader(const char *data, int64_t size)
    : data_(data), size_(size), pos_(0), last_field_id_(0) {}
  int64_t pos() const { return pos_; }
  void set_pos(int64_t p) { pos_ = p; }
  int32_t last_field_id() const { return last_field_id_; }
  void set_last_field_id(int32_t v) { last_field_id_ = v; }

  int read_u8(uint8_t &b)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= size_) { ret = OB_DESERIALIZE_ERROR; }
    else { b = static_cast<uint8_t>(data_[pos_++]); }
    return ret;
  }
  int read_varint(uint64_t &v)
  {
    int ret = OB_SUCCESS;
    v = 0; int shift = 0; bool done = false;
    while (OB_SUCC(ret) && !done) {
      uint8_t b = 0;
      if (OB_FAIL(read_u8(b))) {
      } else {
        v |= static_cast<uint64_t>(b & 0x7f) << shift;
        if (!(b & 0x80)) { done = true; }
        else if ((shift += 7) > 63) { ret = OB_DESERIALIZE_ERROR; }
      }
    }
    return ret;
  }
  int read_zigzag(int64_t &v)
  {
    int ret = OB_SUCCESS;
    uint64_t u = 0;
    if (OB_FAIL(read_varint(u))) {
    } else { int64_t zz = static_cast<int64_t>(u >> 1); if (u & 1) { zz = ~zz; } v = zz; }
    return ret;
  }
  int read_i32(int32_t &v)
  { int64_t z = 0; int ret = read_zigzag(z); v = static_cast<int32_t>(z); return ret; }
  int read_i64(int64_t &v) { return read_zigzag(v); }
  // On return type == TT_STOP means the enclosing struct ended.
  int read_field_begin(uint8_t &type, int32_t &field_id)
  {
    int ret = OB_SUCCESS;
    uint8_t hdr = 0;
    type = TT_STOP; field_id = 0;
    if (OB_FAIL(read_u8(hdr))) {
    } else if (TT_STOP == hdr) {
      type = TT_STOP;
    } else {
      const int32_t delta = hdr >> 4;
      type = hdr & 0x0f;
      if (0 == delta) { int64_t z = 0; if (OB_FAIL(read_zigzag(z))) {} else { field_id = static_cast<int32_t>(z); } }
      else { field_id = last_field_id_ + delta; }
      if (OB_SUCC(ret)) { last_field_id_ = field_id; }
    }
    return ret;
  }
  int read_binary(ObString &s)
  {
    int ret = OB_SUCCESS;
    uint64_t len = 0;
    if (OB_FAIL(read_varint(len))) {
    } else if (pos_ + static_cast<int64_t>(len) > size_) { ret = OB_DESERIALIZE_ERROR; }
    else { s.assign_ptr(data_ + pos_, static_cast<int32_t>(len)); pos_ += static_cast<int64_t>(len); }
    return ret;
  }
  int read_list_begin(uint8_t &elem_type, int32_t &count)
  {
    int ret = OB_SUCCESS;
    uint8_t hdr = 0;
    if (OB_FAIL(read_u8(hdr))) {
    } else {
      int32_t sz = hdr >> 4;
      elem_type = hdr & 0x0f;
      if (15 == sz) { uint64_t big = 0; if (OB_FAIL(read_varint(big))) {} else { sz = static_cast<int32_t>(big); } }
      if (OB_SUCC(ret)) { count = sz; }
    }
    return ret;
  }
  int skip_struct();
  int skip_value(uint8_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
      case TT_BOOL_TRUE: case TT_BOOL_FALSE: break;  // value is in the field header
      case TT_BYTE: { uint8_t b = 0; ret = read_u8(b); break; }
      case TT_I16: case TT_I32: case TT_I64: { int64_t v = 0; ret = read_zigzag(v); break; }
      case TT_DOUBLE: if (pos_ + 8 > size_) { ret = OB_DESERIALIZE_ERROR; } else { pos_ += 8; } break;
      case TT_BINARY: {
        uint64_t len = 0;
        if (OB_FAIL(read_varint(len))) {
        } else if (pos_ + static_cast<int64_t>(len) > size_) { ret = OB_DESERIALIZE_ERROR; }
        else { pos_ += static_cast<int64_t>(len); }
        break;
      }
      case TT_LIST: case TT_SET: {
        uint8_t etype = 0; int32_t cnt = 0;
        if (OB_FAIL(read_list_begin(etype, cnt))) {
        } else { for (int32_t i = 0; OB_SUCC(ret) && i < cnt; ++i) { ret = skip_value(etype); } }
        break;
      }
      case TT_MAP: {
        uint64_t cnt = 0;
        if (OB_FAIL(read_varint(cnt))) {
        } else if (cnt > 0) {
          uint8_t kv = 0;
          if (OB_FAIL(read_u8(kv))) {
          } else {
            const uint8_t kt = kv >> 4, vt = kv & 0x0f;
            for (uint64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
              if (OB_FAIL(skip_value(kt))) { } else { ret = skip_value(vt); }
            }
          }
        }
        break;
      }
      case TT_STRUCT: ret = skip_struct(); break;
      default: ret = OB_DESERIALIZE_ERROR;
    }
    return ret;
  }
private:
  const char *data_; int64_t size_; int64_t pos_; int32_t last_field_id_;
};

int ThriftCompactReader::skip_struct()
{
  int ret = OB_SUCCESS;
  const int32_t saved = last_field_id_;
  last_field_id_ = 0;
  while (OB_SUCC(ret)) {
    uint8_t type = 0; int32_t fid = 0;
    if (OB_FAIL(read_field_begin(type, fid))) {
    } else if (TT_STOP == type) { break; }
    else { ret = skip_value(type); }
  }
  last_field_id_ = saved;
  return ret;
}

// Raw-block Snappy decompressor (no framing).  Writes into a caller buffer.
int snappy_uncompress(const char *input, int64_t input_len,
                      char *output, int64_t output_cap, int64_t &output_len)
{
  int ret = OB_SUCCESS;
  output_len = 0;
  int64_t ip = 0, op = 0;
  uint64_t expected = 0;
  {
    int shift = 0; bool done = false;
    while (OB_SUCC(ret) && !done) {
      if (ip >= input_len) { ret = OB_DESERIALIZE_ERROR; }
      else {
        const uint8_t b = static_cast<uint8_t>(input[ip++]);
        expected |= static_cast<uint64_t>(b & 0x7f) << shift;
        if (!(b & 0x80)) { done = true; }
        else if ((shift += 7) > 63) { ret = OB_DESERIALIZE_ERROR; }
      }
    }
  }
  if (OB_SUCC(ret) && static_cast<int64_t>(expected) > output_cap) { ret = OB_SIZE_OVERFLOW; }
  while (OB_SUCC(ret) && ip < input_len) {
    const uint8_t tag = static_cast<uint8_t>(input[ip++]);
    const uint8_t kind = tag & 0x03;
    if (0 == kind) {  // literal
      int64_t len = tag >> 2;
      if (len < 60) { len += 1; }
      else {
        const int64_t extra = len - 59;
        if (ip + extra > input_len) { ret = OB_DESERIALIZE_ERROR; break; }
        len = 0;
        for (int64_t i = 0; i < extra; ++i) { len |= static_cast<int64_t>(static_cast<uint8_t>(input[ip + i])) << (8 * i); }
        len += 1; ip += extra;
      }
      if (ip + len > input_len || op + len > output_cap) { ret = OB_DESERIALIZE_ERROR; break; }
      MEMCPY(output + op, input + ip, len); ip += len; op += len;
    } else {  // copy
      int64_t len = 0, offset = 0;
      if (1 == kind) {
        len = ((tag >> 2) & 0x07) + 4;
        if (ip + 1 > input_len) { ret = OB_DESERIALIZE_ERROR; break; }
        offset = (static_cast<int64_t>(tag >> 5) << 8) | static_cast<uint8_t>(input[ip]); ip += 1;
      } else if (2 == kind) {
        len = (tag >> 2) + 1;
        if (ip + 2 > input_len) { ret = OB_DESERIALIZE_ERROR; break; }
        offset = static_cast<int64_t>(static_cast<uint8_t>(input[ip]))
               | (static_cast<int64_t>(static_cast<uint8_t>(input[ip + 1])) << 8); ip += 2;
      } else {
        len = (tag >> 2) + 1;
        if (ip + 4 > input_len) { ret = OB_DESERIALIZE_ERROR; break; }
        offset = static_cast<int64_t>(static_cast<uint8_t>(input[ip]))
               | (static_cast<int64_t>(static_cast<uint8_t>(input[ip + 1])) << 8)
               | (static_cast<int64_t>(static_cast<uint8_t>(input[ip + 2])) << 16)
               | (static_cast<int64_t>(static_cast<uint8_t>(input[ip + 3])) << 24); ip += 4;
      }
      if (offset <= 0 || offset > op || op + len > output_cap) { ret = OB_DESERIALIZE_ERROR; break; }
      for (int64_t i = 0; i < len; ++i) { output[op] = output[op - offset]; ++op; }
    }
  }
  if (OB_SUCC(ret)) {
    if (op != static_cast<int64_t>(expected)) { ret = OB_DESERIALIZE_ERROR; }
    else { output_len = op; }
  }
  return ret;
}

int32_t bit_width_of(int32_t max_level)
{ int32_t w = 0; while (max_level > 0) { ++w; max_level >>= 1; } return w; }

// RLE / bit-packing hybrid decoder for levels (buf has no length prefix).
int decode_levels(const char *buf, int64_t buf_len, int32_t bit_width, int32_t count, int32_t *out)
{
  int ret = OB_SUCCESS;
  int32_t produced = 0;
  if (0 == bit_width) {
    for (int32_t i = 0; i < count; ++i) { out[i] = 0; }
    produced = count;
  } else {
    int64_t pos = 0;
    while (OB_SUCC(ret) && produced < count && pos < buf_len) {
      uint64_t header = 0;
      {
        int shift = 0; bool done = false;
        while (!done) {
          if (pos >= buf_len) { ret = OB_DESERIALIZE_ERROR; break; }
          const uint8_t b = static_cast<uint8_t>(buf[pos++]);
          header |= static_cast<uint64_t>(b & 0x7f) << shift;
          if (!(b & 0x80)) { done = true; } else { shift += 7; }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (header & 1) {  // bit-packed run
        const int32_t total = static_cast<int32_t>(header >> 1) * 8;
        const int64_t nbytes = (static_cast<int64_t>(total) * bit_width + 7) / 8;
        if (pos + nbytes > buf_len) { ret = OB_DESERIALIZE_ERROR; break; }
        for (int32_t i = 0; OB_SUCC(ret) && i < total && produced < count; ++i) {
          const int64_t base_bit = static_cast<int64_t>(i) * bit_width;
          uint64_t accum = 0;
          for (int32_t b = 0; b < bit_width; ++b) {
            const int64_t bit_index = base_bit + b;
            const int64_t byte_index = bit_index / 8;
            const int32_t bit_offset = static_cast<int32_t>(bit_index % 8);
            if (pos + byte_index < buf_len) {
              accum |= static_cast<uint64_t>((static_cast<uint8_t>(buf[pos + byte_index]) >> bit_offset) & 1) << b;
            }
          }
          out[produced++] = static_cast<int32_t>(accum);
        }
        pos += nbytes;
      } else {  // RLE run
        const int32_t run = static_cast<int32_t>(header >> 1);
        const int64_t nbytes = (bit_width + 7) / 8;
        if (pos + nbytes > buf_len) { ret = OB_DESERIALIZE_ERROR; break; }
        uint64_t val = 0;
        for (int64_t i = 0; i < nbytes; ++i) { val |= static_cast<uint64_t>(static_cast<uint8_t>(buf[pos + i])) << (8 * i); }
        pos += nbytes;
        for (int32_t i = 0; i < run && produced < count; ++i) { out[produced++] = static_cast<int32_t>(val); }
      }
    }
    if (OB_SUCC(ret) && produced < count) { ret = OB_DESERIALIZE_ERROR; }
  }
  return ret;
}

int32_t physical_type_size(int32_t t)
{
  int32_t sz = -1;
  switch (t) { case PQ_INT32: sz = 4; break; case PQ_INT64: sz = 8; break;
               case PQ_FLOAT: sz = 4; break; case PQ_DOUBLE: sz = 8; break; default: sz = -1; }
  return sz;
}
uint32_t read_le32(const char *p)
{
  return static_cast<uint32_t>(static_cast<uint8_t>(p[0]))
       | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8)
       | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16)
       | (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24);
}
uint64_t read_le64(const char *p)
{
  uint64_t u = 0;
  for (int i = 0; i < 8; ++i) { u |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i); }
  return u;
}

struct SchemaElement {
  bool has_type_; int32_t type_; bool has_repetition_; int32_t repetition_; int32_t num_children_; ObString name_;
  SchemaElement() : has_type_(false), type_(0), has_repetition_(false), repetition_(0), num_children_(0) {}
};
struct LeafColumn {
  ObString name_; int32_t physical_type_; int32_t repetition_; int32_t max_def_level_; int32_t max_rep_level_;
  LeafColumn() : physical_type_(0), repetition_(0), max_def_level_(0), max_rep_level_(0) {}
};
struct ColumnChunkInfo {
  int32_t physical_type_; int32_t codec_; int64_t num_values_; int64_t data_page_offset_;
  ColumnChunkInfo() : physical_type_(0), codec_(0), num_values_(0), data_page_offset_(-1) {}
};
struct PageInfo {
  int32_t page_type_; int32_t uncompressed_page_size_; int32_t compressed_page_size_;
  int32_t num_values_; int32_t num_nulls_; int32_t encoding_; bool is_v2_;
  PageInfo() : page_type_(0), uncompressed_page_size_(0), compressed_page_size_(0),
               num_values_(0), num_nulls_(0), encoding_(0), is_v2_(false) {}
};

int parquet_read_whole_file(const ObString &path, ObIAllocator &allocator, char *&data, int64_t &size)
{
  int ret = OB_SUCCESS;
  data = nullptr; size = 0;
  char path_buf[1024];
  FILE *file = nullptr;
  if (path.empty() || path.length() >= static_cast<int64_t>(sizeof(path_buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path for read_parquet", K(ret), K(path));
  } else {
    MEMCPY(path_buf, path.ptr(), path.length());
    path_buf[path.length()] = '\0';
    if (OB_ISNULL(file = ::fopen(path_buf, "rb"))) {
      ret = OB_IO_ERROR;
      LOG_WARN("failed to open parquet file", K(ret), K(path));
    } else {
      int64_t file_size = 0;
      if (0 != ::fseek(file, 0, SEEK_END) || (file_size = ::ftell(file)) < 0 || 0 != ::fseek(file, 0, SEEK_SET)) {
        ret = OB_IO_ERROR;
      } else if (file_size > 0 && OB_ISNULL(data = static_cast<char *>(allocator.alloc(file_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (file_size > 0 && ::fread(data, 1, file_size, file) != static_cast<size_t>(file_size)) {
        ret = OB_IO_ERROR;
      } else { size = file_size; }
      ::fclose(file);
    }
  }
  return ret;
}

int parse_schema_element(ThriftCompactReader &r, SchemaElement &el)
{
  int ret = OB_SUCCESS;
  const int32_t saved = r.last_field_id();
  r.set_last_field_id(0);
  while (OB_SUCC(ret)) {
    uint8_t type = 0; int32_t fid = 0;
    if (OB_FAIL(r.read_field_begin(type, fid))) {
    } else if (TT_STOP == type) { break; }
    else if (1 == fid) { el.has_type_ = true; ret = r.read_i32(el.type_); }
    else if (3 == fid) { el.has_repetition_ = true; ret = r.read_i32(el.repetition_); }
    else if (4 == fid && TT_BINARY == type) { ret = r.read_binary(el.name_); }
    else if (5 == fid) { ret = r.read_i32(el.num_children_); }
    else { ret = r.skip_value(type); }
  }
  r.set_last_field_id(saved);
  return ret;
}
int parse_column_metadata(ThriftCompactReader &r, ColumnChunkInfo &cc)
{
  int ret = OB_SUCCESS;
  const int32_t saved = r.last_field_id();
  r.set_last_field_id(0);
  while (OB_SUCC(ret)) {
    uint8_t type = 0; int32_t fid = 0;
    if (OB_FAIL(r.read_field_begin(type, fid))) {
    } else if (TT_STOP == type) { break; }
    else if (1 == fid) { ret = r.read_i32(cc.physical_type_); }
    else if (4 == fid) { ret = r.read_i32(cc.codec_); }
    else if (5 == fid) { ret = r.read_i64(cc.num_values_); }
    else if (9 == fid) { ret = r.read_i64(cc.data_page_offset_); }
    else { ret = r.skip_value(type); }
  }
  r.set_last_field_id(saved);
  return ret;
}
int parse_column_chunk(ThriftCompactReader &r, ColumnChunkInfo &cc)
{
  int ret = OB_SUCCESS;
  const int32_t saved = r.last_field_id();
  r.set_last_field_id(0);
  while (OB_SUCC(ret)) {
    uint8_t type = 0; int32_t fid = 0;
    if (OB_FAIL(r.read_field_begin(type, fid))) {
    } else if (TT_STOP == type) { break; }
    else if (3 == fid && TT_STRUCT == type) { ret = parse_column_metadata(r, cc); }
    else { ret = r.skip_value(type); }
  }
  r.set_last_field_id(saved);
  return ret;
}
// RowGroup: field 1 = columns (list), field 3 = num_rows.
int parse_row_group(ThriftCompactReader &r, int64_t &num_rows, ObIArray<ColumnChunkInfo> &chunks)
{
  int ret = OB_SUCCESS;
  const int32_t saved = r.last_field_id();
  r.set_last_field_id(0);
  while (OB_SUCC(ret)) {
    uint8_t type = 0; int32_t fid = 0;
    if (OB_FAIL(r.read_field_begin(type, fid))) {
    } else if (TT_STOP == type) { break; }
    else if (1 == fid && TT_LIST == type) {
      uint8_t etype = 0; int32_t cnt = 0;
      if (OB_FAIL(r.read_list_begin(etype, cnt))) {
      } else {
        for (int32_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
          ColumnChunkInfo cc;
          if (OB_FAIL(parse_column_chunk(r, cc))) { } else { ret = chunks.push_back(cc); }
        }
      }
    } else if (3 == fid) { ret = r.read_i64(num_rows); }
    else { ret = r.skip_value(type); }
  }
  r.set_last_field_id(saved);
  return ret;
}
// Pre-order walk of the schema list; index advances by reference.
int collect_leaves(const ObIArray<SchemaElement> &schema, int32_t &index,
                   int32_t parent_def, int32_t parent_rep, ObIArray<LeafColumn> &leaves)
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= schema.count()) { ret = OB_DESERIALIZE_ERROR; }
  else {
    const SchemaElement el = schema.at(index);
    int32_t def = parent_def, rep = parent_rep;
    if (el.has_repetition_) {
      if (PQ_OPTIONAL == el.repetition_) { def = parent_def + 1; }
      else if (PQ_REPEATED == el.repetition_) { def = parent_def + 1; rep = parent_rep + 1; }
    }
    ++index;
    if (el.has_type_) {
      LeafColumn leaf;
      leaf.name_ = el.name_; leaf.physical_type_ = el.type_;
      leaf.repetition_ = el.has_repetition_ ? el.repetition_ : PQ_REQUIRED;
      leaf.max_def_level_ = def; leaf.max_rep_level_ = rep;
      ret = leaves.push_back(leaf);
    } else {
      for (int32_t i = 0; OB_SUCC(ret) && i < el.num_children_; ++i) {
        ret = collect_leaves(schema, index, def, rep, leaves);
      }
    }
  }
  return ret;
}
int parse_data_page_header_v1(ThriftCompactReader &r, PageInfo &info)
{
  int ret = OB_SUCCESS;
  const int32_t saved = r.last_field_id();
  r.set_last_field_id(0);
  while (OB_SUCC(ret)) {
    uint8_t type = 0; int32_t fid = 0;
    if (OB_FAIL(r.read_field_begin(type, fid))) {
    } else if (TT_STOP == type) { break; }
    else if (1 == fid) { ret = r.read_i32(info.num_values_); }
    else if (2 == fid) { ret = r.read_i32(info.encoding_); }
    else { ret = r.skip_value(type); }
  }
  r.set_last_field_id(saved);
  return ret;  // V1 carries no num_nulls; derived from the levels.
}
int parse_data_page_header_v2(ThriftCompactReader &r, PageInfo &info)
{
  int ret = OB_SUCCESS;
  const int32_t saved = r.last_field_id();
  r.set_last_field_id(0);
  while (OB_SUCC(ret)) {
    uint8_t type = 0; int32_t fid = 0;
    if (OB_FAIL(r.read_field_begin(type, fid))) {
    } else if (TT_STOP == type) { break; }
    else if (1 == fid) { ret = r.read_i32(info.num_values_); }
    else if (2 == fid) { ret = r.read_i32(info.num_nulls_); }
    else if (4 == fid) { ret = r.read_i32(info.encoding_); }
    else { ret = r.skip_value(type); }
  }
  r.set_last_field_id(saved);
  return ret;
}
// PageHeader: 1=type 2=uncompressed 3=compressed 5=v1 header 8=v2 header.
int parse_page_header(ThriftCompactReader &r, PageInfo &info)
{
  int ret = OB_SUCCESS;
  r.set_last_field_id(0);
  while (OB_SUCC(ret)) {
    uint8_t type = 0; int32_t fid = 0;
    if (OB_FAIL(r.read_field_begin(type, fid))) {
    } else if (TT_STOP == type) { break; }
    else if (1 == fid) { ret = r.read_i32(info.page_type_); }
    else if (2 == fid) { ret = r.read_i32(info.uncompressed_page_size_); }
    else if (3 == fid) { ret = r.read_i32(info.compressed_page_size_); }
    else if (5 == fid && TT_STRUCT == type) { ret = parse_data_page_header_v1(r, info); }
    else if (8 == fid && TT_STRUCT == type) { info.is_v2_ = true; ret = parse_data_page_header_v2(r, info); }
    else { ret = r.skip_value(type); }
  }
  return ret;
}

// Decode one value from a PLAIN buffer into an ObObj.
int read_plain_value(const char *p, int32_t physical_type, ObObj &obj)
{
  int ret = OB_SUCCESS;
  switch (physical_type) {
    case PQ_INT32: obj.set_int(static_cast<int64_t>(static_cast<int32_t>(read_le32(p)))); break;
    case PQ_INT64: obj.set_int(static_cast<int64_t>(read_le64(p))); break;
    case PQ_FLOAT: { uint32_t u = read_le32(p); float f = 0; MEMCPY(&f, &u, 4); obj.set_double(static_cast<double>(f)); break; }
    case PQ_DOUBLE: { uint64_t u = read_le64(p); double d = 0; MEMCPY(&d, &u, 8); obj.set_double(d); break; }
    default: ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "read_parquet value physical type");
  }
  return ret;
}

// Read every data page of one column chunk, appending num_rows values to out.
int read_column_values(const char *data, int64_t size, const ColumnChunkInfo &cc,
                       const LeafColumn &leaf, int64_t num_rows, ObIAllocator &allocator,
                       ObIArray<ObObj> &out)
{
  int ret = OB_SUCCESS;
  const int32_t value_size = physical_type_size(leaf.physical_type_);
  if (value_size < 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "read_parquet column physical type");
  } else if (cc.data_page_offset_ < 0) {
    ret = OB_DESERIALIZE_ERROR;
  }
  const int32_t def_bit_width = bit_width_of(leaf.max_def_level_);
  int64_t offset = cc.data_page_offset_;
  int64_t rows_read = 0;
  while (OB_SUCC(ret) && rows_read < num_rows) {
    ThriftCompactReader pr(data, size);
    pr.set_pos(offset);
    PageInfo info;
    if (OB_FAIL(parse_page_header(pr, info))) { break; }
    const int64_t page_data_start = pr.pos();
    if (PQ_DICT_PAGE == info.page_type_) { offset = page_data_start + info.compressed_page_size_; continue; }
    if (PQ_DATA_PAGE != info.page_type_ && PQ_DATA_PAGE_V2 != info.page_type_) {
      ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "read_parquet page type"); break;
    }
    const int32_t page_num_values = info.num_values_;
    if (page_num_values <= 0 || page_data_start + info.compressed_page_size_ > size) { ret = OB_DESERIALIZE_ERROR; break; }
    int32_t *defs = static_cast<int32_t *>(allocator.alloc(sizeof(int32_t) * page_num_values));
    if (OB_ISNULL(defs)) { ret = OB_ALLOCATE_MEMORY_FAILED; break; }
    const char *value_src = nullptr;
    int64_t value_src_len = 0;
    if (info.is_v2_) {
      // V2: levels stored uncompressed & unprefixed, then compressed values.
      const int64_t num_non_null = page_num_values - info.num_nulls_;
      const int64_t value_bytes = static_cast<int64_t>(value_size) * num_non_null;
      const int64_t def_len = info.uncompressed_page_size_ - value_bytes;  // rep_len == 0 (flat)
      const int64_t comp_val_len = info.compressed_page_size_ - def_len;
      if (def_len < 0 || comp_val_len < 0) { ret = OB_DESERIALIZE_ERROR; break; }
      const char *def_ptr = data + page_data_start;
      const char *comp_val_ptr = data + page_data_start + def_len;
      if (OB_FAIL(decode_levels(def_ptr, def_len, def_bit_width, page_num_values, defs))) { break; }
      char *vbuf = static_cast<char *>(allocator.alloc(value_bytes > 0 ? value_bytes : 1));
      if (OB_ISNULL(vbuf)) { ret = OB_ALLOCATE_MEMORY_FAILED; break; }
      if (PQ_UNCOMPRESSED == cc.codec_) {
        if (comp_val_len < value_bytes) { ret = OB_DESERIALIZE_ERROR; break; }
        MEMCPY(vbuf, comp_val_ptr, value_bytes); value_src_len = value_bytes;
      } else if (PQ_SNAPPY == cc.codec_) {
        if (OB_FAIL(snappy_uncompress(comp_val_ptr, comp_val_len, vbuf, value_bytes, value_src_len))) { break; }
      } else { ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "read_parquet compression codec"); break; }
      value_src = vbuf;
    } else {
      // V1: whole page (levels + values) compressed together; levels length-prefixed.
      const int64_t comp_len = info.compressed_page_size_;
      const int64_t uncomp_len = info.uncompressed_page_size_;
      char *pbuf = static_cast<char *>(allocator.alloc(uncomp_len > 0 ? uncomp_len : 1));
      if (OB_ISNULL(pbuf)) { ret = OB_ALLOCATE_MEMORY_FAILED; break; }
      int64_t got = 0;
      if (PQ_UNCOMPRESSED == cc.codec_) {
        if (comp_len < uncomp_len) { ret = OB_DESERIALIZE_ERROR; break; }
        MEMCPY(pbuf, data + page_data_start, uncomp_len); got = uncomp_len;
      } else if (PQ_SNAPPY == cc.codec_) {
        if (OB_FAIL(snappy_uncompress(data + page_data_start, comp_len, pbuf, uncomp_len, got))) { break; }
      } else { ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "read_parquet compression codec"); break; }
      int64_t p = 0;
      if (OB_SUCC(ret) && leaf.max_rep_level_ > 0) {
        if (p + 4 > got) { ret = OB_DESERIALIZE_ERROR; }
        else { const int64_t rl = read_le32(pbuf + p); p += 4 + rl; }
      }
      if (OB_SUCC(ret) && leaf.max_def_level_ > 0) {
        if (p + 4 > got) { ret = OB_DESERIALIZE_ERROR; }
        else {
          const int64_t dl = read_le32(pbuf + p); p += 4;
          if (p + dl > got) { ret = OB_DESERIALIZE_ERROR; }
          else if (OB_FAIL(decode_levels(pbuf + p, dl, def_bit_width, page_num_values, defs))) { }
          else { p += dl; }
        }
      } else if (OB_SUCC(ret)) {
        for (int32_t i = 0; i < page_num_values; ++i) { defs[i] = leaf.max_def_level_; }
      }
      if (OB_SUCC(ret)) { value_src = pbuf + p; value_src_len = got - p; }
    }
    // Emit values; a value is present when its definition level == max_def_level.
    int64_t vp = 0;
    for (int32_t i = 0; OB_SUCC(ret) && i < page_num_values; ++i) {
      ObObj obj;
      if (defs[i] == leaf.max_def_level_) {
        if (vp + value_size > value_src_len) { ret = OB_DESERIALIZE_ERROR; break; }
        if (OB_FAIL(read_plain_value(value_src + vp, leaf.physical_type_, obj))) { break; }
        vp += value_size;
      } else { obj.set_null(); }
      if (OB_SUCC(ret)) { ret = out.push_back(obj); }
    }
    if (OB_SUCC(ret)) { rows_read += page_num_values; offset = page_data_start + info.compressed_page_size_; }
  }
  return ret;
}

// Parse the whole file.  need_values=false stops after schema inference.
int parse_parquet(const char *data, int64_t size, ObIAllocator &allocator,
                  ObIArray<FileTableColumn> &columns, ObIArray<ObObj> &col_major,
                  int64_t &row_count, int64_t &col_count, bool need_values)
{
  int ret = OB_SUCCESS;
  row_count = 0; col_count = 0;
  columns.reuse(); col_major.reuse();
  if (size < 12 || 0 != MEMCMP(data, "PAR1", 4) || 0 != MEMCMP(data + size - 4, "PAR1", 4)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "read_parquet: file is not a valid Parquet file");
  }
  ObSEArray<SchemaElement, 16> schema;
  ObSEArray<LeafColumn, 8> leaves;
  ObSEArray<int64_t, 4> rg_num_rows;
  ObSEArray<ColumnChunkInfo, 8> chunks;
  int64_t fm_num_rows = 0;
  if (OB_SUCC(ret)) {
    const uint32_t footer_len = read_le32(data + size - 8);
    const int64_t footer_start = size - 8 - static_cast<int64_t>(footer_len);
    if (footer_len <= 0 || footer_start < 4) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "read_parquet: bad footer length");
    } else {
      ThriftCompactReader r(data + footer_start, footer_len);
      r.set_last_field_id(0);
      while (OB_SUCC(ret)) {
        uint8_t type = 0; int32_t fid = 0;
        if (OB_FAIL(r.read_field_begin(type, fid))) { break; }
        if (TT_STOP == type) { break; }
        else if (2 == fid && TT_LIST == type) {
          uint8_t etype = 0; int32_t cnt = 0;
          if (OB_FAIL(r.read_list_begin(etype, cnt))) {
          } else {
            for (int32_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
              SchemaElement el;
              if (OB_FAIL(parse_schema_element(r, el))) { } else { ret = schema.push_back(el); }
            }
          }
        } else if (3 == fid) { ret = r.read_i64(fm_num_rows); }
        else if (4 == fid && TT_LIST == type) {
          uint8_t etype = 0; int32_t cnt = 0;
          if (OB_FAIL(r.read_list_begin(etype, cnt))) {
          } else {
            for (int32_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
              int64_t nr = 0;
              if (OB_FAIL(parse_row_group(r, nr, chunks))) { } else { ret = rg_num_rows.push_back(nr); }
            }
          }
        } else { ret = r.skip_value(type); }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int32_t index = 0;
    if (schema.count() <= 0) { ret = OB_DESERIALIZE_ERROR; }
    else { ret = collect_leaves(schema, index, 0, 0, leaves); }
  }
  if (OB_SUCC(ret)) {
    col_count = leaves.count();
    for (int64_t c = 0; OB_SUCC(ret) && c < col_count; ++c) {
      const LeafColumn &leaf = leaves.at(c);
      FileTableColumn col;
      char *nbuf = static_cast<char *>(allocator.alloc(leaf.name_.length() > 0 ? leaf.name_.length() : 1));
      if (OB_ISNULL(nbuf)) { ret = OB_ALLOCATE_MEMORY_FAILED; }
      else {
        if (leaf.name_.length() > 0) { MEMCPY(nbuf, leaf.name_.ptr(), leaf.name_.length()); }
        col.name_.assign_ptr(nbuf, static_cast<int32_t>(leaf.name_.length()));
        switch (leaf.physical_type_) {
          case PQ_INT32: case PQ_INT64: col.type_ = ObIntType; break;
          case PQ_FLOAT: case PQ_DOUBLE: col.type_ = ObDoubleType; break;
          default: ret = OB_NOT_SUPPORTED; LOG_USER_ERROR(OB_NOT_SUPPORTED, "read_parquet column physical type");
        }
        col.nullable_ = (leaf.repetition_ != PQ_REQUIRED);
        if (OB_SUCC(ret)) { ret = columns.push_back(col); }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t total = 0;
    for (int64_t g = 0; g < rg_num_rows.count(); ++g) { total += rg_num_rows.at(g); }
    if (0 == total) { total = fm_num_rows; }
    row_count = total;
  }
  if (OB_SUCC(ret) && need_values) {
    for (int64_t c = 0; OB_SUCC(ret) && c < col_count; ++c) {
      ObSEArray<ObObj, 64> col_vals;
      for (int64_t g = 0; OB_SUCC(ret) && g < rg_num_rows.count(); ++g) {
        const int64_t ci = g * col_count + c;
        if (ci >= chunks.count()) { ret = OB_DESERIALIZE_ERROR; break; }
        ret = read_column_values(data, size, chunks.at(ci), leaves.at(c), rg_num_rows.at(g), allocator, col_vals);
      }
      for (int64_t rr = 0; OB_SUCC(ret) && rr < row_count; ++rr) {
        ObObj v;
        if (rr < col_vals.count()) { v = col_vals.at(rr); } else { v.set_null(); }
        ret = col_major.push_back(v);
      }
    }
  }
  return ret;
}

class ReadParquetScan final : public ITableFunctionScan
{
public:
  explicit ReadParquetScan(const TableFunctionScanContext &context)
    : arguments_(context.arguments_), projection_(context.column_ids_), allocator_("ReadParquet"),
      data_(nullptr), size_(0), row_count_(0), col_count_(0), row_index_(0)
  { row_.cells_ = nullptr; row_.count_ = 0; }
  ~ReadParquetScan() override { (void) close(); }
  int open() override { return rescan(arguments_); }
  int rescan(const ObIArray<ObObj> &arguments) override
  {
    int ret = OB_SUCCESS;
    data_ = nullptr; size_ = 0; row_count_ = 0; col_count_ = 0; row_index_ = 0;
    allocator_.reuse(); col_major_.reuse(); cells_.reuse();
    if (arguments.count() < 1 || arguments.at(0).is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "read_parquet requires a non-NULL file path");
    } else if (OB_FAIL(parquet_read_whole_file(arguments.at(0).get_string(), allocator_, data_, size_))) {
    } else {
      ObSEArray<FileTableColumn, 16> columns;
      if (OB_FAIL(parse_parquet(data_, size_, allocator_, columns, col_major_, row_count_, col_count_, true))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < projection_.count(); ++i) { ret = cells_.push_back(ObObj()); }
        row_.count_ = cells_.count();
      }
    }
    return ret;
  }
  int get_next_row(ObNewRow *&row) override
  {
    int ret = OB_SUCCESS;
    if (row_index_ >= row_count_) { ret = OB_ITER_END; }
    else {
      for (int64_t i = 0; OB_SUCC(ret) && i < projection_.count(); ++i) {
        const int64_t position = static_cast<int64_t>(projection_.at(i)) - OB_APP_MIN_COLUMN_ID;
        const int64_t idx = position * row_count_ + row_index_;
        if (position < 0 || position >= col_count_ || idx < 0 || idx >= col_major_.count()) {
          ret = OB_ERR_UNEXPECTED;
        } else { cells_.at(i) = col_major_.at(idx); }
      }
      if (OB_SUCC(ret)) {
        row_.cells_ = cells_.get_data(); row_.count_ = cells_.count(); row = &row_; ++row_index_;
      }
    }
    return ret;
  }
  int close() override
  {
    data_ = nullptr; size_ = 0; row_count_ = 0; col_count_ = 0; row_index_ = 0;
    col_major_.reuse(); allocator_.reset();
    return OB_SUCCESS;
  }
private:
  const ObIArray<ObObj> &arguments_;
  const ObIArray<uint64_t> &projection_;
  ObArenaAllocator allocator_;
  char *data_; int64_t size_;
  int64_t row_count_; int64_t col_count_; int64_t row_index_;
  ObSEArray<ObObj, 64> col_major_;
  ObSEArray<ObObj, 16> cells_;
  ObNewRow row_;
};
} // namespace

int infer_parquet_file_schema(const ObString &path, ObIAllocator &allocator,
                              ObIArray<FileTableColumn> &columns)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator file_alloc("ParquetSchema");
  char *data = nullptr;
  int64_t size = 0;
  if (OB_FAIL(parquet_read_whole_file(path, file_alloc, data, size))) {
  } else {
    ObSEArray<ObObj, 64> col_major;
    int64_t row_count = 0, col_count = 0;
    ret = parse_parquet(data, size, allocator, columns, col_major, row_count, col_count, false);
  }
  return ret;
}

int create_read_parquet_scan(const TableFunctionScanContext &context, ITableFunctionScan *&scan)
{
  void *memory = context.allocator_.alloc(sizeof(ReadParquetScan));
  if (OB_ISNULL(memory)) { return OB_ALLOCATE_MEMORY_FAILED; }
  scan = new (memory) ReadParquetScan(context);
  return OB_SUCCESS;
}
} // namespace sql
} // namespace oceanbase
