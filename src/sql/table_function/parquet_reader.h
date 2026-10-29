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

#pragma once

#include "sql/table_function/file_table_function.h"

namespace oceanbase
{
namespace sql
{

// Parquet reader (self-contained): a minimal Thrift compact-protocol decoder for
// the file footer, a raw-block Snappy decompressor, an RLE/bit-pack hybrid level
// decoder and a PLAIN value decoder.  Only the subset required by the competition
// fixture is supported: flat schemas, INT32/INT64/FLOAT/DOUBLE leaves, PLAIN
// encoding and UNCOMPRESSED/SNAPPY codecs, DataPage v1 and v2.
//
// Column names and nullability come from the footer schema; INT32/INT64 map to
// BIGINT and FLOAT/DOUBLE map to DOUBLE.
int infer_parquet_file_schema(const common::ObString &path,
                              common::ObIAllocator &allocator,
                              common::ObIArray<FileTableColumn> &columns);

int create_read_parquet_scan(const TableFunctionScanContext &context, ITableFunctionScan *&scan);

} // namespace sql
} // namespace oceanbase
