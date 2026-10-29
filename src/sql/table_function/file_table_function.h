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

#include "sql/table_function/table_function.h"

namespace oceanbase
{
namespace sql
{

constexpr uint64_t READ_CSV_FUNCTION_ID = 5;
constexpr uint64_t GLOB_FUNCTION_ID = 6;
constexpr uint64_t READ_JSON_FUNCTION_ID = 7;

// Column inferred from a file.  The resolver registers one ColumnItem per entry,
// the scan converts every field with the same type class.
struct FileTableColumn
{
  common::ObString name_;
  common::ObObjType type_;
  bool nullable_;
};

// Reads the whole file at bind time and infers the column list from the header
// plus every data row.  sample_size is intentionally unsupported: inference is
// always a full scan, as the competition contract requires.
int infer_csv_file_schema(const common::ObString &path,
                          common::ObIAllocator &allocator,
                          common::ObIArray<FileTableColumn> &columns);

int create_read_csv_scan(const TableFunctionScanContext &context, ITableFunctionScan *&scan);

// Expands a local glob pattern into one row per matching path.
int create_glob_scan(const TableFunctionScanContext &context, ITableFunctionScan *&scan);

// JSONL (newline-delimited JSON): infers columns from the union of all keys.
int infer_json_file_schema(const common::ObString &path,
                           common::ObIAllocator &allocator,
                           common::ObIArray<FileTableColumn> &columns);

int create_read_json_scan(const TableFunctionScanContext &context, ITableFunctionScan *&scan);

} // namespace sql
} // namespace oceanbase
