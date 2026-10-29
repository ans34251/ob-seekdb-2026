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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_STMT_H_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{

class ObCopyToStmt : public ObCMDStmt
{
public:
  static const int64_t MAX_COPY_COLUMNS = 256;

  ObCopyToStmt()
    : ObCMDStmt(stmt::T_COPY_TO),
      has_header_(false),
      null_str_(ObString::make_string("NULL"))
  {}
  virtual ~ObCopyToStmt() {}

  void set_select_sql(const common::ObString &sql) { select_sql_ = sql; }
  const common::ObString &get_select_sql() const { return select_sql_; }

  void set_file_path(const common::ObString &path) { file_path_ = path; }
  const common::ObString &get_file_path() const { return file_path_; }

  void set_has_header(bool v) { has_header_ = v; }
  bool has_header() const { return has_header_; }

  void set_null_str(const common::ObString &s) { null_str_ = s; }
  const common::ObString &get_null_str() const { return null_str_; }

  int add_column_name(const common::ObString &name) { return column_names_.push_back(name); }
  const common::ObIArray<common::ObString> &get_column_names() const { return column_names_; }
  int64_t get_column_count() const { return column_names_.count(); }

  TO_STRING_KV(K_(select_sql), K_(file_path), K_(has_header), K_(null_str));

private:
  common::ObString select_sql_;
  common::ObString file_path_;
  bool has_header_;
  common::ObString null_str_;
  common::ObSEArray<common::ObString, 16> column_names_;

  DISALLOW_COPY_AND_ASSIGN(ObCopyToStmt);
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_STMT_H_ */
