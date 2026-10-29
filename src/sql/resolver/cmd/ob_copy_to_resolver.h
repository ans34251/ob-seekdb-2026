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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_RESOLVER_H_

#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObCopyToStmt;

class ObCopyToResolver : public ObCMDResolver
{
public:
  explicit ObCopyToResolver(ObResolverParams &params) : ObCMDResolver(params) {}
  virtual ~ObCopyToResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  int resolve_options_(const ParseNode &option_list, ObCopyToStmt &stmt);
  int resolve_column_names_(const ParseNode &select_node, ObCopyToStmt &stmt);

  DISALLOW_COPY_AND_ASSIGN(ObCopyToResolver);
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_RESOLVER_H_ */
