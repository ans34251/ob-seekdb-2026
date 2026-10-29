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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_copy_to_resolver.h"
#include "sql/resolver/cmd/ob_copy_to_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable_init.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

int ObCopyToResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.allocator_)
      || T_COPY_TO != parse_tree.type_ || 4 != parse_tree.num_child_
      || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree for COPY TO", K(ret));
  }

  ObCopyToStmt *copy_stmt = NULL;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(copy_stmt = create_stmt<ObCopyToStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      stmt_ = copy_stmt;
    }
  }

  // Check enable_file_sql gate
  bool file_sql_enabled = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(session_info_->get_sys_variable(SYS_VAR_ENABLE_FILE_SQL, file_sql_enabled))) {
      LOG_WARN("failed to get enable_file_sql", K(ret));
    } else if (!file_sql_enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "COPY TO file, set session enable_file_sql = ON first");
    }
  }

  // Extract file path and check secure_file_priv
  if (OB_SUCC(ret)) {
    const ParseNode *path_node = parse_tree.children_[1];
    if (OB_ISNULL(path_node) || OB_ISNULL(path_node->str_value_) || path_node->str_len_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "COPY TO requires a valid file path");
    } else {
      ObString path(path_node->str_len_, path_node->str_value_);
      ObString secure_file_priv;
      if (OB_FAIL(session_info_->get_secure_file_priv(secure_file_priv))) {
        LOG_WARN("failed to get secure_file_priv", K(ret));
      } else if (!secure_file_priv.empty() && 0 != secure_file_priv.case_compare("NULL")) {
        if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, path))) {
          LOG_WARN("secure_file_priv check failed for COPY TO", K(ret), K(path));
        }
      }
      if (OB_SUCC(ret)) {
        // Deep copy path into statement allocator
        char *buf = static_cast<char *>(params_.allocator_->alloc(path.length() + 1));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(buf, path.ptr(), path.length());
          buf[path.length()] = '\0';
          copy_stmt->set_file_path(ObString(path.length(), buf));
        }
      }
    }
  }

  // Extract SELECT SQL text
  if (OB_SUCC(ret)) {
    const ParseNode *select_text_node = parse_tree.children_[0];
    if (OB_ISNULL(select_text_node) || OB_ISNULL(select_text_node->str_value_)
        || select_text_node->str_len_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "COPY TO requires a valid SELECT query");
    } else {
      ObString sel_sql(select_text_node->str_len_, select_text_node->str_value_);
      char *buf = static_cast<char *>(params_.allocator_->alloc(sel_sql.length() + 1));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf, sel_sql.ptr(), sel_sql.length());
        buf[sel_sql.length()] = '\0';
        copy_stmt->set_select_sql(ObString(sel_sql.length(), buf));
      }
    }
  }

  // Resolve options
  if (OB_SUCC(ret)) {
    const ParseNode *option_list = parse_tree.children_[2];
    if (OB_NOT_NULL(option_list) && T_COPY_OPTION_LIST == option_list->type_) {
      if (OB_FAIL(resolve_options_(*option_list, *copy_stmt))) {
        LOG_WARN("failed to resolve COPY options", K(ret));
      }
    }
  }

  // Resolve column names from SELECT parse tree (for header output)
  if (OB_SUCC(ret) && copy_stmt->has_header()) {
    const ParseNode *select_node = parse_tree.children_[3];
    if (OB_NOT_NULL(select_node)) {
      if (OB_FAIL(resolve_column_names_(*select_node, *copy_stmt))) {
        LOG_WARN("failed to resolve column names", K(ret));
      }
    }
  }

  return ret;
}

int ObCopyToResolver::resolve_options_(const ParseNode &option_list, ObCopyToStmt &stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < option_list.num_child_; ++i) {
    const ParseNode *opt_node = option_list.children_[i];
    if (OB_ISNULL(opt_node) || T_COPY_OPTION != opt_node->type_ || 2 != opt_node->num_child_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid copy option node", K(ret), K(i));
    } else {
      const ParseNode *name_node = opt_node->children_[0];
      const ParseNode *value_node = opt_node->children_[1];
      if (OB_ISNULL(name_node) || OB_ISNULL(name_node->str_value_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid option name", K(ret));
      } else {
        ObString opt_name(name_node->str_len_, name_node->str_value_);
        if (0 == opt_name.case_compare("format")) {
          // format option: only 'csv' is supported
          if (OB_NOT_NULL(value_node) && OB_NOT_NULL(value_node->str_value_)) {
            ObString fmt(value_node->str_len_, value_node->str_value_);
            if (0 != fmt.case_compare("csv")) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "COPY TO format other than csv");
            }
          }
        } else if (0 == opt_name.case_compare("header")) {
          stmt.set_has_header(true);
        } else if (0 == opt_name.case_compare("nullstr")) {
          if (OB_NOT_NULL(value_node) && OB_NOT_NULL(value_node->str_value_)
              && value_node->str_len_ > 0) {
            ObString ns(value_node->str_len_, value_node->str_value_);
            char *buf = static_cast<char *>(params_.allocator_->alloc(ns.length() + 1));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(buf, ns.ptr(), ns.length());
              buf[ns.length()] = '\0';
              stmt.set_null_str(ObString(ns.length(), buf));
            }
          }
        } else {
          // Unknown option: ignore silently for forward compatibility
          LOG_INFO("unknown COPY TO option, ignored", K(opt_name));
        }
      }
    }
  }
  return ret;
}

int ObCopyToResolver::resolve_column_names_(const ParseNode &select_node, ObCopyToStmt &stmt)
{
  int ret = OB_SUCCESS;
  // Navigate: T_SELECT -> children_[PARSE_SELECT_SELECT] -> T_PROJECT_LIST
  if (T_SELECT != select_node.type_ || OB_ISNULL(select_node.children_)) {
    // If not a simple SELECT (e.g., UNION), skip column name extraction.
    // The executor will fall back to generic names.
  } else {
    const ParseNode *project_list = select_node.children_[PARSE_SELECT_SELECT];
    if (OB_ISNULL(project_list) || OB_ISNULL(project_list->children_)) {
      // no select list found, skip
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < project_list->num_child_; ++i) {
        const ParseNode *proj = project_list->children_[i];
        if (OB_ISNULL(proj)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          ObString col_name;
          // T_PROJECT_STRING wraps the actual expression or alias
          if (T_PROJECT_STRING == proj->type_ && proj->num_child_ > 0
              && OB_NOT_NULL(proj->children_[0])) {
            const ParseNode *inner = proj->children_[0];
            if (T_ALIAS == inner->type_ && inner->num_child_ >= 2
                && OB_NOT_NULL(inner->children_[1])
                && OB_NOT_NULL(inner->children_[1]->str_value_)) {
              // Aliased: use alias name
              col_name = ObString(inner->children_[1]->str_len_,
                                  inner->children_[1]->str_value_);
            } else if (OB_NOT_NULL(proj->str_value_) && proj->str_len_ > 0) {
              // Use the projection display string
              col_name = ObString(proj->str_len_, proj->str_value_);
            }
          } else if (OB_NOT_NULL(proj->str_value_) && proj->str_len_ > 0) {
            col_name = ObString(proj->str_len_, proj->str_value_);
          }

          if (col_name.empty()) {
            // Fallback: generate "col_N"
            char buf[32];
            int64_t len = snprintf(buf, sizeof(buf), "col_%ld", i);
            col_name = ObString(len, buf);
          }
          // Deep copy
          char *name_buf = static_cast<char *>(params_.allocator_->alloc(col_name.length() + 1));
          if (OB_ISNULL(name_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(name_buf, col_name.ptr(), col_name.length());
            name_buf[col_name.length()] = '\0';
            if (OB_FAIL(stmt.add_column_name(ObString(col_name.length(), name_buf)))) {
              LOG_WARN("failed to add column name", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
