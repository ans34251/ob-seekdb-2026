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
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/parser/ob_parser.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase;
using namespace sql;
using namespace common;

namespace
{
const char *obj_type_to_describe_name(ObObjType type)
{
  switch (type) {
    case ObIntType:      return "BIGINT";
    case ObTinyIntType:  return "BOOLEAN";
    case ObDoubleType:   return "DOUBLE";
    case ObDateType:     return "DATE";
    case ObDateTimeType: return "DATETIME";
    case ObVarcharType:  return "VARCHAR";
    default:             return "VARCHAR";
  }
}

bool is_pure_file_table_select(const ObSelectStmt *stmt)
{
  if (OB_ISNULL(stmt)) { return false; }
  const ObIArray<TableItem *> &tables = stmt->get_table_items();
  if (tables.count() != 1) { return false; }
  if (!tables.at(0)->is_function_table()) { return false; }
  if (stmt->get_condition_exprs().count() > 0) { return false; }
  if (stmt->get_group_exprs().count() > 0) { return false; }
  if (stmt->get_having_exprs().count() > 0) { return false; }
  return true;
}
} // anonymous namespace
const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;
int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_  && 5 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt *child_stmt = NULL;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *child_node,
                                   child_stmt))) {
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else if (child_stmt->is_select_stmt()
                 && is_pure_file_table_select(static_cast<ObSelectStmt *>(child_stmt))) {
        // DESCRIBE SELECT * FROM <table_function>: emit schema rows
        ObSelectStmt *sel = static_cast<ObSelectStmt *>(child_stmt);
        ObSqlString sql;
        for (int64_t i = 0; OB_SUCC(ret) && i < sel->get_select_item_size(); ++i) {
          if (i > 0) { OZ (sql.append(" UNION ALL ")); }
          const SelectItem &item = sel->get_select_item(i);
          const char *type_name = obj_type_to_describe_name(item.expr_->get_result_type().get_type());
          const char *null_str = item.expr_->is_not_null_for_read() ? "NO" : "YES";
          OZ (sql.append_fmt(
              "SELECT '%.*s' AS column_name, '%s' AS column_type, "
              "'%s' AS `null`, 'NULL' AS `key`, 'NULL' AS `default`, 'NULL' AS extra",
              static_cast<int>(item.alias_name_.length()), item.alias_name_.ptr(),
              type_name, null_str));
        }
        if (OB_SUCC(ret)) {
          ParseResult schema_result;
          ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
          if (OB_FAIL(parser.parse(sql.string(), schema_result))) {
            LOG_WARN("failed to parse schema sql", K(ret));
          } else if (OB_ISNULL(schema_result.result_tree_)
                     || schema_result.result_tree_->num_child_ < 1
                     || OB_ISNULL(schema_result.result_tree_->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            ObResolver inner_resolver(params_);
            ObStmt *schema_stmt = nullptr;
            if (OB_FAIL(inner_resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                              *schema_result.result_tree_->children_[0],
                                              schema_stmt))) {
              LOG_WARN("failed to resolve schema sql", K(ret));
            } else {
              set_basic_stmt(schema_stmt);
              return ret;
            }
          }
        }
        // If schema path failed, fall through to normal EXPLAIN
        if (OB_FAIL(ret)) { ret = OB_SUCCESS; }
      }
      if (OB_SUCC(ret) && nullptr != explain_stmt && OB_NOT_NULL(child_stmt)
          && stmt_ == static_cast<ObStmt *>(explain_stmt)) {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
      }
    }
    ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
    if (OB_SUCC(ret) && NULL != opt_node) {
      switch (opt_node->type_) {
      case T_PRETTY_COLOR:
        opt.with_color_ = true;
        // pass
      case T_PRETTY:
        opt.with_tree_line_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      explain_stmt->set_display_opt(opt);
    }
    ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
    if (OB_SUCC(ret) && NULL != into_table_node) {
      ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
      explain_stmt->set_into_table(into_table);
    }
    ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
    if (OB_SUCC(ret) && NULL != statement_id_node) {
      ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
      explain_stmt->set_statement_id(statement_id);
    }
  }

  return ret;
}
