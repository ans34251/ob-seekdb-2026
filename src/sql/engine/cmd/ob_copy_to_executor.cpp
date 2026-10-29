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

#include "sql/engine/cmd/ob_copy_to_executor.h"
#include "sql/resolver/cmd/ob_copy_to_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "common/mysqlclient/ob_isql_client.h"
#include "common/object/ob_object.h"
#include "common/timezone/ob_time_convert.h"
#include "lib/utility/ob_print_utils.h"

#include <string>
#include <cstdio>
#include <cstdlib>
#include <cmath>

namespace oceanbase
{
using namespace common;
namespace sql
{

// Format an ObObj into its plain-text representation for CSV output.
static int format_obj_to_text(const ObObj &obj, ObIAllocator &alloc,
                              const ObTimeZoneInfo *tz_info, ObString &out)
{
  int ret = OB_SUCCESS;
  char stack_buf[512];
  int64_t pos = 0;
  bool formatted_into_stack_buf = true;
  switch (obj.get_type()) {
    case ObIntType: {
      // NOTE: databuff_printf returns OB_SUCCESS/OB_SIZE_OVERFLOW and advances pos by reference
      ret = databuff_printf(stack_buf, sizeof(stack_buf), pos, "%ld", obj.get_int());
      break;
    }
    case ObUInt64Type: {
      ret = databuff_printf(stack_buf, sizeof(stack_buf), pos, "%lu", obj.get_uint64());
      break;
    }
    case ObFloatType:
    case ObDoubleType: {
      // shortest representation that round-trips
      double d = obj.get_double();
      if (std::isnan(d) || std::isinf(d)) {
        ret = databuff_printf(stack_buf, sizeof(stack_buf), pos, "%s",
                              std::isnan(d) ? "nan" : (d > 0 ? "inf" : "-inf"));
      } else {
        char tmp[64];
        for (int prec = 15; prec <= 17; ++prec) {
          snprintf(tmp, sizeof(tmp), "%.*g", prec, d);
          if (strtod(tmp, NULL) == d) {
            break;
          }
        }
        ret = databuff_printf(stack_buf, sizeof(stack_buf), pos, "%s", tmp);
      }
      break;
    }
    case ObNumberType: {
      number::ObNumber num = obj.get_number();
      pos = num.to_string(stack_buf, sizeof(stack_buf));
      break;
    }
    case ObVarcharType:
    case ObCharType:
    case ObTinyTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObTextType: {
      out = obj.get_string();
      formatted_into_stack_buf = false;
      break;
    }
    case ObDateTimeType:
    case ObTimestampType: {
      ret = ObTimeConverter::datetime_to_str(obj.get_datetime(), tz_info, obj.get_scale(),
                                             stack_buf, sizeof(stack_buf), pos);
      break;
    }
    case ObDateType: {
      ret = ObTimeConverter::date_to_str(obj.get_date(), stack_buf, sizeof(stack_buf), pos);
      break;
    }
    case ObTimeType: {
      ret = ObTimeConverter::time_to_str(obj.get_time(), obj.get_scale(),
                                         stack_buf, sizeof(stack_buf), pos);
      break;
    }
    case ObYearType: {
      ret = ObTimeConverter::year_to_str(static_cast<uint8_t>(obj.get_year()),
                                         stack_buf, sizeof(stack_buf), pos);
      break;
    }
    default: {
      // generic fallback via ObObj::to_string
      pos = obj.to_string(stack_buf, sizeof(stack_buf));
      break;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to format object for CSV output", K(ret), K(obj.get_type()));
  } else {
    if (formatted_into_stack_buf) {
      out = ObString(static_cast<int32_t>(pos), stack_buf);
    }
    // deep copy into allocator so the caller never holds a dangling stack buffer
    char *buf = static_cast<char *>(alloc.alloc(out.length() + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buffer for CSV field", K(ret), "len", out.length());
    } else {
      if (out.length() > 0) {
        MEMCPY(buf, out.ptr(), out.length());
      }
      buf[out.length()] = '\0';
      out = ObString(out.length(), buf);
    }
  }
  return ret;
}

// RFC4180-style escaping: quote the field if it contains comma, quote, CR or LF;
// embedded double quotes are doubled.
static void append_csv_field(std::string &buf, const ObString &field)
{
  bool need_quote = false;
  for (int64_t i = 0; i < field.length(); ++i) {
    const char c = field.ptr()[i];
    if (',' == c || '"' == c || '\n' == c || '\r' == c) {
      need_quote = true;
      break;
    }
  }
  if (!need_quote) {
    buf.append(field.ptr(), field.length());
  } else {
    buf.push_back('"');
    for (int64_t i = 0; i < field.length(); ++i) {
      const char c = field.ptr()[i];
      if ('"' == c) {
        buf.push_back('"');
      }
      buf.push_back(c);
    }
    buf.push_back('"');
  }
}

int ObCopyToExecutor::execute(ObExecContext &ctx, ObCopyToStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();
  sqlclient::ObISQLConnection *conn = NULL;
  sqlclient::ObISQLConnectionGuard conn_guard;

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (stmt.get_select_sql().empty() || stmt.get_file_path().empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid COPY TO statement", K(ret), K(stmt));
  } else if (OB_FAIL(query::ObInnerSQLConnectionAccess::
                         create_spi_connection_with_external_session(session, conn_guard))) {
    LOG_WARN("failed to create inner sql connection", K(ret));
  } else if (OB_ISNULL(conn = conn_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner sql connection is null", K(ret));
  }

  std::string file_buf;
  int64_t row_count = 0;
  if (OB_SUCC(ret)) {
    common::ObISQLClient::ReadResult res;
    if (OB_FAIL(conn->execute_read(stmt.get_select_sql(), res, true /*is_user_sql*/))) {
      LOG_WARN("failed to execute COPY TO query", K(ret), "select_sql", stmt.get_select_sql());
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        const int64_t col_count = result->get_column_count();
        const ObIArray<ObString> &col_names = stmt.get_column_names();
        // header row
        if (stmt.has_header()) {
          for (int64_t c = 0; OB_SUCC(ret) && c < col_count; ++c) {
            if (c > 0) {
              file_buf.push_back(',');
            }
            if (c < col_names.count()) {
              append_csv_field(file_buf, col_names.at(c));
            } else {
              char name_buf[32];
              int64_t len = snprintf(name_buf, sizeof(name_buf), "col_%ld", c);
              append_csv_field(file_buf, ObString(static_cast<int32_t>(len), name_buf));
            }
          }
          if (OB_SUCC(ret)) {
            file_buf.push_back('\n');
          }
        }
        // data rows
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to iterate result set", K(ret));
            }
          } else {
            for (int64_t c = 0; OB_SUCC(ret) && c < col_count; ++c) {
              if (c > 0) {
                file_buf.push_back(',');
              }
              ObObj obj;
              if (OB_FAIL(result->get_obj(c, obj))) {
                LOG_WARN("failed to get object", K(ret), K(c));
              } else if (obj.is_null() || obj.is_nop_value()) {
                append_csv_field(file_buf, stmt.get_null_str());
              } else {
                ObString text;
                if (OB_FAIL(format_obj_to_text(obj, ctx.get_allocator(),
                                               session->get_timezone_info(), text))) {
                  LOG_WARN("failed to format object", K(ret), K(c), K(obj));
                } else {
                  // field content may live on the stack buffer of format_obj_to_text,
                  // so append it before the next call
                  append_csv_field(file_buf, text);
                }
              }
            }
            if (OB_SUCC(ret)) {
              file_buf.push_back('\n');
              ++row_count;
            }
          }
        }
      }
    }
  }

  // write the output file
  if (OB_SUCC(ret)) {
    const ObString &path = stmt.get_file_path();
    std::string path_str(path.ptr(), path.length());
    FILE *fp = fopen(path_str.c_str(), "wb");
    if (OB_ISNULL(fp)) {
      ret = OB_IO_ERROR;
      LOG_WARN("failed to open output file for COPY TO", K(ret), K(path));
      char err_msg[OB_MAX_ERROR_MSG_LEN] = "";
      snprintf(err_msg, sizeof(err_msg), "open file \"%s\" for writing, COPY TO file is",
               path_str.c_str());
      LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
    } else {
      size_t written = 0;
      if (!file_buf.empty()) {
        written = fwrite(file_buf.data(), 1, file_buf.size(), fp);
      }
      if (written != file_buf.size() || 0 != fflush(fp)) {
        ret = OB_IO_ERROR;
        LOG_WARN("failed to write output file for COPY TO", K(ret), K(path));
      }
      if (0 != fclose(fp)) {
        fp = NULL;
        if (OB_SUCC(ret)) {
          ret = OB_IO_ERROR;
          LOG_WARN("failed to close output file for COPY TO", K(ret), K(path));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("COPY TO finished", "file_path", stmt.get_file_path(), K(row_count));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
