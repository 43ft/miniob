/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "common/type/attr_type.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(Table* table, const char* attribute_name, const Value* value,
                       const std::vector<ConditionSqlNode>& conditions)
    : table_(table), attribute_name_(attribute_name), value_(value), conditions_(conditions) {}

RC UpdateStmt::create(Db* db, const UpdateSqlNode& update_sql, Stmt*& stmt) {
    const char* table_name = update_sql.relation_name.c_str();
    if (!db || !table_name) {
        LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
        return RC::INVALID_ARGUMENT;
    }

    Table* table = db->find_table(table_name);
    if (!table) {
        LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
        return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    const TableMeta& table_meta = table->table_meta();
    const FieldMeta* field_meta = table_meta.field(update_sql.attribute_name.c_str());
    if (!field_meta) {
        LOG_WARN("no such field. field=%s", update_sql.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
    }

    if (field_meta->type() != update_sql.value.attr_type()) {
        LOG_WARN("field type mismatch. table field type=%d, value_type=%d",
                 field_meta->type(), update_sql.value.attr_type());
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }

    if (field_meta->type() == AttrType::CHARS && update_sql.value.data()) {
        size_t field_len = field_meta->len();
        size_t value_len = strlen(static_cast<const char*>(update_sql.value.data()));
        if (field_len < value_len) {
            LOG_WARN("value length too long. field len=%zu, value len=%zu", field_len, value_len);
            return RC::INVALID_ARGUMENT;
        }
    }

    stmt = new UpdateStmt(table, update_sql.attribute_name.c_str(), &update_sql.value, update_sql.conditions);
    return RC::SUCCESS;
}