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

#pragma once

#include "common/sys/rc.h"
#include "sql/stmt/stmt.h"
#include "sql/parser/parse_defs.h"
#include <vector>

class Table;
class Db;

/**
 * @brief SQL更新语句的封装类
 * @ingroup Statement
 * 
 * 用于表示SQL UPDATE语句的语义结构，包含目标表、更新字段、更新值及过滤条件等信息。
 */
class UpdateStmt : public Stmt
{
public:
    UpdateStmt() = default;
    UpdateStmt(Table* table, const char* attr_name, const Value* val,
               const std::vector<ConditionSqlNode>& update_conds);

    StmtType type() const override { return StmtType::UPDATE; }

public:
    static RC create(Db* db, const UpdateSqlNode& sql_node, Stmt*& stmt);

public:
    Table* target_table() const { return table_; }
    const char* update_field() const { return attribute_name_; }
    const Value* new_value() const { return value_; }
    const std::vector<ConditionSqlNode>& update_conditions() const { return conditions_; }

private:
    Table* table_ = nullptr;                /**< 要更新的目标表 */
    const char* attribute_name_ = nullptr;  /**< 待更新的字段名称 */
    const Value* value_ = nullptr;          /**< 字段更新后的值 */
    std::vector<ConditionSqlNode> conditions_; /**< 筛选记录的条件表达式 */
};