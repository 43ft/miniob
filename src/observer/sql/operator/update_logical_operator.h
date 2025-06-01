#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"
#include <string>

/**
 * @brief SQL UPDATE 语句的逻辑算子
 * @ingroup LogicalOperator
 * 
 * 负责将SQL UPDATE语句转换为可执行的逻辑操作树，
 * 包含目标表、要更新的字段名及新值等信息。
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, const char *attribute_name, const Value *value);
  ~UpdateLogicalOperator() override = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  OpType get_op_type() const override { return OpType::LOGICALUPDATE; }

  Table *table() const { return table_; }
  const char *attribute_name() const { return attribute_name_; }
  const Value *value() const { return value_; }

  RC validate() const;

private:
  Table *table_ = nullptr;               
  const char *attribute_name_ = nullptr; 
  const Value *value_ = nullptr;         
};