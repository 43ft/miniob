#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse_defs.h"
#include <vector>

/**
 * @brief SQL UPDATE 语句的物理执行算子
 * @ingroup PhysicalOperator
 * 
 * 负责执行表数据的更新操作，通过事务管理器确保操作的原子性。
 * 支持批量更新符合条件的记录，并在更新失败时提供恢复机制。
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, const char *attr_name, const Value *value);
  ~UpdatePhysicalOperator() override = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }
  OpType get_op_type() const override { return OpType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override { return RC::RECORD_EOF; }
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  // 执行单条记录的原子性更新（删除+插入）
  RC perform_atomic_update(Record &old_record, Record &new_record);
  
  // 执行所有记录的更新操作
  RC execute_updates();

  // 查找目标字段索引并验证类型兼容性
  RC find_and_validate_field(int &field_index) const;

private:
  Table* table_ = nullptr;                 // 目标表
  const char* attribute_name_ = nullptr;   // 要更新的字段名
  const Value* value_ = nullptr;           // 更新后的值
  Trx* trx_ = nullptr;                     // 当前事务
  std::vector<Record> records_;            // 待更新的记录集合
  std::vector<Value> new_values_;          // 记录的新值集合
};