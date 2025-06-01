#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/record/record_manager.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const char *attribute_name, const Value *value)
    : table_(table), attribute_name_(attribute_name), value_(value)
{
  // 参数校验
  if (!table_ || !attribute_name_ || !value_) {
    LOG_ERROR("Invalid parameters for UpdatePhysicalOperator");
    throw std::invalid_argument("Table, attribute name, and value cannot be null");
  }
}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  // 初始化
  trx_ = trx;
  records_.clear();
  new_values_.clear();

  // 无子节点直接返回成功
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  // 打开子算子（通常是表扫描或过滤）
  PhysicalOperator *child = children_[0].get();
  RC rc = child->open(trx_);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while ((rc = child->next()) == RC::SUCCESS) {
    Tuple *tuple = child->current_tuple();
    if (!tuple) {
      LOG_WARN("Failed to get current tuple");
      rc = RC::INTERNAL;
      break;
    }

    RowTuple *row_tuple = static_cast<RowTuple*>(tuple);
    records_.push_back(row_tuple->record());
  }

  child->close(); // 无论成功与否都关闭子算子

  if (rc != RC::RECORD_EOF) { // 非EOF错误需要处理
    LOG_WARN("Error while collecting records: %s", strrc(rc));
    return rc;
  }

  return execute_updates();
}

RC UpdatePhysicalOperator::execute_updates()
{
  if (records_.empty()) {
    return RC::SUCCESS;
  }

  const TableMeta &table_meta = table_->table_meta();
  const int field_num = table_meta.field_num();
  new_values_.resize(field_num);

  // 查找字段索引（提前查找避免循环内重复查找）
  int field_index = table_meta.find_field_index_by_name(attribute_name_);
  if (field_index < 0) {
    LOG_WARN("Field not found: %s", attribute_name_);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // 遍历所有记录执行更新
  for (Record &old_record : records_) {
    // 获取旧记录所有字段值
    RC rc = table_->get_record_values(old_record, new_values_.data());
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to get record values: %s", strrc(rc));
      return rc;
    }

    // 更新目标字段
    new_values_[field_index] = *value_;

    // 创建新记录
    Record new_record;
    rc = table_->make_record(field_num, new_values_.data(), new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to create new record: %s", strrc(rc));
      return rc;
    }

    // 原子性更新（删除+插入）
    rc = perform_atomic_update(old_record, new_record);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::perform_atomic_update(Record &old_record, Record &new_record)
{
  // 删除旧记录
  RC rc = trx_->delete_record(table_, old_record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to delete record: %s", strrc(rc));
    return rc;
  }

  // 插入新记录
  rc = trx_->insert_record(table_, new_record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to insert new record: %s", strrc(rc));
    
    // 尝试回滚（注意：这不是真正的事务回滚，仅恢复当前记录）
    RC rollback_rc = trx_->insert_record(table_, old_record);
    if (rollback_rc != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback delete operation: %s", strrc(rollback_rc));
      // 继续返回原始错误，优先处理主要问题
    }
    
    return rc;
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::close()
{
  // 清理资源
  records_.clear();
  new_values_.clear();
  trx_ = nullptr;
  return RC::SUCCESS;
}