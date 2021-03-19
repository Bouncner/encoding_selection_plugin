#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"
#include "cache/abstract_cache.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class MetaPlanCacheOperators {
 public:
  using PlanCacheSnapshotEnty = typename AbstractCache<std::string, std::shared_ptr<AbstractOperator>>::SnapshotEntry;
  using PlanCache = typename std::unordered_map<std::string, PlanCacheSnapshotEnty>;

  inline static const std::set<OperatorType> detailed_operator_types = {OperatorType::TableScan, OperatorType::Aggregate,
  														  OperatorType::Projection, OperatorType::JoinHash,
  														  OperatorType::JoinNestedLoop, OperatorType::JoinSortMerge,
  														  OperatorType::GetTable};

  inline static const std::set<OperatorType> unsupported_operator_types = {OperatorType::CreateView, OperatorType::DropView,
  															 OperatorType::TableWrapper};

  void set_plan_cache_snapshot(const PlanCache);
  void unset_plan_cache_snapshot();

 protected:
  std::optional<std::unordered_map<std::string, PlanCacheSnapshotEnty>> _plan_cache_snapshot;
};

class MetaPlanCacheAggregates : public AbstractMetaTable, public MetaPlanCacheOperators {
 public:
  MetaPlanCacheAggregates(const TableColumnDefinitions table_column_definitions);
  MetaPlanCacheAggregates();
  const std::string& name() const final;

  static TableColumnDefinitions get_table_column_definitions();

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

class MetaPlanCacheTableScans : public AbstractMetaTable, public MetaPlanCacheOperators {
 public:
  MetaPlanCacheTableScans();
  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

class MetaPlanCacheJoins : public AbstractMetaTable, public MetaPlanCacheOperators {
 public:
  MetaPlanCacheJoins(const TableColumnDefinitions table_column_definitions);
  MetaPlanCacheJoins();
  const std::string& name() const final;

  static TableColumnDefinitions get_table_column_definitions();

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

class MetaPlanCacheProjections : public AbstractMetaTable, public MetaPlanCacheOperators {
 public:
  MetaPlanCacheProjections(const TableColumnDefinitions table_column_definitions);
  MetaPlanCacheProjections();
  const std::string& name() const final;

  static TableColumnDefinitions get_table_column_definitions();

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

}  // namespace opossum
