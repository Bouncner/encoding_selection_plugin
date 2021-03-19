#include "workload_handler.hpp"

#include "abstract_benchmark_item_runner.hpp"
#include "benchmark_config.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_constants.hpp"


namespace {

using namespace opossum;  // NOLINT

class TPCHBenchmarkItemExporter : public TPCHBenchmarkItemRunner {
 public:
  TPCHBenchmarkItemExporter()
      : TPCHBenchmarkItemRunner(std::shared_ptr<BenchmarkConfig>{}, false, 1.0f, ClusteringConfiguration::None) {}

 protected:
  bool _on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor&) override {
    std::cout << item_id << std::endl;

    return true;
  }
};

}  // namespace


namespace opossum {

MetaWorkloadItems::MetaWorkloadItems()
    : AbstractMetaTable(TableColumnDefinitions{{"benchmark_name", DataType::String, false},
                                               {"item_name", DataType::String, false},
                                               {"subitem_id", DataType::Int, false},
                                               {"sql_statement_string", DataType::String, false}}) {}

const std::string& MetaWorkloadItems::name() const {
  static const auto name = std::string{"workload_items"};
  return name;
}

std::shared_ptr<Table> MetaWorkloadItems::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  auto tpch_item_exporter = TPCHBenchmarkItemExporter{};
  for (const auto item : tpch_item_exporter.items()) {
    tpch_item_exporter.execute_item(item);
    output_table->append({pmr_string{"TPC-H"}, pmr_string{tpch_item_exporter.item_name(item)}, 0, pmr_string{"not implemented"}});
  }

  return output_table;
}


std::string WorkloadHandlerPlugin::description() const { return "This is the Hyrise WorkloadHandlerPlugin"; }

void WorkloadHandlerPlugin::start() {
  std::cout << "Started WorkloadHandlerPlugin" << std::endl;

  // auto projections_table = std::make_shared<MetaPlanCacheProjections>();

  // const auto pqp_cache_snapshot = Hyrise::get().default_pqp_cache->snapshot();
  // aggregate_table->set_plan_cache_snapshot(pqp_cache_snapshot);
  // table_scan_table->set_plan_cache_snapshot(pqp_cache_snapshot);
  // joins_table->set_plan_cache_snapshot(pqp_cache_snapshot);
  // projections_table->set_plan_cache_snapshot(pqp_cache_snapshot);

  // Hyrise::get().meta_table_manager.add_table(std::move(aggregate_table));
}

void WorkloadHandlerPlugin::stop() {}

EXPORT_PLUGIN(WorkloadHandlerPlugin)

}  // namespace opossum
