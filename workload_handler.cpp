#include "workload_handler.hpp"

#include "abstract_benchmark_item_runner.hpp"
#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_constants.hpp"


namespace {

using namespace opossum;  // NOLINT

class TPCHBenchmarkItemExporter : public TPCHBenchmarkItemRunner {
 public:
  TPCHBenchmarkItemExporter(const std::shared_ptr<BenchmarkConfig>& config)
      : TPCHBenchmarkItemRunner(config, false, 1.0f, ClusteringConfiguration::None) {}

  std::vector<std::string> get_sql_statements_for_benchmark_item(const BenchmarkItemID item_id) {
    auto unique_statements = std::unordered_set<std::string>{};
    
    constexpr auto MAX_TRIES = size_t{10'000};
    for (auto counter = size_t{0}; counter < MAX_TRIES; ++counter) {
      unique_statements.insert(_build_query(item_id));
    }

    const auto statements = std::vector<std::string>{unique_statements.begin(), unique_statements.end()};
    return statements;
  }
};

}  // namespace


namespace opossum {

MetaBenchmarkItems::MetaBenchmarkItems()
    : AbstractMetaTable(TableColumnDefinitions{{"benchmark_name", DataType::String, false},
                                               {"item_name", DataType::String, false},
                                               {"sql_statement_string", DataType::String, false}}) {}

const std::string& MetaBenchmarkItems::name() const {
  static const auto name = std::string{"benchmark_items"};
  return name;
}

std::shared_ptr<Table> MetaBenchmarkItems::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  auto tpch_item_exporter = TPCHBenchmarkItemExporter{config};
  for (const auto item : tpch_item_exporter.items()) {
    tpch_item_exporter.execute_item(item);
    const auto item_instances = tpch_item_exporter.get_sql_statements_for_benchmark_item(item);
    for (const auto& item_instance : item_instances) {
      output_table->append({pmr_string{"TPC-H"}, pmr_string{tpch_item_exporter.item_name(item)}, pmr_string{item_instance}});
    }
  }

  return output_table;
}

WorkloadHandlerPlugin::WorkloadHandlerPlugin() {}

std::string WorkloadHandlerPlugin::description() const { return "This is the Hyrise WorkloadHandlerPlugin"; }

void WorkloadHandlerPlugin::start() {
  std::cout << "Started WorkloadHandlerPlugin" << std::endl;

  auto workload_items_table = std::make_shared<MetaBenchmarkItems>();
  Hyrise::get().meta_table_manager.add_table(std::move(workload_items_table));
}

void WorkloadHandlerPlugin::stop() {}

EXPORT_PLUGIN(WorkloadHandlerPlugin)

}  // namespace opossum
