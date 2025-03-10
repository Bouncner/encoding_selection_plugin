#include "command_executor.hpp"

#include <algorithm>
#include <fstream>
#include <random>

#include <boost/algorithm/string.hpp>
#include <nlohmann/json.hpp>

#include "abstract_benchmark_item_runner.hpp"
#include "benchmark_config.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/encoding_type.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_constants.hpp"


namespace {
  
using namespace hyrise;  // NOLINT

enum class CompressionApplicationMode { Sequential, Scheduler, OSThreads };

void apply_compression_configuration(const std::string& json_configuration_path, CompressionApplicationMode mode,
                                     size_t task_count = 0) {
  Assert(std::filesystem::is_regular_file(json_configuration_path), "No such file: " + json_configuration_path);
  Assert(mode != CompressionApplicationMode::Sequential, "Sequential compression mode is not implemented.");
  
  auto sstream = std::stringstream{};
  sstream << "Starting to apply compression configuration '" + json_configuration_path
          << "' (mode: " << magic_enum::enum_name(mode);
  if (task_count != 0) {
    sstream << ", thread count: " << task_count;
  }
  sstream << ").";
  Hyrise::get().log_manager.add_message("CommandExecutorPlugin", sstream.str(), LogLevel::Info);

  auto segment_count_pipeline = SQLPipelineBuilder{std::string{"SELECT COUNT(*) FROM meta_segments;"}}.create_pipeline();
  const auto [segment_count_pipeline_status, segment_count_table] = segment_count_pipeline.get_result_table();
  const auto segment_count = *segment_count_table->get_value<int64_t>(ColumnID{0}, 0ul);

  auto config_stream = std::ifstream{json_configuration_path};
  auto config = nlohmann::json{};
  config_stream >> config;

  Assert(config.contains("configuration"), "Configuration dictionary missing in compression configuration JSON.");

  auto encoded_segment_count = std::atomic_int{0};
  auto& storage_manager = Hyrise::get().storage_manager;
  for (const auto& [table_name, chunk_configs] : config["configuration"].items()) {
    const auto& table = storage_manager.get_table(table_name);
    const auto& chunk_count = table->chunk_count();
    const auto& column_count = table->column_count();
    const auto& data_types = table->column_data_types();

    auto chunk_encoding_functors = std::vector<std::function<void()>>{};
    for (const auto& [chunk_id_str, column_configs] : chunk_configs.items()) {
      const auto chunk_id = std::stoul(chunk_id_str);
      auto encoding_specs = std::vector<SegmentEncodingSpec>(column_count);
      for (const auto& [column_id_str, json_segment_encoding_spec] : column_configs.items()) {
        const auto column_id = std::stoul(column_id_str);
        auto segment_encoding_spec = SegmentEncodingSpec{};
        segment_encoding_spec.encoding_type =
            encoding_type_to_string.right.at(json_segment_encoding_spec["SegmentEncodingType"]);

        if (json_segment_encoding_spec.contains("VectorEncodingType")) {
          segment_encoding_spec.vector_compression_type =
              vector_compression_type_to_string.right.at(json_segment_encoding_spec["VectorEncodingType"]);
        }
        encoding_specs[column_id] = segment_encoding_spec;
      }

      chunk_encoding_functors.push_back([&, table_name=table_name, chunk_id, encoding_specs] {
        if (chunk_id >= chunk_count) {
          std::cerr << "WARNING: configuration attempts applying encoding to an invalide chunkID.\n";
          return;
        }
        const auto& chunk = table->get_chunk(ChunkID{static_cast<uint32_t>(chunk_id)});
        ChunkEncoder::encode_chunk(chunk, data_types, encoding_specs);
        encoded_segment_count += encoding_specs.size();
      });
    }

    // For have an "even" progress of the reencoding process, we shuffle the configuration. For approach that put more
    // thoughts into the order of the configuration, this should probably be adapted.
    auto rng = std::default_random_engine{};
    std::shuffle(std::begin(chunk_encoding_functors), std::end(chunk_encoding_functors), rng);

    auto next_task = std::atomic_uint{0};
    if (mode == CompressionApplicationMode::OSThreads) {
      auto thread_count = task_count == 0 ? std::thread::hardware_concurrency() : task_count;
      thread_count = std::min(static_cast<size_t>(chunk_count), thread_count);
      auto threads = std::vector<std::thread>{};
      threads.reserve(thread_count);

      for (auto thread_id = size_t{0}; thread_id < thread_count; ++thread_id) {
        threads.emplace_back([&] {
          while (true) {
            const auto my_task_id = next_task++;
            if (my_task_id >= chunk_encoding_functors.size()) return;

            chunk_encoding_functors[my_task_id]();
          }
        });
      }

      for (auto& thread : threads) {
        thread.join();
      }
    } else if (mode == CompressionApplicationMode::Scheduler) {
      // TODO: merge thread and scheduler code paths
      auto job_count = task_count == 0 ? chunk_encoding_functors.size() : task_count;
      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(job_count);

      for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
        jobs.emplace_back(std::make_shared<JobTask>([&] {
          while (true) {
            const auto my_task_id = next_task++;
            if (my_task_id >= chunk_encoding_functors.size()) return;

            chunk_encoding_functors[my_task_id]();
          }
        }));
      } 
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
    } else {
      Fail("Unsupported mode");
    }
  }
  Assert(segment_count == static_cast<int64_t>(encoded_segment_count), "JSON did probably not include encoding specifications for all segments (of "
                                                                       "" + std::to_string(segment_count) + " segments, only "
                                                                       "" + std::to_string(encoded_segment_count) + " have been encoded)");
}

}


namespace hyrise {

MetaCommandExecutor::MetaCommandExecutor()
    : AbstractMetaTable(TableColumnDefinitions{{"command", DataType::String, false}}) {}

const std::string& MetaCommandExecutor::name() const {
  static const auto name = std::string{"command_executor"};
  return name;
}

std::shared_ptr<Table> MetaCommandExecutor::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto command = Hyrise::get().settings_manager.get_setting("Plugin::Executor::Command")->get();
  if (command == "DROP PQP PLANCACHE") {
    Hyrise::get().default_pqp_cache->clear();
    Hyrise::get().log_manager.add_message("CommandExecutorPlugin", "Dropped PQP cache", LogLevel::Info);
  } else if (command == "DROP LQP PLANCACHE") {
    Hyrise::get().default_lqp_cache->clear();
    Hyrise::get().log_manager.add_message("CommandExecutorPlugin", "Dropped LQP cache", LogLevel::Info);
  } else if (command.starts_with("SET SERVER CORES ")) {
    const auto last_space_pos = command.find_last_of(' ');
    const auto core_count = std::stoul(command.substr(last_space_pos + 1));

    auto sstream = std::stringstream{};
    sstream << "Set Hyrise scheduler to use " << core_count << " cores.";
    Hyrise::get().log_manager.add_message("CommandExecutorPlugin", sstream.str(), LogLevel::Info);
    Hyrise::get().scheduler()->finish();
    Hyrise::get().topology.use_default_topology(core_count);
    // reset scheduler
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  } else if (command.starts_with("APPLY COMPRESSION CONFIGURATION ")) {
    auto apply_compression_config = std::vector<std::string>{};
    // Split benchmark name and sizing factor
    boost::split(apply_compression_config, command, boost::is_any_of(" "), boost::token_compress_on);
    Assert(apply_compression_config.size() > 3 && apply_compression_config.size() < 7,
           "Expecting at least one and max. 3 parameters. Usage: APPLY COMPRESSION CONFIGURATION file [mode] [os_thread_count]");

    const auto file_path_str = apply_compression_config[3];
    if (apply_compression_config.size() == 4) {
      apply_compression_configuration(file_path_str, CompressionApplicationMode::OSThreads, std::thread::hardware_concurrency());
      output_table->append({pmr_string{"Command executed successfully."}});
      return output_table;
    }

    const auto mode = magic_enum::enum_cast<CompressionApplicationMode>(apply_compression_config[4]);
    if (!mode) {
      auto modes_str = std::string{};
      for (const auto& mode : magic_enum::enum_names<CompressionApplicationMode>()) {
        modes_str += " ";
        modes_str += mode;
      }
      Fail("Unknown mode. Mode must be one of: " + modes_str);
    }

    if (apply_compression_config.size() == 6) {
      apply_compression_configuration(file_path_str, *mode, std::stoul(apply_compression_config[5]));
      output_table->append({pmr_string{"Command executed successfully."}}); 
      return output_table;
    }
    apply_compression_configuration(file_path_str, *mode);
  } else {
    output_table->append({pmr_string{"Unknown command."}});
    return output_table;
  }
 
  output_table->append({pmr_string{"Command executed successfully."}});
  return output_table;
}

CommandExecutorPlugin::CommandExecutorPlugin() {}

std::string CommandExecutorPlugin::description() const {
  return "This is the Hyrise CommandExecutorPlugin";
}

void CommandExecutorPlugin::start() {
  auto workload_command_executor_table = std::make_shared<MetaCommandExecutor>();
  Hyrise::get().meta_table_manager.add_table(std::move(workload_command_executor_table));

  _command_setting = std::make_shared<CommandExecutorSetting>();
  _command_setting->register_at_settings_manager();
}

void CommandExecutorPlugin::stop() {
  _command_setting->unregister_at_settings_manager();
}

EXPORT_PLUGIN(CommandExecutorPlugin)

}  // namespace hyrise
