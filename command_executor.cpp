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
  
using namespace opossum;  // NOLINT

enum class CompressionApplicationMode { Sequential, Scheduler, OSThreads };

void apply_compression_configuration(const std::string& json_configuration_path, CompressionApplicationMode mode,
																		 size_t os_thread_count = 0ul) {
  Assert(std::filesystem::is_regular_file(json_configuration_path), "No such file: " + json_configuration_path);
  Assert(mode != CompressionApplicationMode::Sequential, "Sequnential compression  mode is not implemented.");
  
  std::stringstream ss;
  ss << "Starting to apply compression configuration " + json_configuration_path
  	 << " (mode: " << magic_enum::enum_name(mode);
  if (os_thread_count != 0) {
  	ss << ", OS thread count: " << os_thread_count;
  }
  ss << ").";
  Hyrise::get().log_manager.add_message("CommandExecutorPlugin", ss.str(), LogLevel::Info);

  auto segment_count_pipeline = SQLPipelineBuilder{std::string{"SELECT COUNT(*) FROM meta_segments;"}}.create_pipeline();
  const auto [segment_count_pipeline_status, segment_count_table] = segment_count_pipeline.get_result_table();
  const auto segment_count = *segment_count_table->get_value<int64_t>(ColumnID{0}, 0ul);

  std::ifstream config_stream(json_configuration_path);
	nlohmann::json config;
	config_stream >> config;

	Assert(config.contains("configuration"), "Configuration dictionary missing in compression configuration JSON.");

	auto encoded_segment_count = std::atomic_int{0};
	auto& storage_manager = Hyrise::get().storage_manager;
	for (const auto& [table_name, chunk_configs] : config["configuration"].items()) {
	  auto active_jobs = std::atomic_int{0};

		const auto& table = storage_manager.get_table(table_name);
		const auto& chunk_count = table->chunk_count();
		const auto& column_count = table->column_count();
		const auto& data_types = table->column_data_types();

		std::vector<std::function<void()>> chunk_encoding_functors;
		for (const auto& [chunk_id_str, column_configs] : chunk_configs.items()) {
			const auto chunk_id = std::stoul(chunk_id_str);
			auto encoding_specs = std::vector<SegmentEncodingSpec>(column_count);
			for (const auto& [column_id_str, json_segment_encoding_spec] : column_configs.items()) {
				const auto column_id = std::stoul(column_id_str);
				SegmentEncodingSpec segment_encoding_spec;
				segment_encoding_spec.encoding_type =
						encoding_type_to_string.right.at(json_segment_encoding_spec["SegmentEncodingType"]);

				if (json_segment_encoding_spec.contains("VectorEncodingType")) {
					segment_encoding_spec.vector_compression_type =
							vector_compression_type_to_string.right.at(json_segment_encoding_spec["VectorEncodingType"]);
				}
				encoding_specs[column_id] = segment_encoding_spec;
			}

			chunk_encoding_functors.push_back([&, chunk_id, encoding_specs] {
				const auto& chunk = table->get_chunk(ChunkID{chunk_id});
				ChunkEncoder::encode_chunk(chunk, data_types, encoding_specs);
				encoded_segment_count += encoding_specs.size();
        --active_jobs;
			});
		}

		// For have an "even" progress of the reencoding process, we shuffle the configuration. For approach that put more
		// thoughts into the order of the configuration, this should probably be adapted.
		auto rng = std::default_random_engine{};
		std::shuffle(std::begin(chunk_encoding_functors), std::end(chunk_encoding_functors), rng);

    if (mode == CompressionApplicationMode::OSThreads) {
      const auto thread_count = std::min(static_cast<size_t>(chunk_count), os_thread_count);
      auto threads = std::vector<std::thread>{};
      threads.reserve(thread_count);

      for (auto thread_id = 0u; thread_id < thread_count; ++thread_id) {
        threads.emplace_back([&] {
          while (true) {
            const auto my_task_id = next_task++;
            if (my_task_id >= chunk_encoding_functors.size()) return;

            chunk_encoding_functors[my_task_id]();
          }
        });
      }

	    for (auto& thread : threads) thread.join();
    } else if (mode == CompressionApplicationMode::Scheduler) {
      for (const auto& functor : chunk_encoding_functors) {
        while (true) {
        }
      }
    }
	}
	Assert(segment_count == static_cast<int64_t>(encoded_segment_count), "JSON did probably not include encoding specifications for all segments (of "
																								 "" + std::to_string(segment_count) + " segments, only "
																								 "" + std::to_string(encoded_segment_count) + " have been encoded)");
}

}


namespace opossum {

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
  } else if (command == "DROP LQP PLANCACHE") {
  	Hyrise::get().default_lqp_cache->clear();
  } else if (command.starts_with("SET SERVER CORES ")) {
  	const auto last_space_pos = command.find_last_of(' ');
  	const auto core_count = std::stoul(command.substr(last_space_pos + 1));

  	std::stringstream ss;
  	ss << "Set Hyrise scheduler to use " << core_count << " cores.";
  	Hyrise::get().log_manager.add_message("CommandExecutorPlugin", ss.str(), LogLevel::Info);
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
		  output_table->append({pmr_string{"Executed successful."}});
	  	  return output_table;
		}

  	const auto mode = magic_enum::enum_cast<CompressionApplicationMode>(apply_compression_config[4]);
  	if (!mode) {
  	  auto modes_str = std::string{};
  	  for (const auto& mode : magic_enum::enum_names<CompressionApplicationMode>()) {
  	  	modes_str += " ";
  	  	modes_str += mode;
  	  }
  	  Fail("Unknown mode. Mode must be one of:" + modes_str);
  	}

  	if (apply_compression_config.size() == 6) {
  	  Assert(apply_compression_config[4] == "OSThreads", "OS thread count parameter only valid for mode 'OSThreads'.");
  	  const auto thread_count = apply_compression_config[5] == "0" ?
  	  							std::thread::hardware_concurrency() : std::stoul(apply_compression_config[5]);
		  apply_compression_configuration(file_path_str, *mode, thread_count);
		  output_table->append({pmr_string{"Executed successful."}});
	  	 return output_table;
		}
		apply_compression_configuration(file_path_str, *mode);
  } else {
  	output_table->append({pmr_string{"Unknown command."}});
  	return output_table;
  }
  
  output_table->append({pmr_string{"Executed successful."}});
  return output_table;
}

CommandExecutorPlugin::CommandExecutorPlugin() {}

std::string CommandExecutorPlugin::description() const { return "This is the Hyrise CommandExecutorPlugin"; }

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

}  // namespace opossum
