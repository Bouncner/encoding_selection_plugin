#include "data_characteristics.hpp"

#include <numeric>  // std::accumulate

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"


namespace opossum {

MetaDataCharacteristics::MetaDataCharacteristics()
    : AbstractMetaTable(TableColumnDefinitions{{"table_name", DataType::String, false},
                                               {"chunk_id", DataType::Int, false},
                                               {"column_id", DataType::Int, false},
                                               {"value_switches", DataType::Long, true},
                                               {"avg_string_length", DataType::Long, true},
                                               {"max_string_length", DataType::Long, true}}) {}

const std::string& MetaDataCharacteristics::name() const {
  static const auto name = std::string{"data_characteristics"};
  return name;
}

std::shared_ptr<Table> MetaDataCharacteristics::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      if (!chunk) continue;  // Skip physically deleted chunks

      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        const auto& segment = chunk->get_segment(column_id);

        const auto data_type = pmr_string{data_type_to_string.left.at(table->column_data_type(column_id))};

        std::vector<size_t> string_lengths(chunk->size());
        auto value_switches = size_t{0};
        resolve_data_and_segment_type(*segment, [&](auto segment_data_type, auto& typed_segment) {
          using ColumnDataType = typename decltype(segment_data_type)::type;

          auto write_index = size_t{0};
          ColumnDataType previous_value = {};

          auto iterable = create_iterable_from_segment<ColumnDataType>(typed_segment);
          iterable.for_each([&](const auto& value) {
            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              string_lengths[write_index] = value.value().length();
            }

            // Execute if first iterat
            if (write_index == 0) {
              previous_value = value.value();
              ++write_index;
              return;
            }

            if (previous_value != value.value()) {
              ++value_switches;
              previous_value = value.value();
            }

            ++write_index;
          });
        });

        AllTypeVariant avg_string_length = NULL_VALUE;
        AllTypeVariant max_string_length = NULL_VALUE;
        if (data_type == "string") {
          avg_string_length = static_cast<int64_t>(std::accumulate(string_lengths.cbegin(), string_lengths.cend(), 0ul)
                                                   / string_lengths.size());
          max_string_length = static_cast<int64_t>(*std::max_element(string_lengths.cbegin(), string_lengths.cend()));
        }

        output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              static_cast<int64_t>(value_switches), avg_string_length, max_string_length});
      }
    }
  }
  return output_table;
}


DataCharacteristicsPlugin::DataCharacteristicsPlugin() {}

std::string DataCharacteristicsPlugin::description() const { return "This is the Hyrise DataCharacteristicsPlugin"; }

void DataCharacteristicsPlugin::start() {
  const auto data_characteristics_table = std::make_shared<MetaDataCharacteristics>();
  Hyrise::get().meta_table_manager.add_table(std::move(data_characteristics_table));
}

void DataCharacteristicsPlugin::stop() {}

EXPORT_PLUGIN(DataCharacteristicsPlugin)

}  // namespace opossum
