#pragma once

#include "cache/abstract_cache.hpp"
#include "operators/abstract_operator.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class MetaDataCharacteristics : public AbstractMetaTable {
 public:
  MetaDataCharacteristics();
  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

class DataCharacteristicsPlugin : public AbstractPlugin, public Singleton<DataCharacteristicsPlugin> {
 public:
  DataCharacteristicsPlugin();
  std::string description() const final;
  void start() final;
  void stop() final;
};

}  // namespace opossum
