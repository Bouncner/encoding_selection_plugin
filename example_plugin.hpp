#pragma once

#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class ExamplePlugin : public AbstractPlugin, public Singleton<ExamplePlugin> {
 public:
  ExamplePlugin() : sm(StorageManager::get()) {}

  const std::string description() const final;

  void start() const final;

  void stop() const final;

  StorageManager& sm;
};

}  // namespace opossum
