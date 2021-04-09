#pragma once

#include "utils/abstract_plugin.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"
#include "utils/settings/abstract_setting.hpp"
#include "utils/singleton.hpp"


namespace opossum {

class MetaCommandExecutor : public AbstractMetaTable {
 public:
  MetaCommandExecutor();
  const std::string& name() const final;

  void on_tables_loaded() {}

 protected:
  std::shared_ptr<Table> _on_generate() const final;
};

class CommandExecutorPlugin : public AbstractPlugin, public Singleton<CommandExecutorPlugin> {
 public:
  CommandExecutorPlugin();
  std::string description() const final;
  void start() final;
  void stop() final;

 private:
  class CommandExecutorSetting : public AbstractSetting {
   public:
    CommandExecutorSetting() : AbstractSetting("Plugin::Executor::Command") {}
    const std::string& description() const final {
      static const auto description = std::string{"Command. Syntax does not matter as long as it is handled in the plugin."};
      return description;
    }
    const std::string& get() final { return _value; }
    void set(const std::string& value) final { _value = value; }

    std::string _value = "No op";
  };

  std::shared_ptr<CommandExecutorSetting> _command_setting;
};

}  // namespace opossum
