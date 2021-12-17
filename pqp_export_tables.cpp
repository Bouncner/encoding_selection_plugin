#include "pqp_export_tables.hpp"

#include "boost/functional/hash.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/pqp_utils.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"


namespace {

using namespace opossum;  // NOLINT

const auto shuffledness_column = TableColumnDefinition{"input_shuffledness", DataType::Double, false};

pmr_string operator_hash(const std::shared_ptr<const AbstractOperator>& op) {
  auto seed = size_t{0};

  boost::hash_combine(seed, op);
  boost::hash_combine(seed, op->description());
  const auto& perf_data = op->performance_data;
  boost::hash_combine(seed, perf_data->output_row_count);
  boost::hash_combine(seed, static_cast<size_t>(perf_data->walltime.count()));

  std::stringstream pseudo_hash;
  pseudo_hash << std::hex << seed;

  return pmr_string{pseudo_hash.str()};
}

/**
 *  Estimate how "shuffled" the pos list is. Per definition, a physical table has a shuffledness of 0.0.
 *  A join in contrast might return a pos list that is completely shuffled (i.e. 1.0).
 *  This is a rough estimation. We do not have the means to calculate the shuffledness afterwards accurately.
 *
 *  We do not care about the actual OP that is passed in, but about its inputs. For that we have an entry method.
 */
std::pair<double, std::optional<double>> estimate_pos_list_shuffledness(const std::shared_ptr<const AbstractOperator>& root_op,
                                      const std::shared_ptr<const AbstractOperator>& op, const ColumnID column_id,
                                      std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes) {
  /**
   *    Check for early exits. Early exists are operators for which we do not have to further traverse the children to
   *    know the shuffledness (usually operators that materialize a table and thu there is no pos list):
   *      - aggregates (always fully materialize)
   *      - projections (partially materialize, we simply assume they fully materialize here)
   *      - joins (no materialization, the opposite; usually a scambled pos list)
   */
  if (root_op != op) { // we have to be sure that we are not at the input node but have already traversed.
    if (op->type() == OperatorType::GetTable || op->type() == OperatorType::UnionAll) {
      return {0.0, std::nullopt};
    } else if (op->type() == OperatorType::UnionPositions || op->type() == OperatorType::JoinSortMerge) {
      // Joins and UnionPositions all pretty much shuffle the whole input. 
      return {1.0, std::nullopt};
    } else if (op->type() == OperatorType::Projection) {
      const auto lqp_node = op->lqp_node;
      const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(lqp_node);

      for (const auto& node_expression : projection_node->node_expressions) {
        if (node_expression->type != ExpressionType::LQPColumn) {
          return {0.0, std::nullopt};
        }
      }
      return {1.0, std::nullopt};
    } else if (op->type() == OperatorType::JoinHash) {
      if (const auto join_hash_op = dynamic_pointer_cast<const JoinHash>(op)) {
        // The hash join linearily walks over the input for semi/anti* joins. Thus it behaves like a scan, the data
        // does not get shuffled. The only exception are clustered semi/anti* joins. Radix clustering also yields more
        // or less shuffled data. For all other join types, we always assume a shuffled pos list.
        if (join_hash_op->mode() != JoinMode::Semi &&
            join_hash_op->mode() != JoinMode::AntiNullAsTrue &&
            join_hash_op->mode() != JoinMode::AntiNullAsFalse) {
          return {1.0, std::nullopt};
        }

        const auto& operator_perf_data = dynamic_cast<const JoinHash::PerformanceData&>(*join_hash_op->performance_data);
        if (operator_perf_data.radix_bits > 0 &&
            (join_hash_op->mode() == JoinMode::Semi ||
             join_hash_op->mode() == JoinMode::AntiNullAsTrue ||
             join_hash_op->mode() == JoinMode::AntiNullAsFalse)) {
          // In case we create two radix partitions, the data of each chunk is consecutive within each of the clusters.
          // The higher the number of clusters, the smaller the consecutive write. Thus more shuffledness.
          return {1.0 - (1.0 / std::pow(2, operator_perf_data.radix_bits)), std::nullopt};
        }
      }
    }
  }

  visited_pqp_nodes.insert(op);

  const auto left_input = op->left_input();
  const auto right_input = op->right_input();
  auto left_value = 0.0;
  if (left_input && !visited_pqp_nodes.contains(left_input)) {
    left_value = estimate_pos_list_shuffledness(root_op, left_input, column_id, visited_pqp_nodes).first;
  }
  if (right_input && !visited_pqp_nodes.contains(right_input)) {
    const auto right_value = estimate_pos_list_shuffledness(root_op, right_input, column_id, visited_pqp_nodes).first;
    Assert(op->type() == OperatorType::JoinHash ||
           op->type() == OperatorType::JoinNestedLoop ||
           op->type() == OperatorType::JoinIndex ||
           op->type() == OperatorType::JoinSortMerge,
           "Unexpected operator type of " + std::string{magic_enum::enum_name(op->type())});
    return {left_value, right_value};
  }

  return {left_value, std::nullopt};

}

std::pair<double, std::optional<double>> estimate_pos_list_shuffledness(const std::shared_ptr<const AbstractOperator>& op, const ColumnID column_id) {
  std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_pqp_nodes;
  return estimate_pos_list_shuffledness(op, op, column_id, visited_pqp_nodes);
}

void process_pqp_op(const std::shared_ptr<const AbstractOperator>& op, const pmr_string& query_hex_hash,
                    const pmr_string& query_statement_hex_hash, const std::optional<OperatorType> operator_type,
                    std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes,
                    std::vector<std::tuple<pmr_string, pmr_string, std::shared_ptr<const AbstractOperator>>>& gathered_operators) {
  if ((operator_type && op->type() == *operator_type) || 
       // If instances of a particular operator are requested, only collect those
      (!operator_type && !MetaPlanCacheOperators::specialized_operator_types.contains(op->type())
        && !MetaPlanCacheOperators::unsupported_operator_types.contains(op->type()))) {
       // If no operator to collect is given, we emit all others (this is done for the misc. table!; it's not for an
       // all-operators-table). Also skip if operator type is not supported.
    gathered_operators.push_back(std::make_tuple(query_hex_hash, query_statement_hex_hash, op));
  }

  visited_pqp_nodes.insert(op);

  const auto left_input = op->left_input();
  const auto right_input = op->right_input();
  if (left_input && !visited_pqp_nodes.contains(left_input)) {
    process_pqp_op(left_input, query_hex_hash, query_statement_hex_hash, operator_type, visited_pqp_nodes, gathered_operators);
    // visited_pqp_nodes.insert(std::move(left_input));
  }
  if (right_input && !visited_pqp_nodes.contains(right_input)) {
    process_pqp_op(right_input, query_hex_hash, query_statement_hex_hash, operator_type, visited_pqp_nodes, gathered_operators);
    // visited_pqp_nodes.insert(std::move(right_input));
  }
}

std::vector<std::tuple<pmr_string, pmr_string, std::shared_ptr<const AbstractOperator>>>
get_operators_from_plan_cache(const MetaPlanCacheOperators::PlanCache plan_cache,
                              const std::optional<OperatorType> operator_type) {
  auto gathered_operators = std::vector<std::tuple<pmr_string, pmr_string, std::shared_ptr<const AbstractOperator>>>{};
  gathered_operators.reserve(plan_cache.size());

  for (const auto& [sql_string, snapshot_entry] : plan_cache) {
    const auto& physical_query_plan = snapshot_entry.value;
    const auto& frequency = snapshot_entry.frequency;
    Assert(frequency, "Optional frequency is unexpectedly unset.");

    // We create two hashes. First, a (hopefully) unique hash for a PQP query. We use the actual PQP to get different
    // hashes for the same SQL queries. This allows us to later differentiate between query instances without
    // requiring an additional key column.
    // Second, we have a hash von the SQL query string. This hash helps us to find identical queries.
    auto seed = size_t{0};
    boost::hash_combine(seed, physical_query_plan);
    boost::hash_combine(seed, sql_string);
    visit_pqp(physical_query_plan, [&](const auto& node) {
      boost::hash_combine(seed, operator_hash(node));
      return PQPVisitation::VisitInputs;
    });
    std::stringstream query_hex_hash;
    query_hex_hash << std::hex << seed;
    
    std::stringstream query_statement_hex_hash;
    query_statement_hex_hash << std::hex << std::hash<std::string>{}(sql_string);
    

    std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_pqp_nodes;
    process_pqp_op(physical_query_plan, pmr_string{query_hex_hash.str()}, pmr_string{query_statement_hex_hash.str()},
                   operator_type, visited_pqp_nodes, gathered_operators);
  }

  return gathered_operators;
}

// TODO: we might switch from a boolean return value to keep track of single columns, this might be especially
// important for joins along the way (as with projections since Oct/Nov 2020).
bool operator_result_is_probably_materialized(const std::shared_ptr<const AbstractOperator>& op) {
  auto left_input_table_probably_materialized = false;
  auto right_input_table_probably_materialized = false;

  // TODO: going up will always yield true here ...  removed: ` || op->type() == OperatorType::GetTable`
  // if (op->type() == OperatorType::Aggregate) {
  //   return true;
  // }

  if (op->type() == OperatorType::Projection) {
    const auto node = op->lqp_node;
    const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(node);

    for (const auto& el : projection_node->node_expressions) {
      if (el->requires_computation()) {
        return true;
      }
    }
  }

  if (op->left_input()) {
    left_input_table_probably_materialized = operator_result_is_probably_materialized(op->left_input());
  }

  if (op->right_input()) {
    right_input_table_probably_materialized = operator_result_is_probably_materialized(op->right_input());
  }

  return left_input_table_probably_materialized || right_input_table_probably_materialized;
}

std::string camel_to_csv_row_title(const std::string& title) {
  auto result = std::string{};
  auto string_index = size_t{0};
  for (unsigned char character : title) {
    if (string_index > 0 && std::isupper(character)) {
      result += '_';
      result += std::tolower(character);
      ++string_index;
      continue;
    }
    result += std::tolower(character);
    ++string_index;
  }
  return result;
}

}  // namespace


namespace opossum {

void MetaPlanCacheOperators::set_plan_cache_snapshot(const std::unordered_map<std::string, PlanCacheSnapshotEnty> plan_cache_snapshot) {
  _plan_cache_snapshot = plan_cache_snapshot;
}

void MetaPlanCacheOperators::unset_plan_cache_snapshot() {
  _plan_cache_snapshot = std::nullopt;
}

MetaPlanCacheAggregates::MetaPlanCacheAggregates(const TableColumnDefinitions table_column_definitions)
    : AbstractMetaTable(std::move(table_column_definitions)), MetaPlanCacheOperators() {}

MetaPlanCacheAggregates::MetaPlanCacheAggregates()
    : AbstractMetaTable(get_table_column_definitions()), MetaPlanCacheOperators() {}

const std::string& MetaPlanCacheAggregates::name() const {
  static const auto name = std::string{"plan_cache_aggregates"};
  return name;
}

TableColumnDefinitions MetaPlanCacheAggregates::get_table_column_definitions() {
  auto definitions = TableColumnDefinitions{{"query_hash", DataType::String, false},
                                            {"query_statement_hash", DataType::String, false},
                                            {"operator_hash", DataType::String, false},
                                            {"left_input_operator_hash", DataType::String, false},
                                            {"column_type", DataType::String, false},
                                            {"table_name", DataType::String, true},
                                            {"column_name", DataType::String, true},
                                            shuffledness_column,
                                            {"group_by_column_count", DataType::Int, false},
                                            {"aggregate_column_count", DataType::Int, false},
                                            {"is_group_by_column", DataType::Int, false},
                                            {"is_count_star", DataType::Int, false},
                                            {"is_distinct_aggregate", DataType::Int, false},
                                            {"is_any_aggregate", DataType::Int, false},
                                            {"input_chunk_count", DataType::Long, false},
                                            {"input_row_count", DataType::Long, false},
                                            {"output_chunk_count", DataType::Long, false},
                                            {"output_row_count", DataType::Long, false}};

  for (const auto step_name : magic_enum::enum_values<AggregateHash::OperatorSteps>()) {
    const auto step_column_name = camel_to_csv_row_title(std::string{magic_enum::enum_name(step_name)}) + "_ns";
    definitions.push_back({step_column_name, DataType::Long, true});
  }

  definitions.push_back({"runtime_ns", DataType::Long, false});
  definitions.push_back({"description", DataType::String, true});

  return definitions;
}

std::shared_ptr<Table> MetaPlanCacheAggregates::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto aggregations = get_operators_from_plan_cache(_plan_cache_snapshot ? *_plan_cache_snapshot : Hyrise::get().default_pqp_cache->snapshot(), OperatorType::Aggregate);
  for (const auto& [query_hex_hash, query_statement_hex_hash, op] : aggregations) {
    // Check if the input to the aggregate is materialized
    const auto input_is_materialized = operator_result_is_probably_materialized(op->left_input());

    const auto node = op->lqp_node;
    const auto aggregate_node = std::static_pointer_cast<const AggregateNode>(node);

    const auto node_expression_count = aggregate_node->node_expressions.size();
    const auto group_by_column_count = aggregate_node->aggregate_expressions_begin_idx;

    auto expression_id = size_t{0};
    for (const auto& el : aggregate_node->node_expressions) {
      auto is_count_star = int32_t{0};
      auto is_distinct_aggregate = int32_t{0};
      auto is_any_aggregate = int32_t{0};
      auto column_type = pmr_string{"DATA"};
      AllTypeVariant table_name = NULL_VALUE;
      AllTypeVariant column_name = NULL_VALUE;
      const auto parse_column_access = [&](const auto& expression) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
        const auto original_node = column_expression->original_node.lock();

        if (original_node->type == LQPNodeType::StoredTable) {
          if (original_node == node->left_input()) {
            column_type = pmr_string{"DATA"};
          } else {
            column_type = pmr_string{"REFERENCE"};
          }

          const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
          const auto& stored_table_name = stored_table_node->table_name;

          const auto original_column_id = column_expression->original_column_id;

          const auto sm_table = Hyrise::get().storage_manager.get_table(stored_table_name);
          Assert(original_column_id != INVALID_COLUMN_ID, "Unexpected column id. Count(*) should not reach this point.");
          const auto stored_column_name = sm_table->column_names()[original_column_id];

          table_name = pmr_string{stored_table_name};
          column_name = pmr_string{stored_column_name};
        } else {
          Fail("Found unexpected: " + std::string{magic_enum::enum_name(original_node->type)});
        }
      };

      const auto is_group_by_column = static_cast<int32_t>(expression_id < group_by_column_count);
      Assert(is_group_by_column || el->requires_computation(), "Unexpected aggregation columns that does not require computation.");

      const auto& perf_data = op->performance_data;

      if (el->type == ExpressionType::LQPColumn) {
        // The table has not been materialized before (e.g., in a projection) and the expression is an LQPColumn
        // (e.g., GROUP BY l_linenumber)
        parse_column_access(el);
      } else {
        // It is not a direct column reference. This part handles count(*), aggregations (e.g., SUM),
        // or functions (e.g., CASE), or accesses to a materialized table.
        const auto aggregate_expression = std::static_pointer_cast<const AggregateExpression>(el);
        Assert(aggregate_expression, "Found an aggregate column that is neither an LQPColumn nor a aggregate expression.");
        Assert(el->type == ExpressionType::Aggregate || el->type == ExpressionType::Function || el->type == ExpressionType::Arithmetic ||
               (input_is_materialized && el->type == ExpressionType::LQPColumn),
               "Found an unexpected expression in an aggregate node: " + std::string{magic_enum::enum_name(el->type)});

        // ANY and COUNT are not really accessing the columns (simplicification). So a NULL column access is emitted.
        if (aggregate_expression->aggregate_function == AggregateFunction::Any) {
          is_any_aggregate = 1;
          parse_column_access(aggregate_expression->argument());
        } else if (AggregateExpression::is_count_star(*aggregate_expression)) {
          is_count_star = 1;
        } else if (!input_is_materialized && aggregate_expression->argument()->type == ExpressionType::LQPColumn) {
          parse_column_access(aggregate_expression->argument());
        } // else: we do not have a column access to an actual column (e.g., column of a materialized temp table)

        if (aggregate_expression->aggregate_function == AggregateFunction::CountDistinct) {
          is_distinct_aggregate = 1;
        }
      }

      auto values_to_append = std::vector<AllTypeVariant>{query_hex_hash, query_statement_hex_hash,
        operator_hash(op), operator_hash(op->left_input()),
        column_type, table_name, column_name, estimate_pos_list_shuffledness(op, ColumnID{0}).first,
        static_cast<int32_t>(group_by_column_count),
        static_cast<int32_t>(node_expression_count - group_by_column_count),
        static_cast<int32_t>(is_group_by_column), is_count_star, is_distinct_aggregate, is_any_aggregate,
        static_cast<int64_t>(op->left_input()->performance_data->output_chunk_count),
        static_cast<int64_t>(op->left_input()->performance_data->output_row_count),
        static_cast<int64_t>(perf_data->output_chunk_count),
        static_cast<int64_t>(perf_data->output_row_count)};

      if (const auto aggregate_hash_op = dynamic_pointer_cast<const AggregateHash>(op)) {
        const auto& operator_perf_data = dynamic_cast<const OperatorPerformanceData<AggregateHash::OperatorSteps>&>(*aggregate_hash_op->performance_data);
        for (const auto step_name : magic_enum::enum_values<AggregateHash::OperatorSteps>()) {
          values_to_append.push_back(static_cast<int64_t>(operator_perf_data.get_step_runtime(step_name).count()));
        }
      } else {
        for (auto step_id = size_t{0}; step_id < magic_enum::enum_count<AggregateHash::OperatorSteps>(); ++step_id) {
          values_to_append.push_back(0);
        }
      }
      values_to_append.push_back(static_cast<int64_t>(perf_data->walltime.count()));
      values_to_append.push_back(pmr_string{op->description()});

      output_table->append(values_to_append);

      ++expression_id;
    }
  }

  return output_table;
}

MetaPlanCacheTableScans::MetaPlanCacheTableScans()
    : AbstractMetaTable(TableColumnDefinitions{{"query_hash", DataType::String, false},
                                               {"query_statement_hash", DataType::String, false},
                                               {"operator_hash", DataType::String, false},
                                               {"left_input_operator_hash", DataType::String, false},
                                               {"column_type", DataType::String, false},
                                               {"left_table_name", DataType::String, true},
                                               {"left_column_name", DataType::String, true},
                                               {"right_table_name", DataType::String, true},
                                               {"right_column_name", DataType::String, true},
                                               shuffledness_column,
                                               {"predicate_condition", DataType::String, true},
                                               {"pruned_chunk_count", DataType::Long, false},
                                               {"all_rows_matched_count", DataType::Long, false},
                                               {"binary_search_count", DataType::Long, false},
                                               {"input_chunk_count", DataType::Long, false},
                                               {"input_row_count", DataType::Long, false},
                                               {"output_chunk_count", DataType::Long, false},
                                               {"output_row_count", DataType::Long, false},
                                               {"runtime_ns", DataType::Long, false},
                                               {"description", DataType::String, false}}), MetaPlanCacheOperators() {}

const std::string& MetaPlanCacheTableScans::name() const {
  static const auto name = std::string{"plan_cache_table_scans"};
  return name;
}

std::shared_ptr<Table> MetaPlanCacheTableScans::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto table_scans = get_operators_from_plan_cache(_plan_cache_snapshot ? *_plan_cache_snapshot : Hyrise::get().default_pqp_cache->snapshot(), OperatorType::TableScan);
  for (const auto& [query_hex_hash, query_statement_hex_hash, op] : table_scans) {
    const auto node = op->lqp_node;
    const auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);

    const auto predicate = predicate_node->predicate();
    const auto predicate_lqp_column_count = std::count_if(predicate->arguments.cbegin(), predicate->arguments.cend(),
                                                          [](const auto argument) {
                                                            return argument->type == ExpressionType::LQPColumn;
                                                          });

    // The following assertions can fail any time, when a benchmark/workload executes an unexpected table scan.
    // We rather fail in this case instead of silently producing erroneous exports.
    Assert(predicate_lqp_column_count > 0 || op->description().find("ColumnVsColumn") == std::string::npos,
           "Unhandled case of a column vs column scan without any LQPColumn expressions.");
    Assert(predicate_lqp_column_count < 3, "Unexpected case of table scan with three LQPColumns.");

    // We have several cases to handle here. A table scan usually contains a single LQPColumn and this is the one we
    // are interested the most in. But there are several special cases. First, column vs column scans. Second, table
    // scans on temporary columns (e.g., after a projection or comparing against an (uncorrelated) LQPSubquery result).
    // In this case, we don't search for LQPColumns but still output a scan, so that we are able to predict the runtime
    // of this scan.
    auto left_table_name = NULL_VALUE;
    auto left_column_name = NULL_VALUE;
    auto right_table_name = NULL_VALUE;
    auto right_column_name = NULL_VALUE;
    auto scan_predicate_condition = NULL_VALUE;
    auto column_type = pmr_string{"DATA"};
    auto column_id = ColumnID{0};

    const auto table_scan_op = dynamic_pointer_cast<const TableScan>(op);
    Assert(table_scan_op, "Unexpected non-table-scan operators");
    const auto& operator_perf_data = dynamic_cast<const TableScan::PerformanceData&>(*table_scan_op->performance_data);

    for (auto argument_id = size_t{0}; argument_id < predicate->arguments.size(); ++argument_id) {
      const auto expression = predicate->arguments[argument_id];

      if (expression->type != ExpressionType::LQPColumn &&
          !(predicate_lqp_column_count == 0 && argument_id == (predicate->arguments.size() - 1))) {
        // We usually only care about LQPColumns, unless there is none. In this case, we'll output a table scan on a
        // temporary column (i.e., table_name and column_name are NULL).
        continue;
      } else if (expression->type == ExpressionType::LQPColumn) {
        const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
        const auto original_node = column_expression->original_node.lock();

        // Check if scan on data or reference table (this should ignore scans of temporary columns)
        if (original_node->type == LQPNodeType::StoredTable) {
          if (original_node != node->left_input()) {
            column_type = "REFERENCE";
          }

          const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
          if (argument_id == 0) {
            left_table_name = pmr_string{stored_table_node->table_name};  
          } else if (argument_id == 1) {
            right_table_name = pmr_string{stored_table_node->table_name};
          }

          const auto operator_scan_predicates = OperatorScanPredicate::from_expression(*predicate, *stored_table_node);
          if (operator_scan_predicates->size() == 1) {
            const auto condition = (*operator_scan_predicates)[0].predicate_condition;
            scan_predicate_condition = pmr_string{magic_enum::enum_name(condition)};
          }

          Assert(!variant_is_null(left_table_name), "Unexpected NULL value for table name");
          const auto original_column_id = column_expression->original_column_id;
          // Using left_table_name should be safe here as it must be the same as right_table_name for ColumnVsColumn scans.
          auto table = Hyrise::get().storage_manager.get_table(std::string{boost::get<pmr_string>(left_table_name)});
          if (argument_id == 1) {
            table = Hyrise::get().storage_manager.get_table(std::string{boost::get<pmr_string>(right_table_name)});
          }
          if (original_column_id != INVALID_COLUMN_ID) {
            if (argument_id == 0) {
              left_column_name = pmr_string{table->column_names()[original_column_id]};
            } else {
              right_column_name = pmr_string{table->column_names()[original_column_id]};
            }
          } else {
            if (argument_id == 0) {
              left_column_name = pmr_string{"COUNT(*)"};
            } else if (argument_id == 1) {
              right_column_name = pmr_string{"COUNT(*)"};
            }
          }
        } else {
          Fail("Unexpected path taken during LQPColumn analysis.");
        }
      }

      try {
        column_id = node->left_input()->get_column_id(*predicate->arguments[0]);
      } catch (...) {}
    }
    output_table->append({query_hex_hash, query_statement_hex_hash, operator_hash(op), operator_hash(op->left_input()), column_type,
                          left_table_name, left_column_name, right_table_name, right_column_name,
                          estimate_pos_list_shuffledness(op, column_id).first,
                          scan_predicate_condition,
                          static_cast<int64_t>(operator_perf_data.num_chunks_with_early_out),
                          static_cast<int64_t>(operator_perf_data.num_chunks_with_all_rows_matching),
                          static_cast<int64_t>(operator_perf_data.num_chunks_with_binary_search),
                          static_cast<int64_t>(op->left_input()->performance_data->output_chunk_count),
                          static_cast<int64_t>(op->left_input()->performance_data->output_row_count),
                          static_cast<int64_t>(operator_perf_data.output_chunk_count),
                          static_cast<int64_t>(operator_perf_data.output_row_count),
                          static_cast<int64_t>(operator_perf_data.walltime.count()),
                          pmr_string{op->description()}});
  }

  return output_table;
}

MetaPlanCacheJoins::MetaPlanCacheJoins(const TableColumnDefinitions table_column_definitions)
    : AbstractMetaTable(std::move(table_column_definitions)), MetaPlanCacheOperators() {}

MetaPlanCacheJoins::MetaPlanCacheJoins()
    : AbstractMetaTable(get_table_column_definitions()), MetaPlanCacheOperators() {}

const std::string& MetaPlanCacheJoins::name() const {
  static const auto name = std::string{"plan_cache_joins"};
  return name;
}

TableColumnDefinitions MetaPlanCacheJoins::get_table_column_definitions() {
  auto definitions = TableColumnDefinitions{{"query_hash", DataType::String, false},
                                            {"query_statement_hash", DataType::String, false},
                                            {"operator_hash", DataType::String, false},
                                            {"left_input_operator_hash", DataType::String, false},
                                            {"right_input_operator_hash", DataType::String, false},
                                            {"join_mode", DataType::String, false},
                                            {"left_table_name", DataType::String, true},
                                            {"left_column_name", DataType::String, true},
                                            {"left_input_shuffledness", DataType::Double, false},
                                            {"left_table_row_count", DataType::Long, false},
                                            {"left_table_chunk_count", DataType::Long, false},
                                            {"left_column_type", DataType::String, false},
                                            {"right_table_name", DataType::String, true},
                                            {"right_column_name", DataType::String, true},
                                            {"right_input_shuffledness", DataType::Double, false},
                                            {"right_table_row_count", DataType::Long, false},
                                            {"right_table_chunk_count", DataType::Long, false},
                                            {"right_column_type", DataType::String, false},
                                            {"predicate_count", DataType::Int, false},
                                            {"primary_predicate", DataType::String, false},
                                            {"output_chunk_count", DataType::Long, false},
                                            {"output_row_count", DataType::Long, false},
                                            {"is_flipped", DataType::Int, false},
                                            {"radix_bits", DataType::Int, true},
                                            {"build_side_materialized_value_count", DataType::Long, true},
                                            {"probe_side_materialized_value_count", DataType::Long, true},
                                            {"hash_tables_distinct_value_count", DataType::Long, true},
                                            {"hash_tables_position_count", DataType::Long, true}};

  for (const auto step_name : magic_enum::enum_values<JoinHash::OperatorSteps>()) {
    const auto step_column_name = camel_to_csv_row_title(std::string{magic_enum::enum_name(step_name)}) + "_ns";
    definitions.push_back({step_column_name, DataType::Long, true});
  }

  definitions.push_back({"runtime_ns", DataType::Long, false});
  definitions.push_back({"description", DataType::String, true});

  return definitions;
}

std::shared_ptr<Table> MetaPlanCacheJoins::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto process_joins = [&](const auto& joins) {
    for (const auto& [query_hex_hash, query_statement_hex_hash, op] : joins) {
      // Check if the input to the aggregate is materialized
      // const auto input_is_materialized = operator_result_is_probably_materialized(op->left_input());

      const auto node = op->lqp_node;
      const auto join_node = std::dynamic_pointer_cast<const JoinNode>(node);

      const auto operator_predicate = OperatorJoinPredicate::from_expression(*(join_node->node_expressions[0]),
                                                                             *node->left_input(), *node->right_input());

      const auto& perf_data = op->performance_data;

      if (operator_predicate.has_value()) {
        const auto join_predicate_expression = std::dynamic_pointer_cast<const AbstractPredicateExpression>(join_node->node_expressions[0]);

        // The single source of truth for the workload parsing is the PQP. But in certain situations, the PQP lacks
        // relevant information. One example is obtaining the referenced table of a predicate. To access the table,
        // we thus access the LQP.
        // Note, the LQP translator might swith the predicates. See below for calls of this lambda.
        const auto obtain_column_information = [&](const auto& predicate_expression, const auto& node_input) {  // node_input for the data check
          auto table_name = NULL_VALUE;
          auto column_name = NULL_VALUE;
          auto original_column_id = ColumnID{};
          auto column_type = pmr_string{"DATA"};

          if (predicate_expression->type == ExpressionType::LQPColumn) {
            const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate_expression);
            original_column_id = column_expression->original_column_id;

            const auto original_node = column_expression->original_node.lock();
            if (original_node->type == LQPNodeType::StoredTable) {
              const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
              table_name = pmr_string{stored_table_node->table_name};

              if (original_node != node_input && node_input->type != LQPNodeType::StoredTable
                  && node_input->type != LQPNodeType::StaticTable) {
                // It's a reference table if the originally referenced not is not the input AND the input is not a table node.
                // The reason to check for the table nodes is that a pruned GetTable is a new node.
                column_type = pmr_string{"REFERENCE"};
              } else {
                column_type = pmr_string{"DATA"};
              }
            }
          }

          // In cases where the join partner is not a column, we fall back to empty column names.
          // Exemplary query is the rewrite of TPC-H Q2 where `min(ps_supplycost)` is joined with `ps_supplycost`.
          if (!variant_is_null(table_name)) {
            const auto table_name_str = std::string{boost::get<pmr_string>(table_name)};
            const auto sm_table = Hyrise::get().storage_manager.get_table(table_name_str);
            column_name = pmr_string{sm_table->column_names()[original_column_id]};
          }

          return std::tuple{table_name, column_name, column_type};
        };

        // Check if the join predicate has been flipped (hence, it differs between LQP and PQP) which is done when
        // table A and B are joined but the join predicate is "flipped" (e.g., SELECT * FROM a, b WHERE b.x = a.x).
        // The idea of flipping is that the predicates are in the order (left/right) as the join input tables are.
        // We pull the stored table information from the LQP nodes, thus we need to flip the information that is gathered
        // from the LQP nodes. However, when we check for the join predicate's stored table node input (if there is
        // any), we take the original input since inputs are  not flipped (only the predicate).
        const auto [left_table_name, left_column_name, left_column_type] = operator_predicate->is_flipped() ?
            obtain_column_information(join_predicate_expression->arguments[1], node->left_input()) :
            obtain_column_information(join_predicate_expression->arguments[0], node->right_input());

        const auto [right_table_name, right_column_name, right_column_type] = operator_predicate->is_flipped() ?
            obtain_column_information(join_predicate_expression->arguments[0], node->right_input()) :
            obtain_column_information(join_predicate_expression->arguments[1], node->left_input());

        const auto& left_input_perf_data = op->left_input()->performance_data;
        const auto& right_input_perf_data = op->right_input()->performance_data;

        int64_t left_table_row_count = left_input_perf_data->output_row_count;
        int64_t left_table_chunk_count = left_input_perf_data->output_chunk_count;
        int64_t right_table_row_count = right_input_perf_data->output_row_count;
        int64_t right_table_chunk_count = right_input_perf_data->output_chunk_count;

        auto values_to_append = std::vector<AllTypeVariant>{query_hex_hash, query_statement_hex_hash,
          operator_hash(op), operator_hash(op->left_input()), operator_hash(op->right_input()),
          pmr_string{join_mode_to_string.left.at(join_node->join_mode)},
          left_table_name,
          left_column_name,
          estimate_pos_list_shuffledness(op, ColumnID{0}).first,
          left_table_row_count,
          left_table_chunk_count,
          left_column_type,
          right_table_name, right_column_name, *estimate_pos_list_shuffledness(op, ColumnID{0}).second,
          right_table_row_count, right_table_chunk_count, right_column_type,
          static_cast<int32_t>(join_node->node_expressions.size()),
          pmr_string{predicate_condition_to_string.left.at((*operator_predicate).predicate_condition)},
          static_cast<int64_t>(perf_data->output_chunk_count),
          static_cast<int64_t>(perf_data->output_row_count)};

        if (const auto join_hash_op = dynamic_pointer_cast<const JoinHash>(op)) {
          const auto& operator_perf_data = dynamic_cast<const JoinHash::PerformanceData&>(*join_hash_op->performance_data);
          values_to_append.push_back(static_cast<int32_t>(!operator_perf_data.left_input_is_build_side));
          values_to_append.push_back(static_cast<int32_t>(operator_perf_data.radix_bits));
          values_to_append.push_back(static_cast<int64_t>(operator_perf_data.build_side_materialized_value_count));
          values_to_append.push_back(static_cast<int64_t>(operator_perf_data.probe_side_materialized_value_count));
          values_to_append.push_back(static_cast<int64_t>(operator_perf_data.hash_tables_distinct_value_count));
          const auto hash_tables_position_count = operator_perf_data.hash_tables_position_count
                                                  ? *operator_perf_data.hash_tables_position_count : 0;
          values_to_append.push_back(static_cast<int64_t>(hash_tables_position_count));
          for (const auto step_name : magic_enum::enum_values<JoinHash::OperatorSteps>()) {
            values_to_append.push_back(static_cast<int64_t>(operator_perf_data.get_step_runtime(step_name).count()));
          }
        } else {
          values_to_append.push_back(0);  // is flipped
          values_to_append.push_back(NULL_VALUE);  // radix bits
          values_to_append.push_back(NULL_VALUE);  // build_side_materialized_value_count
          values_to_append.push_back(NULL_VALUE);  // probe_side_materialized_value_count
          values_to_append.push_back(NULL_VALUE);  // hash_tables_distinct_value_count
          values_to_append.push_back(NULL_VALUE);  // hash_tables_position_count
          for (auto step_id = size_t{0}; step_id < magic_enum::enum_count<JoinHash::OperatorSteps>(); ++step_id) {
            values_to_append.push_back(NULL_VALUE);
          }
        }
        values_to_append.push_back(static_cast<int64_t>(perf_data->walltime.count()));
        values_to_append.push_back(pmr_string{op->description()});

        output_table->append(values_to_append);
      } else {
        Fail("Unexpected join operator_predicate.has_value()");
      }
    }
  };

  const auto plan_cache_snapshot = _plan_cache_snapshot ? *_plan_cache_snapshot : Hyrise::get().default_pqp_cache->snapshot();
  const auto hash_joins = get_operators_from_plan_cache(plan_cache_snapshot, OperatorType::JoinHash);
  process_joins(hash_joins);
  const auto sort_merge_joins = get_operators_from_plan_cache(plan_cache_snapshot, OperatorType::JoinSortMerge);
  process_joins(sort_merge_joins);

  return output_table;
}

MetaPlanCacheProjections::MetaPlanCacheProjections(const TableColumnDefinitions table_column_definitions)
    : AbstractMetaTable(std::move(table_column_definitions)), MetaPlanCacheOperators() {}

MetaPlanCacheProjections::MetaPlanCacheProjections()
    : AbstractMetaTable(get_table_column_definitions()), MetaPlanCacheOperators() {}

const std::string& MetaPlanCacheProjections::name() const {
  static const auto name = std::string{"plan_cache_projections"};
  return name;
}

TableColumnDefinitions MetaPlanCacheProjections::get_table_column_definitions() {
  auto definitions = TableColumnDefinitions{{"query_hash", DataType::String, false},
                                            {"query_statement_hash", DataType::String, false},
                                            {"operator_hash", DataType::String, false},
                                            {"left_input_operator_hash", DataType::String, false},
                                            {"column_type", DataType::String, false},
                                            {"table_name", DataType::String, true},
                                            {"column_name", DataType::String, true},
                                            shuffledness_column,
                                            {"requires_computation", DataType::Int, false},
                                            {"input_chunk_count", DataType::Long, false},
                                            {"input_row_count", DataType::Long, false},
                                            {"output_chunk_count", DataType::Long, false},
                                            {"output_row_count", DataType::Long, false}};

  for (const auto step_name : magic_enum::enum_values<Projection::OperatorSteps>()) {
    const auto step_column_name = camel_to_csv_row_title(std::string{magic_enum::enum_name(step_name)}) + "_ns";
    definitions.push_back({step_column_name, DataType::Long, true});
  }

  definitions.push_back({"runtime_ns", DataType::Long, false});
  definitions.push_back({"description", DataType::String, true});

  return definitions;
}

std::shared_ptr<Table> MetaPlanCacheProjections::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto plan_cache_snapshot = _plan_cache_snapshot ? *_plan_cache_snapshot : Hyrise::get().default_pqp_cache->snapshot();
  const auto projections = get_operators_from_plan_cache(plan_cache_snapshot, OperatorType::Projection);

  for (const auto& [query_hex_hash, query_statement_hex_hash, op] : projections) {
    const auto input_is_materialized = operator_result_is_probably_materialized(op->left_input());

    const auto node = op->lqp_node;
    const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(node);

    const auto& perf_data = op->performance_data;
    const auto& left_input_perf_data = op->left_input()->performance_data;

    const auto description = pmr_string{op->lqp_node->description()};

    const auto add_remaining_fields_and_add_to_table = [&, op=op](auto& values, const auto expression) {
      values.insert(values.end(), {estimate_pos_list_shuffledness(op, ColumnID{0}).first,
                                   static_cast<int32_t>(expression->requires_computation()),
                                   static_cast<int64_t>(left_input_perf_data->output_chunk_count),
                                   static_cast<int64_t>(left_input_perf_data->output_row_count),
                                   static_cast<int64_t>(perf_data->output_chunk_count),
                                   static_cast<int64_t>(perf_data->output_row_count)});

      const auto& operator_perf_data = dynamic_cast<const OperatorPerformanceData<Projection::OperatorSteps>&>(*op->performance_data);
      for (const auto step_name : magic_enum::enum_values<Projection::OperatorSteps>()) {
        values.push_back(static_cast<int64_t>(operator_perf_data.get_step_runtime(step_name).count()));
      }

      values.insert(values.end(), {static_cast<int64_t>(perf_data->walltime.count()),
                                                       description});
      output_table->append(values);
    };

    for (const auto& el : projection_node->node_expressions) {
      auto values_to_append = std::vector<AllTypeVariant>{query_hex_hash, query_statement_hex_hash,
                                                          operator_hash(op), operator_hash(op->left_input())};

      if (input_is_materialized) {
        auto values_to_append = std::vector<AllTypeVariant>{query_hex_hash, query_statement_hex_hash,
                                                            operator_hash(op), operator_hash(op->left_input()),
                                                            pmr_string{"DATA"}, NULL_VALUE, NULL_VALUE};
        add_remaining_fields_and_add_to_table(values_to_append, el);
      } else {
        // In case we have a more "complex" expression (e.g., l_price * 1.19f), we need to traverse the expressions
        // in order to find any potential column references.
        visit_expression(el, [&, query_hex_hash=query_hex_hash, query_statement_hex_hash=query_statement_hex_hash, op=op](const auto& expression) {
          if (expression->type == ExpressionType::LQPColumn) {
            const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
            const auto original_node = column_expression->original_node.lock();

            if (original_node->type == LQPNodeType::StoredTable) {
              const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(original_node);
              const auto table_name = stored_table_node->table_name;
            
              const auto original_column_id = column_expression->original_column_id;
              const auto column_type = (original_node == node->left_input()) ? pmr_string{"DATA"} : pmr_string{"REFERENCE"};
            
              const auto sm_table = Hyrise::get().storage_manager.get_table(table_name);
              auto column_name = NULL_VALUE;
              if (original_column_id != INVALID_COLUMN_ID) {
                column_name = pmr_string{sm_table->column_names()[original_column_id]};
              } else {
                column_name = pmr_string{"COUNT(*)"};
              }

              auto values_to_append = std::vector<AllTypeVariant>{query_hex_hash, query_statement_hex_hash,
                                                                  operator_hash(op), operator_hash(op->left_input()),
                                                                  column_type, pmr_string{table_name}, column_name};
              add_remaining_fields_and_add_to_table(values_to_append, el);
            }
          }

          return ExpressionVisitation::VisitArguments;
        });
      }
    }
  }

  return output_table;
}

MetaPlanCacheGetTables::MetaPlanCacheGetTables()
    : AbstractMetaTable(TableColumnDefinitions{{"query_hash", DataType::String, false},
                                               {"query_statement_hash", DataType::String, false},
                                               {"operator_hash", DataType::String, false},
                                               {"table_name", DataType::String, false},
                                               {"pruned_chunk_count", DataType::Long, false},
                                               {"pruned_column_count", DataType::Long, false},
                                               {"output_chunk_count", DataType::Long, false},
                                               {"output_row_count", DataType::Long, false},
                                               {"runtime_ns", DataType::Long, false},
                                               {"description", DataType::String, false}}), MetaPlanCacheOperators() {}

const std::string& MetaPlanCacheGetTables::name() const {
  static const auto name = std::string{"plan_cache_get_tables"};
  return name;
}

std::shared_ptr<Table> MetaPlanCacheGetTables::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto get_tables = get_operators_from_plan_cache(_plan_cache_snapshot ? *_plan_cache_snapshot : Hyrise::get().default_pqp_cache->snapshot(), OperatorType::GetTable);
  for (const auto& [query_hex_hash, query_statement_hex_hash, op] : get_tables) {
    const auto get_table_op = dynamic_pointer_cast<const GetTable>(op);
    Assert(get_table_op, "Processing GetTable operator but another operator was passed.");

    const auto& operator_perf_data = op->performance_data;
    output_table->append({query_hex_hash,  query_statement_hex_hash,operator_hash(op),
                          pmr_string{get_table_op->table_name()},
                          static_cast<int64_t>(get_table_op->pruned_chunk_ids().size()),
                          static_cast<int64_t>(get_table_op->pruned_column_ids().size()),
                          static_cast<int64_t>(operator_perf_data->output_chunk_count),
                          static_cast<int64_t>(operator_perf_data->output_row_count),
                          static_cast<int64_t>(operator_perf_data->walltime.count()), pmr_string{op->description()}});
  }

  return output_table;
}

MetaPlanCacheMiscOperators::MetaPlanCacheMiscOperators()
    : AbstractMetaTable(TableColumnDefinitions{{"operator", DataType::String, false},
                                               {"query_hash", DataType::String, false},
                                               {"query_statement_hash", DataType::String, false},
                                               {"operator_hash", DataType::String, false},
                                               {"output_chunk_count", DataType::Long, false},
                                               {"output_row_count", DataType::Long, false},
                                               {"runtime_ns", DataType::Long, false},
                                               {"description", DataType::String, false}}), MetaPlanCacheOperators() {}

const std::string& MetaPlanCacheMiscOperators::name() const {
  static const auto name = std::string{"plan_cache_misc_operators"};
  return name;
}

std::shared_ptr<Table> MetaPlanCacheMiscOperators::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto misc_operators = get_operators_from_plan_cache(_plan_cache_snapshot ? *_plan_cache_snapshot
                                                            : Hyrise::get().default_pqp_cache->snapshot(),
                                                            std::nullopt);
  for (const auto& [query_hex_hash, query_statement_hex_hash, op] : misc_operators) {
    const auto& operator_perf_data = op->performance_data;
    output_table->append({pmr_string{magic_enum::enum_name(op->type())}, query_hex_hash,
                          query_statement_hex_hash, operator_hash(op),
                          static_cast<int64_t>(operator_perf_data->output_chunk_count),
                          static_cast<int64_t>(operator_perf_data->output_row_count),
                          static_cast<int64_t>(operator_perf_data->walltime.count()), pmr_string{op->description()}});
  }

  return output_table;
}

PQPExportTablesPlugin::PQPExportTablesPlugin() {}

std::string PQPExportTablesPlugin::description() const { return "This is the Hyrise PQPExportTablesPlugin"; }

void PQPExportTablesPlugin::start() {
  auto aggregate_table = std::make_shared<MetaPlanCacheAggregates>();
  auto joins_table = std::make_shared<MetaPlanCacheJoins>();
  auto projections_table = std::make_shared<MetaPlanCacheProjections>();
  auto table_scans_table = std::make_shared<MetaPlanCacheTableScans>();
  auto get_tables_table = std::make_shared<MetaPlanCacheGetTables>();
  auto misc_operators_table = std::make_shared<MetaPlanCacheMiscOperators>();

  constexpr auto FIXED_PLAN_CACHE = false;
  if (FIXED_PLAN_CACHE) {
    const auto pqp_cache_snapshot = Hyrise::get().default_pqp_cache->snapshot();
    aggregate_table->set_plan_cache_snapshot(pqp_cache_snapshot);
    joins_table->set_plan_cache_snapshot(pqp_cache_snapshot);
    projections_table->set_plan_cache_snapshot(pqp_cache_snapshot);
    table_scans_table->set_plan_cache_snapshot(pqp_cache_snapshot);
    get_tables_table->set_plan_cache_snapshot(pqp_cache_snapshot);
    misc_operators_table->set_plan_cache_snapshot(pqp_cache_snapshot);
  }

  Hyrise::get().meta_table_manager.add_table(std::move(aggregate_table));
  Hyrise::get().meta_table_manager.add_table(std::move(joins_table));
  Hyrise::get().meta_table_manager.add_table(std::move(projections_table));
  Hyrise::get().meta_table_manager.add_table(std::move(table_scans_table));
  Hyrise::get().meta_table_manager.add_table(std::move(get_tables_table));
  Hyrise::get().meta_table_manager.add_table(std::move(misc_operators_table));
}

void PQPExportTablesPlugin::stop() {}

EXPORT_PLUGIN(PQPExportTablesPlugin)

}  // namespace opossum
