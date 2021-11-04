if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(HYRISE_DEBUG 1)
else()
    set(HYRISE_DEBUG 0)
endif()

add_definitions(-DHYRISE_DEBUG=${HYRISE_DEBUG})

# Usually, you should not have to touch the following part ##########################################
cmake_minimum_required(VERSION 3.16 FATAL_ERROR)                                                    #
                                                                                                    #
project("Hyrise Workload Suite")                                                                    #
                                                                                                    #
# Include CMakeLists.txt of Hyrise (all the build magic is contained there)                         #
add_subdirectory(hyrise/)                                                                           #
                                                                                                    #
# Add hyrise sources to be able to include headers                                                  #
include_directories(                                                                                #
    ${PROJECT_SOURCE_DIR}/hyrise/src/lib/                                                           #
                                                                                                    #
    # BenchmarkLib to run TPC-C in the workload handler plugin                                      #
    ${PROJECT_SOURCE_DIR}/hyrise/src/benchmarklib/                                                  #
    # We currently need to include a few third party libraries, mostly as we inlude the hash join   #
    # and the hash aggregate. Forward declaration does currently not work as we access internals of #
    # the operators' performance structs. For now, including is the easier way, but not necessarily #
    # the best or most beautiful way.                                                               #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/compact_vector/include                                 #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/flat_hash_map                                          #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/magic_enum/include                                     #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/nlohmann_json/single_include  # encoding config        #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/robin-map/include                                      #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/sql-parser/src                                         #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/uninitialized_vector                                   #
                                                                                                    #
    ${Boost_INCLUDE_DIR}                                                                            #
)                                                                                                   #
                                                                                                    #
include_directories(                                                                                #
    SYSTEM                                                                                          #
                                                                                                    #
    # TBB as system include to ignore error: 'const volatile long long' is deprecated               #
    ${TBB_INCLUDE_DIR}                                                                              #
                                                                                                    #
    ${PROJECT_SOURCE_DIR}/hyrise/third_party/                                                       #
)                                                                                                   #
                                                                                                    #
# Global flags and include directories                                                              #
#   '-Wno-unknown-warning-option' added to avoid "unknown warning group '-Wmaybe-uninitialized'"    #
#   in fixed_size_byte_aligned_decompressor.hpp                                                     #
add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter                  #
-Wno-dollar-in-identifier-extension -Wno-unknown-pragmas -Wno-unknown-warning-option)               #
                                                                                                    #
set(CMAKE_CXX_STANDARD_REQUIRED ON)                                                                 #
set(CMAKE_CXX_STANDARD 20)                                                                          #
#####################################################################################################

### Plugin to generate meta tables for exported PQP information
add_plugin(NAME WorkloadStatisticsPlugin SRCS pqp_export_tables.cpp pqp_export_tables.hpp DEPS hyriseServer)

### Plugin to expose benchmark queries as a meta table and run TPC-C
add_plugin(NAME WorkloadHandlerPlugin SRCS workload_handler.cpp workload_handler.hpp DEPS hyriseServer)

### Plugin to accept simple commands
add_plugin(NAME CommandExecutorPlugin SRCS command_executor.cpp command_executor.hpp DEPS hyriseServer)

### Plugin to provide a data table with additional data characteristics
add_plugin(NAME DataCharacteristicsPlugin SRCS data_characteristics.cpp data_characteristics.hpp DEPS hyriseServer)


# Add a link to resources/ at the root of the build directory.
add_custom_target(resourceBenchmarkDataLink
    COMMAND rm -f ${CMAKE_BINARY_DIR}/resources  # remove link used for hyriseTest (see main CMakeLists)
    COMMAND ln -fs ${CMAKE_BINARY_DIR}/../hyrise/resources ${CMAKE_BINARY_DIR}/
    COMMAND ln -fs ${CMAKE_BINARY_DIR}/../hyrise/third_party ${CMAKE_BINARY_DIR}/
)
add_dependencies(WorkloadStatisticsPlugin resourceBenchmarkDataLink)
