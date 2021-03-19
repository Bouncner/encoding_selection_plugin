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
add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter                  #
-Wno-dollar-in-identifier-extension -Wno-unknown-pragmas)                                           #
                                                                                                    #
set(CMAKE_CXX_STANDARD_REQUIRED ON)                                                                 #
set(CMAKE_CXX_STANDARD 20)                                                                          #
#####################################################################################################


### Plugin to generate meta tables for exported PQP information
add_plugin(NAME WorkloadStatistics SRCS pqp_export_tables.cpp pqp_export_tables.hpp DEPS hyriseServer)

### Plugin to expose benchmark queries and run TPC-C
add_plugin(NAME WorkloadHandler SRCS workload_handler.cpp workload_handler.hpp DEPS hyriseServer)

### Plugin to accept simple commands
# add_plugin(NAME SimpleExecutor SRCS pqp_export_tables.cpp pqp_export_tables.hpp DEPS hyriseServer)
