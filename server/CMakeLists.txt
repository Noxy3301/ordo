cmake_minimum_required(VERSION 3.16)
project(lineairdb-server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)

# LineairDB library path
set(LINEAIRDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/LineairDB")

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../proto)
include_directories(${LINEAIRDB_ROOT}/include)

# Generate protobuf files
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../proto")
set(PROTO_FILE "${PROTO_DIR}/lineairdb.proto")
set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/lineairdb.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/lineairdb.pb.h")

# Custom command to generate protobuf files
add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR} --proto_path=${PROTO_DIR} ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating protobuf files"
)

# Add LineairDB as subdirectory
add_subdirectory(${LINEAIRDB_ROOT} ${CMAKE_CURRENT_BINARY_DIR}/LineairDB)

# Source files
set(SOURCES
    # Core
    main.cc
    lineairdb_server.cc
    lineairdb_server.hh
    
    # Network layer
    network/tcp_server.cc
    network/tcp_server.hh
    network/message_handler.cc
    network/message_handler.hh
    
    # RPC layer
    rpc/lineairdb_rpc.cc
    rpc/lineairdb_rpc.hh
    rpc/predicate_evaluator.cc
    rpc/predicate_evaluator.hh
    
    # Storage layer
    storage/database_manager.cc
    storage/database_manager.hh
    storage/transaction_manager.cc
    storage/transaction_manager.hh

    # Protocol
    protocol/message.hh
    
    # Protobuf generated files
    ${PROTO_SRCS}
)

# Create executable
add_executable(lineairdb-server ${SOURCES})

# Link libraries
target_link_libraries(lineairdb-server
    lineairdb
    ${Protobuf_LIBRARIES}
    pthread
)

# Additional include directories for generated files
target_include_directories(lineairdb-server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# Compiler flags to suppress warnings
target_compile_options(lineairdb-server PRIVATE -O3 -Wno-error -Wno-unused-parameter)

# ----------------------------------------------------------------------------
# bench_client: LDB 直撃ベンチツール。MySQL をバイパスして lineairdb-server に
# 直接 TCP+protobuf で RPC を叩く。LDB 最適化の local 回帰検知用。
# 共有リソース: ${PROTO_SRCS}, protocol/message.hh
# ----------------------------------------------------------------------------
set(BENCH_CLIENT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../tools/bench_client")
set(BENCH_CLIENT_SOURCES
    ${BENCH_CLIENT_DIR}/main.cc
    ${BENCH_CLIENT_DIR}/wire.cc
    ${BENCH_CLIENT_DIR}/client.cc
    ${BENCH_CLIENT_DIR}/key_encoder.cc
    ${BENCH_CLIENT_DIR}/workload_tpcc.cc
    ${BENCH_CLIENT_DIR}/stats.cc
    ${PROTO_SRCS}
)

add_executable(bench_client ${BENCH_CLIENT_SOURCES})

target_link_libraries(bench_client
    ${Protobuf_LIBRARIES}
    pthread
)

target_include_directories(bench_client PRIVATE
    ${BENCH_CLIENT_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}       # for protocol/message.hh
    ${CMAKE_CURRENT_BINARY_DIR}       # for lineairdb.pb.h
)

target_compile_options(bench_client PRIVATE -O3 -Wno-error -Wno-unused-parameter)
