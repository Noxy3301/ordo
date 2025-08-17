cmake_minimum_required(VERSION 3.16)
project(lineairdb-service)

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
    lineairdb_server.cc
    lineairdb_server.hh
    ${PROTO_SRCS}
)

# Create executable
add_executable(lineairdb-service ${SOURCES})

# Link libraries
target_link_libraries(lineairdb-service
    lineairdb
    ${Protobuf_LIBRARIES}
    pthread
)

# Additional include directories for generated files
target_include_directories(lineairdb-service PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# Compiler flags to suppress warnings
target_compile_options(lineairdb-service PRIVATE -Wno-error -Wno-unused-parameter)