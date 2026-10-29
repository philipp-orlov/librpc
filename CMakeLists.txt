# librpc: RPC over WebSocket on top of libhttp and libbufs, with a protocol compiler (rpcc) that writes
# header-only C++ server and client stubs and TypeScript clients from a schema with rpc_service blocks.
# See README.md.
#
#   add_subdirectory(librpc)
#   rpc_generate(my_target SCHEMAS api.bufs [TS] [SKELETONS] [INCLUDE_DIRS dir...])
#   target_link_libraries(my_target PRIVATE rpc::rpc)

cmake_minimum_required(VERSION 3.16)
project(librpc VERSION 0.1.0 LANGUAGES CXX)

if(NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(FATAL_ERROR "librpc requires a 64-bit target")
endif()

option(RPC_BUILD_TESTS "Build the test suite" ON)
option(RPC_BUILD_EXAMPLES "Build the examples" ON)
option(RPC_BUILD_BENCHMARKS "Build the benchmark (bench/)" OFF)
option(RPC_WITH_TLS "wss:// (needs OpenSSL; libhttp is built with HTTP_WITH_SSL)" OFF)

set(RPC_BUFS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../libbufs" CACHE PATH "Source directory of libbufs")
set(RPC_HTTP_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../libhttp" CACHE PATH "Source directory of libhttp")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    enable_testing()
endif()

find_package(Threads REQUIRED)

# --- siblings -----------------------------------------------------------------------------------------------
if(NOT TARGET bufs::bufs)
    if(NOT EXISTS "${RPC_BUFS_DIR}/CMakeLists.txt")
        message(FATAL_ERROR "libbufs not found. Point -DRPC_BUFS_DIR at it.")
    endif()
    set(BUFS_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(BUFS_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    add_subdirectory("${RPC_BUFS_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/bufs")
endif()
if(NOT TARGET http)
    if(NOT EXISTS "${RPC_HTTP_DIR}/CMakeLists.txt")
        message(FATAL_ERROR "libhttp not found. Point -DRPC_HTTP_DIR at it.")
    endif()
    set(HTTP_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(HTTP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(HTTP_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    if(RPC_WITH_TLS)
        set(HTTP_WITH_SSL ON CACHE BOOL "" FORCE)
    endif()
    add_subdirectory("${RPC_HTTP_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/http")
endif()

# --- the protocol compiler ----------------------------------------------------------------------------------
# rpcc is bufsc with services: it links bufsc's parser and C++ generator (libbufs/bufsc) rather than copying them.
set(RPCC_EXECUTABLE "" CACHE FILEPATH "A host rpcc to use when cross-compiling")
add_executable(rpcc
    rpcc/main.cpp rpcc/common.cpp rpcc/gen_cpp.cpp rpcc/gen_ts.cpp rpcc/gen_cs.cpp rpcc/gen_py.cpp
    ${RPC_BUFS_DIR}/bufsc/parser.cpp ${RPC_BUFS_DIR}/bufsc/check.cpp ${RPC_BUFS_DIR}/bufsc/generate_cpp.cpp
    ${RPC_BUFS_DIR}/bufsc/generators.cpp)
target_include_directories(rpcc PRIVATE rpcc ${RPC_BUFS_DIR}/bufsc)
target_compile_options(rpcc PRIVATE $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall -Wextra>)
if(NOT CMAKE_CROSSCOMPILING)
    add_executable(rpc::rpcc ALIAS rpcc)
endif()

include(cmake/RpcGenerate.cmake)

# --- the runtime --------------------------------------------------------------------------------------------
set(RPC_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
add_library(rpc STATIC src/status.cpp src/server.cpp src/client.cpp)
add_library(rpc::rpc ALIAS rpc)
target_include_directories(rpc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${RPC_GENERATED_DIR}>)
target_link_libraries(rpc PUBLIC http bufs::bufs Threads::Threads)
target_compile_options(rpc PRIVATE $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall -Wextra>)
# The envelope of the wire protocol: rpc/wire/rpc.bufs.hpp.
bufs_generate(rpc SCHEMAS ${CMAKE_CURRENT_SOURCE_DIR}/include/rpc/wire/rpc.bufs
              OUTPUT_DIR ${RPC_GENERATED_DIR}/rpc/wire)
target_link_libraries(rpc PUBLIC bufs::bufs)

if(RPC_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(RPC_BUILD_TESTS)
    add_subdirectory(tests)
endif()

if(RPC_BUILD_BENCHMARKS)
    add_subdirectory(bench)
endif()
