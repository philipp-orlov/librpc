add_executable(rpc_bench bench_rpc.cpp)
rpc_generate(rpc_bench SCHEMAS ${CMAKE_CURRENT_SOURCE_DIR}/../examples/calc.bufs)
target_include_directories(rpc_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../examples)
set_target_properties(rpc_bench PROPERTIES OUTPUT_NAME bench_rpc)
