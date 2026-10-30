add_executable(rpc_example_calc_server calc_server.cpp)
rpc_generate(rpc_example_calc_server SCHEMAS ${CMAKE_CURRENT_SOURCE_DIR}/calc.bufs)
target_include_directories(rpc_example_calc_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(rpc_example_calc_server PROPERTIES OUTPUT_NAME calc_server)

add_executable(rpc_example_calc_client calc_client.cpp)
rpc_generate(rpc_example_calc_client SCHEMAS ${CMAKE_CURRENT_SOURCE_DIR}/calc.bufs)
set_target_properties(rpc_example_calc_client PROPERTIES OUTPUT_NAME calc_client)

# The TypeScript examples (examples/ts) are built with:  node examples/ts/build.mjs <rpcc>

# Every language's calc client against calc_server, checked against expected_output.txt (languages without tools are skipped).
find_program(RPC_PYTHON_EXAMPLES NAMES python3 python)
if(RPC_PYTHON_EXAMPLES AND RPC_BUILD_TESTS)
    find_program(RPC_EXAMPLES_NODE node)
    find_program(RPC_EXAMPLES_DOTNET dotnet HINTS $ENV{DOTNET_ROOT} $ENV{HOME}/.dotnet)
    find_program(RPC_EXAMPLES_ESBUILD esbuild HINTS ${CMAKE_CURRENT_SOURCE_DIR}/../ts/node_modules/.bin NO_DEFAULT_PATH)
    set(example_args --server $<TARGET_FILE:rpc_example_calc_server> --rpcc $<TARGET_FILE:rpcc> --cpp $<TARGET_FILE:rpc_example_calc_client>
                    --python ${RPC_PYTHON_EXAMPLES} --work ${CMAKE_CURRENT_BINARY_DIR}/run)
    if(RPC_EXAMPLES_NODE AND RPC_EXAMPLES_ESBUILD)
        list(APPEND example_args --node ${RPC_EXAMPLES_NODE} --esbuild ${RPC_EXAMPLES_ESBUILD})
    endif()
    if(RPC_EXAMPLES_DOTNET)
        list(APPEND example_args --dotnet ${RPC_EXAMPLES_DOTNET})
    endif()
    add_test(NAME rpc_examples COMMAND ${RPC_PYTHON_EXAMPLES} ${CMAKE_CURRENT_SOURCE_DIR}/run_examples.py ${example_args})
    set_tests_properties(rpc_examples PROPERTIES TIMEOUT 300)
endif()
