set(examples ${CMAKE_CURRENT_SOURCE_DIR}/../examples)

add_executable(rpc_test_rpc test_rpc.cpp)
rpc_generate(rpc_test_rpc SCHEMAS ${examples}/calc.bufs ${CMAKE_CURRENT_SOURCE_DIR}/kitchen.bufs)
target_include_directories(rpc_test_rpc PRIVATE ${examples} ${CMAKE_CURRENT_SOURCE_DIR})
add_test(NAME rpc_integration COMMAND rpc_test_rpc $<TARGET_FILE:rpc_interop_server>)
set_tests_properties(rpc_integration PROPERTIES TIMEOUT 180)

# The server the TypeScript tests talk to, and the tests themselves (ts/tests).
add_executable(rpc_interop_server interop_server.cpp)
rpc_generate(rpc_interop_server SCHEMAS ${examples}/calc.bufs ${CMAKE_CURRENT_SOURCE_DIR}/kitchen.bufs)
target_include_directories(rpc_interop_server PRIVATE ${examples} ${CMAKE_CURRENT_SOURCE_DIR})

find_program(RPC_NODE node)
if(RPC_NODE AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/../ts/node_modules/.bin/esbuild)
    add_test(NAME rpc_typescript
             COMMAND ${RPC_NODE} ${CMAKE_CURRENT_SOURCE_DIR}/../ts/tests/run.mjs
                     $<TARGET_FILE:rpc_interop_server> $<TARGET_FILE:rpcc>
                     ${CMAKE_CURRENT_BINARY_DIR}/ts)
    set_tests_properties(rpc_typescript PROPERTIES TIMEOUT 180)
else()
    message(STATUS "librpc: TypeScript tests skipped (node, or `npm install` in ts/, is missing)")
endif()

# The generated TypeScript wire codec checked in at ts/src/rpc.bufs.ts must match rpc.bufs.
add_test(NAME rpc_ts_wire_current
         COMMAND ${CMAKE_COMMAND} -DRPCC=$<TARGET_FILE:rpcc> -DSCHEMA=${CMAKE_CURRENT_SOURCE_DIR}/../include/rpc/wire/rpc.bufs
                 -DCHECKED_IN=${CMAKE_CURRENT_SOURCE_DIR}/../ts/src/rpc.bufs.ts -DWORK=${CMAKE_CURRENT_BINARY_DIR}/wire_ts
                 -P ${CMAKE_CURRENT_SOURCE_DIR}/wire_current.cmake)

# rpcc rejects what it cannot serve.
add_test(NAME rpc_rcc_rejects_bidi
         COMMAND rpcc -o ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/bad/bidi.bufs)
set_tests_properties(rpc_rcc_rejects_bidi PROPERTIES
    PASS_REGULAR_EXPRESSION "rpcc: .*bidi.bufs:[0-9]+: .*streaming: \"bidi\" needs the signature form")
add_test(NAME rpc_rcc_rejects_unknown_type
         COMMAND rpcc -o ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/bad/not_a_table.bufs)
set_tests_properties(rpc_rcc_rejects_unknown_type PROPERTIES
    PASS_REGULAR_EXPRESSION "rpcc: .*not_a_table.bufs:[0-9]+: .*is not a table")

# A warm server and client allocate nothing per call.
add_executable(rpc_test_alloc test_alloc.cpp)
rpc_generate(rpc_test_alloc SCHEMAS ${examples}/calc.bufs)
target_include_directories(rpc_test_alloc PRIVATE ${examples})
add_test(NAME rpc_allocations COMMAND rpc_test_alloc)
set_tests_properties(rpc_allocations PROPERTIES TIMEOUT 120)

# The C# client (cs/) against the same server.
find_program(RPC_DOTNET dotnet HINTS $ENV{DOTNET_ROOT} $ENV{HOME}/.dotnet)
if(RPC_DOTNET)
    add_test(NAME rpc_csharp
             COMMAND ${CMAKE_COMMAND} -DRPCC=$<TARGET_FILE:rpcc> -DSERVER=$<TARGET_FILE:rpc_interop_server>
                     -DDOTNET=${RPC_DOTNET} -DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/.. -P ${CMAKE_CURRENT_SOURCE_DIR}/csharp.cmake)
    set_tests_properties(rpc_csharp PROPERTIES TIMEOUT 300 ENVIRONMENT "DOTNET_ROOT=${RPC_DOTNET}/..")
else()
    message(STATUS "librpc: C# tests skipped (dotnet not found)")
endif()

# The checked-in wire codecs of the other runtimes match rpc.bufs too.
add_test(NAME rpc_py_wire_current
         COMMAND ${CMAKE_COMMAND} -DRPCC=$<TARGET_FILE:rpcc> -DSCHEMA=${CMAKE_CURRENT_SOURCE_DIR}/../include/rpc/wire/rpc.bufs
                 -DCHECKED_IN=${CMAKE_CURRENT_SOURCE_DIR}/../python/librpc/rpc_bufs.py -DWORK=${CMAKE_CURRENT_BINARY_DIR}/wire_py
                 "-DFLAGS=--py" -DPRODUCED=rpc_bufs.py -P ${CMAKE_CURRENT_SOURCE_DIR}/wire_current.cmake)
add_test(NAME rpc_cs_wire_current
         COMMAND ${CMAKE_COMMAND} -DRPCC=$<TARGET_FILE:rpcc> -DSCHEMA=${CMAKE_CURRENT_SOURCE_DIR}/../include/rpc/wire/rpc.bufs
                 -DCHECKED_IN=${CMAKE_CURRENT_SOURCE_DIR}/../cs/Librpc/rpc.bufs.cs -DWORK=${CMAKE_CURRENT_BINARY_DIR}/wire_cs
                 "-DFLAGS=--cs" -DPRODUCED=rpc.bufs.cs -P ${CMAKE_CURRENT_SOURCE_DIR}/wire_current.cmake)

# The Python client against the same server.
find_program(RPC_PYTHON NAMES python3 python)
if(RPC_PYTHON)
    add_test(NAME rpc_python
             COMMAND ${CMAKE_COMMAND} -DRPCC=$<TARGET_FILE:rpcc> -DSERVER=$<TARGET_FILE:rpc_interop_server> -DPYTHON=${RPC_PYTHON}
                     -DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/.. -DWORK=${CMAKE_CURRENT_BINARY_DIR}/python -P ${CMAKE_CURRENT_SOURCE_DIR}/python.cmake)
    set_tests_properties(rpc_python PROPERTIES TIMEOUT 300)
else()
    message(STATUS "librpc: Python tests skipped (python3 not found)")
endif()
