# rpc_generate(target SCHEMAS a.bufs [b.bufs ...] [INCLUDE_DIRS dir...] [OUTPUT_DIR dir]
#              [TS] [CS] [PY] [TS_RUNTIME module] [SKELETONS] [NO_LINK])
#
# Runs rpcc at build time. For each schema stem it writes, into OUTPUT_DIR (default
# ${CMAKE_CURRENT_BINARY_DIR}/rpc_generated/<target>, added to the target's include path):
#   <stem>.bufs.hpp and <stem>.rpc.hpp      the messages and the header-only stubs
#   <stem>.bufs.ts                          with TS: TypeScript messages and typed clients
#   <stem>.bufs.cs                          with CS: C# messages and typed async clients (cs/Librpc)
#   <stem>_bufs.py                          with PY: Python messages and typed asyncio clients (python/librpc)
#   <stem>.server.cpp, <stem>.client.cpp    with SKELETONS: programs to start from. They are not added to the target:
#                                           copy them into your tree, or name them in add_executable() yourself
#                                           (in the same directory, so CMake knows how to make them).
# and links the target against rpc::rpc (unless NO_LINK: the caller links it, e.g. as a build-tree dependency).
if(POLICY CMP0116)
    cmake_policy(SET CMP0116 NEW)
endif()

function(rpc_generate target)
    cmake_parse_arguments(ARG "TS;CS;PY;SKELETONS;NO_LINK" "OUTPUT_DIR;TS_RUNTIME" "SCHEMAS;INCLUDE_DIRS" ${ARGN})
    if(NOT ARG_OUTPUT_DIR)
        set(ARG_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/rpc_generated/${target}")
    endif()
    if(RPCC_EXECUTABLE)
        set(rpcc "${RPCC_EXECUTABLE}")
        set(rpcc_depends "")
    else()
        set(rpcc $<TARGET_FILE:rpcc>)
        set(rpcc_depends rpcc)
    endif()
    set(includes "")
    foreach(dir ${ARG_INCLUDE_DIRS})
        list(APPEND includes -I "${dir}")
    endforeach()
    set(flags "")
    if(ARG_TS)
        list(APPEND flags --ts)
    endif()
    if(ARG_CS)
        list(APPEND flags --cs)
    endif()
    if(ARG_PY)
        list(APPEND flags --py)
    endif()
    if(ARG_TS_RUNTIME)
        list(APPEND flags --ts-runtime "${ARG_TS_RUNTIME}")
    endif()
    if(ARG_SKELETONS)
        list(APPEND flags --skeleton)
    endif()
    set(outputs "")
    foreach(schema ${ARG_SCHEMAS})
        get_filename_component(path "${schema}" ABSOLUTE)
        get_filename_component(stem "${schema}" NAME_WE)
        set(products "${ARG_OUTPUT_DIR}/${stem}.bufs.hpp" "${ARG_OUTPUT_DIR}/${stem}.rpc.hpp")
        if(ARG_TS)
            list(APPEND products "${ARG_OUTPUT_DIR}/${stem}.bufs.ts")
        endif()
        if(ARG_CS)
            list(APPEND products "${ARG_OUTPUT_DIR}/${stem}.bufs.cs")
        endif()
        if(ARG_PY)
            list(APPEND products "${ARG_OUTPUT_DIR}/${stem}_bufs.py")
        endif()
        set(extras "")
        if(ARG_SKELETONS)
            set(extras "${ARG_OUTPUT_DIR}/${stem}.server.cpp" "${ARG_OUTPUT_DIR}/${stem}.client.cpp")
        endif()
        set(depfile "${ARG_OUTPUT_DIR}/${stem}.rpc.d")
        add_custom_command(
            OUTPUT ${products} ${extras}
            COMMAND ${CMAKE_COMMAND} -E make_directory "${ARG_OUTPUT_DIR}"
            COMMAND ${rpcc} -o "${ARG_OUTPUT_DIR}" ${includes} ${flags} --depfile "${depfile}" "${path}"
            DEPENDS "${path}" ${rpcc_depends}
            DEPFILE "${depfile}"
            COMMENT "rpcc ${stem}.bufs"
            VERBATIM)
        list(APPEND outputs ${products})
    endforeach()
    target_sources(${target} PRIVATE ${outputs})
    target_include_directories(${target} PRIVATE "${ARG_OUTPUT_DIR}")
    if(NOT ARG_NO_LINK)
        target_link_libraries(${target} PRIVATE rpc::rpc)
    endif()
endfunction()
