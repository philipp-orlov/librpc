# Generates the Python for the example schemas, then runs python/tests/test_interop.py against the interop server.
#   cmake -DRPCC=… -DSERVER=… -DPYTHON=… -DSOURCE=<librpc dir> -DWORK=<dir> -P python.cmake
file(REMOVE_RECURSE ${WORK})
file(MAKE_DIRECTORY ${WORK})
foreach(schema ${SOURCE}/examples/calc.bufs ${SOURCE}/tests/kitchen.bufs)
    execute_process(COMMAND ${RPCC} --no-cpp --py -o ${WORK} ${schema} RESULT_VARIABLE rc)
    if(rc)
        message(FATAL_ERROR "rpcc --py failed on ${schema}")
    endif()
endforeach()
execute_process(COMMAND ${PYTHON} -I ${SOURCE}/python/tests/test_interop.py ${SERVER} ${WORK} RESULT_VARIABLE rc)
if(rc)
    message(FATAL_ERROR "Python tests failed")
endif()
