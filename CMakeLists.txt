cmake_minimum_required(VERSION 3.20)

project(stream_gzip VERSION 1.0.0 LANGUAGES CXX)

find_package(Boost CONFIG REQUIRED COMPONENTS asio iostreams)
find_package(ZLIB REQUIRED)

add_executable(stream_gzip
    src/main.cpp
)

target_compile_features(stream_gzip PRIVATE cxx_std_17)
target_link_libraries(stream_gzip PRIVATE
    Boost::asio
    Boost::iostreams
    ZLIB::ZLIB
)

if(WIN32)
    target_compile_definitions(stream_gzip PRIVATE _WIN32_WINNT=0x0601)
endif()

if(MSVC)
    target_compile_options(stream_gzip PRIVATE /W4 /permissive-)
else()
    target_compile_options(stream_gzip PRIVATE -Wall -Wextra -Wpedantic)
endif()

include(CTest)
if(BUILD_TESTING)
    find_package(Python3 COMPONENTS Interpreter)
    if(Python3_Interpreter_FOUND)
        add_test(
            NAME stream_gzip_integration
            COMMAND "${Python3_EXECUTABLE}"
                    "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_test.py"
                    "$<TARGET_FILE:stream_gzip>"
        )
        set_tests_properties(stream_gzip_integration PROPERTIES TIMEOUT 20)
    endif()
endif()
