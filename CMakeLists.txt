cmake_minimum_required(VERSION 3.10)
project(SMC)

set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_FLAGS_DEBUG "-Wall -Wextra")
#set(CMAKE_CXX_FLAGS_RELEASE "-O3 -s -march=native")

set(CMAKE_CXX_FLAGS_DEBUG  "-O3 -s -march=native")

find_package(Threads)

#Find BOOST
set(BOOST_ROOT "/usr/local")
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.71.0 COMPONENTS system thread random regex date_time filesystem REQUIRED)
include_directories(${Boost_INCLUDE_DIR})

#Main Library
add_library(shared_memory STATIC
        SMHashTable.cpp SMHashTable.h)

#Google Test
#mkdir libs && cd libs && git clone https://github.com/google/googletest.git
add_subdirectory("libs/googletest")
include_directories("libs/googletest/googletest/include")

#Google test
add_executable(run_gtest
        tests/TestUtils.cpp
        tests/SMHashTable_test.cpp
        tests/HashFunctions_test.cpp)

target_link_libraries(run_gtest PRIVATE
        ${Boost_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
        shared_memory
        rt
        gtest
        gtest_main
        )