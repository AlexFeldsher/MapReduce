cmake_minimum_required(VERSION 3.6.1)
project(MapReduce2)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g -std=c++11 -pthread")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "/cs/usr/feld/safe/OS/MapReduce2/cmake-build-debug")

set(SOURCE_FILES Search.cpp MapReduceClient.h MapReduceFramework.h debug.h MapReduceFramework.cpp Search.h)
add_executable(MapReduce2 ${SOURCE_FILES})