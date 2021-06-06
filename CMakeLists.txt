cmake_minimum_required(VERSION 3.16.3)
project(lab5)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -pthread")
add_executable(lab5 main.cpp jobs.h Circular.h Coordinator.h)