cmake_minimum_required(VERSION 3.0)
project(F_CSM306_Lab02_Starter)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(F_CSM306_Lab02_Starter
        itasksys.h
        main.cpp
        tasksys.cpp
        tasksys.h)
