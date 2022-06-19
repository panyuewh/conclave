cmake_minimum_required(VERSION 3.16)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Debug)
project(MotionTemplate)

add_executable(mpcexec workflow.cxx controller.cxx)
target_include_directories(mpcexec PUBLIC
        "/usr/include/motioncore"
        "/usr/local/include/motioncore")
target_link_libraries(mpcexec PUBLIC
        motion
        boost_program_options
        boost_context
        boost_fiber
        boost_log_setup
        boost_log
        boost_filesystem
        boost_system
        boost_thread
        boost_json
        pthread
        gomp
        crypto
        fmt)
