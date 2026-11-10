cmake_minimum_required(VERSION 3.28)
project(Helix.Jobs LANGUAGES CXX)

if(NOT COMMAND helix_add_library)
    include("${CMAKE_CURRENT_SOURCE_DIR}/external/Helix.CMake/Helix.cmake")
endif()

helix_require(Core)
helix_require(Platform)
helix_require(Profiler)
helix_add_library(helix_jobs ALIAS Jobs PUBLIC_DEPS Helix::Core Helix::Platform Helix::Profiler)

helix_add_test_suite(helix_jobs_tests DEPS Helix::Jobs)
helix_add_benchmark_suite(helix_jobs_bench DEPS Helix::Jobs)

if(PROJECT_IS_TOP_LEVEL)
    helix_add_executable(helix_frame_demo SOURCES examples/frame_demo.cpp DEPS Helix::Jobs)
    if(HELIX_PLATFORM STREQUAL "web")
        set_target_properties(helix_frame_demo PROPERTIES SUFFIX ".html")
        target_link_options(helix_frame_demo PRIVATE -sENVIRONMENT=web --shell-file "${CMAKE_CURRENT_SOURCE_DIR}/examples/web_shell.html")
    endif()
endif()
