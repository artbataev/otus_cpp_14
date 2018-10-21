cmake_minimum_required(VERSION 3.8)

if ($ENV{TRAVIS_BUILD_NUMBER})
    set(PROJECT_VERSION 0.0.$ENV{TRAVIS_BUILD_NUMBER})
else ()
    set(PROJECT_VERSION 0.0.1)
endif ()

project(yamr VERSION ${PROJECT_VERSION} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

add_executable(yamr src/main.cpp)
