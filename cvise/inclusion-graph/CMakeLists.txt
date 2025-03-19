cmake_minimum_required(VERSION 3.14)
project(inclusion-graph)

find_package(LLVM REQUIRED CONFIG NO_CMAKE_BUILDS_PATH)
find_package(Clang REQUIRED CONFIG NO_CMAKE_BUILDS_PATH)
message(STATUS "Using clang: ${LLVM_VERSION_MAJOR}")
set(LLVM_CMAKE_MODULE_PATH "${CLANG_INCLUDE_DIRS}/../lib/cmake")
list(APPEND CMAKE_MODULE_PATH "${LLVM_CMAKE_MODULE_PATH}/clang"
     "${LLVM_CMAKE_MODULE_PATH}/llvm")
include(AddLLVM)
include(AddClang)

include_directories(${LLVM_INCLUDE_DIRS})
include_directories(${CLANG_INCLUDE_DIRS})

set(LLVM_LINK_COMPONENTS
  FrontendOpenMP
  Support
  )

add_clang_executable(inclusion-graph
  main.cc
  )

clang_target_link_libraries(inclusion-graph
  PRIVATE
  clangAST
  clangASTMatchers
  clangBasic
  clangFrontend
  clangTooling
  clangToolingRefactoring
  )
