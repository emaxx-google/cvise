cmake_minimum_required(VERSION 3.14)
project(calc-include-depth)

# find_package(LLVM) is done by the topmost "CMakeLists.txt" file.
# find_package(Clang) is done by the topmost "CMakeLists.txt" file.
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

add_clang_executable(calc-include-depth
  main.cc
  )

clang_target_link_libraries(calc-include-depth
  PRIVATE
  clangAST
  clangASTMatchers
  clangBasic
  clangFrontend
  clangTooling
  clangToolingRefactoring
  )

install(TARGETS calc-include-depth
  DESTINATION "${CMAKE_INSTALL_LIBEXECDIR}/${cvise_PACKAGE}/"
  )
