cmake_minimum_required(VERSION 3.14)

project(tree-sitter-delta)

set(CMAKE_CXX_STANDARD 20)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(../tree-sitter/lib/src/ ../tree-sitter/lib/include)

add_executable(tree-sitter-delta
    main.cc
    ../tree-sitter/lib/src/lib.c
    ../tree-sitter-cpp/src/parser.c
    ../tree-sitter-cpp/src/scanner.c)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
set_source_files_properties(../tree-sitter-cpp/src/scanner.c PROPERTIES COMPILE_FLAGS "-Wno-strict-prototypes")
endif()

install(TARGETS tree-sitter-delta
  DESTINATION "${CMAKE_INSTALL_LIBEXECDIR}/${cvise_PACKAGE}/"
  )
