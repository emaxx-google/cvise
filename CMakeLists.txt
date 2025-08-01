## -*- mode: CMake -*-
##
## Copyright (c) 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019 The University of Utah
## All rights reserved.
##
## This file is distributed under the University of Illinois Open Source
## License.  See the file COPYING for details.

###############################################################################

cmake_minimum_required(VERSION 3.14)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

include(CheckIncludeFile)
include(CheckCXXCompilerFlag)
include(GetGitRevisionDescription)

project(cvise)

if(CMAKE_SOURCE_DIR STREQUAL PROJECT_BINARY_DIR)
  message(FATAL_ERROR "CMAKE_SOURCE_DIR should be different from PROJECT_BINARY_DIR")
endif()

##############################################################################

# Locate LLVM and check its version.  Do this here because we need the LLVM
# package definitions in the "CMakeLists.txt" files for multiple subdirs.
#
find_package(LLVM REQUIRED CONFIG NO_CMAKE_BUILDS_PATH)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in ${LLVM_DIR}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "9.0")
  message(FATAL_ERROR "C-Vise requires LLVM 9.0 or later")
endif()
#
# Do this here, too, just to keep it with the LLVM check above.
#
find_package(Clang REQUIRED CONFIG NO_CMAKE_BUILDS_PATH)
message(STATUS "Using ClangConfig.cmake in ${Clang_DIR}")

# Locate Python and check its version.
#
find_package(Python3)

# Locate pytest
execute_process(COMMAND ${Python3_EXECUTABLE} -m pytest --version
  OUTPUT_VARIABLE PYTEST_output
  ERROR_VARIABLE  PYTEST_error
  RESULT_VARIABLE PYTEST_result)
if(NOT ${PYTEST_result} EQUAL 0)
  message(WARNING "Pytest package not available: ${PYTEST_error}")
endif()

# Locate Pebble
execute_process(COMMAND ${Python3_EXECUTABLE} -c "import pebble"
  OUTPUT_VARIABLE PEBBLE_output
  ERROR_VARIABLE  PEBBLE_error
  RESULT_VARIABLE PEBBLE_result)
if(NOT ${PEBBLE_result} EQUAL 0)
  message(WARNING "Pebble package not available: ${PEBBLE_error}")
endif()

# Locate psutil
execute_process(COMMAND ${Python3_EXECUTABLE} -c "import psutil"
  OUTPUT_VARIABLE PSUTIL_output
  ERROR_VARIABLE  PSUTIL_error
  RESULT_VARIABLE PSUTIL_result)
if(NOT ${PSUTIL_result} EQUAL 0)
  message(WARNING "psutil package not available: ${PSUTIL_error}")
endif()

# Locate flex.  Do this here because we need the package definitions in the
# "CMakeLists.txt" files for multiple subdirs.
#
find_package(FLEX REQUIRED)

find_program(UNIFDEF unifdef)
if(NOT UNIFDEF)
  message(WARNING "unifdef not available")
endif()

###############################################################################

# Determine the short git hash for the source tree.
#
## METHOD 1: The source tree is the result of `git archive'.
# `git archive' inserts the abbreviated hash of the archive's commit into this
# file.  (See the `.gitattributes' file.)
#
set(GIT_HASH "$Format:%h$")
if(GIT_HASH MATCHES "^\\$")
  ## METHOD 2: The source tree is a git repository.
  get_git_head_revision(GIT_REFSPEC GIT_HASH)
  if(NOT GIT_HASH STREQUAL "GITDIR-NOTFOUND")
    # Trim to the short hash.
    string(SUBSTRING "${GIT_HASH}" 0 7 GIT_HASH)
  else()
    ## METHOD 3: Give up.
    set(GIT_HASH "unknown")
  endif()
endif()

###############################################################################

# Generate the "config.h" file.
#
set(ENABLE_TRANS_ASSERT ON CACHE BOOL
  "Use assert() in clang_delta transformations.")

check_include_file("dlfcn.h"		HAVE_DLFCN_H)
check_include_file("inttypes.h"		HAVE_INTTYPES_H)
check_include_file("memory.h"		HAVE_MEMORY_H)
check_include_file("stdint.h"		HAVE_STDINT_H)
check_include_file("stdlib.h"		HAVE_STDLIB_H)
check_include_file("strings.h"		HAVE_STRINGS_H)
check_include_file("string.h"		HAVE_STRING_H)
check_include_file("sys/stat.h"		HAVE_SYS_STAT_H)
check_include_file("sys/types.h"	HAVE_SYS_TYPES_H)
check_include_file("unistd.h"		HAVE_UNISTD_H)

set(cvise_PACKAGE                   "cvise")
set(cvise_PACKAGE_BUGREPORT         "https://github.com/marxin/cvise/issues")
set(cvise_PACKAGE_NAME              "cvise")
set(cvise_PACKAGE_STRING            "cvise 2.11.0")
set(cvise_PACKAGE_TARNAME           "cvise")
set(cvise_PACKAGE_URL               "https://github.com/marxin/cvise/")
set(cvise_PACKAGE_VERSION           "2.11.0")
set(cvise_VERSION                   "2.11.0")
set(cvise_LLVM_VERSION              "${LLVM_PACKAGE_VERSION}")
set(cvise_SCRIPT_TO_PACKAGE_PATH    "../..")
set(cvise_SHARE_DIR_SUFFIX          "cvise")

configure_file("cmake_config.h.in" "${PROJECT_BINARY_DIR}/config.h")
add_definitions("-DHAVE_CONFIG_H")

###############################################################################

# Use option `-fvisibility-inlines-hidden` if the C++ compiler supports it.
# See LLVM file `share/llvm/cmake/HandleLLVMOptions.cmake`.
#
check_cxx_compiler_flag(
  "-fvisibility-inlines-hidden"
  SUPPORTS_FVISIBILITY_INLINES_HIDDEN_FLAG
)

check_cxx_compiler_flag(
  "-Wmismatched-new-delete"
  SUPPORTS_WMISMATCHED_NEW_DELETE
)

check_cxx_compiler_flag(
  "-Wmaybe-uninitialized"
  SUPPORTS_MAYBE_UNINITIALIZED
)

check_cxx_compiler_flag(
  "-Wclass-memaccess"
  SUPPORTS_CLASS_MEMACCESS
)

check_cxx_compiler_flag(
  "-Wnon-template-friend"
  SUPPORTS_NON_TEMPLATE_FRIEND
)

check_cxx_compiler_flag(
  "-Wdeprecated-declarations"
  SUPPORTS_DEPRECATED_DECLARATIONS
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
    OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # XXX figure out how to get "-std=c++17 -fno-rtti" from LLVM.  That's how we
  # get those options in the Automake path...
  set(COMMON_FLAGS "-Wall -Wextra -Werror -pedantic -Wno-unused-parameter")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS} -std=c++17 -fno-rtti -fno-strict-aliasing")
  if(SUPPORTS_FVISIBILITY_INLINES_HIDDEN_FLAG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility-inlines-hidden")
  endif()
  if(SUPPORTS_WMISMATCHED_NEW_DELETE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=mismatched-new-delete")
  endif()
  if(SUPPORTS_MAYBE_UNINITIALIZED)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=maybe-uninitialized")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=maybe-uninitialized")
  endif()
  if(SUPPORTS_CLASS_MEMACCESS)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=class-memaccess")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=class-memaccess")
  endif()
  if(SUPPORTS_NON_TEMPLATE_FRIEND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=non-template-friend")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=non-template-friend")
  endif()
  if(SUPPORTS_DEPRECATED_DECLARATIONS)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=deprecated-declarations")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=deprecated-declarations")
  endif()

  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /std:c++17 /GR-")
endif()

###############################################################################

# Enable tests
enable_testing()
add_test(NAME all COMMAND ${Python3_EXECUTABLE} -m pytest)

###############################################################################

# Needs to be sourced after we have a compiler tested
include(GNUInstallDirs)

add_subdirectory(clang_delta)
add_subdirectory(clex)
add_subdirectory(cvise)
add_subdirectory(delta)
add_subdirectory(tree-sitter)

# Copy top-level cvise script
configure_file(
  "${PROJECT_SOURCE_DIR}/cvise-cli.py"
  "${PROJECT_BINARY_DIR}/cvise-cli.py"
  @ONLY
)

configure_file(
  "${PROJECT_SOURCE_DIR}/cvise-delta.py"
  "${PROJECT_BINARY_DIR}/cvise-delta.py"
  @ONLY
)

configure_file(
  "${PROJECT_SOURCE_DIR}/cvise/utils/externalprograms.py"
  "${PROJECT_BINARY_DIR}/cvise/utils/externalprograms.py"
  @ONLY
)

configure_file(
  "${PROJECT_SOURCE_DIR}/tests/test_cvise.py"
  "${PROJECT_BINARY_DIR}/tests/test_cvise.py"
  COPYONLY
)

configure_file(
  "${PROJECT_SOURCE_DIR}/tests/sources/blocksort-part.c"
  "${PROJECT_BINARY_DIR}/tests/sources/blocksort-part.c"
  COPYONLY
)

configure_file(
  "${PROJECT_SOURCE_DIR}/setup.cfg"
  "${PROJECT_BINARY_DIR}/setup.cfg"
  COPYONLY
)

install(PROGRAMS "${PROJECT_BINARY_DIR}/cvise-cli.py"
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  RENAME "cvise"
)

install(PROGRAMS "${PROJECT_BINARY_DIR}/cvise-delta.py"
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  RENAME "cvise-delta"
)

###############################################################################

## End of file.
