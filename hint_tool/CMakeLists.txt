cmake_minimum_required(VERSION 3.14)

project(hint_tool)

find_program(CARGO cargo)

set(ARGS --target-dir "${CMAKE_BINARY_DIR}/hint_tool")
if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(ARGS ${ARGS} --release)
    set(ARTIFACT_DIR release)
else()
    set(ARTIFACT_DIR debug)
endif()

add_custom_target(
    hint_tool ALL
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    COMMAND ${CARGO} build ${ARGS})

add_custom_target(hint_tool_copy ALL
    DEPENDS hint_tool)
add_custom_command(
    TARGET hint_tool_copy POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_BINARY_DIR}/hint_tool/${ARTIFACT_DIR}/hint_tool"
        "${CMAKE_BINARY_DIR}/hint_tool/hint_tool")

install(PROGRAMS "${CMAKE_BINARY_DIR}/hint_tool/hint_tool"
    DESTINATION "${CMAKE_INSTALL_LIBEXECDIR}/${cvise_PACKAGE}"
    )
