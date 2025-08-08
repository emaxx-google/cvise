#include "Parsers.h"
#include "ReplaceFunctionDefWithDecl.h"

#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

#include <tree_sitter/api.h>

namespace {

class FuncRemover {
 public:
  FuncRemover()
    : Query(nullptr, ts_query_delete) {
    constexpr char QueryStr[] = R"(
      [
        (
          function_definition
        ) @capture0
        (
          template_declaration
          (function_definition)
        ) @capture1
      ]
    )";
    uint32_t ErrorOffset = 0;
    TSQueryError ErrorType = TSQueryErrorNone;
    Query.reset(ts_query_new(tree_sitter_cpp(), QueryStr, std::size(QueryStr) - 1,
                            &ErrorOffset, &ErrorType));
    if (!Query) {
      std::cerr << "Failed to init Tree-sitter query: error " << ErrorType
                << " offset " << ErrorOffset << "\n";
      std::exit(-1);
    }
    // The (empty) hint vocabulary.
    std::cout << "[]\n";
  }

  void processFile(TSTree &Tree) {
    std::unique_ptr<TSQueryCursor, decltype(&ts_query_cursor_delete)> Cursor(
        ts_query_cursor_new(), ts_query_cursor_delete);
    ts_query_cursor_exec(Cursor.get(), Query.get(), ts_tree_root_node(&Tree));

    TSQueryMatch Match;
    uint32_t LastEnd = 0;
    while (ts_query_cursor_next_match(Cursor.get(), &Match)) {
      TSNode Node = Match.captures[0].node;
      uint32_t StartByte = ts_node_start_byte(Node);
      uint32_t EndByte = ts_node_end_byte(Node);
      if (StartByte < LastEnd)
        continue;
      LastEnd = EndByte;
      std::cout << "{\"p\":[{\"l\":" << StartByte << ",\"r\":" << EndByte << "}]}\n";
    }
  }

 private:
  std::unique_ptr<TSQuery, decltype(&ts_query_delete)> Query;
};

class ClassRemover {
 public:
  ClassRemover()
    : Query(nullptr, ts_query_delete) {
    constexpr char QueryStr[] = R"(
      [
        (
          class_specifier
          body: (_)
        ) @capture0
        (
          struct_specifier
          body: (_)
        ) @capture1
      ]
    )";
    uint32_t ErrorOffset = 0;
    TSQueryError ErrorType = TSQueryErrorNone;
    Query.reset(ts_query_new(tree_sitter_cpp(), QueryStr, std::size(QueryStr) - 1,
                            &ErrorOffset, &ErrorType));
    if (!Query) {
      std::cerr << "Failed to init Tree-sitter query: error " << ErrorType
                << " offset " << ErrorOffset << "\n";
      std::exit(-1);
    }
    // The (empty) hint vocabulary.
    std::cout << "[]\n";
  }

  void processFile(TSTree &Tree) {
    std::unique_ptr<TSQueryCursor, decltype(&ts_query_cursor_delete)> Cursor(
        ts_query_cursor_new(), ts_query_cursor_delete);
    ts_query_cursor_exec(Cursor.get(), Query.get(), ts_tree_root_node(&Tree));

    TSQueryMatch Match;
    uint32_t LastEnd = 0;
    while (ts_query_cursor_next_match(Cursor.get(), &Match)) {
      TSNode Node = Match.captures[0].node;
      uint32_t StartByte = ts_node_start_byte(Node);
      uint32_t EndByte = ts_node_end_byte(Node);
      if (StartByte < LastEnd)
        continue;
      LastEnd = EndByte;
      std::cout << "{\"p\":[{\"l\":" << StartByte << ",\"r\":" << EndByte << "}]}\n";
    }
  }

 private:
  std::unique_ptr<TSQuery, decltype(&ts_query_delete)> Query;
};

class NamespaceRemover {
 public:
  NamespaceRemover()
    : Query(nullptr, ts_query_delete) {
    constexpr char QueryStr[] = R"(
      (
        namespace_definition
        body: (_)
      ) @capture0
    )";
    uint32_t ErrorOffset = 0;
    TSQueryError ErrorType = TSQueryErrorNone;
    Query.reset(ts_query_new(tree_sitter_cpp(), QueryStr, std::size(QueryStr) - 1,
                            &ErrorOffset, &ErrorType));
    if (!Query) {
      std::cerr << "Failed to init Tree-sitter query: error " << ErrorType
                << " offset " << ErrorOffset << "\n";
      std::exit(-1);
    }
    // The (empty) hint vocabulary.
    std::cout << "[]\n";
  }

  void processFile(TSTree &Tree) {
    std::unique_ptr<TSQueryCursor, decltype(&ts_query_cursor_delete)> Cursor(
        ts_query_cursor_new(), ts_query_cursor_delete);
    ts_query_cursor_exec(Cursor.get(), Query.get(), ts_tree_root_node(&Tree));

    TSQueryMatch Match;
    uint32_t LastEnd = 0;
    while (ts_query_cursor_next_match(Cursor.get(), &Match)) {
      TSNode Node = Match.captures[0].node;
      uint32_t StartByte = ts_node_start_byte(Node);
      uint32_t EndByte = ts_node_end_byte(Node);
      if (StartByte < LastEnd)
        continue;
      LastEnd = EndByte;
      std::cout << "{\"p\":[{\"l\":" << StartByte << ",\"r\":" << EndByte << "}]}\n";
    }
  }

 private:
  std::unique_ptr<TSQuery, decltype(&ts_query_delete)> Query;
};

}

static bool readFile(const std::string &Path, std::string &Contents) {
  std::error_code Error;
  auto Size = std::filesystem::file_size(Path, Error);
  if (Error) {
    std::cerr << "Failed to obtain size of file " << Path << " : "
              << Error.message() << "\n";
    return false;
  }
  Contents.resize(Size);
  std::ifstream File(Path, std::ios::binary);
  if (File.fail()) {
    std::cerr << "Failed to read file " << Path << "\n";
    return false;
  }
  File.read(Contents.data(), Size);
  auto ActuallyRead = File.gcount();
  Contents.resize(ActuallyRead);
  return true;
}

int main(int argc, char *argv[]) {
  std::ios::sync_with_stdio(false); // speed up C++ I/O streams

  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " transformation input/file/path\n"
              << "  transformation: currently only "
                 "\"replace-function-def-with-decl\"";
    return -1;
  }
  const std::string Transformation = argv[1];
  const std::string InputPath = argv[2];

  // Prepare the common parsing state.
  std::unique_ptr<TSParser, decltype(&ts_parser_delete)> Parser(
      ts_parser_new(), ts_parser_delete);
  ts_parser_set_language(Parser.get(), tree_sitter_cpp());

  // Parse the input.
  std::string Contents;
  if (!readFile(InputPath, Contents)) {
    // The error details are logged by the function.
    return 1;
  }
  std::unique_ptr<TSTree, decltype(&ts_tree_delete)> Tree(
      ts_parser_parse_string(Parser.get(), /*old_tree=*/nullptr,
                             Contents.c_str(), Contents.length()),
      ts_tree_delete);
  if (!Tree) {
    std::cerr << "Failed to parse " << InputPath << "\n";
    return 1;
  }

  // Run heuristics and emit hints.
  if (Transformation == "replace-function-def-with-decl") {
    FuncDefWithDeclReplacer().processFile(Contents, *Tree);
  } else if (Transformation == "remove-function") {
    FuncRemover().processFile(*Tree);
  } else if (Transformation == "remove-class") {
    ClassRemover().processFile(*Tree);
  } else if (Transformation == "remove-namespace") {
    NamespaceRemover().processFile(*Tree);
  } else {
    std::cerr << "Unknown transformation: " << Transformation << "\n";
    return 1;
  }
}
