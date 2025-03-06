#include <stdint.h>
#include <tree_sitter/api.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

extern "C" const TSLanguage *tree_sitter_cpp();

namespace {

void WalkFileOrDir(const std::filesystem::path &start_path,
                   std::vector<std::filesystem::path> &discovered_paths) {
  if (!std::filesystem::is_directory(start_path)) {
    discovered_paths.push_back(start_path);
    return;
  }
  for (const auto &dir_entry :
       std::filesystem::recursive_directory_iterator(start_path)) {
    if (dir_entry.is_directory() || dir_entry.is_symlink()) {
      continue;
    }
    if (dir_entry.path().extension() == ".txt" ||
        dir_entry.path().extension() == ".cppmap") {
      continue;
    }
    discovered_paths.push_back(dir_entry.path());
  }
}

void ReadFile(const std::filesystem::path &path, std::string &contents) {
  const auto size = std::filesystem::file_size(path);
  contents.resize(size);
  std::ifstream file(path, std::ios::binary);
  file.read(contents.data(), size);
  const auto actually_read = file.gcount();
  contents.resize(actually_read);
}

void WriteFile(const std::filesystem::path &path, const std::string &contents) {
  std::ofstream file(path, std::ios::binary | std::ios::trunc);
  file.write(contents.data(), contents.length());
}

struct Instance {
  uint32_t start_byte = 0;
  uint32_t end_byte = 0;
  bool needs_semicolon = false;
};

bool ParseFile(const std::string &contents, TSParser *parser, TSQuery *query,
               std::vector<Instance> &instances) {
  std::unique_ptr<TSTree, decltype(&ts_tree_delete)> tree(
      ts_parser_parse_string(parser, /*old_tree=*/nullptr, contents.c_str(),
                             contents.length()),
      ts_tree_delete);
  if (!tree) {
    return false;
  }

  std::unique_ptr<TSQueryCursor, decltype(&ts_query_cursor_delete)> cursor(
      ts_query_cursor_new(), ts_query_cursor_delete);
  ts_query_cursor_exec(cursor.get(), query, ts_tree_root_node(tree.get()));
  TSQueryMatch match;
  while (ts_query_cursor_next_match(cursor.get(), &match)) {
    std::vector<TSNode> captures(ts_query_capture_count(query));
    for (int i = 0; i < match.capture_count; ++i) {
      auto index = match.captures[i].index;
      captures[index] = match.captures[i].node;
    }
    const TSNode &missing_double_colon = captures[0];
    const TSNode &qual_id = captures[1];
    const TSNode &init_list = captures[2];
    const TSNode &body = captures[3];
    assert(!ts_node_is_null(body));
    const TSNode &func_def = captures[4];
    assert(!ts_node_is_null(func_def));

    // Walk up until the first "template <>" decl, if there's any.
    TSNode template_ancestor = func_def;
    for (;;) {
      TSNode parent = ts_node_parent(template_ancestor);
      if (ts_node_is_null(parent) || ts_node_type(parent) != std::string("template_declaration")) {
        break;
      }
      template_ancestor = parent;
    }

    Instance instance{
        .start_byte = ts_node_start_byte(body),
        .end_byte = ts_node_end_byte(body),
        .needs_semicolon = true,
    };
    if (!ts_node_is_null(qual_id) && ts_node_is_null(missing_double_colon)) {
      // An out-of-line declaration of a member has to be deleted completely.
      instance.start_byte = ts_node_start_byte(template_ancestor);
      instance.needs_semicolon = false;
    } else if (!ts_node_is_null(init_list)) {
      // In case of a constructor, initializer lists have to be deleted as well.
      instance.start_byte = ts_node_start_byte(init_list);
    }
    assert(instance.start_byte < instance.end_byte);

    // Delete overlapping segments: leave only the most detailed matches.
    auto inters = std::ranges::remove_if(instances, [&](const auto &other) {
      return std::max(other.start_byte, instance.start_byte) <
             std::min(other.end_byte, instance.end_byte);
    });
    instances.erase(inters.begin(), inters.end());

    instances.push_back(instance);
  }
  return true;
}

void RemoveInstance(const Instance &instance, std::string &contents) {
  assert(instance.start_byte < instance.end_byte);
  assert(instance.end_byte <= contents.length());
  if (instance.needs_semicolon) {
    contents.erase(instance.start_byte + 1,
                   instance.end_byte - instance.start_byte - 1);
    contents[instance.start_byte] = ';';
  } else {
    contents.erase(instance.start_byte,
                   instance.end_byte - instance.start_byte);
  }
}

} // namespace

int main(int argc, char *argv[]) {
  std::ios::sync_with_stdio(false); // speed up C++ I/O streams

  // Extract input arguments.
  std::vector<std::filesystem::path> paths;
  if (argc == 2 || argc == 4) {
    WalkFileOrDir(std::filesystem::path(argv[1]), paths);
  } else if (argc == 1 || argc == 3) {
    std::string line;
    while (std::getline(std::cin, line)) {
      if (!line.empty()) {
        WalkFileOrDir(std::filesystem::path(line), paths);
      }
    }
  } else {
    std::cerr << "Usage: " << argv[0]
              << " [path/to/file/or/dir] [to_remove_start] [to_remove_end]"
              << std::endl;
    return 1;
  }
  std::optional<int> to_remove_start;
  std::optional<int> to_remove_end;
  if (argc >= 3) {
    int start_idx = argc == 3 ? 1 : 2;
    to_remove_start = std::stoi(argv[start_idx]);
    to_remove_end = std::stoi(argv[start_idx + 1]);
  }

  // Prepare the common parsing state.
  std::unique_ptr<TSParser, decltype(&ts_parser_delete)> parser(
      ts_parser_new(), ts_parser_delete);
  ts_parser_set_language(parser.get(), tree_sitter_cpp());

  const char kQueryStr[] = R"(
    (
      function_definition
      declarator: (
        function_declarator
        declarator: (
          qualified_identifier
          (MISSING "::")? @idx0
        )? @idx1
      )
      (field_initializer_list)? @idx2
      body: (_) @idx3
    ) @idx4
    )";
  uint32_t error_offset = 0;
  TSQueryError error_type = TSQueryErrorNone;
  std::unique_ptr<TSQuery, decltype(&ts_query_delete)> query(
      ts_query_new(tree_sitter_cpp(), kQueryStr, strlen(kQueryStr),
                   &error_offset, &error_type),
      ts_query_delete);
  if (!query) {
    std::cerr << "Failed to init Tree-sitter query: error " << error_type
              << " offset " << error_offset << std::endl;
    return 1;
  }

  // Parse and, if requested, edit input files.
  int instance_count = 0;
  // As an optimization, reuse the buffers across different files:
  std::string contents;
  std::vector<Instance> instances, instances_to_remove;
  for (const auto &path : paths) {
    ReadFile(path, contents);

    instances.clear();
    if (!ParseFile(contents, parser.get(), query.get(), instances)) {
      std::cerr << "Failed to parse " << path << std::endl;
      return 1;
    }

    instances_to_remove.clear();
    for (const auto &instance : instances) {
      ++instance_count;
      if (to_remove_start && to_remove_end &&
          *to_remove_start <= instance_count &&
          instance_count <= *to_remove_end) {
        instances_to_remove.push_back(instance);
      }
    }

    if (!instances_to_remove.empty()) {
      const auto old_size = contents.length();
      std::ranges::reverse(instances_to_remove);
      for (const auto &instance : instances_to_remove) {
        RemoveInstance(instance, contents);
      }
      std::cerr << "editing " << path << ": old size " << old_size
                << " new size " << contents.length() << " instance shift "
                << instance_count - instances.size() << " instance count "
                << instances.size() << std::endl;
      WriteFile(path, contents);
    }
  }
  std::cout << "Total instances: " << instance_count << std::endl;
}
