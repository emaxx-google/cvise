// Calculates the "depth" of each header needed for the given source file.
//
// Usage:
//   calc-include-depth file.cc -- -Ifoo -std=gnu++20 ...other flags...
//
// In the common case of creduce'ing a multi-file target with a makefile, the
// compilation commands (after the "--") could be built as:
//   $(grep -P -A1 '^[^\t].*\.o:' target.makefile | tail -n1 | sed
//   's/\S*[-]fmodule\S*//g') \
//     -resource-dir=$($(grep -P -A1 '^[^\t].*\.o:' target.makefile | tail -n1)
//     --print-resource-dir)

#include <map>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "clang/Basic/FileEntry.h"
#include "clang/Basic/SourceLocation.h"
#include "clang/Basic/SourceManager.h"
#include "clang/Frontend/CompilerInstance.h"
#include "clang/Frontend/FrontendAction.h"
#include "clang/Frontend/FrontendActions.h"
#include "clang/Lex/PPCallbacks.h"
#include "clang/Lex/Preprocessor.h"
#include "clang/Lex/Token.h"
#include "clang/Tooling/CommonOptionsParser.h"
#include "clang/Tooling/Execution.h"
#include "llvm/ADT/StringExtras.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Support/CommandLine.h"
#include "llvm/Support/Signals.h"
#include "llvm/Support/raw_ostream.h"

using namespace clang;
using namespace clang::tooling;
using namespace llvm;

// Graph with nodes being source/header file paths.
using Node = std::string;
using AdjacencyList = std::vector<Node>;
using Graph = std::map<Node, AdjacencyList>;

namespace {

// Observes every inclusion directive (#include et al.) and adds corresponding
// edges to the graph.
class InclusionGraphPPCallback : public PPCallbacks {
public:
  InclusionGraphPPCallback(const SourceManager &SourceMgr,
                           Graph &InclusionGraph)
      : SourceMgr(SourceMgr), InclusionGraph(InclusionGraph) {}

  void InclusionDirective(SourceLocation HashLoc, const Token &IncludeTok,
                          StringRef FileName, bool IsAngled,
                          CharSourceRange FilenameRange,
                          OptionalFileEntryRef File, StringRef SearchPath,
                          StringRef RelativePath, const Module *SuggestedModule,
                          bool ModuleImported,
                          SrcMgr::CharacteristicKind FileType) override {
    if (!File) {
      // Ignore broken includes.
      return;
    }
    Node From(SourceMgr.getFilename(HashLoc));
    Node To(File->getName());
    InclusionGraph[From].push_back(To);
  }

private:
  const SourceManager &SourceMgr;
  Graph &InclusionGraph;
};

// Frontend action that instantiates and enables `InclusionGraphPPCallback`.
class InclusionGraphAction : public PreprocessOnlyAction {
public:
  explicit InclusionGraphAction(Graph &InclusionGraph)
      : InclusionGraph(InclusionGraph) {}

  bool BeginSourceFileAction(CompilerInstance &CI) override {
    CI.getPreprocessor().addPPCallbacks(
        std::make_unique<InclusionGraphPPCallback>(CI.getSourceManager(),
                                                   InclusionGraph));
    return true;
  }

private:
  Graph &InclusionGraph;
};

// Factory for `InclusionGraphAction`, which builds the inclusion graph.
class InclusionGraphActionFactory : public FrontendActionFactory {
public:
  explicit InclusionGraphActionFactory(Graph &InclusionGraph)
      : InclusionGraph(InclusionGraph) {}

  std::unique_ptr<FrontendAction> create() override {
    return std::make_unique<InclusionGraphAction>(InclusionGraph);
  }

private:
  Graph &InclusionGraph;
};

} // namespace

// Calculates distances from root nodes to all others in the given graph.
static std::map<Node, int> calculateDistances(const Graph &InclusionGraph) {
  std::map<Node, int> InDegree;
  for (const auto &[From, Edges] : InclusionGraph) {
    for (const auto &To : Edges) {
      ++InDegree[To];
    }
  }

  // Run breadth-first search from root nodes (normally there's only one root:
  // the compiled source file).
  std::map<Node, int> Distance;
  std::queue<Node> Queue;
  for (const auto &[V, _] : InclusionGraph) {
    if (InDegree[V] == 0) {
      Distance[V] = 0;
      Queue.push(V);
    }
  }
  while (!Queue.empty()) {
    const Node V = Queue.front();
    Queue.pop();
    auto It = InclusionGraph.find(V);
    if (It == InclusionGraph.end()) {
      continue;
    }
    for (const auto &To : It->second) {
      if (!Distance.count(To)) {
        Queue.push(To);
        Distance[To] = Distance[V] + 1;
      }
    }
  }
  return Distance;
}

static cl::extrahelp CommonHelp(CommonOptionsParser::HelpMessage);
static cl::OptionCategory ToolCategory("calc-include-depth options");

int main(int argc, const char **argv) {
  llvm::sys::PrintStackTraceOnErrorSignal(argv[0]);

  auto Executor = createExecutorFromCommandLineArgs(argc, argv, ToolCategory);
  if (!Executor) {
    errs() << toString(Executor.takeError()) << "\n";
    return 1;
  }

  Graph InclusionGraph;
  auto Err = Executor->get()->execute(
      std::make_unique<InclusionGraphActionFactory>(InclusionGraph));
  if (Err) {
    errs() << toString(std::move(Err)) << "\n";
  }

  const std::map<Node, int> Distances = calculateDistances(InclusionGraph);
  for (const auto &[V, Dist] : Distances) {
    outs() << V << " " << Dist << "\n";
  }
}
