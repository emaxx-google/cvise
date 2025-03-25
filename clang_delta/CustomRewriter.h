#ifndef CUSTOM_REWRITER_H
#define CUSTOM_REWRITER_H

#include <vector>
#include <string>
#include "clang/Basic/SourceManager.h"
#include "clang/Rewrite/Core/Rewriter.h"
#include "llvm/Support/FormatVariadic.h"

namespace clang {
  class SourceManager;
}

class CustomRewriter {
 public:
  void setGenerateHintsFlag(bool Flag) {
    GenerateHints = Flag;
  }

  void setSourceMgr(clang::SourceManager &SM, const clang::LangOptions &LO) {
    SourceMgr = &SM;
    UnderlyingRewriter.setSourceMgr(SM, LO);
  }

  clang::SourceManager &getSourceMgr() const {
    return *SourceMgr;
  }

#if LLVM_VERSION_MAJOR >= 20
  const llvm::RewriteBuffer*
#else
  const clang::RewriteBuffer*
#endif
  getRewriteBufferFor(clang::FileID FID) const {
    if (GenerateHints)
      return nullptr;
    return UnderlyingRewriter.getRewriteBufferFor(FID);
  }

  int getRangeSize(clang::CharSourceRange Range, clang::Rewriter::RewriteOptions opts = clang::Rewriter::RewriteOptions()) const {
    return UnderlyingRewriter.getRangeSize(Range);
  }

  int getRangeSize(clang::SourceRange Range, clang::Rewriter::RewriteOptions opts = clang::Rewriter::RewriteOptions()) const {
    return getRangeSize(clang::CharSourceRange::getTokenRange(Range), opts);
  }

  bool RemoveText(clang::SourceRange range,
                  clang::Rewriter::RewriteOptions opts = clang::Rewriter::RewriteOptions()) {
    return RemoveText(range.getBegin(), getRangeSize(range, opts), opts);
  }

  bool RemoveText(clang::SourceLocation Start, unsigned Length,
                  clang::Rewriter::RewriteOptions opts = clang::Rewriter::RewriteOptions()) {
    llvm::errs() << "RemoveText: Start=" << SourceMgr->getFileOffset(Start) << " Length=" << Length << " Dbg="
                 << std::string(SourceMgr->getCharacterData(Start), Length) << "\n";
    if (GenerateHints) {
      Hint H;
      H.Left = SourceMgr->getFileOffset(Start);
      H.Right = H.Left + Length;
      HintGroups.back().push_back(H);
      return true;
    } else {
      return UnderlyingRewriter.RemoveText(Start, Length, opts);
    }
  }

  bool RemoveText(clang::CharSourceRange range,
                  clang::Rewriter::RewriteOptions opts = clang::Rewriter::RewriteOptions()) {
    return RemoveText(range.getBegin(), getRangeSize(range, opts), opts);
  }

  std::string getRewrittenText(clang::CharSourceRange Range) const {
    if (GenerateHints) {
      assert(false);
      throw;
    } else {
      return UnderlyingRewriter.getRewrittenText(Range);
    }
  }

  std::vector<std::string> GetHints() const {
    std::vector<std::string> Jsons;
    for (const auto& Group : HintGroups) {
      if (Group.empty())
        continue;
      if (Group.size() == 1) {
        Jsons.push_back(Group[0].ToJson());
      } else {
        std::string Arr;
        for (const auto& Hint : Group) {
          if (!Arr.empty())
            Arr += ",";
          Arr += Hint.ToJson();
        }
        Jsons.push_back(llvm::formatv(R"({{"multi":[{0}]})", Arr));
      }
    }
    return Jsons;
  }

  std::string getRewrittenText(clang::SourceRange Range) const {
    if (GenerateHints) {
      assert(false);
      throw;
    } else {
      return UnderlyingRewriter.getRewrittenText(Range);
    }
  }

  bool InsertTextAfterToken(clang::SourceLocation Loc, clang::StringRef Str) {
    llvm::errs() << "InsertTextAfterToken\n";
    if (GenerateHints) {
      // TODO
      assert(false);
      throw;
      return false;
    } else {
      return UnderlyingRewriter.InsertTextAfterToken(Loc, Str);
    }
  }

  bool InsertTextBefore(clang::SourceLocation Loc, clang::StringRef Str) {
    llvm::errs() << "InsertTextBefore\n";
    if (GenerateHints) {
      // TODO
      assert(false);
      throw;
      return false;
    } else {
      return UnderlyingRewriter.InsertTextBefore(Loc, Str);
    }
  }

  bool ReplaceText(clang::SourceLocation Start, unsigned OrigLength,
                   clang::StringRef NewStr) {
    llvm::errs() << "ReplaceText: Start=" << SourceMgr->getFileOffset(Start) << " OrigLength=" << OrigLength
                 << " NewStr=" << NewStr << " Dbg="
                 << std::string(SourceMgr->getCharacterData(Start), OrigLength) << "\n";
    if (GenerateHints) {
      Hint H;
      H.Left = SourceMgr->getFileOffset(Start);
      H.Right = H.Left + OrigLength;
      H.NewValue = NewStr;
      HintGroups.back().push_back(H);
      return true;
    } else {
      return UnderlyingRewriter.ReplaceText(Start, OrigLength, NewStr);
    }
  }

  bool ReplaceText(clang::SourceRange range, clang::StringRef NewStr) {
    return ReplaceText(range.getBegin(), getRangeSize(range), NewStr);
  }

  bool InsertText(clang::SourceLocation Loc, clang::StringRef Str,
                  bool InsertAfter = true) {
    llvm::errs() << "InsertText\n";
    if (GenerateHints) {
      // TODO
      assert(false);
      throw;
      return false;
    } else {
      return UnderlyingRewriter.InsertText(Loc, Str, InsertAfter);
    }
  }

  void StartNewHintGroup() {
    if (HintGroups.empty() || !HintGroups.back().empty()) {
      HintGroups.push_back({});
    }
  }

 private:
  struct Hint {
    unsigned Left;
    unsigned Right;
    std::string NewValue;

    std::string ToJson() const {
      if (NewValue.empty())
        return llvm::formatv(R"({{"l":{0},"r":{1}})", Left, Right);
      return llvm::formatv(R"({{"l":{0},"r":{1},"v":"{2}"})", Left, Right, NewValue);
    }
  };

  bool GenerateHints = false;
  clang::SourceManager *SourceMgr = nullptr;
  clang::Rewriter UnderlyingRewriter;
  std::vector<std::vector<Hint>> HintGroups;
};

#endif
