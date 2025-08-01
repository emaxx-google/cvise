/* topformflat.l; see License.txt for copyright and terms of use
 *
 * Scott McPeak        <smcpeak@cs.berkeley.edu>
 *
 * flatten all toplevel forms to single lines.
 * very heuristic... */

%{
#include <stdlib.h>     // atoi

// debugging diagnostic, emitted when enabled
void diag(char const *str);

// handle possible toplevel block boundaries if nesting <= threshold
void possibleChunkBegin(int pos);
void possibleChunkEnd(int pos);

// index of byte where the current yytext ends in the input.
int token_end_pos = 0;

// index of byte where the current toplevel block started.
int chunk_start_pos = 0;

// keep track of brace nesting (0 means not inside any pair)
int nesting = 0;

// nesting threshold; when nesting is greater than threshold,
// newlines are suppressed
int threshold = 0;

// indices of bytes of the most recent consecutive whitespace block.
int whitespace_start_pos = -1;
int whitespace_end_pos = -1;

// update the position whenever a token is about to be processed
// (this macro is triggered by Flex prior to executing the matched rule’s action).
#define YY_USER_ACTION token_end_pos += yyleng;

%}

/* optimize the scanner for performance */
%option fast
/* don't try to call yywrap() */
%option noyywrap
/* dsw: don't define yyunput() */
%option nounput
/* support all characters, including UTF-8 */
%option 8bit

/* start condition for strings */
%x STRING
%x CHARLIT


%%

";"           { possibleChunkEnd(token_end_pos); }

"/\n"         { }     /* end of C comment */

"{"           { nesting++;
                possibleChunkBegin(token_end_pos);      // so the header is separated from the components
              }

"}"(";"?)     { nesting--;
                possibleChunkEnd(token_end_pos);
              }

  /* parse a preprocessor directive:
   * start of the line with possible leading whitespaces, a hash, then some non-newlines.
   * then, possibly, an escaped newline followed by more non-newlines (repeat as needed).
   * finally, a newline */
^[[:blank:]]*"#".*("\\\n".*)*"\n" {
                int start_pos = token_end_pos - yyleng;
                if (start_pos > 0)
                  possibleChunkEnd(start_pos - 1);          /* make sure starts on own line */
                possibleChunkEnd(token_end_pos - 1);        /* minus one to preserve the newline */
                possibleChunkBegin(token_end_pos);          /* don't eat the newline in the subsequent chunk */
              }

"//".*"\n"    { }                                           /* C++ comment */

"\""          { diag("<STR>"); BEGIN(STRING); }             /* start quote */

<STRING>{
  "\\"(.|\n)  { }                                           /* escaped character */
  "\""        { diag("</STR>"); BEGIN(INITIAL); }           /* close quote */
  (.|\n)      { }                                           /* ordinary char */
}

"\'"          { diag("<CHAR>"); BEGIN(CHARLIT); }           /* start tick */

<CHARLIT>{
  "\\"(.|\n)  { }                                           /* escaped character */
  "\'"        { diag("</CHAR>"); BEGIN(INITIAL); }          /* close tick */
  (.|\n)      {  }                                          /* ordinary char */
}

"\n"          |                                             /* line break, not matched in any other rule */
[[:blank:]]+  {                                             /* chunk of whitespaces */
                int token_start_pos = token_end_pos - yyleng;
                // Start a new whitespace block, unless the previous one is to be continued.
                if (token_start_pos != whitespace_end_pos)
                  whitespace_start_pos = token_start_pos;
                whitespace_end_pos = token_end_pos;
              }

.             { }

%%

void diag(char const *str)
{
  //printf("%s", str);
}

void possibleChunkBegin(int pos)
{
  if (nesting <= threshold) {
    chunk_start_pos = pos;
  }
}

void possibleChunkEnd(int pos)
{
  // Only attempt deleting chunks exactly at the specified nesting.
  if (nesting == threshold && chunk_start_pos < pos) {
    // Skip deleting chunks consisting of whitespaces only.
    if (chunk_start_pos < whitespace_start_pos || pos > whitespace_end_pos) {
      printf("{\"p\":[{\"l\":%d,\"r\":%d}]}\n", chunk_start_pos, pos);
    }
  }
  possibleChunkBegin(pos);
}

char *version = "2025.6.25";
int main(int argc, char *argv[])
{
  if (isatty(0)) {
    printf("topformflat_hints version %s\n", version);
    printf("usage: %s [threshold] <input.c >hints.jsonl\n", argv[0]);
    printf("  The threshold (default: 0) specifies at what nesting level\n"
           "  of braces will line breaks be allowed (or inserted).  By\n"
           "  starting with 0, you get all top-level forms, one per line\n"
           "  (roughly).  Increasing the threshold leads to finer-grained\n"
           "  structure on each line.  The intent is to use the delta\n"
           "  minimizer on each level of granularity.\n");
    return 0;
  }

  if (argc >= 2) {
    threshold = atoi(argv[1]);    // user-specified threshold
  }

  yyin = stdin;
  yylex();
  // In case there's some chunk of text remaining - e.g., if the file is not
  // EOL-terminated and the last line was a preprocessor directive.
  possibleChunkEnd(token_end_pos);
  return 0;
}
