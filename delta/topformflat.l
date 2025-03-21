/* topformflat.l; see License.txt for copyright and terms of use
 *
 * Scott McPeak        <smcpeak@cs.berkeley.edu>
 *
 * flatten all toplevel forms to single lines.
 * very heuristic... */

%{
#include <stdlib.h>     // atoi

int pos = 0;
int state_before_multicomment = 0;
int multicomment_start = 0;
int chunk_start = 0;

#define YY_USER_ACTION pos += yyleng;

// emit yytext as-is
void emit(void);

// debugging diagnostic, emitted when enabled
void diag(char const *str);

// add a newline if nesting <= threshold
void possibleNewline(void);

// keep track of brace nesting (0 means not inside any pair)
int nesting = 0;

// nesting threshold; when nesting is greater than threshold,
// newlines are suppressed
int threshold = 0;

%}

/* don't try to call yywrap() */
%option noyywrap
/* dsw: don't define yyunput() */
%option nounput

/* start condition for strings */
%x STRING
%x CHARLIT
%x MACRO
%x MULTICOMMENT

%%

<INITIAL,MACRO>"/*" {
                      multicomment_start = pos - yyleng;
                      state_before_multicomment = YY_START;
                      BEGIN(MULTICOMMENT);
                    }
<MULTICOMMENT>"*/"  {
                      if (threshold == 0) {
                        printf("{\"t\":\"comment\",\"l\":%d,\"r\":%d}\n", multicomment_start, pos);
                        multicomment_start = -1;
                      }
                      BEGIN(state_before_multicomment);
                      state_before_multicomment = -1;
                    }
<MULTICOMMENT>[^*\n]+
<MULTICOMMENT>"*"
<MULTICOMMENT>\n

";"           { emit(); possibleNewline(); }

"/\n"         { if (nesting <= threshold) {    /* end of C comment */
                  emit();
                }
                else {
                  // printf("%c", yytext[0]);
                }
              }

"{"           { nesting++;
                emit();
                possibleNewline();      // so the header is separated from the components
              }

"}"(";"?)     { nesting--;
                emit();
                possibleNewline();
              }

"\\\n"        {
                // printf(" ");
              }

"\n"          {
                // printf(" ");
              }     /* not any above case, eat it*/

<INITIAL,MACRO>"//".*    {
                if (threshold == 0) {
                  printf("{\"t\":\"comment\",\"l\":%d,\"r\":%d}\n", pos-yyleng, pos);
                }
                emit();
              }          /* C++ comment */

"#"           {
                pos -= yyleng;
                possibleNewline();
                pos += yyleng;
                BEGIN(MACRO);
              }
<MACRO>.
<MACRO>"\\\n"
<MACRO>\n     {
                possibleNewline();
                BEGIN(INITIAL);
              }

"\""          { diag("<STR>"); emit(); BEGIN(STRING); }     /* start quote */

<STRING>{
  "\\"(.|\n)  { emit(); }                                   /* escaped character */
  "\""        { emit(); diag("</STR>"); BEGIN(INITIAL); }   /* close quote */
  (.|\n)      { emit(); }                                   /* ordinary char */
}

"\'"          { diag("<CHAR>"); emit(); BEGIN(CHARLIT); }   /* start tick */

<CHARLIT>{
  "\\"(.|\n)  { emit(); }                                   /* escaped character */
  "\'"        { emit(); diag("</CHAR>"); BEGIN(INITIAL); }  /* close tick */
  (.|\n)      { emit(); }                                   /* ordinary char */
}

[0-9a-f][0-9a-f']*[0-9a-f] {
                emit(); /* possibly integer literal */
              }

.             { emit(); }

%%

void emit(void)
{
  //printf("%.*s", (int) yyleng, yytext);
}

void diag(char const *str)
{
  //printf("%s", str);
}

void possibleNewline(void)
{
  if (nesting <= threshold && chunk_start < pos) {
    printf("{\"t\":\"curly::%d\",\"l\":%d,\"r\":%d}\n", threshold, chunk_start, pos);
    chunk_start = pos;
  }
}

char *version = "2003.7.14";
int main(int argc, char *argv[])
{
  if (isatty(0) && argc < 3) {
    printf("topformflat version %s\n", version);
    printf("usage: %s [threshold] <input.c >output.c\n", argv[0]);
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
  if (argc >= 3) {
    yyin = fopen(argv[2], "rt");
    if (!yyin) {
      fprintf(stderr, "Failed to open %s .\n", argv[2]);
      return 1;
    }
  }
  yylex();
  return 0;
}
