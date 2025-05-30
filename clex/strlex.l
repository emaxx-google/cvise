%{
/*
 * Copyright (c) 2013 The University of Utah
 * All rights reserved.
 *
 * This file is distributed under the University of Illinois Open Source
 * License.  See the file COPYING for details.
 */

int parser_pos = 0;

#define YY_USER_ACTION parser_pos += yyleng;
%}

D			[0-9]
L			[a-zA-Z_]
H			[a-fA-F0-9]
E			[Ee][+-]?{D}+
FS			(f|F|l|L)
IS			(u|U|l|L)*

%option noyywrap

%{

#if HAVE_CONFIG_H
#  include <config.h>
#endif

#include <defs.h>

%}

%%

"auto"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"break"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"case"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"char"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"const"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"continue"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"default"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"do"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"double"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"else"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"enum"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"extern"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"float"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"for"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"goto"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"if"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"int"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"long"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"register"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"return"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"short"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"signed"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"sizeof"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"static"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"struct"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"switch"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"typedef"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"union"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"unsigned"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"void"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"volatile"		{ process_token(TOK_KEYWORD, parser_pos - yyleng); }
"while"			{ process_token(TOK_KEYWORD, parser_pos - yyleng); }

{L}({L}|{D})*		{ process_token(TOK_IDENT, parser_pos - yyleng); }

0[xX]{H}+{IS}?		{ process_token(TOK_NUMBER, parser_pos - yyleng); }
0{D}+{IS}?		{ process_token(TOK_NUMBER, parser_pos - yyleng); }
{D}+{IS}?		{ process_token(TOK_NUMBER, parser_pos - yyleng); }
L?'(\\.|[^\\'])+'	{ process_token(TOK_OTHER, parser_pos - yyleng); }

{D}+{E}{FS}?		{ process_token(TOK_NUMBER, parser_pos - yyleng); }
{D}*"."{D}+({E})?{FS}?	{ process_token(TOK_NUMBER, parser_pos - yyleng); }
{D}+"."{D}*({E})?{FS}?	{ process_token(TOK_NUMBER, parser_pos - yyleng); }

L?\"(\\.|[^\\"])*\"	{ process_token(TOK_STRING, parser_pos - yyleng); }

"..."			{ process_token(TOK_OTHER, parser_pos - yyleng); }
">>="			{ process_token(TOK_OP, parser_pos - yyleng); }
"<<="			{ process_token(TOK_OP, parser_pos - yyleng); }
"+="			{ process_token(TOK_OP, parser_pos - yyleng); }
"-="			{ process_token(TOK_OP, parser_pos - yyleng); }
"*="			{ process_token(TOK_OP, parser_pos - yyleng); }
"/="			{ process_token(TOK_OP, parser_pos - yyleng); }
"%="			{ process_token(TOK_OP, parser_pos - yyleng); }
"&="			{ process_token(TOK_OP, parser_pos - yyleng); }
"^="			{ process_token(TOK_OP, parser_pos - yyleng); }
"|="			{ process_token(TOK_OP, parser_pos - yyleng); }
">>"			{ process_token(TOK_OP, parser_pos - yyleng); }
"<<"			{ process_token(TOK_OP, parser_pos - yyleng); }
"++"			{ process_token(TOK_OP, parser_pos - yyleng); }
"--"			{ process_token(TOK_OP, parser_pos - yyleng); }
"->"			{ process_token(TOK_OP, parser_pos - yyleng); }
"&&"			{ process_token(TOK_OP, parser_pos - yyleng); }
"||"			{ process_token(TOK_OP, parser_pos - yyleng); }
"<="			{ process_token(TOK_OP, parser_pos - yyleng); }
">="			{ process_token(TOK_OP, parser_pos - yyleng); }
"=="			{ process_token(TOK_OP, parser_pos - yyleng); }
"!="			{ process_token(TOK_OP, parser_pos - yyleng); }
";"			{ process_token(TOK_OP, parser_pos - yyleng); }
("{"|"<%")		{ process_token(TOK_OP, parser_pos - yyleng); }
("}"|"%>")		{ process_token(TOK_OP, parser_pos - yyleng); }
","			{ process_token(TOK_OP, parser_pos - yyleng); }
":"			{ process_token(TOK_OP, parser_pos - yyleng); }
"="			{ process_token(TOK_OP, parser_pos - yyleng); }
"("			{ process_token(TOK_OP, parser_pos - yyleng); }
")"			{ process_token(TOK_OP, parser_pos - yyleng); }
("["|"<:")		{ process_token(TOK_OP, parser_pos - yyleng); }
("]"|":>")		{ process_token(TOK_OP, parser_pos - yyleng); }
"."			{ process_token(TOK_OP, parser_pos - yyleng); }
"&"			{ process_token(TOK_OP, parser_pos - yyleng); }
"!"			{ process_token(TOK_OP, parser_pos - yyleng); }
"~"			{ process_token(TOK_OP, parser_pos - yyleng); }
"-"			{ process_token(TOK_OP, parser_pos - yyleng); }
"+"			{ process_token(TOK_OP, parser_pos - yyleng); }
"*"			{ process_token(TOK_OP, parser_pos - yyleng); }
"/"			{ process_token(TOK_OP, parser_pos - yyleng); }
"%"			{ process_token(TOK_OP, parser_pos - yyleng); }
"<"			{ process_token(TOK_OP, parser_pos - yyleng); }
">"			{ process_token(TOK_OP, parser_pos - yyleng); }
"^"			{ process_token(TOK_OP, parser_pos - yyleng); }
"|"			{ process_token(TOK_OP, parser_pos - yyleng); }
"?"			{ process_token(TOK_OP, parser_pos - yyleng); }
"#"                     { process_token(TOK_OTHER, parser_pos - yyleng); }
"\\"                    { process_token(TOK_OTHER, parser_pos - yyleng); }

"/*"        {
                     for ( ; ; )  {
                         int c;
                         /* eat up text of comment */
                         do {
                            c = input();
                            ++parser_pos;
                            if (c == '*' || c == EOF)
                                break;
                         } while (c != '*' && c != EOF);

                         if ( c == '*' )
                             {
                             do {
                                c = input();
                                ++parser_pos;
                             } while (c == '*');
                             if ( c == '/' )
                                 break;    /* found the end */
                             }

                         if ( c == EOF )
                             {
                             fprintf(stderr, "EOF in comment" );
			     assert (0);
                             }
                    }
           }

[ \t\v\n\f]		{ process_token(TOK_WS, parser_pos - yyleng); }

.			{ fprintf (stderr, "didn't expect to see '%s'\n", yytext);
                          assert (0); }

%%

int count = 0;

