%{
/*-----------------------------------------------------------------------------
 * wscalc.l
 *
 * History:
 * 20111108 RDS Added NOT, AND, OR token for logical evaluation.
 *---------------------------------------------------------------------------*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "wscalc.h"
#include "wscalc.tab.h"

#ifdef YY_FLEX_SUBMINOR_VERSION
# define FLEX_VERSION \
  (YY_FLEX_MAJOR_VERSION) * 1000000 \
+ (YY_FLEX_MINOR_VERSION) * 1000 \
+ (YY_FLEX_SUBMINOR_VERSION)
#else
# define FLEX_VERSION \
  (YY_FLEX_MAJOR_VERSION) * 1000000 \
+ (YY_FLEX_MINOR_VERSION) * 1000
#endif


#if FLEX_VERSION < 2005009
// yylex_destroy was not defined before Flex 2.5.9...we'll use an implementation that seems to work with Flex 2.5.4
// (see https://lists.gnu.org/archive/html/bison-patches/2012-12/msg00026.html for more details)
# define wscalclex_destroy() wscalc_delete_buffer(YY_CURRENT_BUFFER)
#endif

%}
%option nounput
%option noinput
%option batch
delim		[ \t\n]
whitesp		{delim}+
digit		[0-9]
integer		[0-9]+
decimal		[0-9]*\.?[0-9]+|[0-9]+\.
charstring     [A-Za-z#_][A-Za-z0-9_]*
stringlit      \"(\\.|[^"])*\"
%%
{stringlit}    {int len = strlen(wscalctext); wscalclval.value = makeWSCalcValueStringRaw(wscalctext+1, len-2); return NUMBER;}
{integer}  {wscalclval.value = makeWSCalcValueUInteger(strtoll(wscalctext, (char**)NULL, 10)); return NUMBER;}
{decimal}  {wscalclval.value = makeWSCalcValueDouble(atof(wscalctext)); return NUMBER;}
"++"      {return INCREMENT;}
"--"      {return DECREMENT;}
"+="      {return INCREMENTC;}
"-="      {return DECREMENTC;}
"+"	     {return PLUS;}
"-"	     {return MINUS;}
"/="      {return SLASHC;}
"/"	     {return SLASH;}
">"       {return GREATER;}
">="      {return GREATEREQUAL;}
"<"       {return LESS;}
"<="      {return LESSEQUAL;}
"=="      {return DOUBLEEQUAL;}
"!="      {return NOTEQUAL;}
"!"       {return NOT;}
"&&"      {return AND;}
"||"      {return OR;}
"%"       {return MOD;}
"%="      {return MODC;}
"&"       {return BITAND;}
"&="      {return BITANDC;}
"|"       {return BITIOR;}
"|="      {return BITIORC;}
"^"       {return BITXOR;}
"^="      {return BITXORC;}
"~"       {return COMPLMNT;}
"<<"      {return LEFTSHIFT;}
"<<="     {return LEFTSHIFTC;}
">>"      {return RIGHTSHIFT;}
">>="     {return RIGHTSHIFTC;}
"**"      {return POWER;} 
"*"	     {return ASTERISK;}
"*="	     {return ASTERISKC;}
"("	     {return LPAREN;}
")"	     {return RPAREN;}
"["       {return LBRACKET;}
"]"       {return RBRACKET;}
";"       {return SEMICOLON;}
"="       {return EQUALS;}
","       {return COMMA;}
"cast"    {strncpy(wscalclval.name, wscalctext, 50); wscalclval.name[49] =0; return CAST;}
"double"  {strncpy(wscalclval.name, wscalctext, 50); wscalclval.name[49] =0; return TYPE;}
"int"     {strncpy(wscalclval.name, wscalctext, 50); wscalclval.name[49] =0; return TYPE;}
"uint"    {strncpy(wscalclval.name, wscalctext, 50); wscalclval.name[49] =0; return TYPE;}
"bool"    {strncpy(wscalclval.name, wscalctext, 50); wscalclval.name[49] =0; return TYPE;}
"string"  {strncpy(wscalclval.name, wscalctext, 50); wscalclval.name[49] =0; return TYPE;}
"time"    {strncpy(wscalclval.name, wscalctext, 50); wscalclval.name[49] =0; return TYPE;}
"IF"      {return IF;}
"if"      {return IF;}
"THEN"    {return THEN;}
"then"    {return THEN;}
"ELSE"    {return ELSE;}
"else"    {return ELSE;}
"ENDIF"   {return ENDIF;}
"endif"   {return ENDIF;}
"FALSE"   {return FALSE;}
"TRUE"    {return TRUE;}
"label"   {return WSCLABEL;}
"LABEL"   {return WSCLABEL;}
"flush"   {return WSFLUSH;}
"FLUSH"   {return WSFLUSH;}
"exists"  {return EXISTS;}
"EXISTS"  {return EXISTS;}
{whitesp}	{ /* no action and no return */}
{charstring}    {strncpy(wscalclval.name, wscalctext, 49); wscalclval.name[49]=0; return NAME;}

%%


struct progData_t {
     FILE **file_array;
     const char *string_input;
};
static struct progData_t g_parseFiles;

int wscalc_parse_script(void *callerState, wscalcPart **wscalc_output,
                        int *wscalc_error, FILE **fileList, const char *extra_script) {
     g_parseFiles.file_array = fileList;
     g_parseFiles.string_input = extra_script;

     *wscalc_error = 0;
     if ( wscalcwrap() ) { /* Set up the first scan entry */
          *wscalc_error = 1;
          fprintf(stderr, "Nothing specified to compile.\n");
          return *wscalc_error;
     }
     wscalcparse(callerState, wscalc_output, wscalc_error);
     wscalclex_destroy();
     return *wscalc_error;
}

int wscalcwrap() {
     YY_BUFFER_STATE currState = YY_CURRENT_BUFFER;
     wscalc_delete_buffer(currState);
     if ( g_parseFiles.file_array && *(g_parseFiles.file_array) ) {
          fprintf(stderr, "Setting new Input Script file!\n");
          FILE *fp = *(g_parseFiles.file_array);
          wscalc_switch_to_buffer(wscalc_create_buffer(fp, YY_BUF_SIZE));
          g_parseFiles.file_array++;
          return 0;
     } else if ( g_parseFiles.string_input ) {
          fprintf(stderr, "calling wscalc_scan_string\n");
          wscalc_scan_string(g_parseFiles.string_input);
          g_parseFiles.string_input = NULL;
          return 0;
     }
     return 1;
}

