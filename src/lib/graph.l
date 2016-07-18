%{
#include <stdio.h>
#include "sysutil.h"
#include "graph.tab.hh"

/*#define YY_DECL extern "C" int pglex() */
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
int pglex_destroy() { return 0; }
#endif

static char g_current_file[4096];
static char string_buf[4096];
static char *string_buf_ptr = NULL;
static uint32_t pg_lineno = 0;

extern int pgparse();

static int parse_string(std::string s);
static int parse_file(const char *fname);
static std::string expand_env_vars(std::string s, int *error);

%}
include   ^#include
whitesp   [ \t]+
number    [[:digit:]]+
word      [[:alnum:]\./_-](\\.|[^ ;\t\n}:%<>\$\|\[\]\(\)])*
name      [[:alnum:]_]+
comment   #
varRef    \${name}
endStmt   ((\r\n)|\n|;)+

%x STR1 STR2
%x VAR
%x COMMENT
%x FILEEND
%option noyywrap 
%%

\"      { string_buf_ptr = string_buf; BEGIN(STR1); }
<STR1>{
\"        { /* Closing Quote */ BEGIN(0); *string_buf_ptr++ = '\0'; pglval.sval = strdup(string_buf); return STRINGLIT; }
\\\"      { *string_buf_ptr++ = '\"'; }
[^\"]+    { char *yptr = pgtext;  while(*yptr) *string_buf_ptr++ = *yptr++; }
}

\'      { string_buf_ptr = string_buf; BEGIN(STR2); }
<STR2>{
\'        { /* Closing Quote */ BEGIN(0); *string_buf_ptr++ = '\0'; pglval.sval = strdup(string_buf); return STRINGLIT; }
\\\'      { *string_buf_ptr++ = '\''; }
[^\']+    {    char *yptr = pgtext; while(*yptr) *string_buf_ptr++ = *yptr++; }
}

{varRef}    { BEGIN(VAR); pglval.sval = strdup(pgtext); return VARREF; }
<VAR>{
"."       { return PERIOD; }
":"       { return COLON; }
{name}    { pglval.sval = strdup(pgtext); return WORD; }
[ },);\t\n|]  { BEGIN(0); unput(*pgtext); }
}


{comment} { BEGIN(COMMENT); }
<COMMENT>{
[^\n]*     ;
\n         { pg_lineno++; BEGIN(0); }
}

{include} { return INCLUDE; }
"("       { return LPAREN; }
")"       { return RPAREN; }
"{"       { return LBRACE; }
"}"       { return RBRACE; }
"%extern" { return EXTERN; }
"%thread" { return THREAD; }
"%func"   { return FUNC; }
"%"       { return PERCENT; }
"@"       { return ATSIGN; }
"||"      { return DOUBLEPIPE; }
"@||"     { return ATDOUBLEPIPE; }
"|"       { return PIPE; }
","       { return COMMA; }
"."       { return PERIOD; }
":"       { return COLON; }
"->"      { return ARROW; }
";"       { return ENDSTMT; }
"\n"      { pg_lineno++; return ENDSTMT; }
{number}       { pglval.ival = atoi(pgtext); return NUMBER; }
{word}         { pglval.sval = strdup(pgtext); return WORD; }
{whitesp} ;
<FILEEND><<EOF>> { BEGIN(0); return 0; }
<<EOF>> { BEGIN(FILEEND); return ENDSTMT; }
%%

int pg_parse_graph(ASTNode *root, SymbolTable *symTab, nhqueue_t *files, const char *str)
{
     int retval = 0;
     gASTRoot = root;
     gSymTab.push_back(symTab);
     gFileQueue = files;

     int parseRet = 0;

     pgdebug = getenv("WS_PARSE_DEBUG") != NULL;

     if ( str ) {
          sprintf(g_current_file, "%s", "Command Line");
          parseRet = parse_string(str);
     }

     char *fname = NULL;
     while (parseRet == 0 && ((fname = (char*)queue_remove(gFileQueue)) != NULL) ) {
          sprintf(g_current_file, "%s", fname);
          parseRet = parse_file(fname);

          free(fname); fname = NULL;
     }

     retval = !parseRet;

     pglex_destroy();
     gASTRoot = NULL;
     gSymTab.clear();
     gFileQueue = NULL;
     return retval;
}



void pgerror(const char *s) {
     error_print("Metaproc Parse Error: %s in %s", s, g_current_file);
     error_print("Unexpected token '%s' on line %u", pgtext, pg_lineno);
}



static int parse_string(std::string s)
{
     int parseRet = 0;
     std::string expStr = expand_env_vars(s, &parseRet);
     if ( !parseRet ) {
          YY_BUFFER_STATE lexState = pg_scan_string(expStr.c_str());
          if ( (parseRet = pgparse()) != 0 ) {
               error_print("Failed to parse command-line string.\n");
          }
          pg_delete_buffer(lexState);
     }
     return parseRet;
}


static int parse_file(const char *fname)
{
     FILE *fp = sysutil_config_fopen(fname, "r");
     if ( !fp ) {
          error_print("Failed to open file %s\n", fname);
          return 1;
     }

     /* Calculate size of file */
     fseek(fp, 0, SEEK_END);
     long fsize = ftell(fp);
     rewind(fp);

     if ( fsize <= 0 ) {
          error_print("Error (%d) reading %s\n", errno, fname);
          sysutil_config_fclose(fp); fp = NULL;
          return 1;
     }

     char* buffer = (char*)calloc(fsize+1, sizeof(char));
     if ( !buffer ) {
          error_print("Malloc failed!");
          sysutil_config_fclose(fp); fp = NULL;
          return 1;
     }
     size_t ret = fread(buffer, sizeof(char), fsize, fp);
     sysutil_config_fclose(fp); fp = NULL;
     if ( ret != (size_t) fsize ) {
          error_print("I/O Error (%d) reading file %s", errno, fname);
          free(buffer);
          return 1;
     }

     pg_lineno = 1;

     int retVal = parse_string(buffer);

     free(buffer);

     return retVal;
}


#define EEV_LIMIT_NESTEDNESS  (100)
static std::string expand_env_vars(std::string s, int *error)
{
     // returns string as is if no matching pair of environment variable marker (${...})
     // exists; otherwise, it returns string with all enviroment variables expanded out...
     // from right to left or innermost nested variable to the outermost
     static uint32_t expand_limit = 0;
     expand_limit++;
     if( expand_limit > EEV_LIMIT_NESTEDNESS) {
          error_print("Nested expansion limit (%d) reached for environment variables", EEV_LIMIT_NESTEDNESS);
          *error = 1;
          return s;
     }

     size_t begin = s.find( "${" );
     if ( begin == std::string::npos ) return s;

     std::string pre  = s.substr( 0, begin );
     std::string remnant;
     if( s.find( "${", begin + 2) ) { //another (possibly nested) occurrence exists
          remnant = expand_env_vars( s.substr(begin+2, std::string::npos), error );
     } else {
          remnant = s.substr(begin+2, std::string::npos);
     }

     size_t end = remnant.find( '}' );
     if ( end == std::string::npos ) return (pre + "${" + remnant);
     expand_limit--; // found a matching end-marker, so decrement nest count

     std::string post = remnant.substr( end + 1, std::string::npos );

     size_t varlen = end;
     std::string variable = remnant.substr( 0, varlen );

     static bool err_once = true;
     char *value = getenv( variable.c_str() );
     if ( ! value ) {
          if(err_once) {
               error_print("Failed to find environment variable '%s'", variable.c_str());
               *error = 1;
          }
          err_once = false;
          return (pre + "${" + remnant);
     }

     return expand_env_vars( pre + value + post, error );
}
