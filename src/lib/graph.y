%code top{
#include <stdio.h>
#include <inttypes.h>
#include <stdlib.h>

#define YYDEBUG 1
}

%code requires{
#include <ast.h>
#include <list>
#include "wsqueue.h"

extern nhqueue_t *gFileQueue;
extern ASTNode *gASTRoot;
extern std::list<SymbolTable*> gSymTab;
extern uint32_t gInFuncDecl;
extern int pgdebug;
}


%union {
     ASTNode *node;
     char *sval;
     long long ival;
}

%code{

extern int pglex();
extern int pgerror(const char *);

nhqueue_t *gFileQueue;
ASTNode *gASTRoot = NULL;
std::list<SymbolTable*> gSymTab;
uint32_t gInFuncDecl = 0;

}


%token INCLUDE
%token LPAREN RPAREN LBRACE RBRACE PERIOD COLON ENDSTMT ATSIGN
%token THREAD FUNC EXTERN DOUBLEPIPE PIPE ATDOUBLEPIPE COMMA ARROW PERCENT
%token <sval> WORD STRINGLIT VARREF
%token <ival> NUMBER
%left WORD NUMBER STRINGLIT
%type <ival> pipe_sep
%type <node> statement_list scoped_statements statement
%type <node> thread_decl func_decl func_header func_call extern_decl pipeline pipe_source
%type <node> sourceVars sourceVar varList varRef kid_list sinkVar kid_def
%type <node> optSourceVars optVarList
%type <sval> varFilter
%type <sval> varTarget
%%

root: statement_list { gASTRoot->addChild($1); }
    ;

statement_list: { $$ = new ASTStatementList(); }
              | statement_list statement ENDSTMT { if ( $2 ) $1->addChild($2); $$ = $1; }
              | statement_list statement RBRACE { if ( $2 ) $1->addChild($2); $$ = $1; yychar = RBRACE; }
              | statement_list ENDSTMT { $$ = $1; }
              ;

scoped_statements:  LBRACE statement_list RBRACE { $$ = $2; }
                 ;

statement: thread_decl { $$ = $1; }
         | func_decl { $$ = $1; }
         | func_call { $$ = $1; }
         | extern_decl { $$ = $1; }
         | pipeline { $$ = $1; }
         | include { $$ = NULL; }
         ;


include: INCLUDE WORD { queue_add(gFileQueue, $2); }
       | INCLUDE STRINGLIT { queue_add(gFileQueue, $2); }
       ;

thread_decl: THREAD LPAREN NUMBER COMMA NUMBER RPAREN scoped_statements { unsigned long long tid = (($3 << 32) | $5); $$ = new ASTThreadDecl(tid, true, $7); }
           | THREAD LPAREN NUMBER RPAREN scoped_statements { $$ = new ASTThreadDecl($3, false, $5); }
           ;

extern_decl: EXTERN varList {
                    ASTVarList *l = (ASTVarList*)$2;
                    for ( size_t n = 0 ; n < l->childCount() ; n++ ) {
                         l->getVar(n)->setExtern();
                    }
                    delete l;
                    $$ = new ASTNULL();
               }
           ;

func_decl: func_header LPAREN optVarList ARROW varList RPAREN scoped_statements {
                    ASTFuncDecl *decl = (ASTFuncDecl*)$1;
                    decl->addSource($3);
                    decl->addDests($5);
                    decl->addStatements($7);
                    decl->fixup();
                    gSymTab.pop_back();
                    gInFuncDecl--;
                    $$ = $1;
               }
         | func_header LPAREN optVarList RPAREN scoped_statements {
                    ASTFuncDecl *decl = (ASTFuncDecl*)$1;
                    decl->addSource($3);
                    decl->addDests(new ASTVarList());
                    decl->addStatements($5);
                    decl->fixup();
                    gSymTab.pop_back();
                    gInFuncDecl--;
                    $$ = $1;
               }
         ;

func_header: FUNC WORD {
                    ASTFuncDecl *decl = new ASTFuncDecl($2, gSymTab.back());
                    $$ = decl;
                    gSymTab.push_back(decl->getSymbolTable());
                    gInFuncDecl++;
               }
           ;

func_call: PERCENT WORD LPAREN optSourceVars ARROW varList RPAREN { $$ = new ASTFuncCall($2, $4, $6, gSymTab.back()); }
         | PERCENT WORD LPAREN optSourceVars RPAREN { $$ = new ASTFuncCall($2, $4, new ASTVarList(), gSymTab.back()); }
         ;

pipeline: pipe_source kid_list sinkVar {
                    ASTVarList *svar = dynamic_cast<ASTVarList*>($1);
                    ASTKidDef  *skid = dynamic_cast<ASTKidDef*>($1);
                    ASTKidList *list = static_cast<ASTKidList*>($2);
                    if ( skid ) {
                        list->insertChild(skid);
                    }
                    if ( !list->hasChildren() ) {
                         /* $foo, $bar -> $baz  # is requested.  Insert a noop */
                         ASTKidDef *noop = new ASTKidDef(strdup("noop"));
                         noop->setInPipeType(ASTKidDef::PIPE);
                         list->insertChild(noop);
                    }
                    $$ = new ASTPipeline( svar, list, $3, (gInFuncDecl==0) );
               }
        ;


pipe_source: sourceVars { $$ = $1; }
           | kid_def { $$ = $1; }
           ;

sourceVars: sourceVar { $$ = new ASTVarList($1); }
          | sourceVars COMMA sourceVar { $1->addChild($3); $$ = $1; }
          ;

optSourceVars: { $$ = new ASTVarList(); }
             | sourceVars { $$ = $1; }
             ;

sourceVar: varRef { $$ = $1; }
         | varRef varFilter { $$ = $1; ((ASTVar*)$1)->setFilter($2); }
         | varRef varFilter varTarget { $$ = $1; ((ASTVar*)$1)->setFilter($2);  ((ASTVar*)$1)->setTargetPort($3); }
         | varRef varTarget { $$ = $1; ((ASTVar*)$1)->setTargetPort($2); }
         | varRef varTarget varFilter { $$ = $1; ((ASTVar*)$1)->setFilter($3);  ((ASTVar*)$1)->setTargetPort($2); }
         ;

varFilter: PERIOD WORD { $$ = $2;; }
         ;

varTarget: COLON WORD { $$ = $2; }
         ;

varRef: VARREF { $$ = new ASTVar($1, gSymTab.back()); }
      | ATSIGN VARREF { ASTVar *var = new ASTVar($2, gSymTab.back()); var->setBundle(); $$ = var; }
      ;

varList: varRef { $$ = new ASTVarList($1); }
       | varList COMMA varRef { $1->addChild($3); $$ = $1; }
       ;

optVarList: { $$ = new ASTVarList(); }
          | varList { $$ = $1; }
          ;

sinkVar: ARROW varRef { $$ = $2; }
       | { $$ = NULL; }
       ;


kid_list: { $$ = new ASTKidList(); }
        | pipe_sep kid_def kid_list {
               if ( $1 == (-1*ASTKidDef::DOUBLEPIPE) ) {
                    /* @|| was the separator.  Need bundle/unbundle */
                    ASTKidDef *bun = new ASTKidDef(strdup("bundle"));
                    ASTKidDef *unbun = new ASTKidDef(strdup("unbundle"));

                    bun->setInPipeType(ASTKidDef::PIPE);
                    unbun->setInPipeType(ASTKidDef::DOUBLEPIPE);
                    ((ASTKidDef*)$2)->setInPipeType(ASTKidDef::PIPE);
                    $3->insertChild($2);
                    $3->insertChild(unbun);
                    $3->insertChild(bun);
               } else {
                    ((ASTKidDef*)$2)->setInPipeType((ASTKidDef::KIDOUT)$1);
                    $3->insertChild($2);
               }
               $$ = $3;
          }
        ;

/* TODO:   PORT:kid */
kid_def: WORD COLON kid_def { ((ASTKidDef*)$3)->setSourcePort($1); $$ = $3; }
       | WORD { $$ = new ASTKidDef($1); }
       | WORD kid_def { ((ASTKidDef*)$2)->prefaceItem($1); $$ = $2; }
       | NUMBER { $$ = new ASTKidDef($1); }
       | NUMBER kid_def { ((ASTKidDef*)$2)->prefaceItem($1); $$ = $2; }
       | STRINGLIT { $$ = new ASTKidDef($1); }
       | STRINGLIT kid_def { ((ASTKidDef*)$2)->prefaceItem($1); $$ = $2; }
       ;

pipe_sep: PIPE { $$ = ASTKidDef::PIPE; }
        | DOUBLEPIPE { $$ = ASTKidDef::DOUBLEPIPE; }
        | ATDOUBLEPIPE { $$ = -1* ASTKidDef::DOUBLEPIPE; }
        ;

%%

