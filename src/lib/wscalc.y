%parse-param { void *callerState }
%parse-param {wscalcPart **wscalc_output}
%parse-param {int *wscalc_error}
%error-verbose

%{
/*-----------------------------------------------------------------------------
 * wscalc.y
 *
 * History:
 * 20111108 RDS Added grammar rules and code for conditional logic evaluation
 *              (e.g., expr && expr, expr || expr, !expr, etc)
 *---------------------------------------------------------------------------*/

#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "wscalc.h"
#include "wstypes.h"
#include "waterslide.h"
#include "datatypes/wsdt_string.h"
#include <stdio.h>

#ifndef __cplusplus
#ifndef true
#define true 1
#endif
#ifndef false
#define false 0
#endif
#endif

#define WSDTSTRING(x) ((wsdt_string_t*)(x.v.s->data))


typedef struct _paramList_t {
     wscalcPart *part;
     wscalcValue value;
     struct _paramList_t *next;
} paramList_t;

typedef wscalcValue (*calcFunction)(void *extra, paramList_t *params, void *runtimeToken);

typedef struct fEntry {
     char *name;
     calcFunction fnct;
     void *extra;
     size_t numParams;
} fEntry_t;

//the next four function pointers are where this code ties
//back to WS.  first two called during initialization to get labels
//second two called at runtime to assign and retrieve
//variable values
extern wscalcValue (*getVarValue)(void *, void *, int);
extern int (*setVarValue)(wscalcValue, int, void *, void *);
extern void (*destroyVar)(void *);
extern void (*flushVar)(void *);
extern uint8_t (*nameExists)(void *, void *);

extern void *(*initializeVarReference)(char *, char*, void *);
extern void *(*initializeLabelAssignment)(char *, char *, void *);
extern int (*assignLabel)(void *, void *);
extern void (*wsflush)(void *);

/* a few handy constants */
static const wscalcValue wscalcZERO = {0};

//these functions defined below. 
static wscalcValueType promoteTypes(const wscalcValueType a, const wscalcValueType b);
static wscalcPart *GetConstantProducer(wscalcValue);
static wscalcPart *GetConstantProducerInteger(int64_t);
static wscalcPart *GetPlusProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetMinusProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetModProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetMultiplyProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetDivideProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetPowerProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetLeftShiftProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetRightShiftProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetBitAndProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetBitIorProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetBitXorProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetComplementProducer(wscalcPart *);
static wscalcPart *GetLogicalOrProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetLessProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetLessEqualProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetGreaterProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetGreaterEqualProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetDoubleEqualProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetNotEqualProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetLogicalOrProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetLogicalAndProducer(wscalcPart *, wscalcPart *);
static wscalcPart *GetLogicalNotProducer(wscalcPart *);
static wscalcPart *GetStatementList(wscalcPart *currentList, wscalcPart *newStatement);
static wscalcPart *GetFunctionCallProducer(const fEntry_t *entry, paramList_t *params);
static wscalcPart *GetAssignment(void *varRef, int borrowedReference, wscalcPart *value, void *callerState);
static wscalcPart *GetVarValue(void *varRef, void *callerState);
static void *GetVarRef(char *name, char *keyindex, void *callerState);
static wscalcPart *NameExists(void *varRef, void *callerState);
static wscalcPart *GetUnaryMinusProducer(wscalcPart *aWSCalcPart);
static wscalcPart *GetIfProducer(wscalcPart *cond, wscalcPart *wsThen, wscalcPart *wsElse);
static wscalcPart *GetLabelProducer(char *newlabel, char *existinglabel, void *callerState);
static wscalcPart *GetFlushProducer(void *callerState);
static wscalcPart *GetCastProducer(char *, wscalcPart *);
static paramList_t *GetParamListProducer(paramList_t *list, wscalcPart * part);
static int wscalcerror(void *callerState, wscalcPart **wscalc_output, int *wscalc_error, const char *s);

static paramList_t* getParam(size_t n, paramList_t *list);
static wscalcValue VarValueExec(wscalcPart *aWSCalcPart, void *runtimetoken);
static wscalcValue doMathFunc(void* function, paramList_t *params, void *runtimeToken);
static wscalcValue enqueueExec(void* function, paramList_t *params, void *runtimeToken);
static wscalcValue queueOpFunc(void* function, paramList_t *params, void *runtimeToken);
static wscalcValue doStringFunc(void* function, paramList_t *params, void *runtimeToken);
static int convertStringToTime(const wsdt_string_t *wsstr, struct timeval *tv);
static wscalcValue doParseDateFunc(void* function, paramList_t *params, void *runtimeToken);

enum strFuncKey {
     STRF_LEN = 1,
     STRF_FIND,
     STRF_SUBSTR,
     STRF_REPL,
     STRF_REPLALL
};

static fEntry_t *getFunction(fEntry_t *lookupTable, char *lookupName);
static fEntry_t functionTable[] = {
     {"sin",        doMathFunc,    sin,                1},
     {"cos",        doMathFunc,    cos,                1},
     {"atan",       doMathFunc,    atan,               1},
     {"ln",         doMathFunc,    log,                1},
     {"exp",        doMathFunc,    exp,                1},
     {"sqrt",       doMathFunc,    sqrt,               1},
     {"abs",        doMathFunc,    fabs,               1},
     {"trunc",      doMathFunc,    trunc,              1},
     {"floor",      doMathFunc,    floor,              1},
     {"ceil",       doMathFunc,    ceil,               1},
     {"round",      doMathFunc,    round,              1},
     {"enqueue",    enqueueExec,   NULL,               3},
     {"qcount",     queueOpFunc,   (void*)WSR_CNT,     1},
     {"qsum",       queueOpFunc,   (void*)WSR_SUM,     1},
     {"qavg",       queueOpFunc,   (void*)WSR_AVG,     1},
     {"qmax",       queueOpFunc,   (void*)WSR_MAX,     1},
     {"qmin",       queueOpFunc,   (void*)WSR_MIN,     1},
     {"qspan",      queueOpFunc,   (void*)WSR_SPAN,    1},
     {"qstdev",     queueOpFunc,   (void*)WSR_STDEV,   1},
     {"strlen",     doStringFunc,  (void*)STRF_LEN,    1},
     {"strfind",    doStringFunc,  (void*)STRF_FIND,   2},
     {"substr",     doStringFunc,  (void*)STRF_SUBSTR, 3},
     {"strRepl",    doStringFunc,  (void*)STRF_REPL,   3},
     {"strReplAll", doStringFunc,  (void*)STRF_REPLALL,3},
     {"parseDate",  doParseDateFunc, NULL,             1},
     {0, 0, 0, 0}
};



%}
%union {
     char name[50];
     wscalcValue value;
     struct _wscalcPart *aWSCalcPart;
     void *varRef;
     struct _paramList_t *paramList;
}
%token <value> NUMBER;
%token <name> NAME;
%token <name> EXISTS;
%token <name> CAST TYPE;
%token SEMICOLON LPAREN RPAREN LBRACKET RBRACKET COMMA IF THEN ELSE ENDIF
%token <aWSCalcPart> PLUS MINUS SLASH ASTERISK EQUALS POWER INCREMENT DECREMENT INCREMENTC DECREMENTC SLASHC ASTERISKC
%token <aWSCalcPart> GREATER GREATEREQUAL LESS LESSEQUAL DOUBLEEQUAL NOTEQUAL AND OR NOT
%token <aWSCalcPart> FALSE TRUE WSCLABEL WSFLUSH
%token <aWSCalcPart> MOD MODC BITAND BITANDC BITIOR BITORC BITXOR BITXORC COMPLMNT LEFTSHIFT LEFTSHIFTC RIGHTSHIFT RIGHTSHIFTC
%type <aWSCalcPart> statement_list statement expr
%type <varRef> varRef
%type <paramList> paramList

%right INCREMENTC DECREMENTC ASTERISKC SLASHC
%right MODC BITANDC BITIORC BITXORC LEFTSHIFTC RIGHTSHIFTC 
%right EQUALS
%left OR
%left AND
%left BITIOR
%left BITXOR
%left BITAND
%left DOUBLEEQUAL NOTEQUAL
%left GREATER GREATEREQUAL LESS LESSEQUAL
%left LEFTSHIFT RIGHTSHIFT
%left MINUS PLUS
%left POWER ASTERISK SLASH MOD
%nonassoc UMINUS
%nonassoc COMPLMNT
%nonassoc NOT
%left INCREMENT DECREMENT
%%
statement_list: statement endstmt { wscalcPart *aWSCalcPart = GetStatementList(NULL, $1);
                                    $$ = aWSCalcPart;
                                    *wscalc_output = aWSCalcPart;
                                  }
        | statement_list statement endstmt { wscalcPart *aWSCalcPart = GetStatementList($1, $2);
                                    $$ = aWSCalcPart;
                                    *wscalc_output = aWSCalcPart;}
        ;
endstmt: SEMICOLON
       | SEMICOLON endstmt
       ;
statement: varRef EQUALS expr     { $$ = GetAssignment($1, false, $3, callerState); }
        | varRef INCREMENTC expr
             { $$ = GetAssignment($1, true, GetPlusProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef DECREMENTC expr
             { $$ = GetAssignment($1, true, GetMinusProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef MODC expr
             { $$ = GetAssignment($1, true, GetModProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef ASTERISKC expr
             { $$ = GetAssignment($1, true, GetMultiplyProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef SLASHC expr
             { $$ = GetAssignment($1, true, GetDivideProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef LEFTSHIFTC expr
             { $$ = GetAssignment($1, true, GetLeftShiftProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef RIGHTSHIFTC expr
             { $$ = GetAssignment($1, true, GetRightShiftProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef BITANDC expr
             { $$ = GetAssignment($1, true, GetBitAndProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef BITIORC expr
             { $$ = GetAssignment($1, true, GetBitIorProducer(GetVarValue($1, callerState), $3), callerState); }
        | varRef BITXORC expr
             { $$ = GetAssignment($1, true, GetBitXorProducer(GetVarValue($1, callerState), $3), callerState); }
        | IF expr THEN statement_list ENDIF
             { $$ = GetIfProducer($2, $4, NULL);}
        | IF expr THEN statement_list ELSE statement_list ENDIF
             { $$ = GetIfProducer($2, $4, $6);}
        | expr 		        { $$ = $1; }
	;
varRef: NAME                  { $$ = GetVarRef($1, NULL, callerState); }
        | NAME LBRACKET NAME RBRACKET { $$ = GetVarRef($1, $3, callerState); }
     ;
paramList: expr               { $$ = GetParamListProducer(NULL, $1); }
        | expr COMMA paramList { $$ = GetParamListProducer($3, $1); }
     ;
expr: expr PLUS expr		{ $$ = GetPlusProducer($1, $3); }
        | expr MINUS expr 	{ $$ = GetMinusProducer($1, $3); }
        | MINUS expr %prec UMINUS  { $$ = GetUnaryMinusProducer($2); }
        | expr ASTERISK expr 	{ $$ = GetMultiplyProducer($1, $3); }
        | expr SLASH expr	{ $$ = GetDivideProducer($1, $3); }
        | expr POWER expr     { $$ = GetPowerProducer($1, $3); }
        | expr LESS expr     { $$ = GetLessProducer($1, $3); }
        | expr LESSEQUAL expr     { $$ = GetLessEqualProducer($1, $3); }
        | expr GREATER expr     { $$ = GetGreaterProducer($1, $3); }
        | expr GREATEREQUAL expr     { $$ = GetGreaterEqualProducer($1, $3); }
        | expr DOUBLEEQUAL expr     { $$ = GetDoubleEqualProducer($1, $3); }
        | expr NOTEQUAL expr     { $$ = GetNotEqualProducer($1, $3); }
        | expr OR expr           { $$ = GetLogicalOrProducer($1, $3); }
        | expr AND expr           { $$ = GetLogicalAndProducer($1, $3); }
        | expr MOD expr       { $$ = GetModProducer($1, $3); }
        | expr LEFTSHIFT expr { $$ = GetLeftShiftProducer($1, $3); }
        | expr RIGHTSHIFT expr { $$ = GetRightShiftProducer($1, $3); }
        | expr BITAND expr    { $$ = GetBitAndProducer($1, $3); }
        | expr BITIOR expr    { $$ = GetBitIorProducer($1, $3); }
        | expr BITXOR expr    { $$ = GetBitXorProducer($1, $3); }
        | COMPLMNT expr       { $$ = GetComplementProducer($2); }
        | NOT expr            { $$ = GetLogicalNotProducer($2); }
        | EXISTS LPAREN varRef RPAREN
                              { $$ = NameExists($3, callerState); /* RDS */}
        | WSCLABEL LPAREN NAME RPAREN
                              { $$ = GetLabelProducer($3, NULL, callerState); }
        | WSCLABEL LPAREN NAME COMMA NAME RPAREN
                              { $$ = GetLabelProducer($3, $5, callerState); }
        | CAST LPAREN TYPE COMMA expr RPAREN { $$ = GetCastProducer($3, $5); }
        | NAME LPAREN paramList RPAREN { struct fEntry *entry = getFunction(functionTable, $1); 
                                    if (entry==NULL) {
                                        wscalcerror(callerState, wscalc_output, wscalc_error,
                                                     "No such function");
                                    } else {
                                        $$ = GetFunctionCallProducer(entry, $3);
                                    }
                                }
        | varRef INCREMENT
             { $$ = GetAssignment($1, true, GetPlusProducer(GetVarValue($1,callerState),GetConstantProducerInteger(1)),callerState);}
        | varRef DECREMENT
             { $$ = GetAssignment($1, true, GetMinusProducer(GetVarValue($1,callerState),GetConstantProducerInteger(1)),callerState);}
        | WSFLUSH
             { $$ = GetFlushProducer(callerState); }
        | LPAREN expr RPAREN	{ $$ = $2; }
        | FALSE {$$ = GetConstantProducerInteger(0); }
        | TRUE {$$ = GetConstantProducerInteger(1); }
        | NUMBER                { $$ = GetConstantProducer($1); }
        | varRef { $$ = GetVarValue($1, callerState); }
	;
%%
#include <stdio.h>
#include <math.h>

static fEntry_t *getFunction(fEntry_t *lookupTable, char *lookupName) {
     int i;
     for (i = 0; lookupTable[i].name!=0; i++) {
          if (strcasecmp(lookupName, lookupTable[i].name)==0) {
               return &lookupTable[i];
          }
     }
     return 0;
}

static int wscalcerror(void *callerState, wscalcPart **wscalc_output, int *wscalc_error, const char *s)
{
     error_print("Calc Error:  %s", s);
     *wscalc_error=1;
	return 0;
}

/**
   Helper functions for managing a lookup tables.
   These functions are useful for setting up local
   variables at "compile" time.
*/

void *wscalc_createLookupTable(void) {
     void *tmp = calloc(1, sizeof(wscalc_lookupTableEntry));

     if (!tmp) {
          error_print("failed wscalc_int_createLookupTable calloc of tmp");
          return NULL;
     }

     return tmp;
}

wscalc_lookupTableEntry *wscalc_getEntry(char *name, void *table, int *created) {
     wscalc_lookupTableEntry *ptr = table;
     wscalc_lookupTableEntry *lastPtr = NULL;
     *created = 0;
     while (ptr!=NULL) {
          if (strcmp(ptr->name, name)==0) {
               return ptr;
          } else {
               lastPtr = ptr;
               ptr = ptr->next;
          }
     }
     lastPtr->next = calloc(1, sizeof(wscalc_lookupTableEntry));
     if (!lastPtr->next) {
          error_print("failed wscalc_getEntry calloc of lastPtr->next");
          return NULL;
     }
     strcpy(lastPtr->next->name, name);
     *created = 1;
     return lastPtr->next;
     
}

void wscalc_destroyTable(void *table) {
     wscalc_lookupTableEntry *ptr = table;
     wscalc_lookupTableEntry *nextPtr = NULL;

     while (ptr!=NULL) {
          nextPtr = ptr->next;
          free(ptr);
          ptr = nextPtr;
     }
}

/**
   The rest of this file defines the actual function
   pointers that get called during wscalc execution.
*/

/*==== flush operation ====*/

static void FlushDestroy(wscalcPart *aWSCalcPart) {
     free(aWSCalcPart);
}

static void FlushFlush(wscalcPart *aWSCalcPart) {
     //do nothing
}

static wscalcValue FlushExec(wscalcPart *aWSCalcPart, void *runtimetoken) {
     wsflush(aWSCalcPart->params);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     res.v.u = 1;
     return res;
}


static wscalcPart *GetFlushProducer(void *callerState) {
     wscalcPart *answer = calloc(1, sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetFlushProducer calloc of answer");
          return NULL;
     }
     answer->params = callerState;
     answer->go = FlushExec;
     answer->destroy = FlushDestroy;
     answer->flush = FlushFlush;
     return answer;
}

/*==== label operation ====*/

static void LabelDestroy(wscalcPart *aWSCalcPart) {
     free(aWSCalcPart);
}

static void LabelFlush(wscalcPart *aWSCalcPart) {
     //do nothing
}

static wscalcValue LabelExec(wscalcPart *aWSCalcPart, void *runtimetoken) {
     wscalcValue res;
     res.type = WSCVT_INTEGER;
     res.v.i = assignLabel(aWSCalcPart->params, runtimetoken);
     return res;
}


static wscalcPart *GetLabelProducer(char *newlabel, char *existinglabel, void *callerState) {
     wscalcPart *answer = calloc(1, sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetLabelProducer calloc of answer");
          return NULL;
     }
     answer->params = initializeLabelAssignment(newlabel, existinglabel, callerState);
     answer->go = LabelExec;
     answer->destroy = LabelDestroy;
     answer->flush = LabelFlush;
     return answer;
}


/*====== wscalcPart for windowed operations ====*/
static wscalcValue enqueueExec(void* vfunc, paramList_t *params, void *runtimeToken)
{
     /* Verify (as well as we can, that the first parameter is a varRef */
     if ( getParam(0, params)->part->go != VarValueExec ) {
          error_print("First parameter to enqueue() must be a variable name!");
          return wscalcZERO;
     }
     void *varRef = getParam(0, params)->part->params;
     int sizeValue = getWSCVInt(getParam(1, params)->value);
     wscalcValue value = getParam(2, params)->value;
     setVarValue(value, sizeValue, varRef, runtimeToken);
     return value;
}

static wscalcValue queueOpFunc(void* vfunc, paramList_t *params, void *runtimeToken)
{
     int func = (int)(intptr_t)vfunc; // Convert back to WSR_*
     /* Verify (as well as we can, that the first parameter is a varRef */
     if ( getParam(0, params)->part->go != VarValueExec ) {
          error_print("Parameter to queue functions must be a variable name!");
          return wscalcZERO;
     }
     void *varRef = getParam(0, params)->part->params;

     return getVarValue(varRef, runtimeToken, func);
}

/*====== wscalcPart for ifs ====*/

typedef struct _wsIf {
     wscalcPart *cond;
     wscalcPart *wsThen;
     wscalcPart *wsElse;
} wsIf_type;

static void IfDestroy(wscalcPart *aWSCalcPart) {
     wsIf_type *ifs = aWSCalcPart->params;
     if (ifs->cond) {
          ifs->cond->destroy(ifs->cond);
     }

     if (ifs->wsThen) {
          ifs->wsThen->destroy(ifs->wsThen);
     }

     if (ifs->wsElse) {
          ifs->wsElse->destroy(ifs->wsElse);
     }

     free(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void IfFlush(wscalcPart *aWSCalcPart) {
     wsIf_type *ifs = aWSCalcPart->params;

     if (ifs->cond) {
          ifs->cond->flush(ifs->cond);
     }

     if (ifs->wsThen) {
          ifs->wsThen->flush(ifs->wsThen);
     }

     if (ifs->wsElse) {
          ifs->wsElse->flush(ifs->wsElse);
     }
}

static wscalcValue IfExec(wscalcPart *aWSCalcPart, void *runtimetoken) {
     wsIf_type *ifs = aWSCalcPart->params;
     if (!ifs->cond) {
          return wscalcZERO;
     }

     wscalcValue ans = ifs->cond->go(ifs->cond, runtimetoken);
     if (getWSCVBool(ans)) {
          if (ifs->wsThen) {
               return ifs->wsThen->go(ifs->wsThen, runtimetoken);
          } else {
               return wscalcZERO;
          }
     } else {
          if (ifs->wsElse) {
               return ifs->wsElse->go(ifs->wsElse, runtimetoken);
          } else {
               return wscalcZERO;
          }
     }
}

static wscalcPart *GetIfProducer(wscalcPart *cond, wscalcPart *wsThen, wscalcPart *wsElse) {
     wscalcPart *answer = calloc(1, sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetIfProducer calloc of answer");
          return NULL;
     }
     wsIf_type *ifs = calloc(1, sizeof(wscalcPart));
     if (!ifs) {
          error_print("failed GetIfProducer calloc of ifs");
          return NULL;
     }
     ifs->cond = cond;
     ifs->wsThen = wsThen;
     ifs->wsElse = wsElse;
     answer->params = ifs;
     answer->go = IfExec;
     answer->flush=IfFlush;
     answer->destroy=IfDestroy;
     return answer;
}


/*==== wscalcPart for unary minus ====*/
static void UnaryMinusDestroy(wscalcPart *aWSCalcPart) {
     ((wscalcPart*)aWSCalcPart->params)->destroy(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void UnaryMinusFlush(wscalcPart *aWSCalcPart) {
     ((wscalcPart*)aWSCalcPart->params)->flush(aWSCalcPart->params);
}

static wscalcValue UnaryMinusExec(wscalcPart *aWSCalcPart, void *runtimetoken) {
     wscalcValue res = ((wscalcPart*)aWSCalcPart->params)->go(aWSCalcPart->params, runtimetoken);
     if ( res.type < WSCVT_BOOLEAN ) { // Integral types
          res.v.i = - res.v.i;
          if ( res.type == WSCVT_UINTEGER ) res.type = WSCVT_INTEGER;
     } else if ( res.type == WSCVT_DOUBLE ) {
          res.v.d = - res.v.d;
     } else {
          error_print("Cannot take Unary Minus of this type.");
     }
     return res;
}

static wscalcPart *GetUnaryMinusProducer(wscalcPart *aWSCalcPart) {
     wscalcPart *answer = (wscalcPart*)malloc(sizeof(wscalcPart));
     if (!answer) {
	  error_print("failed GetUnaryMinusProducer malloc of answer");
	  return NULL;
     }
     answer->params = malloc(sizeof(void*));
     if (!answer->params) {
	  error_print("failed GetUnaryMinusProducer malloc of answer->params");
	  return NULL;
     }
     answer->params = aWSCalcPart;
     answer->go = UnaryMinusExec;
     answer->destroy = UnaryMinusDestroy;
     answer->flush = UnaryMinusFlush;
     return answer;
}

/*==== wscalcPart for variable retrieval ====*/
static void GetVarValueDestroy(wscalcPart *aWSCalcPart) {
     destroyVar(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void GetVarValueFlush(wscalcPart *aWSCalcPart) {
     flushVar(aWSCalcPart->params);
}

static wscalcValue VarValueExec(wscalcPart *aWSCalcPart, void *runtimetoken) {
     return getVarValue(aWSCalcPart->params, runtimetoken, WSR_TAIL);
}

static wscalcPart *GetVarValue(void* varRef, void *callerState) {
     wscalcPart *answer = (wscalcPart*)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetVarValue malloc of answer");
          return NULL;
     }
     answer->params = varRef;
     answer->go = VarValueExec;
     answer->destroy = GetVarValueDestroy;
     answer->flush = GetVarValueFlush;
     return answer;
}

/*==== for variable reference ====*/
static void *GetVarRef(char *name, char *keyindex, void *callerState) {
     return initializeVarReference(name, keyindex, callerState);
}


/*====wscalcPart for variable lookup ====*/
static void NameExistsDestroy(wscalcPart *aWSCalcPart) {
     destroyVar(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void NameExistsFlush(wscalcPart *aWSCalcPart) {
     flushVar(aWSCalcPart->params);
}

static wscalcValue NameExistsExec(wscalcPart *aWSCalcPart, void *runtimetoken) {
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     res.v.u = nameExists(aWSCalcPart->params, runtimetoken);
     return res;
}

static wscalcPart *NameExists(void *name, void *callerState) {
     wscalcPart *answer = (wscalcPart*)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed NameExists malloc of answer");
          return NULL;
     }
     answer->params = name;
     answer->go = NameExistsExec;
     answer->destroy = NameExistsDestroy;
     answer->flush = NameExistsFlush;
     return answer;
}



/*==== wscalcPart for variable assignment ====*/
static void AssignmentDestroy(wscalcPart *aWSCalcPart) {
     void **theParams = (void**)aWSCalcPart->params;
     int isBorrowed = (theParams[1] != NULL);
     if ( !isBorrowed )
          destroyVar(theParams[0]);
     ((wscalcPart*)theParams[2])->destroy((wscalcPart*)theParams[2]);
     free(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void AssignmentFlush(wscalcPart *aWSCalcPart) {
     void **theParams = (void**)aWSCalcPart->params;
     flushVar(theParams[0]);
     ((wscalcPart*)theParams[2])->flush((wscalcPart*)theParams[2]);
}

static wscalcValue AssignmentExec(wscalcPart *aWSCalcPart, void *runtimetoken) {
     void **theParams = (void**)aWSCalcPart->params;
     wscalcValue p = ((wscalcPart*)theParams[2])->go((wscalcPart*)theParams[2], runtimetoken);
     if ( p.type == WSCVT_STRING ) {
          wsdata_t *newws = wsdata_alloc(dtype_string);
          wsdata_t *oldws = p.v.s;
          wsdt_string_t *nstr = (wsdt_string_t*)newws->data;
          wsdt_string_t *ostr = (wsdt_string_t*)oldws->data;
          nstr->len = ostr->len;
          nstr->buf = ostr->buf;
          wsdata_add_reference(newws);
          wsdata_assign_dependency(oldws, newws);
          wsdata_delete(oldws);
          p.v.s = newws;
     }
     setVarValue(p, 0, theParams[0], runtimetoken);
     return p;
}

static wscalcPart *GetAssignment(void *varRef, int isBorrwedReference, wscalcPart *value, void *callerState) {
     wscalcPart *answer = (wscalcPart*)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetAssignment malloc of answer");
          return NULL;
     }
     answer->params = malloc(sizeof(void*)*3);
     if (!answer->params) {
          error_print("failed GetAssignment malloc of answer->params");
          return NULL;
     }
     void **theParams = (void**)answer->params;
     theParams[0]=varRef;
     theParams[1]=isBorrwedReference ? varRef : NULL;
     theParams[2]=value;
     answer->go=AssignmentExec;
     answer->destroy=AssignmentDestroy;
     answer->flush=AssignmentFlush;
     return answer;
}

/*==== wscalcPart for executing single parameter function */
static void ExecParamList(paramList_t *list, void *runtimeToken) {
     while ( list != NULL ) {
          list->value = list->part->go(list->part, runtimeToken);
          list = list->next;
     }
};

static void CleanParamList(paramList_t *list) {
     while ( list != NULL ) {
          if ( list->value.type == WSCVT_STRING )
               wsdata_delete(list->value.v.s);
          list->value = wscalcZERO;
          list = list->next;
     }
};


static void FunctionCallProducerDestroy(wscalcPart *aWSCalcPart) {
     void **theParams = (void**)aWSCalcPart->params;
     paramList_t *paramList = (paramList_t*)theParams[2];
     while ( paramList != NULL ) {
          paramList_t *next = paramList->next;
          paramList->part->destroy(paramList->part);
          free(paramList);
          paramList = next;
     }
     free(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void FunctionCallProducerFlush(wscalcPart *aWSCalcPart) {
     void **theParams = (void**)aWSCalcPart->params;
     paramList_t *paramList = (paramList_t*)theParams[2];
     while ( paramList != NULL ) {
          paramList->part->flush(paramList->part);
          paramList = paramList->next;
     }
}

static wscalcValue FunctionCallProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     void **theParams = aWSCalcPart->params;
     wscalcValue (*f)(void *, paramList_t*) = theParams[0];
     paramList_t *params = (paramList_t*)theParams[2];
     ExecParamList(params, runtimeToken);

     wscalcValue res = (*f)(theParams[1], params); /* Single parameter */

     CleanParamList(params);

     return res;
}

static wscalcPart *GetFunctionCallProducer(const fEntry_t *entry, paramList_t *params) {
     wscalcPart *answer = (wscalcPart*)calloc(1, sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetFunctionCallProducer calloc of answer");
          return NULL;
     }
     answer->params = malloc(sizeof(void*)*3);
     if (!answer->params) {
          error_print("failed GetFunctionCallProducer malloc of answer->params");
          return NULL;
     }
     size_t numParams = 0;
     paramList_t *tp = params;
     while ( tp ) {
          numParams++;
          tp = tp->next;
     }

     if ( numParams != entry->numParams ) {
          error_print("Error:  Incorrect number of parameters for function %s().  (Want %zu, have %zu)",
               entry->name, entry->numParams, numParams);
          return NULL;
     }

     void **theParams = (void**)answer->params;
     theParams[0]=entry->fnct;
     theParams[1]=entry->extra;
     theParams[2]=params;
     answer->go = FunctionCallProducerExec;
     answer->destroy = FunctionCallProducerDestroy;
     answer->flush = FunctionCallProducerFlush;
     return answer;
}

typedef double (*mathFunc)(double);
static wscalcValue doMathFunc(void *vfunc, paramList_t *params, void *runtimeToken)
{
     mathFunc f = (mathFunc)vfunc;
     wscalcValue res;
     res.type = WSCVT_DOUBLE;
     res.v.d = (*f)(getWSCVDouble(params->value)); /* Use 1st value */
     return res;
}

/*==== wscalcPart for execution of a statement list ====*/

static void StatementListDestroy(wscalcPart *aWSCalcPart) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     if (parts[0]!=NULL) {
          parts[0]->destroy(parts[0]);
     }
     if (parts[1]!=NULL) {
          parts[1]->destroy(parts[1]);
     }
     free(parts);
     free(aWSCalcPart);
}

static void StatementListFlush(wscalcPart *aWSCalcPart) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     if (parts[0]!=NULL) {
          parts[0]->flush(parts[0]);
     }
     
     if (parts[1]!=NULL) {
          parts[1]->flush(parts[1]);
     }
}

static wscalcValue StatementListExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue answer = wscalcZERO;
     if (parts[0]!=NULL) {
          answer = parts[0]->go(parts[0], runtimeToken);
          if ( answer.type == WSCVT_STRING && answer.v.s ) {
               wsdata_delete(answer.v.s);
               answer.v.s = NULL;
          }
     }

     if (parts[1]!=NULL) {
          answer = parts[1]->go(parts[1], runtimeToken);
          if ( answer.type == WSCVT_STRING && answer.v.s ) {
               wsdata_delete(answer.v.s);
               answer.v.s = NULL;
          }
     }

     return answer;
}

static wscalcPart *GetStatementList(wscalcPart *currentList, wscalcPart *newStatement) {
     wscalcPart *answer = (wscalcPart *)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetStatementList malloc of answer");
          return NULL;
     }
     answer->params = malloc(sizeof(wscalcPart*)*2);
     if (!answer->params) {
          error_print("failed GetStatementList malloc of answer->params");
          return NULL;
     }
     wscalcPart **parts = (wscalcPart**)answer->params;
     parts[0]=currentList;
     parts[1]=newStatement;
     answer->go = StatementListExec;
     answer->destroy = StatementListDestroy;
     answer->flush = StatementListFlush;
     return answer;
}

/*==== wscalcPart for addition, subtraction, multiplication, subtraction  ====*/
static void BinaryInfixDestroy(wscalcPart *aWSCalcPart) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     parts[0]->destroy(parts[0]);
     parts[1]->destroy(parts[1]);
     free(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void BinaryInfixFlush(wscalcPart *aWSCalcPart) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     parts[0]->flush(parts[0]);
     parts[1]->flush(parts[1]);
}

static wscalcPart *GetBinaryInfixProducer(wscalcPart *first, wscalcPart *second, 
				wscalcValue(*specificProducer)(wscalcPart *, void*)) {
     wscalcPart *answer = (wscalcPart *)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetBinaryInfixProducer malloc of answer");
          return NULL;
     }
     answer->params = malloc(sizeof(void*)*2);
     if (!answer->params) {
          error_print("failed GetBinaryInfixProducer malloc of answer->params");
          return NULL;
     }
     wscalcPart **parts = (wscalcPart**)answer->params;
     parts[0]=first;
     parts[1]=second;
     answer->go = specificProducer;
     answer->destroy=BinaryInfixDestroy;
     answer->flush=BinaryInfixFlush;
     return answer;
}




static wscalcValue PlusProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) + getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) + getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.d = getWSCVDouble(a) + getWSCVDouble(b);    break;
     case WSCVT_STRING: {
          wsdata_t *aD = getWSCVString(a);
          wsdt_string_t *aS = (wsdt_string_t*)aD->data;
          wsdata_t *bD = getWSCVString(b);
          wsdt_string_t *bS = (wsdt_string_t*)bD->data;
          size_t fullLen = aS->len + bS->len;

          char *buf = NULL;
          res.v.s = createStringData(fullLen, &buf);
          memcpy(buf,           aS->buf, aS->len);
          memcpy(buf + aS->len, bS->buf, bS->len);
          wsdata_delete(aD);
          wsdata_delete(bD);
          break;
     }
     case WSCVT_TIME:    {
          struct timeval tva = getWSCVTime(a);
          struct timeval tvb = getWSCVTime(b);
          timeradd(&tva, &tvb, &res.v.t);
          break;
     }
     }
     return res;
}

static wscalcPart *GetPlusProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, PlusProducerExec);
}

static wscalcValue MinusProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) - getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) - getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.d = getWSCVDouble(a) - getWSCVDouble(b);    break;
     case WSCVT_STRING:       error_print("Cannot subtract from strings."); res.v.s = NULL; break;
     case WSCVT_TIME:    {
          struct timeval tva = getWSCVTime(a);
          struct timeval tvb = getWSCVTime(b);
          timersub(&tva, &tvb, &res.v.t);
          break;
     }
     }
     return res;
}

static wscalcPart *GetMinusProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, MinusProducerExec);
}

static wscalcValue MultiplyProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) * getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) * getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.d = getWSCVDouble(a) * getWSCVDouble(b);    break;
     case WSCVT_STRING:       error_print("Cannot multiply from strings."); res.v.s = NULL; break;
     case WSCVT_TIME:         error_print("Cannot multiply time."); res.v.i = 0; break;
     }
     return res;
}

static wscalcPart *GetMultiplyProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, MultiplyProducerExec);
}

static wscalcValue DivideProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) / getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) / getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.d = getWSCVDouble(a) / getWSCVDouble(b);    break;
     case WSCVT_STRING:       error_print("Cannot divide from strings."); res.v.s = NULL; break;
     case WSCVT_TIME:         error_print("Cannot divide time."); res.v.i = 0; break;
     }
     return res;
}

static wscalcPart *GetDivideProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, DivideProducerExec);
}

static wscalcValue PowerProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     double answ = pow(getWSCVDouble(a), getWSCVDouble(b));
     switch (res.type) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = (int64_t) answ;     break;
     case WSCVT_UINTEGER:     res.v.u = (uint64_t) answ;    break;
     case WSCVT_DOUBLE:       res.v.d = answ;               break;
     case WSCVT_STRING:       error_print("Cannot take a power from strings."); res.v.s = NULL; break;
     case WSCVT_TIME:         error_print("Cannot take a power of time."); res.v.i = 0; break;
     }
     return res;
}

static wscalcPart *GetPowerProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, PowerProducerExec);
}


static wscalcValue ModProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) % getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) % getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       error_print("Cannot perform the mod operation on a double."); break;
     case WSCVT_STRING:       error_print("Cannot perform the mod operation on a string."); break;
     case WSCVT_TIME:         error_print("Cannot perform the mod operation on a time."); break;
     }
     return res;
}

static wscalcPart *GetModProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, ModProducerExec);
}

static wscalcValue LeftShiftProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) << getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) << getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       error_print("Cannot perform the left shift operation on a double."); break;
     case WSCVT_STRING:       error_print("Cannot perform the left shift operation on a string."); break;
     case WSCVT_TIME:         error_print("Cannot perform the left shift operation on a time."); break;
     }
     return res;
}

static wscalcPart *GetLeftShiftProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, LeftShiftProducerExec);
}

static wscalcValue RightShiftProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) >> getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) >> getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       error_print("Cannot perform the right shift operation on a double."); break;
     case WSCVT_STRING:       error_print("Cannot perform the right shift operation on a string."); break;
     case WSCVT_TIME:         error_print("Cannot perform the right shift operation on a time."); break;
     }
     return res;
}

static wscalcPart *GetRightShiftProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, RightShiftProducerExec);
}

static wscalcValue BitAndProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) & getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) & getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       error_print("Cannot perform the bitwise-and operation on a double."); break;
     case WSCVT_STRING:       error_print("Cannot perform the bitwise-and operation on a string."); break;
     case WSCVT_TIME:         error_print("Cannot perform the bitwise-and operation on a time."); break;
     }
     return res;
}

static wscalcPart *GetBitAndProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, BitAndProducerExec);
}

static wscalcValue BitIorProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) | getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) | getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       error_print("Cannot perform the bitwise-or operation on a double."); break;
     case WSCVT_STRING:       error_print("Cannot perform the bitwise-or operation on a string."); break;
     case WSCVT_TIME:         error_print("Cannot perform the bitwise-or operation on a time."); break;
     }
     return res;
}

static wscalcPart *GetBitIorProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, BitIorProducerExec);
}

static wscalcValue BitXorProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = promoteTypes(a.type, b.type);
     switch ( res.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = getWSCVInt(a) ^ getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) ^ getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       error_print("Cannot perform the bitwise-xor operation on a double."); break;
     case WSCVT_STRING:       error_print("Cannot perform the bitwise-xor operation on a string."); break;
     case WSCVT_TIME:         error_print("Cannot perform the bitwise-xor operation on a time."); break;
     }
     return res;
}

static wscalcPart *GetBitXorProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, BitXorProducerExec);
}



static wscalcValue LessProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     if ( a.type == WSCVT_STRING || b.type == WSCVT_STRING ) {
          if ( a.type != b.type ) {
               error_print("Cannot compare strings to numbers");
               res.v.u = 0;
               return res;
          }
     }
     switch ( promoteTypes(a.type, b.type) ) {
     case WSCVT_BOOLEAN: /* promote to integer */
     case WSCVT_INTEGER:      res.v.u = getWSCVInt(a) < getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) < getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.u = getWSCVDouble(a) < getWSCVDouble(b);    break;
     case WSCVT_STRING:       {
          wsdata_t *aD = getWSCVString(a);
          wsdt_string_t *aS = (wsdt_string_t*)aD->data;
          wsdata_t *bD = getWSCVString(b);
          wsdt_string_t *bS = (wsdt_string_t*)bD->data;
          size_t min = (aS->len < bS->len) ? aS->len : bS->len;
          int cmp = memcmp(aS->buf, bS->buf, min);
          if ( cmp == 0 ) {
               /* strings match, compare length */
               res.v.u = aS->len < bS->len;
          } else {
               res.v.u = (cmp < 0);
          }
          wsdata_delete(aD);
          wsdata_delete(bD);
          break;
     }
     case WSCVT_TIME:       res.v.u = getWSCVDouble(a) < getWSCVDouble(b);    break;
     }
     return res;
}

static wscalcPart *GetLessProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, LessProducerExec);
}
static wscalcValue LessEqualProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     if ( a.type == WSCVT_STRING || b.type == WSCVT_STRING ) {
          if ( a.type != b.type ) {
               error_print("Cannot compare strings to numbers");
               res.v.u = 0;
               return res;
          }
     }
     switch ( promoteTypes(a.type, b.type) ) {
     case WSCVT_BOOLEAN: /* promote to integer */
     case WSCVT_INTEGER:      res.v.u = getWSCVInt(a) <= getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) <= getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.u = getWSCVDouble(a) <= getWSCVDouble(b);    break;
     case WSCVT_STRING:       {
          wsdata_t *aD = getWSCVString(a);
          wsdt_string_t *aS = (wsdt_string_t*)aD->data;
          wsdata_t *bD = getWSCVString(b);
          wsdt_string_t *bS = (wsdt_string_t*)bD->data;
          size_t min = (aS->len < bS->len) ? aS->len : bS->len;
          int cmp = memcmp(aS->buf, bS->buf, min);
          if ( cmp == 0 ) {
               /* strings match, compare length */
               res.v.u = aS->len <= bS->len;
          } else {
               res.v.u = (cmp <= 0);
          }
          wsdata_delete(aD);
          wsdata_delete(bD);
          break;
     }
     case WSCVT_TIME:       res.v.u = getWSCVDouble(a) <= getWSCVDouble(b);    break;
     }
     return res;
}

static wscalcPart *GetLessEqualProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, LessEqualProducerExec);
}
static wscalcValue GreaterProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     if ( a.type == WSCVT_STRING || b.type == WSCVT_STRING ) {
          if ( a.type != b.type ) {
               error_print("Cannot compare strings to numbers");
               res.v.u = 0;
               return res;
          }
     }
     switch ( promoteTypes(a.type, b.type) ) {
     case WSCVT_BOOLEAN: /* promote to integer */
     case WSCVT_INTEGER:      res.v.u = getWSCVInt(a) > getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) > getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.u = getWSCVDouble(a) > getWSCVDouble(b);    break;
     case WSCVT_STRING:       {
          wsdata_t *aD = getWSCVString(a);
          wsdt_string_t *aS = (wsdt_string_t*)aD->data;
          wsdata_t *bD = getWSCVString(b);
          wsdt_string_t *bS = (wsdt_string_t*)bD->data;
          size_t min = (aS->len < bS->len) ? aS->len : bS->len;
          int cmp = memcmp(aS->buf, bS->buf, min);
          if ( cmp == 0 ) {
               /* strings match, compare length */
               res.v.u = aS->len > bS->len;
          } else {
               res.v.u = (cmp > 0);
          }
          wsdata_delete(aD);
          wsdata_delete(bD);
          break;
     }
     case WSCVT_TIME:       res.v.u = getWSCVDouble(a) > getWSCVDouble(b);    break;
     }
     return res;
}

static wscalcPart *GetGreaterProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, GreaterProducerExec);
}
static wscalcValue GreaterEqualProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     if ( a.type == WSCVT_STRING || b.type == WSCVT_STRING ) {
          if ( a.type != b.type ) {
               error_print("Cannot compare strings to numbers");
               res.v.u = 0;
               return res;
          }
     }
     switch ( promoteTypes(a.type, b.type) ) {
     case WSCVT_BOOLEAN: /* promote to integer */
     case WSCVT_INTEGER:      res.v.u = getWSCVInt(a) >= getWSCVInt(b);          break;
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) >= getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.u = getWSCVDouble(a) >= getWSCVDouble(b);    break;
     case WSCVT_STRING:       {
          wsdata_t *aD = getWSCVString(a);
          wsdt_string_t *aS = (wsdt_string_t*)aD->data;
          wsdata_t *bD = getWSCVString(b);
          wsdt_string_t *bS = (wsdt_string_t*)bD->data;
          size_t min = (aS->len < bS->len) ? aS->len : bS->len;
          int cmp = memcmp(aS->buf, bS->buf, min);
          if ( cmp == 0 ) {
               /* strings match, compare length */
               res.v.u = aS->len >= bS->len;
          } else {
               res.v.u = (cmp >= 0);
          }
          wsdata_delete(aD);
          wsdata_delete(bD);
          break;
     }
     case WSCVT_TIME:       res.v.u = getWSCVDouble(a) >= getWSCVDouble(b);    break;
     }
     return res;
}

static wscalcPart *GetGreaterEqualProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, GreaterEqualProducerExec);
}

static wscalcValue DoubleEqualProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     if ( a.type == WSCVT_STRING || b.type == WSCVT_STRING ) {
          if ( a.type != b.type ) {
               error_print("Cannot compare strings to numbers");
               res.v.u = 0;
               return res;
          }
     }
     switch ( promoteTypes(a.type, b.type) ) {
     case WSCVT_BOOLEAN:
     case WSCVT_INTEGER:
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) == getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.u = getWSCVDouble(a) == getWSCVDouble(b);    break;
     case WSCVT_STRING:       {
          wsdata_t *aD = getWSCVString(a);
          wsdt_string_t *aS = (wsdt_string_t*)aD->data;
          wsdata_t *bD = getWSCVString(b);
          wsdt_string_t *bS = (wsdt_string_t*)bD->data;
          size_t min = (aS->len < bS->len) ? aS->len : bS->len;
          int cmp = memcmp(aS->buf, bS->buf, min);
          if ( cmp == 0 ) {
               /* strings match, compare length */
               res.v.u = aS->len == bS->len;
          } else {
               res.v.u = (cmp == 0);
          }
          wsdata_delete(aD);
          wsdata_delete(bD);
          break;
     }
     case WSCVT_TIME:       res.v.u = getWSCVDouble(a) == getWSCVDouble(b);    break;
     }
     return res;
}

static wscalcPart *GetDoubleEqualProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, DoubleEqualProducerExec);
}

static wscalcValue NotEqualProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     if ( a.type == WSCVT_STRING || b.type == WSCVT_STRING ) {
          if ( a.type != b.type ) {
               error_print("Cannot compare strings to numbers");
               res.v.u = 0;
               return res;
          }
     }
     switch ( promoteTypes(a.type, b.type) ) {
     case WSCVT_BOOLEAN:
     case WSCVT_INTEGER:
     case WSCVT_UINTEGER:     res.v.u = getWSCVUInt(a) != getWSCVUInt(b);        break;
     case WSCVT_DOUBLE:       res.v.u = getWSCVDouble(a) != getWSCVDouble(b);    break;
     case WSCVT_STRING:       {
          wsdata_t *aD = getWSCVString(a);
          wsdt_string_t *aS = (wsdt_string_t*)aD->data;
          wsdata_t *bD = getWSCVString(b);
          wsdt_string_t *bS = (wsdt_string_t*)bD->data;
          size_t min = (aS->len < bS->len) ? aS->len : bS->len;
          int cmp = memcmp(aS->buf, bS->buf, min);
          if ( cmp == 0 ) {
               /* strings match, compare length */
               res.v.u = aS->len != bS->len;
          } else {
               res.v.u = (cmp != 0);
          }
          wsdata_delete(aD);
          wsdata_delete(bD);
          break;
     }
     case WSCVT_TIME:       res.v.u = getWSCVDouble(a) != getWSCVDouble(b);    break;
     }
     return res;
}

static wscalcPart *GetNotEqualProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, NotEqualProducerExec);
}


static wscalcValue LogicalOrProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     res.v.u = (getWSCVBool(a) || getWSCVBool(b));
     return res;
}


static wscalcPart *GetLogicalOrProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, LogicalOrProducerExec);
}


static wscalcValue LogicalAndProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue b = parts[1]->go(parts[1], runtimeToken);
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     res.v.u = (getWSCVBool(a) && getWSCVBool(b));
     return res;
}


static wscalcPart *GetLogicalAndProducer(wscalcPart *first, wscalcPart *second) {
     return GetBinaryInfixProducer(first, second, LogicalAndProducerExec);
}


static void ComplementProducerDestroy(wscalcPart *aWSCalcPart) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     parts[0]->destroy(parts[0]);
     free(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void ComplementProducerFlush(wscalcPart *aWSCalcPart) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     parts[0]->flush(parts[0]);
}


static wscalcValue ComplementProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcPart **parts = (wscalcPart**)aWSCalcPart->params;
     wscalcValue a = parts[0]->go(parts[0], runtimeToken);
     wscalcValue res = a;
     switch ( a.type ) {
     case WSCVT_BOOLEAN: /* promote to integer */  res.type = WSCVT_INTEGER; /* no break, fall through */
     case WSCVT_INTEGER:      res.v.i = ~getWSCVInt(a);     break;
     case WSCVT_UINTEGER:     res.v.u = ~getWSCVUInt(a);    break;
     case WSCVT_DOUBLE:       error_print("Cannot perform the complement operation on a double."); break;
     case WSCVT_STRING:       error_print("Cannot perform the complement operation on a string."); break;
     case WSCVT_TIME:         error_print("Cannot perform the complement operation on a time."); break;
     }
     return res;
}

static wscalcPart *GetComplementProducer(wscalcPart *first) {
     wscalcPart *answer = (wscalcPart *)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetComplementProducer malloc of answer");
          return NULL;
     }
     answer->params = malloc(sizeof(void*));
     if (!answer->params) {
          error_print("failed GetComplementProducer malloc of answer->params");
          return NULL;
     }
     wscalcPart **parts = (wscalcPart**)answer->params;
     parts[0] = first;
     answer->go = ComplementProducerExec;
     answer->destroy = ComplementProducerDestroy;
     answer->flush = ComplementProducerFlush;
     return answer;
}



static wscalcValue LogicalNotProducerFunc(void *ign, paramList_t *params, void *runtimeToken) {
     wscalcValue res;
     res.type = WSCVT_BOOLEAN;
     res.v.u = !getWSCVBool(params->value);
     return res;
}


static wscalcPart *GetLogicalNotProducer(wscalcPart *first) {
     static const fEntry_t notEntry = {"not", LogicalNotProducerFunc, NULL, 1};
     paramList_t *params = calloc(1, sizeof(paramList_t));
     if ( !params ) {
          error_print("failed malloc in GetLogicalNotProducer for params");
          return NULL;
     }
     params->part = first;
     return GetFunctionCallProducer(&notEntry, params);
}



/*==== wscalcPart for returning constants ====*/
static wscalcValue ConstantProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     wscalcValue *v = (wscalcValue*)aWSCalcPart->params;
     if ( v->type == WSCVT_STRING ) {
          /* Need to increase reference count */
          wsdata_add_reference((wsdata_t*)(v->v.s));
     }
     return *v;
}

static void ConstantProducerDestroy(wscalcPart *aWSCalcPart) {
     wscalcValue *v = (wscalcValue*)aWSCalcPart->params;
     if ( v->type == WSCVT_STRING ) {
          wsdata_delete((wsdata_t*)(v->v.s));
     }
     free(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void ConstantProducerFlush(wscalcPart *aWSCalcPart) {
     //do nothing
}
static wscalcPart *GetConstantProducer(wscalcValue d) {
     wscalcPart *answer = (wscalcPart *)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetConstantProducer malloc of answer");
          return NULL;
     }
     answer->params = malloc(sizeof(wscalcValue));
     if (!answer->params) {
          error_print("failed GetConstantProducer malloc of answer->params");
          return NULL;
     }
     *((wscalcValue*)answer->params)=d;
     answer->go = ConstantProducerExec;
     answer->destroy = ConstantProducerDestroy;
     answer->flush = ConstantProducerFlush;
     return answer;
}


static wscalcPart *GetConstantProducerInteger(int64_t d) {
     wscalcPart *answer = (wscalcPart *)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed GetConstantProducer malloc of answer");
          return NULL;
     }
     answer->params = malloc(sizeof(wscalcValue));
     if (!answer->params) {
          error_print("failed GetConstantProducer malloc of answer->params");
          return NULL;
     }
     *((wscalcValue*)answer->params)=makeWSCalcValueUInteger(d);
     answer->go = ConstantProducerExec;
     answer->destroy = ConstantProducerDestroy;
     answer->flush = ConstantProducerFlush;
     return answer;
}


/* ===== Parameter List ===== */
static paramList_t *GetParamListProducer(paramList_t *list, wscalcPart *aWSCalcPart)
{
     paramList_t *item = (paramList_t *)malloc(sizeof(paramList_t));
     if ( !item ) {
          error_print("Failed %s malloc of item", __FUNCTION__);
          return NULL;
     }

     item->part = aWSCalcPart;
     item->value = wscalcZERO;
     item->next = list;

     return item;
}

/* ===== String Functions ==== */
static wsdt_string_t* getStringArg(paramList_t *params, size_t N)
{
     wscalcValue arg = getParam(N, params)->value;
     if ( arg.type != WSCVT_STRING ) {
          error_print("Argument %zu to string function is not a string (type: %d)!", N, arg.type);
          return NULL;
     }
     return (wsdt_string_t*)(arg.v.s)->data;
}


static wscalcValue doStringFunc(void* function, paramList_t *params, void *runtimeToken)
{
     wscalcValue res = wscalcZERO;
     wsdt_string_t *baseArgStr = getStringArg(params, 0);
     if ( NULL == baseArgStr ) return res;

     enum strFuncKey func = (enum strFuncKey)(intptr_t)function;
     switch(func) {
          case STRF_LEN:
               res.type = WSCVT_UINTEGER;
               res.v.u = (uint64_t)baseArgStr->len;
               break;
          case STRF_FIND: {
               wsdt_string_t *needle = getStringArg(params, 1);
               if ( NULL == needle ) break;
               res.type = WSCVT_INTEGER;
               char *x = memmem(baseArgStr->buf, baseArgStr->len, needle->buf, needle->len);
               if ( x == NULL ) res.v.i = -1;
               else res.v.i = (int64_t)(x - baseArgStr->buf);
               break;
               }
          case STRF_SUBSTR: {
               int64_t base = getWSCVInt(getParam(1, params)->value);
               int64_t len = getWSCVInt(getParam(2, params)->value);
               if ( base < 0 ) {
                    error_print("Start argument to substring must be positive");
                    break;
               }
               if ( len < 0 ) { /* Take to end of string */
                    len = baseArgStr->len - base;
               }
               if ( (base + len) > baseArgStr->len ) {
                    error_print("Attempting to take a substring past the end of a string");
                    break;
               }

               res.type = WSCVT_STRING;
               res.v.s = wsdata_alloc(dtype_string);
               wsdata_add_reference(res.v.s);
               wsdata_assign_dependency(getParam(0, params)->value.v.s, res.v.s);
               wsdt_string_t *ns = (wsdt_string_t*)(res.v.s)->data;
               ns->buf = baseArgStr->buf + base;
               ns->len = len;
               break;
               }
          case STRF_REPL:
          case STRF_REPLALL: {
               wsdt_string_t *target = getStringArg(params, 1);
               wsdt_string_t *repl = getStringArg(params, 2);
               if ( NULL == target || NULL == repl ) break;


               /* Each replacement will cause an increase in this size */
               int size_diff = repl->len - target->len;

               wsdata_t *orig_wsd = getParam(0, params)->value.v.s;
               wsdt_string_t orig_str = *(wsdt_string_t*)orig_wsd->data;
               wsdata_add_reference(orig_wsd); /* Because we delete this later */

               wsdata_t *res_wsd = orig_wsd;
               wsdt_string_t res_str = {0};

               char *x = NULL;
               uint32_t replCount = 0;
               while ( (x = memmem(orig_str.buf, orig_str.len, target->buf, target->len)) != NULL ) {
               dprint("Replacing [%s] with [%s] in [%s] at [%s]", target->buf, repl->buf, orig_str.buf, x);
                    replCount++;

                    int newSize = orig_str.len + size_diff;
                    size_t prefixLen = (x - orig_str.buf);
                    size_t suffixLen = (orig_str.len - target->len - prefixLen);

                    res_wsd = wsdata_create_buffer(newSize, &res_str.buf, &res_str.len);

                    /* Copy prefix */
                    if ( x != orig_str.buf ) {
                         memcpy(res_str.buf, orig_str.buf, prefixLen);
                    }
                    /* Copy replacement */
                    memcpy(res_str.buf + prefixLen, repl->buf, repl->len);
                    /* Copy suffix */
                    if ( suffixLen > 0 ) {
                         memcpy(res_str.buf + prefixLen + repl->len, orig_str.buf + prefixLen + target->len, suffixLen);
                    }


                    wsdata_delete(orig_wsd);
                    orig_wsd = res_wsd;
                    orig_str = res_str;

                    if ( func == STRF_REPL ) break; /* Only once */
                    dprint("Now checking again in [%s]", orig_str.buf);
               }

               res.type = WSCVT_STRING;
               res.v.s = wsdata_alloc(dtype_string);
               wsdata_add_reference(res.v.s);
               if ( replCount == 0 ) {
                    wsdata_remove_reference(getParam(0, params)->value.v.s); /* Was added earlier */
                    wsdata_assign_dependency(getParam(0, params)->value.v.s, res.v.s);
                    wsdt_string_t *rs = (wsdt_string_t*)res.v.s->data;
                    rs->buf = baseArgStr->buf;
                    rs->len = baseArgStr->len;
               } else {
                    wsdata_assign_dependency(res_wsd, res.v.s);
                    wsdt_string_t *rs = (wsdt_string_t*)res.v.s->data;
                    rs->buf = res_str.buf;
                    rs->len = res_str.len;
               }

               break;
               }
     }
     return res;
}

static int convertStringToTime(const wsdt_string_t *wsstr, struct timeval *tv)
{
     static const char *formats[] = {
          "%a, %d %b %Y %T %Z",  //"Tue, 04 May 2010 04:31:27 GMT"
          "%a %d %b %Y %T %Z",  //"Tue 04 May 2010 04:31:27 GMT"
          "%a %b %d %Y %T %Z",  //"Tue May 04 2010 04:31:27 GMT"
          "%a, %d-%b-%Y %T %Z",  //"Tue, 04-May-2010 04:31:27 GMT"
          "%a %d-%b-%Y %T %Z",  //"Tue 04-May-2010 04:31:27 GMT"
          "%d %b %Y %T %Z",  //"04 May 2010 04:31:27 GMT"
          "%a %b %d %T %Z %Y",  //"Tue May 04 04:31:27 GMT 2010"
          "%c",     // Date and time for current locale
          "%R",     // "%H:%M:%S
          "%A %B %d, %Y, %H:%M:%S",
          "%Y-%m-%d %I:%M:%S %p",
          "%Y-%m-%d %H:%M:%S",
          "%Y-%m-%d %I:%M %p",
          "%m/%d/%y %I:%M %p",
          "%m/%d/%Y %I:%M %p",
          "%m/%d/%y %I:%M:%S %p",
          "%m/%d/%Y %I:%M:%S %p",
          NULL
     };

     const char* str = wsstr->buf;
     struct tm tm = {0};
     const char** p = formats;
     while (*p) {
          char *ok = strptime(str, *p, &tm);
          if ( ok ) {
               tv->tv_sec = mktime(&tm);
               tv->tv_usec = 0;
               return 1;
          }
          p++;
     }
     return 0;
}



static wscalcValue doParseDateFunc(void* function, paramList_t *params, void *runtimeToken)
{
     const wscalcValue res = {0};
     wsdt_string_t *baseArgStr = getStringArg(params, 0);
     if ( NULL == baseArgStr ) return res;

     struct timeval tv;
     if ( convertStringToTime(baseArgStr, &tv) )
          return makeWSCalcValueTimeval(&tv);

     error_print("Failed to convert timestring [%s] to time value!", baseArgStr->buf);
     return res;
}



/*===== Casting =====*/
typedef struct _cast_info_t {
     wscalcValueType type;
     wscalcPart *part;
} cast_info_t;

static void CastProducerDestroy(wscalcPart *aWSCalcPart) {
     cast_info_t *ci = (cast_info_t*)aWSCalcPart->params;
     ci->part->destroy(ci->part);
     free(aWSCalcPart->params);
     free(aWSCalcPart);
}

static void CastProducerFlush(wscalcPart *aWSCalcPart) {
     cast_info_t *ci = (cast_info_t*)aWSCalcPart->params;
     ci->part->flush(ci->part);
}

static wscalcValue CastProducerExec(wscalcPart *aWSCalcPart, void *runtimeToken) {
     cast_info_t *ci = (cast_info_t*)aWSCalcPart->params;
     wscalcValue a = ci->part->go(ci->part, runtimeToken);
     wscalcValue res;
     res.type = ci->type;
     switch ( ci->type ) {
     case WSCVT_INTEGER:   res.v.i = getWSCVInt(a);    break;
     case WSCVT_UINTEGER:  res.v.u = getWSCVUInt(a);   break;
     case WSCVT_BOOLEAN:   res.v.u = getWSCVBool(a);   break;
     case WSCVT_DOUBLE:    res.v.d = getWSCVDouble(a); break;
     case WSCVT_STRING:    res.v.s = getWSCVString(a); break;
     case WSCVT_TIME:      res.v.t = getWSCVTime(a);   break;
     }
     /* Kill the old string, if we didn't do a self-cast */
     if ( a.type == WSCVT_STRING && res.type != WSCVT_STRING ) {
          wsdata_delete(a.v.s);
     }
     return res;
}

static wscalcPart *GetCastProducer(char *type, wscalcPart *bWSCalcPart)
{
     wscalcPart *answer = (wscalcPart*)malloc(sizeof(wscalcPart));
     if (!answer) {
          error_print("failed getCastProducer malloc of answer");
          return NULL;
     }
     answer->params = malloc(sizeof(cast_info_t));
     if (!answer->params) {
          error_print("failed getCastProducer malloc of answer->params");
          return NULL;
     }
     cast_info_t *ci = (cast_info_t*)answer->params;
     if (      !strcasecmp(type, "int")    ) ci->type = WSCVT_INTEGER;
     else if ( !strcasecmp(type, "uint")   ) ci->type = WSCVT_UINTEGER;
     else if ( !strcasecmp(type, "bool")   ) ci->type = WSCVT_BOOLEAN;
     else if ( !strcasecmp(type, "double") ) ci->type = WSCVT_DOUBLE;
     else if ( !strcasecmp(type, "string") ) ci->type = WSCVT_STRING;
     else if ( !strcasecmp(type, "time")   ) ci->type = WSCVT_TIME;
     else {
          error_print("Cannot cast of type '%s'", type);
          return NULL;
     }
     ci->part = bWSCalcPart;
     answer->go = CastProducerExec;
     answer->destroy=CastProducerDestroy;
     answer->flush=CastProducerFlush;
     return answer;
}


/* --- Conversion and accessor functiosn --- */

double getWSCVDouble(wscalcValue v) {
     switch ( v.type ) {
     case WSCVT_DOUBLE:
          return v.v.d;
     case WSCVT_INTEGER:
          return (double)v.v.i;
     case WSCVT_BOOLEAN:
     case WSCVT_UINTEGER:
          return (double)v.v.u;
     case WSCVT_STRING: {
          char *buf = (char*)WSDTSTRING(v)->buf;
          int blen = WSDTSTRING(v)->len;
          char backup = buf[blen];
          buf[blen] = '\0';
          double d = strtod(buf, NULL);
          buf[blen] = backup;
          return d;
     }
     case WSCVT_TIME:
          return v.v.t.tv_sec + (v.v.t.tv_usec*1e-6);
     default:
          error_print("Invalid type to convert. (%d to %d)", v.type, WSCVT_DOUBLE);
          return 0.0;
     }
}

int64_t getWSCVInt(wscalcValue v) {
     switch ( v.type ) {
     case WSCVT_DOUBLE:
          return (int64_t)v.v.d;
     case WSCVT_INTEGER:
          return v.v.i;
     case WSCVT_BOOLEAN:
     case WSCVT_UINTEGER:
          return (int64_t)v.v.u;
     case WSCVT_STRING: {
          char *buf = (char*)WSDTSTRING(v)->buf;
          int blen = WSDTSTRING(v)->len;
          char backup = buf[blen];
          buf[blen] = '\0';
          int64_t d = strtoll(buf, NULL, 0);
          buf[blen] = backup;
          return d;
     }
     case WSCVT_TIME:
          return v.v.t.tv_sec;
     default:
          error_print("Invalid type to convert. (%d to %d)", v.type, WSCVT_INTEGER);
          return 0.0;
     }
}

uint64_t getWSCVUInt(wscalcValue v) {
     switch ( v.type ) {
     case WSCVT_DOUBLE:
          return (uint64_t)v.v.d;
     case WSCVT_INTEGER:
          return (uint64_t)v.v.i;
     case WSCVT_BOOLEAN:
     case WSCVT_UINTEGER:
          return v.v.u;
     case WSCVT_STRING: {
          char *buf = (char*)WSDTSTRING(v)->buf;
          int blen = WSDTSTRING(v)->len;
          char backup = buf[blen];
          buf[blen] = '\0';
          uint64_t d = strtoull(buf, NULL, 0);
          buf[blen] = backup;
          return d;
     case WSCVT_TIME:
          return v.v.t.tv_sec;
     }
     default:
          error_print("Invalid type to convert. (%d to %d)", v.type, WSCVT_UINTEGER);
          return 0.0;
     }
}

uint8_t getWSCVBool(wscalcValue v) {
     switch ( v.type ) {
     case WSCVT_DOUBLE:
          return (fabs(v.v.d) > 0.00001);
     case WSCVT_INTEGER:
     case WSCVT_BOOLEAN:
     case WSCVT_UINTEGER:
          return (v.v.u != 0);
     case WSCVT_STRING:
          return !memcmp(WSDTSTRING(v)->buf, "TRUE", WSDTSTRING(v)->len);
     default:
          error_print("Invalid type to convert. (%d to %d)", v.type, WSCVT_BOOLEAN);
          return 0.0;
     }
}



wsdata_t* getWSCVString(wscalcValue v) {
     char buf[64] = {0};
     switch ( v.type ) {
     case WSCVT_DOUBLE:
          snprintf(buf, 64, "%lf", v.v.d);
          break;
     case WSCVT_INTEGER:
          snprintf(buf, 64, "%" PRIi64, v.v.i);
          break;
     case WSCVT_BOOLEAN:
          snprintf(buf, 64, "%s", (v.v.u == 0) ? "FALSE" : "TRUE");
          break;
     case WSCVT_UINTEGER:
          snprintf(buf, 64, "%" PRIu64, v.v.u);
          break;
     case WSCVT_STRING:
          /* special case, just return our copy */
          return v.v.s;
          break;
     case WSCVT_TIME:
		{
           struct tm gtm;
		 gmtime_r(&v.v.t.tv_sec, &gtm);
		 asctime_r(&gtm, buf);
           //asctime_r(gmtime(&v.v.t.tv_sec), buf);
           buf[24] = '\0'; // Trim off the \n\0
           break;
		}
     default:
          error_print("Invalid type to convert. (%d to %d)", v.type, WSCVT_STRING);
     }

     char *newbuf = NULL;
     size_t len = strlen(buf);
     wsdata_t *wsd = createStringData(len, &newbuf);
     memcpy(newbuf, buf, len);
     return wsd;
}

struct timeval getWSCVTime(wscalcValue v) {
     struct timeval tv = {0};
     switch ( v.type ) {
     case WSCVT_INTEGER:
     case WSCVT_UINTEGER:
          tv.tv_sec = v.v.i;
          break;
     case WSCVT_BOOLEAN:
          error_print("Invalid type to convert. (Bool to Time)");
          break;
     case WSCVT_DOUBLE:
          tv.tv_sec = (time_t)v.v.d;
          tv.tv_usec = (suseconds_t)((v.v.d - tv.tv_sec)*1e6);
          break;
     case WSCVT_STRING:
          if ( !convertStringToTime((wsdt_string_t*)v.v.s->data, &tv) )
               error_print("Unable to convert string [%s] to time.", ((wsdt_string_t*)v.v.s->data)->buf);
          break;
     case WSCVT_TIME:
          return v.v.t;
     }
     return tv;
}

static wscalcValueType promoteTypes(const wscalcValueType a, const wscalcValueType b)
{
     if ( a == b ) return a;
     /* Order implies precedence */
     if ( a == WSCVT_STRING   || b == WSCVT_STRING )   return WSCVT_STRING;
     if ( a == WSCVT_DOUBLE   || b == WSCVT_DOUBLE )   return WSCVT_DOUBLE;
     if ( a == WSCVT_INTEGER  || b == WSCVT_INTEGER )  return WSCVT_INTEGER;
     if ( a == WSCVT_UINTEGER || b == WSCVT_UINTEGER ) return WSCVT_UINTEGER;
     /* Default to Integer */
     return WSCVT_INTEGER;
}

static paramList_t* getParam(size_t n, paramList_t *list)
{
     size_t c = 0;
     while ( (c < n) && (NULL != list) ) {
          list = list->next;
          c++;
     }
     return list;
}

