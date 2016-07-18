/*
No copyright is claimed in the United States under Title 17, U.S. Code.
All Other Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef _WSCALC_H
#define _WSCALC_H

#include <inttypes.h>
#include "wstypes.h"
#include "datatypes/wsdt_string.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/**
   These are constants used to determine what operation
   should be used to retrieve variable values
*/
#define WSR_TAIL 1
#define WSR_SUM 2
#define WSR_AVG 3
#define WSR_CNT 4
#define WSR_MAX 5
#define WSR_MIN 6
#define WSR_SPAN 7
#define WSR_STDEV 8


/**
 * This is a list of integral types that the wscalcValue can take on
 */
typedef enum {
     WSCVT_INTEGER   = 0,
     WSCVT_UINTEGER  = 1,
     WSCVT_BOOLEAN   = 2,
     WSCVT_DOUBLE    = 3,
     WSCVT_STRING    = 4,
     WSCVT_TIME      = 5
} wscalcValueType;

/**
 * Represents a value, may be one of multiple integral types
 */
typedef struct _wscalcValue {
     wscalcValueType     type;
     union {
          double          d;
          int64_t         i;
          uint64_t        u;
          wsdata_t       *s;
          struct timeval  t;
     } v;
} wscalcValue;

int64_t        getWSCVInt     (wscalcValue v);
uint64_t       getWSCVUInt    (wscalcValue v);
uint8_t        getWSCVBool    (wscalcValue v);
double         getWSCVDouble  (wscalcValue v);
wsdata_t*      getWSCVString  (wscalcValue v);
struct timeval getWSCVTime    (wscalcValue v);


static inline wscalcValue makeWSCalcValueInteger(int64_t d ) {
     wscalcValue res;
     res.type = WSCVT_INTEGER;
     res.v.i = d;
     return res;
}

static inline wscalcValue makeWSCalcValueUInteger(uint64_t d ) {
     wscalcValue res;
     res.type = WSCVT_UINTEGER;
     res.v.u = d;
     return res;
}

static inline wscalcValue makeWSCalcValueDouble(double d ) {
     wscalcValue res;
     res.type = WSCVT_DOUBLE;
     res.v.d = d;
     return res;
}

static inline wsdata_t* createStringData(size_t size, char **resBuf)
{
     int bufLen = 0;
     wsdata_t * data = wsdata_create_buffer(size, resBuf, &bufLen);
     wsdata_t * sdata = wsdata_alloc(dtype_string);
     wsdata_add_reference(sdata);
     wsdata_assign_dependency(data, sdata);
     wsdt_string_t *str = (wsdt_string_t*)sdata->data;
     str->buf = *resBuf;
     str->len = bufLen;
     return sdata;
}



static inline wscalcValue makeWSCalcValueString(wsdata_t *d)
{
     wscalcValue res;
     res.type = WSCVT_STRING;

     res.v.s = d;
     wsdata_add_reference(d);
     return res;
}

static inline wscalcValue makeWSCalcValueStringRaw(char *str, int len)
{
     char *buf = NULL;
     int dlen = 0;
     wsdata_t *dep = wsdata_create_buffer(len, &buf, &dlen);
     wsdata_t *wsstr = wsdata_alloc(dtype_string);
     wsdata_assign_dependency(dep, wsstr);
     wsdt_string_t *mstr = (wsdt_string_t*)wsstr->data;
     mstr->buf = buf;
     mstr->len = dlen;
     memcpy(mstr->buf, str, len);

     return makeWSCalcValueString(wsstr);
}


static inline wscalcValue makeWSCalcValueTimeval(const struct timeval *tv) {
     wscalcValue res;
     res.type = WSCVT_TIME;
     res.v.t = *tv;
     return res;
}

/**
   This structure is the "lowest common denominator"
   for operations executed in the wscalc.  Each indivial
   operations in a wscalc gets converted into an instance
   of wscalcPart.  "go" is the function points that
   performs the operation.  "params" is the parameters,
   initialized at runtime, that it needs to perform
   the operations, and "destroy" is a destructor.
   The actual content of "params" depends on the specific
   operations - it will often contain pointers to other
   wscalcParts.
*/
typedef struct _wscalcPart {
     wscalcValue (*go)(struct _wscalcPart*, void *runtimeToken);
     void (*destroy)(struct _wscalcPart*);
     void (*flush)(struct _wscalcPart*);
     void *params;
} wscalcPart;

int wscalclex(void);
int wscalcparse (void *callerState, wscalcPart **wscalc_output, int *wscalc_error);

int wscalc_parse_script(void *callerState, wscalcPart **wscalc_output,
                        int *wscalc_error, FILE **fileList, const char *extra_script);

/**
   This structure is used to help the code using the calc
   functionality to track and use local variables at
   compile time.  It is not used by the parser or lexer
   itself, but is put here because it is what any code using
   the parser and lexer might used to deal with "locaL"
   variables.  Functions to manipulate the table are
   in the parser (wscalc.y).

   It implements a simple linked list and uses a linear
   search for lookup.  That's inefficient, but is ok because
   the lookups should only get performed at compiletime.
*/
typedef struct _wscalcLookupTableEntry {
     char name[50];
     void *reference;
     struct _wscalcLookupTableEntry *next;
} wscalc_lookupTableEntry;

extern void *wscalc_createLookupTable(void);
extern wscalc_lookupTableEntry *wscalc_getEntry(char *name, void *table, int *create);
extern void wscalc_destroyTable(void *table);

//#define YYSTYPE wscalcPart

/**
   These function pointers need to be assigned
   by the calling code (current proc_calc)
   so that the grammar knows how to assign and
   retrieve variable values
*/
#ifndef _WSUTIL
wscalcValue (*getVarValue)(void *, void *, int operation);
int (*setVarValue)(wscalcValue, int, void *, void *); 
void (*destroyVar)(void *);
void (*flushVar)(void *);
uint8_t (*nameExists)(void *, void *);
void *(*initializeVarReference)(char *, char *, void *);
void *(*initializeLabelAssignment)(char *, char *, void *);
int (*assignLabel)(void *, void *);
void (*wsflush)(void *);
#else
void * getVarValue;
void * setVarValue;
void * destroyVarValue;
void * flushVar;
void * destroyVar;
void * initializeVarReference;
void * initializeLabelAssignment;
void * wsflush;
void * assignLabel;
#endif

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSCALC_H
