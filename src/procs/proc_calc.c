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
#define PROC_NAME "calc"
//#define DEBUG 1

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_double.h"
#include "stringhash5.h"
#include "wsqueue.h"
#include "wscalc.h"


char proc_version[]     = "1.5";
char *proc_tags[]     = { "math", NULL };
char *proc_alias[]     = { "mathwscalc", "math", "icalc", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "simple math operations";
char *proc_synopsis[]   = { "calc [-F] [-M <table size>] [-N] [-R]", NULL };

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'F', "", "",
      "flush output at end",0,0},
     {'M', "", "records",
      "maximum table size for local keyed variables",0,0},
     {'N', "", "",
      "run the script for each tuple, but don't pass the tuples through, only expired variables"},
     {'R', "", "",
      "for local indexed variables, only keep around a reference to the matching key"},
     {'S', "script", "<filename>",
          "Parse a file as the script to run.  Any specified script files will be parsed in-order "
               "before any command-line script is run."},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

// TODO: update list of supported arithmetic
char proc_description[] = "Provides a simple programming language for "
     "performing mathematical and logic and state-tracking operations "
     "including: \n"
     "\tbasic math (+, -, *, /, **, %)\n"
     "\tassignment (=, +=, -=, *=, /=, %=)\n"
     "\tfunctions (sin, cos, atan, exp, sqrt, abs, trunk, \n"
     "\t\tfloor, ceil, round, ln)\n"
     "\tlogical operators (||, &&, !, ==, !=, <=, >=, <, >)\n"
     "\tconditionals (if,then, else)\n"
     "\tbit operations (&, |, ^, <<, >>, <<=, >>=)\n"
     "\tstring operations:  (+, <, <=, >, >=, ==, !=)\n"
     "\tstring functions: len=strlen(var)\n"
     "\t                  loc=strfind(var, \"text\")\n"
     "\t                  newStr=substr(var, start, len)\n"
     "\t                  newStr=strRepl(var, target, repl)\n"
     "\t                  newStr=strReplAll(var, target, repl)\n"
     "\tdate/time: parseDate()\n"
     "\texistence (exists(LABEL))\n"
     "\tqueues (enqueue, qavg, qsum, qcount,qmax, qmin, qspan, qstdev -\n"
     "\t\tfor performing sliding window operations)\n"
     "\tlabels (label(NEW_LABEL), label(EXISTING_LABEL, NEW_LABEL))\n"
     "\tpass through (#PASS); don't pass through (#FAIL)\n"
     "\ttype casting: cast(typename, expression)\n"
     "\tvalue assignment; value retrieval.\n"
     "\n"
     "Multiple statements in the same expression must be separated with a "
     "semicolon.  Statements cannot start with a minus (e.g. -1).  NOTE: "
     "the '#' symbol preceding a variable name indicates a local variable "
     "whose state will be stored between events.  (This is important to "
     "remember, for instance, when using the special #PASS and #FAIL "
     "variables.)"
     "\n"
     "calc supports multiple datatypes:\n\t\tint, uint, double, string, bool, time\n"
     "Label-variables bring their type from their originating tuple.  These "
     "types are promoted as necessary to perform operations. (ie, double+int "
     " -> double).   If a particular type is required, the cast operation can "
     "be done to change the type of a variable. (ie: 'VAR=cast(int,DOUBLEVAL)')"
     "";

//TODO: add documentation from users guide

proc_example_t proc_examples[]    = {
     {"... | calc 'RANGE=MAX-MIN' | print -V'","computes the "
     "difference of the two extremes and then prints the range"},
     {"... | calc 'NUMBITS=ln(RANGE)/ln(2);' | print -V'","print out the "
     "log2 of the range (number of bits needed for unique enumeration)"},
     {NULL,""}
};
char proc_requires[]    = "";
const char *proc_input_types[]        = {"tuple,flush", NULL};
const char *proc_output_types[]   = {"tuple", NULL};
proc_port_t proc_input_ports[]  = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
const char *proc_tuple_member_labels[] = {NULL};

char proc_nonswitch_opts[] = "EXPRESSION";


/**
   EXAMPLE SCRIPTS (not particularly useful yet, but
   illustrate how to use the processor):
   WILLY=-1/5;
   WILLY=ln(1.0);
   WILLY=SERVERPORT/2;
   WILLY=SERVERPORT/2; NILLY=ln(WILLY);
   WILLY=SERVERPORT**2;   -- computes SERVERPORT squared

   (assuming that some tuple label "LEN" is the
   length of something, the following uses local
   unindexed variables to keep a running average)
   #LENSUM=#LENSUM+LEN; #LENCNT=#LENCNT+1; LENRUNAVG=#LENSUM/#LENCNT;

   key count on CSTAT_LENGTH data
   ---------
   calc -N -R '#LENCOUNT[CSTAT_LENGTH]++;'

   key average on CSTAT_LENGTH data
   Note that this kicks out LENCOUNT, LENSUM, and LENAVG
   Selective suppression of local keyed variables needs to be
   implemented to make sure that only LENAVG gets flushed out at the end
   --------------------------------
   calc -N -R '#LENCOUNT[CSTAT_LENGTH]++; #LENSUM[CSTAT_LENGTH]+=CSTAT_LENGTH; #LENAVG[CSTAT_LENGTH]=#LENSUM[CSTAT_LENGTH]/#LENCOUNT[CSTAT_LENGTH]'   
*/

static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

/**
   Definitions for managing different types of variables
*/

//A wrapper for all of the variable references
typedef struct _varReference {
     int type;
     void *reference;
} varReference;

#define VARTYPE_LABEL 1 
#define VARTYPE_LOCAL 2
#define VARTYPE_LOCALINDEXED 3

//function prototypes for local functions
static int proc_process(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _wsLocalData_t {
     nhqueue_t *data;
     int refcount;
} wsLocalData_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     int passThrough;
     wsLocalData_t *scriptSpecifiedPass;
     wsLocalData_t *scriptSpecifiedFail;
     int keepOnlyKey;
     char * wscalc;
     wscalcPart *compiledScript;     
     ws_doutput_t * dout;
     wslabel_t * label;
     ws_outtype_t * outtype_tuple;
     uint32_t maxHashTableSize;
     void * wscalc_type_table;
     wscalc_lookupTableEntry *localVarTable;
     int do_flush;
     int issue_flush;
     wsdata_t * wsd_flush;
     ws_outtype_t * outtype_flush;
} proc_instance_t;


/** definitions for local, keyed variables */
typedef struct _varHashTable_data_t {
     wsdata_t * wsd;
     nhqueue_t *data;
} varHashTable_data_t;

typedef struct _wsLocalKeyedData_t {
     wslabel_t *searchLabel;
     wslabel_t *assignLabel;
     
     proc_instance_t *proc;
     stringhash5_t *hashTable;
     int refcount;
} wsLocalKeyedData_t;

static void last_destroy(void *vdata, void *vproc) {
     wsLocalKeyedData_t *lkData = (wsLocalKeyedData_t*)vproc;
     varHashTable_data_t *kd = (varHashTable_data_t*)vdata;
     if (kd && kd->wsd) {
          wsdata_t * outTuple = NULL;
          if (lkData->proc->keepOnlyKey) {
               outTuple = ws_get_outdata(lkData->proc->outtype_tuple);
               if (outTuple) {
                    add_tuple_member(outTuple, kd->wsd);
                    wsdata_delete(kd->wsd);
               } else {
                    wscalcValue *v = NULL;
                    while ((v=queue_remove(kd->data))!=NULL) {
                         if ( v->type == WSCVT_STRING ) wsdata_delete(v->v.s);
                         free(v);
                    }
                    queue_exit(kd->data);
                    wsdata_delete(kd->wsd);
                    return;
               }
          } else {
               outTuple = kd->wsd;
          }


          //only set the outdata, if it is a single value item that is being stored.
          if (kd->data->size==1) {
               tuple_member_create_double(outTuple, *((double*)kd->data->head->data), lkData->assignLabel);
               ws_set_outdata(outTuple, lkData->proc->outtype_tuple, lkData->proc->dout);
          }
          if (!lkData->proc->keepOnlyKey) {               
               wsdata_delete(outTuple);
          }
     }
     kd->wsd=NULL;
     wscalcValue *v = NULL;
     while ((v=queue_remove(kd->data))!=NULL) {
          if ( v->type == WSCVT_STRING ) wsdata_delete(v->v.s);
          free(v);
     }
     queue_exit(kd->data);
}


void destroyVarRefs(void *compileTimeToken) {
     varReference *ref = (varReference*)compileTimeToken;
     wsLocalKeyedData_t *localKeyedData;

     switch (ref->type) {
     case VARTYPE_LOCAL: {
          wsLocalData_t *ld = (wsLocalData_t*)(ref->reference);
          ld->refcount--;
          if ( ld->refcount == 0 ) {
               queue_exit(ld->data);
               free(ld);
          }
          break;
     }
     case VARTYPE_LOCALINDEXED:
          localKeyedData = ref->reference;
          if (localKeyedData) {
               if (localKeyedData->refcount==1) {
                    stringhash5_scour_and_destroy(localKeyedData->hashTable, last_destroy, localKeyedData);
               }
               free(localKeyedData);
          }
          break;
     case VARTYPE_LABEL: {
          //do nothing
          break;
     }
     default:
          //do something here
          break;
     }
     free(compileTimeToken);
}

void flushVarRefs(void *compileTimeToken) {
     varReference *ref = (varReference*)compileTimeToken;
     wsLocalKeyedData_t *localKeyedData;
     
     switch (ref->type) {
     case VARTYPE_LOCAL:
          //do nothing
          break;
     case VARTYPE_LOCALINDEXED:
          localKeyedData = ref->reference;
          if (localKeyedData) {
               stringhash5_scour_and_flush(localKeyedData->hashTable, last_destroy, localKeyedData);
          }
          break;
     case VARTYPE_LABEL:
          //do nothing
          break;
     default:
          //do something here
          break;
     }
}

typedef struct _wsSomeLabels {
     void *newlabel;
     void *existinglabel;
} wsSomeLabels;

void WSFlush(void *vproc) {
     proc_instance_t *proc = (proc_instance_t*)vproc;
     proc->issue_flush=1;
}
          
     
void *WSInitializeLabelAssignment(char *newlabel, char *existinglabel, void *vproc) {
     wsSomeLabels *answer = calloc(1, sizeof(wsSomeLabels));
     if (!answer) {
          error_print("failed calloc of answer");
          return NULL;
     }
     proc_instance_t *proc = (proc_instance_t*)vproc;
     answer->newlabel = wsregister_label(proc->wscalc_type_table, newlabel);
     if (existinglabel) {
          answer->existinglabel = wssearch_label(proc->wscalc_type_table, existinglabel);
     }
     
     return answer;
}

int WSAssignLabel(void *labels, void *runtimetoken) {
     
     wsdata_t *current_tuple = (wsdata_t*)runtimetoken;;
     wsSomeLabels *sl = (wsSomeLabels*)labels;
     if (!sl->existinglabel) {
          wsdata_add_label(current_tuple, sl->newlabel);
     } else {
          int mlen;
          wsdata_t ** members;
          int j;
          if (tuple_find_label(current_tuple, sl->existinglabel, &mlen, &members)) {
               for (j=0; j<mlen; j++) {
                    tuple_add_member_label(current_tuple, members[j], sl->newlabel);
               }
               return 1;
          } else {
               return 0;
               
          }
     }
     return 1;
}


/*
 * Created a reference to a keyed variable.  We don't know the exact value until runtime,
 * so a reference to the hashtable and the label is stored.  This code is executed when
 * the calc script is compiled both for assignment and retrieval nodes.
 */
wsLocalKeyedData_t* acquireKeyedDataReference(char *name, char*keyindex, proc_instance_t *proc) {
      int created;
      wscalc_lookupTableEntry *entry = wscalc_getEntry(name, proc->localVarTable, &created);
      wsLocalKeyedData_t* localKeyedData;
      if (created) {
           //the variable with the given name hadn't been referenced yet, so a new one was
           //created.  The hashtable needs to be created.
           localKeyedData = calloc(1, sizeof(wsLocalKeyedData_t));
           if (!localKeyedData) {
                error_print("failed calloc of localKeyedData");
                return NULL;
           }
           localKeyedData->searchLabel =
                wssearch_label(proc->wscalc_type_table, keyindex);
           localKeyedData->assignLabel =
                wsregister_label(proc->wscalc_type_table, &name[1]);
           localKeyedData->proc=proc;
           localKeyedData->hashTable = stringhash5_create(0, proc->maxHashTableSize,
                                                          sizeof(varHashTable_data_t));
           stringhash5_set_callback(localKeyedData->hashTable, last_destroy, localKeyedData);

           //use the stringhash5-adjusted value of max_records to reset maxHashTableSize
           proc->maxHashTableSize = localKeyedData->hashTable->max_records;

           //Only the initiating localKeyedData is allowed to destroy the hashtable.
           localKeyedData->refcount=1;
           entry->reference=localKeyedData;
           return localKeyedData;
       } else {
           //the variable already existed.  So use the same hashtable and references.  
           wsLocalKeyedData_t* newLocalKeyedData;
           newLocalKeyedData = calloc(1, sizeof(wsLocalKeyedData_t));
           if (!newLocalKeyedData) {
                error_print("failed calloc of newLocalKeyedData");
                return NULL;
           }
           newLocalKeyedData->hashTable = ((wsLocalKeyedData_t*)entry->reference)->hashTable;
           newLocalKeyedData->searchLabel =
                wssearch_label(proc->wscalc_type_table, keyindex);
           newLocalKeyedData->assignLabel =
                wsregister_label(proc->wscalc_type_table, &name[1]);
           newLocalKeyedData->proc=proc;
           //Don't destroy the hashtable when this local keyed data is destroyed
           newLocalKeyedData->refcount=0;
           return newLocalKeyedData;
           //TODO: check that it is the right type
           /*
           localKeyedData=entry->reference;
           localKeyedData->refcount++;
           */
      }
}

//The next function gets called during parsing
void *WSInitializeVarReference(char * name, char *keyindex, void *vproc) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     if (proc==NULL) {
          tool_print("NULL proc instance passed in.  This will not end well.");
          return NULL;
     }

     if (name==NULL) {
          tool_print("NULL name passed in.  This will not end well.");
          return NULL;
     }

     varReference *answer = calloc(1, sizeof(varReference));
     if (!answer) {
          error_print("failed calloc of answer");
          return NULL;
     }

     if (name[0]=='#' && keyindex==NULL) {
          //local, nonindexed, single value variable
          int created;
          wscalc_lookupTableEntry *entry =
               wscalc_getEntry(name, proc->localVarTable, &created);

          if (created) {
               entry->reference = calloc(1, sizeof(wsLocalData_t));
               if (!entry->reference) {
                    error_print("failed calloc of entry->reference");
                    return NULL;
               }
               ((wsLocalData_t*)entry->reference)->data = queue_init();
          }
          ((wsLocalData_t*)entry->reference)->refcount++;
          answer->type = VARTYPE_LOCAL;
          answer->reference = entry->reference;
          if (!strcmp("#PASS", name)) {
               proc->scriptSpecifiedPass=entry->reference;
          }
          if (!strcmp("#FAIL", name)) {
               proc->scriptSpecifiedFail=entry->reference;
          }
          return answer;
     }

     if (name[0]=='#' && keyindex!=NULL) {
          answer->type = VARTYPE_LOCALINDEXED;
          answer->reference = acquireKeyedDataReference(name, keyindex, proc);

          return answer;
     }

     //assign to tuple at runtime
     answer->type = VARTYPE_LABEL;

     /* We both register and search.  This allows us to simplify references.
      * The only downside is that if we are assigning variables that nobody
      * downstream of us will search for, we have un-needingly, increased the
      * search index space.   This should be a very rare instance that a
      * user would create an output from a custom CALC script and not want
      * the answer later. */
     wsregister_label(proc->wscalc_type_table, name);
     answer->reference = wssearch_label(proc->wscalc_type_table, name);
     return answer;
}




wscalcValue getQVal(nhqueue_t *list, int op) {

     switch (op) {
     case WSR_TAIL:
          if (list->tail) {
               wscalcValue v = *((wscalcValue*)list->tail->data);
               if ( v.type == WSCVT_STRING ) {
                    wsdata_t *ostr_wsd = v.v.s;
                    /* Here, we make a new wsdata, so that labels aren't copied */
                    v.v.s = wsdata_alloc(dtype_string);
                    if ( v.v.s ) {
                         wsdt_string_t *ostr = (wsdt_string_t*)ostr_wsd->data;
                         wsdt_string_t *nstr = (wsdt_string_t*)v.v.s->data;
                         nstr->buf = ostr->buf;
                         nstr->len = ostr->len;
                         wsdata_assign_dependency(ostr_wsd, v.v.s);
                    } else {
                         /* Not healthy, but fall back to something safe. */
                         v.v.s = ostr_wsd;
                    }
                    wsdata_add_reference(v.v.s);
               }
               return v;
          } else {
               return makeWSCalcValueUInteger(0);
          }
     case WSR_CNT:
          return makeWSCalcValueInteger(queue_size(list));
     case WSR_SUM:
          {
               double   danswer = 0.0;
               int64_t  ianswer = 0;
               uint64_t uanswer = 0;
               int isInt = 0;
               int isDouble = 0;
               q_node_t *ctr = list->head;
               while (ctr) {
                    wscalcValue* cv = (wscalcValue*)ctr->data;
                    if ( cv->type == WSCVT_DOUBLE ) isDouble = 1;
                    else if ( cv->type == WSCVT_INTEGER ) isInt = 1;

                    danswer+=getWSCVDouble(*cv);
                    ianswer+=getWSCVInt(*cv);
                    uanswer+=getWSCVUInt(*cv);

                    ctr = ctr->next;
               }
               if ( isDouble ) return makeWSCalcValueDouble(danswer);
               else if ( isInt ) return makeWSCalcValueInteger(ianswer);
               return makeWSCalcValueUInteger(uanswer);
               break;
          }
     case WSR_AVG:
          {
               double answer = 0.0;
               q_node_t *ctr = list->head;
               while (ctr) {
                    wscalcValue* cv = (wscalcValue*)ctr->data;
                    answer+=getWSCVDouble(*cv);
                    ctr = ctr->next;
               }
               return makeWSCalcValueDouble(answer/queue_size(list));
          }
     case WSR_MAX:
     case WSR_MIN:
     case WSR_SPAN:
          {
               uint64_t umin = 0, umax = 0;
               int64_t  imin = 0, imax = 0;
               double   dmin = 0, dmax = 0;
               int isDouble = 0, isInt = 0;
               q_node_t *ctr = list->head;
               if ( ctr ) {
                    wscalcValue* cv = (wscalcValue*)ctr->data;
                    if ( cv->type == WSCVT_DOUBLE ) isDouble = 1;
                    else if ( cv->type == WSCVT_INTEGER ) isInt = 1;
                    dmin = dmax = getWSCVDouble(*cv);
                    imin = imax = getWSCVInt(*cv);
                    umin = umax = getWSCVUInt(*cv);
                    ctr = ctr->next;
               } else {
                    dprint("Unable to find any entries in the list.  Returning zero");
                    return makeWSCalcValueUInteger(0);
               }

               while ( ctr ) {
                    wscalcValue* cv = (wscalcValue*)ctr->data;
                    if ( cv->type == WSCVT_DOUBLE ) isDouble = 1;
                    else if ( cv->type == WSCVT_INTEGER ) isInt = 1;
                    double   d = getWSCVDouble(*cv);
                    int64_t  i = getWSCVInt(*cv);
                    uint64_t u = getWSCVUInt(*cv);

                    if ( d < dmin ) dmin = d;
                    if ( d > dmax ) dmax = d;
                    if ( i < imin ) imin = i;
                    if ( i > imax ) imax = i;
                    if ( u < umin ) umin = u;
                    if ( u > umax ) umax = u;

                    ctr = ctr->next;
               }

               if ( op == WSR_MAX ) {
                    if ( isDouble )
                         return makeWSCalcValueDouble(dmax);
                    if ( isInt )
                         return makeWSCalcValueInteger(imax);
                    return makeWSCalcValueUInteger(umax);
               }
               if ( op == WSR_MIN ) {
                    if ( isDouble )
                         return makeWSCalcValueDouble(dmin);
                    if ( isInt )
                         return makeWSCalcValueInteger(imin);
                    return makeWSCalcValueUInteger(umin);
               }
               if ( op == WSR_SPAN) {
                    if ( isDouble )
                         return makeWSCalcValueDouble(dmax-dmin);
                    if ( isInt )
                         return makeWSCalcValueInteger(imax-imin);
                    return makeWSCalcValueUInteger(umax-umin);
               }
               /* Should never get here */
               break;
          }
     case WSR_STDEV:
          {
               double mean = 0.0, stdev = 0.0;
               int len = 0;
               q_node_t *ctr = list->head;
               while ( ctr ) {
                    len++;
                    mean += getWSCVDouble(*((wscalcValue*)ctr->data));
                    ctr = ctr->next;
               }
               mean = mean / len;
               ctr = list->head;
               while ( ctr ) {
                    double cur = getWSCVDouble(*((wscalcValue*)ctr->data)) - mean;
                    stdev += cur*cur;
                    ctr = ctr->next;
               }
               stdev = stdev / len;
               return makeWSCalcValueDouble(stdev);
          }
     default:
          return makeWSCalcValueUInteger(0);
     }
     return makeWSCalcValueUInteger(0);
}

//the next two functions get called during wscalc execution
wscalcValue WSGetVarValue(void *compiletimeToken, void *runtimeToken, int op) {
     varReference *ref = (varReference*)compiletimeToken;

     dprint("Called WSGetVarValue ******************************");
     switch (ref->type) {
     case VARTYPE_LOCAL:
          dprint("Looking for local variable with op %d", op);
          return getQVal(((wsLocalData_t*)ref->reference)->data, op);
     case VARTYPE_LOCALINDEXED: {
          wslabel_t *nameLabel; 
          wsdata_t *current_tuple;
          int mlen;
          wsdata_t ** members;
          int j;
          varHashTable_data_t * kdata = NULL;

          nameLabel = ((wsLocalKeyedData_t*)ref->reference)->searchLabel;
          dprint("Looking for localindex variable %s", nameLabel->name);
          current_tuple = (wsdata_t*)runtimeToken;
          stringhash5_t * key_table = ((wsLocalKeyedData_t*)ref->reference)->hashTable;
          if (tuple_find_label(current_tuple, nameLabel, &mlen, &members)) {
               for (j=0; j<mlen; j++) {
                    double dub = -1.0;
                    dtype_get_double(members[j], &dub);
                    kdata = (varHashTable_data_t*)stringhash5_find_wsdata(key_table, members[j]);
                    if (kdata) {
                         return getQVal(kdata->data, op);
                    }
               }
               //return NAN;
               return makeWSCalcValueUInteger(0);
          }
     }
     case VARTYPE_LABEL: {
          wslabel_t *nameLabel;
          wsdata_t *current_tuple;
          int mlen;
          wsdata_t ** members;
          int j;
          nameLabel = (wslabel_t *)ref->reference;
          dprint("Looking for label variable %s", nameLabel->name);
          current_tuple = (wsdata_t*)runtimeToken;
          if (tuple_find_label(current_tuple, nameLabel, &mlen, &members)) {
               for(j=0; j<mlen; j++) {
                    if (      members[j]->dtype == dtype_uint    ||
                              members[j]->dtype == dtype_uint8   ||
                              members[j]->dtype == dtype_uint16  ||
                              members[j]->dtype == dtype_uint64 ) {
                         uint64_t answer = 0;
                         if (dtype_get_uint64(members[j], &answer)) {
                              dprint("Found unsigned value %" PRIu64, answer);
                              return makeWSCalcValueUInteger(answer);
                         }
                    } else if ( members[j]->dtype == dtype_int ||
                                members[j]->dtype == dtype_int64 ) {
                         int64_t answer = 0;
                         if (dtype_get_int64(members[j], &answer)) {
                              dprint("Found integer value %" PRId64, answer);
                              return makeWSCalcValueInteger(answer);
                         }
                    } else if ( members[j]->dtype == dtype_string     ||
                              members[j]->dtype == dtype_str          ||
                              members[j]->dtype == dtype_fixedstring  ||
                              members[j]->dtype == dtype_bigstring    ||
                              members[j]->dtype == dtype_bigstr       ||
                              members[j]->dtype == dtype_smallstring  ||
                              members[j]->dtype == dtype_tinystring   ||
                              members[j]->dtype == dtype_mediumstring ||
                              members[j]->dtype == dtype_massivestring) {
                         return makeWSCalcValueString(members[j]);
                    } else if ( members[j]->dtype == dtype_ts ) {
                         struct timeval tv;
                         wsdt_ts_t *ts = (wsdt_ts_t*)members[j]->data;
                         tv.tv_sec = ts->sec;
                         tv.tv_usec = ts->usec;
                         return makeWSCalcValueTimeval(&tv);
                    } else {
                         double answer = 0.0;
                         if (dtype_get_double(members[j], &answer)) {
                              dprint("Found double value %lf", answer);
                              return makeWSCalcValueDouble(answer);
                         }
                         error_print("Unable to convert %s",nameLabel->name);
                    }
               }
          }
          dprint("data not found in tuple.  return uint 0");
          return makeWSCalcValueUInteger(0);
     }
     default:
          error_print("Shouldn't have reached here.");
          return makeWSCalcValueUInteger(0);
     }
};


int enqueueValue(nhqueue_t *list, wscalcValue data, int maxsize) {
     if ( data.type == WSCVT_STRING ) {
          wsdata_add_reference(data.v.s);
     }
     if (maxsize<1) {
          if (list->head) {
               wscalcValue *v = (wscalcValue*)list->head->data;
               if ( v->type == WSCVT_STRING ) wsdata_delete(v->v.s);
               *v=data;
          } else {
               wscalcValue *d = malloc(sizeof(wscalcValue));
               if (!d) {
                    error_print("failed malloc of d");
                    return 0;
               }
               *d=data;
               queue_add(list, d);
          }
     } else {
          wscalcValue *d = NULL;
          while (queue_size(list)>(maxsize-1)) {
               wscalcValue *r = queue_remove(list);
               if (d==NULL) {
                    d=r;
               } else {
                    if ( r->type == WSCVT_STRING ) wsdata_delete(r->v.s);
                    free(r);
               }
          }
          if (d==NULL) {
               d = malloc(sizeof(wscalcValue));
               if (!d) {
                    error_print("failed malloc of d");
                    return 0;
               }
          }
          *d = data;
          queue_add(list, d);
     }

     return 1;
}
     
int WSSetVarValue(wscalcValue value, int maxSize, void *compiletimeToken, void *runtimeToken) {
     dprint("Called WSSetVarValue ******************************");
     varReference *ref = (varReference*)compiletimeToken;
     switch (ref->type) {
     case VARTYPE_LOCAL:
          if (!enqueueValue(((wsLocalData_t*)ref->reference)->data, value, maxSize)) {
               return 0;
          }
          break;
     case VARTYPE_LOCALINDEXED: {
          wslabel_t *nameLabel; 
          wsdata_t *current_tuple;
          int mlen;
          wsdata_t ** members;
          int j;
          varHashTable_data_t * kdata = NULL;

          dprint("-- assigning local indexed variable\n");

          nameLabel = ((wsLocalKeyedData_t*)ref->reference)->searchLabel;
          current_tuple = (wsdata_t*)runtimeToken;
          stringhash5_t * key_table = ((wsLocalKeyedData_t*)ref->reference)->hashTable;
          if (tuple_find_label(current_tuple, nameLabel, &mlen, &members)) {
               dprint("-- found %d tuple member(s) with label\n", mlen);
               for (j=0; j<mlen; j++) {
                    #ifdef DEBUG
                    double dub = -1.0;
                    dtype_get_double(members[j], &dub);
                    dprint("-- looking for entry in hashtable, adding if necessary - %f\n", dub);
                    #endif
                    kdata = (varHashTable_data_t*)stringhash5_find_attach_wsdata(key_table, members[j]);
                    if (kdata) {
                         dprint("-- retrieved or created entry in hashtable\n");
                         if (!kdata->data) {
                              dprint("-- it was a new entry\n");
                              kdata->data = queue_init();
                         }
                         /*
                              if (((wsLocalKeyedData_t*)ref->reference)->proc->keepOnlyKey) {
                                   kdata->wsd=members[j];
                                   wsdata_add_reference(members[j]);
                              } else {
                                   kdata->wsd=current_tuple;
                                   wsdata_add_reference(current_tuple);
                              }
                         }
                         */
                         if (!enqueueValue(kdata->data, value, maxSize)) {
                              return 0;
                         }
                    }
               }
          }
          break;
     }
     case VARTYPE_LABEL: {
          wslabel_t *nameLabel = (wslabel_t*)((varReference*)compiletimeToken)->reference;   
          wsdata_t *current_tuple = (wsdata_t*)runtimeToken;
          switch ( value.type ) {
          case WSCVT_INTEGER:
               tuple_member_create_int(current_tuple, value.v.i, nameLabel);
               break;
          case WSCVT_BOOLEAN:
          case WSCVT_UINTEGER:
               tuple_member_create_uint64(current_tuple, value.v.u, nameLabel);
               break;
          case WSCVT_DOUBLE:
               tuple_member_create_double(current_tuple, value.v.d, nameLabel);
               break;
          case WSCVT_STRING:
               wsdata_add_label(value.v.s, nameLabel);
               add_tuple_member(current_tuple, value.v.s);
               break;
          case WSCVT_TIME: {
               wsdt_ts_t ts;
               ts.sec = value.v.t.tv_sec;
               ts.usec = value.v.t.tv_usec;
               tuple_member_create_ts(current_tuple, ts, nameLabel);
               break;
          }
          }
          break;
     }
     default:
          break;
     }

     return 1;
};


uint8_t WSNameExists(void *compiletimeToken, void *runtimeToken) 
{
     varReference *ref = (varReference*)compiletimeToken;

     switch (ref->type) 
     {
	case VARTYPE_LOCAL:
	   return 0;
	case VARTYPE_LOCALINDEXED:
        {
          wslabel_t *nameLabel;
          wsdata_t *current_tuple;
          int mlen;
          wsdata_t ** members;
          int j;
          varHashTable_data_t * kdata = NULL;

          nameLabel = ((wsLocalKeyedData_t*)ref->reference)->searchLabel;
          dprint("Looking for localindex variable %s", nameLabel->name);
          current_tuple = (wsdata_t*)runtimeToken;
          stringhash5_t * key_table = ((wsLocalKeyedData_t*)ref->reference)->hashTable;
          if (tuple_find_label(current_tuple, nameLabel, &mlen, &members)) {
               for (j=0; j<mlen; j++) {
                    double dub = -1.0;
                    dtype_get_double(members[j], &dub);
                    kdata = (varHashTable_data_t*)stringhash5_find_wsdata(key_table, members[j]);
                    if (kdata) {
                         return 1;
                    }
               }
          }
          return 0;
        }
	case VARTYPE_LABEL: 
	{
	   wslabel_t *nameLabel; 
	   wsdata_t *current_tuple;
	   int mlen;
	   wsdata_t ** members;
	   unsigned int i;

	   nameLabel = (wslabel_t *)ref->reference;
	   current_tuple = (wsdata_t*)runtimeToken;
	   
	   if (tuple_find_label(current_tuple, nameLabel, &mlen, &members)) 
	   {
	      return 1;
	   }
	   
	   /* do a search on the tuple itself. */
	   for (i = 0; i < current_tuple->label_len; i++) 
	   {
	      if (current_tuple->labels[i] == nameLabel)
	      {
		 return 1;
	      }
	   }
	   
	   dprint("data not found in tuple.  returning 0");
	   return 0;
	}
	default:
	   error_print("%s/%s:%d:  Shouldn't have reached here.", 
		       __FILE__, __func__, __LINE__);
	   return 0;
     }
};

void trim_trailing_spaces(char *str)
{
     int end = strlen(str) - 1;


     while (isspace(str[end]) && (end >= 0))
          end--;

     str[end + 1] = '\0'; // Null terminate string.
}

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {

     int op;
     proc->passThrough=1;

     FILE *fparray[64] = {0};
     size_t num_files = 0;

     while ((op = getopt(argc, argv, "FM:NRS:")) != EOF) {
          switch (op) {
          case 'F':
               proc->do_flush = 1;
               break;
          case 'M':
               proc->maxHashTableSize=atoi(optarg);
               break;
          case 'N':
               proc->passThrough=0;
               break;
          case 'R':
               proc->keepOnlyKey=1;
               break;
          case 'S':
               fparray[num_files] = sysutil_config_fopen(optarg, "r");
               if ( !fparray[num_files] ) {
                    error_print("Failed to open file '%s'\n", optarg);
                    return 0;
               }
               num_files++;
               break;
          default:
               return 0;
          }
     }

     if (optind < argc) {
          char * script = argv[optind];
          trim_trailing_spaces(script);
          int len = strlen(script);
          if (len && (script[len-1] != ';')) {
               proc->wscalc = malloc(len + 4);
               if (!proc->wscalc) {
                    error_print("failed malloc of proc->wscalc");
                    return 0;
               }
               snprintf(proc->wscalc, len + 4, "%s;", script);
          }
          else {
               proc->wscalc = strdup(script);
          }
          tool_print("Script to execute: %s", proc->wscalc);
     }

     /* Build AST */
     wscalcPart *wscalc_output = NULL;
     int wscalc_error = 0;
     int parse_return_error = wscalc_parse_script(proc, &wscalc_output, &wscalc_error, fparray, proc->wscalc);
     for ( size_t i = 0 ; i < num_files ; i++ ) {
          sysutil_config_fclose(fparray[i]);
     }
     if ( parse_return_error ) {
          tool_print("Syntax error while parsing wscalc");
          return 0;
     }
     if (wscalc_output==NULL) {
          tool_print("Script not assigned");
          return 0;
     }
     proc->compiledScript = wscalc_output;

     if (proc->compiledScript==NULL) {
          tool_print("Script not assigned");
          return 0;
     }

     return 1;
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
//  also register as a source here..
// return 1 if ok
// return 0 if fail
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, ws_sourcev_t * sv,
              void * type_table) {

     //allocate proc instance of this processor
     proc_instance_t * proc =
          (proc_instance_t*)calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     ws_default_statestore(&proc->maxHashTableSize);

     proc->wsd_flush = wsdata_alloc(dtype_flush);
     if (proc->wsd_flush) {
          wsdata_add_reference(proc->wsd_flush);
     } else {
          return 0;
     }
     
     //NOTE: from here until proc_cmd_options needs to be run
     //without another instance of the wscalc kid initializing.
     getVarValue = WSGetVarValue;
     setVarValue = WSSetVarValue;
     initializeVarReference = WSInitializeVarReference;
     nameExists = WSNameExists;
     destroyVar = destroyVarRefs;
     flushVar=flushVarRefs;
     initializeLabelAssignment = WSInitializeLabelAssignment;
     assignLabel = WSAssignLabel;
     wsflush = WSFlush;
     proc->localVarTable = wscalc_createLookupTable();
     proc->wscalc_type_table=type_table;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          wscalc_destroyTable(proc->localVarTable);
          return 0;
     }

     wscalc_destroyTable(proc->localVarTable);
     return 1; 
}


// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if(!proc->outtype_flush) {
          proc->outtype_flush = ws_add_outtype(olist, dtype_flush, NULL);
     }

     if (proc->do_flush) {
          if (wsdatatype_match(type_table, input_type, "FLUSH_TYPE")) {
               return proc_flush;
          }
     }

     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;  // not matching expected type
     }

     wsdatatype_t * out_type;
     out_type = wsdatatype_get(type_table, "TUPLE_TYPE");
     if (!out_type) {
          return NULL; //expected type not available
     }

     proc->outtype_tuple = ws_add_outtype(olist, out_type, NULL);

     // we are happy.. now set the processor function
     return proc_process; // a function pointer
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 2 if not output
static int proc_process(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout=dout;
     
     proc->meta_process_cnt++;

     //TODO: answer gets the value of the last statement executed.
     //an option could be added to the kid to assing that value to some label.
     //double answer = proc->compiledScript->go(proc->compiledScript, input_data);
     proc->compiledScript->go(proc->compiledScript, input_data);

     if ((!proc->scriptSpecifiedPass && proc->passThrough) || 
         (proc->scriptSpecifiedPass && 
          getWSCVBool(getQVal(proc->scriptSpecifiedPass->data, WSR_TAIL)))) {
          if (!proc->scriptSpecifiedFail || 
              (getWSCVBool(getQVal(proc->scriptSpecifiedFail->data, WSR_TAIL))==0)) {
               ws_set_outdata(input_data, proc->outtype_tuple, dout);
               if (proc->issue_flush) {
                    ws_set_outdata(proc->wsd_flush, proc->outtype_flush, dout);
                    proc->outcnt++;
                    proc->issue_flush=0;
               }
          }
          return 1;
     }
     else if (proc->issue_flush) {
          ws_set_outdata(proc->wsd_flush, proc->outtype_flush, dout);
          proc->outcnt++;
          proc->issue_flush=0;
          return 1;
     }
     else {
          return 0;
     }
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->compiledScript->flush(proc->compiledScript);
     return 1;
}
     
//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("outcnt %" PRIu64, proc->outcnt);

     wsdata_delete(proc->wsd_flush);
     proc->compiledScript->destroy(proc->compiledScript);

     //free dynamic allocations
     free(proc->wscalc);
     free(proc);

     return 1;
}

