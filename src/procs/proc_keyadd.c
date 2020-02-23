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
//keeps count of keys.. keep a representative tuple for each key..
#define PROC_NAME "keyadd"

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_double.h"
#include "procloader_keystate.h"

int is_prockeystate = 1;
int prockeystate_gradual_expire = 1;
int prockeystate_multivalue = 1;

char proc_version[]     = "1.5";
char *proc_menus[] = { "Count", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Keeps count of a label's value based on keys";
char *proc_synopsis[] = { "keyadd <LABEL> [-V <LABEL> -P | -R | -M <size> | -L <LABEL>]", NULL};
char *proc_tags[] = {"key", "count", NULL};
char proc_description[] = {"The keyadd kid sums up a label's value, accumulating the sum based on "
			"a label. It stores a seperate COUNT per each LABEL-KEY and adds to it the "
			"value stored at LABEL-VALUE. It is able to calculate the percentage in lieu "
			"of the total sum. You can specify other factors like maximum table size "
			"or keep only the member that matches out of the entire tuple. If no value "
			"is specified via the LABEL-VALUE, it will count the number of times it sees LABEL-KEY."
			""};
proc_example_t proc_examples[] = {
     {"... | keyadd LABEL-KEY -V LABEL-VALUE | ...", "will sum up all of the LABEL-VALUE within a unique LABEL-KEY"},     
	{"... | keyadd LABEL-KEY | ...", "totals the number of times it sees each of type of the LABEL-KEY"},
	{"... | keyadd LABEL-KEY -V -L MYCOUNT | ...", "does the exact same as without -V option and labels the count as MYCOUNT"},
          {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'V',"","label",
     "LABEL of value to add at key (can specify multiple)",0,0},
     {'P',"","",
     "calculate percentage of count",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'L',"","LABEL",
     "label the value sum as LABEL (in order of values)",0,0},
     {'C',"","LABEL",
     "label the count as LABEL",0,0},
     {'R',"","",
      "keep only the member that matches, not the whole tuple",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_requires[] = "none";
char proc_nonswitch_opts[]    = "LABEL of key to count";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"COUNT", NULL};
proc_port_t proc_input_ports[] =  {
     {"none","normal operation"},
     {"EXPIRE","trigger gradual expiration of buffered states"},
     {"DELETE","expire specific key, flush state"},
     {"REMOVE","expire specific key, flust state"},
     {NULL, NULL}
};

char *proc_tuple_conditional_container_labels[] = {NULL};

typedef struct _key_data_t {
     wsdata_t * wsd;
     uint64_t cnt;
} key_data_t;

int prockeystate_state_size = sizeof(key_data_t);

typedef struct _value_data_t {
     uint64_t sum;
} value_data_t;

int prockeystate_value_size = sizeof(value_data_t);

typedef struct _proc_instance_t {
     uint64_t totalcnt;
     uint64_t outcnt;

     wslabel_t * label_cnt;
     wslabel_t * label_pct;
     wslabel_t * label_sum;
     wslabel_t ** label_value_sum;
     int max_values;
     int keep_only_key;
     int do_pct;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"COUNT",offsetof(proc_instance_t, label_cnt)},
     {"PCT",offsetof(proc_instance_t, label_pct)},
     {"SUM",offsetof(proc_instance_t, label_sum)},
     {"",0}
};

char prockeystate_option_str[]    = "PRL:C:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'R':
          proc->keep_only_key = 1;
          break;
     case 'P':
          proc->do_pct = 1;
          break;
     case 'C':
          proc->label_cnt = wsregister_label(type_table, str);
          break;
     case 'L':
          if (!proc->label_value_sum) {
               proc->label_value_sum = (wslabel_t**)calloc(1, sizeof(wslabel_t*));
          }
          else {
               proc->label_value_sum = (wslabel_t**)realloc(proc->label_value_sum,
                                                            (proc->max_values + 1) *
                                                            sizeof(wslabel_t*));
          }
          if (!proc->label_value_sum) {
               return 0;
          }
          proc->label_value_sum[proc->max_values] = wsregister_label(type_table, str);
          proc->max_values++;
          break;
     }
     return 1;
}

static inline void add_scores(proc_instance_t *proc, 
                              wsdata_t * tup, key_data_t * kd, 
                              ws_doutput_t * dout,
                              ws_outtype_t * outtype_tuple,
                              int value_cnt, void * vlist) {
     tuple_member_create_uint64(tup, kd->cnt, proc->label_cnt);
     if (proc->do_pct) {
          tuple_member_create_double(tup,
                                     (double)kd->cnt/(double)proc->totalcnt,
                                     proc->label_pct);
     }
     int i;
     for (i = 0; i < value_cnt; i++) {
          uint8_t * offset = (uint8_t*)vlist + i * sizeof(value_data_t);
          value_data_t * vdata = (value_data_t*)offset;
         
          if (i < proc->max_values) {
               tuple_member_create_uint64(tup, vdata->sum, proc->label_value_sum[i]);
          }
          else {
               tuple_member_create_uint64(tup, vdata->sum, proc->label_sum);
          }
     }
     ws_set_outdata(tup, outtype_tuple, dout);
     proc->outcnt++;
}

// reset the counts
void prockeystate_expire_multi(void * vproc, void * vdata,
                               ws_doutput_t * dout,
                               ws_outtype_t * outtype_tuple, 
                               int value_cnt, void * vlist) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     key_data_t * kd = (key_data_t*)vdata;
     if (kd->wsd) {
          if (proc->keep_only_key) {
               wsdata_t * tup = wsdata_alloc(dtype_tuple);
               if (tup) {
                    add_tuple_member(tup, kd->wsd);
                    add_scores(proc, tup, kd, dout, outtype_tuple,
                               value_cnt,
                               vlist);
               }
          }
          else {
               add_scores(proc, kd->wsd, kd, dout, outtype_tuple,
                          value_cnt, vlist);
          }
          wsdata_delete(kd->wsd);
          kd->wsd = NULL;
     }
     kd->cnt = 0;
}

// Add values to total counts and count
int prockeystate_update_value(void * vproc, void * vstate, wsdata_t * tuple,
                              wsdata_t *key, wsdata_t * value) {
     uint64_t v64 = 0;

     if (!dtype_get_uint(value, &v64)) {
          return 0;
     }

     value_data_t * vdata = (value_data_t *) vstate;

     vdata->sum += v64;
     
     return 0;
}

// Add 1 to totalcount and count
int prockeystate_update(void * vproc, void * vstate, wsdata_t * tuple,
                        wsdata_t *key) {

     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;

     proc->totalcnt++;
     kdata->cnt++;
     if (!kdata->wsd) {
          if (proc->keep_only_key) {
               kdata->wsd = key;
               wsdata_add_reference(key);
          }
          else {
               kdata->wsd = tuple;
               wsdata_add_reference(tuple);
          }
     }

     return 0;
}

// Flush state to be zero
void prockeystate_flush(void * vproc) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     proc->totalcnt = 0;
}

//return 1 if successful
//return 0 if no..
int prockeystate_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     //free(proc); // free this in the calling function

     return 1;
}

