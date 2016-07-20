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
//keeps count of keys per epoch
#define PROC_NAME "keyrate"

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
#include "sysutil.h"

int is_prockeystate = 1;

char proc_version[]     = "1.1";
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "keep count of keys per epoch";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'t',"","duration",
     "epoch duration in seconds (or use m for minutes, h for hours)",0,0},
     {'m',"","cnt",
     "minimum count before reporting",0,0},
     {'x',"","cnt",
     "maximum count per epoch",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'L',"","label",
     "record count as label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of key to count";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"COUNT", NULL};

typedef struct _key_data_t {
     uint32_t epoch;
     uint64_t cnt;
} key_data_t;

int prockeystate_state_size = sizeof(key_data_t);

typedef struct _proc_instance_t {
     wslabel_t * label_cnt;
     wslabel_t * label_rate;
     wslabel_t * label_epoch;
     wslabel_t * label_ts;
     time_boundary_t epoch_boundary;
     uint32_t epoch;
     uint64_t min_cnt;
     uint64_t max_cnt;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"ACC",offsetof(proc_instance_t, label_cnt)},
     {"RATE",offsetof(proc_instance_t, label_rate)},
     {"EPOCH_ID",offsetof(proc_instance_t, label_epoch)},
     {"",0}
};

char prockeystate_option_str[]    = "x:m:t:L:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'm':
          proc->min_cnt = atoi(str);
          break;
     case 'x':
          proc->max_cnt = atoi(str);
          break;
     case 't':
          proc->epoch_boundary.increment_ts = sysutil_get_duration_ts((char *)str);
          if (proc->epoch_boundary.increment_ts) {
               fprintf(stderr,"%s new epoch every ", PROC_NAME);
               sysutil_print_time_interval(stderr,
                                           proc->epoch_boundary.increment_ts);
               fprintf(stderr,"\n");
          }
          else {
               tool_print("time must be divisible by the hour %d",
                          (int)proc->epoch_boundary.increment_ts);
               return 0;
          }
          break;
     case 'L':
          proc->label_cnt = wsregister_label(type_table, str);
          break; 
     }

     return 1;
}

int prockeystate_init(void * vproc, void * type_table, int hasvalue) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (proc->epoch_boundary.increment_ts == 0) {
          proc->epoch_boundary.increment_ts = 60;
     }
     if (!proc->min_cnt) {
          if (hasvalue) {
               proc->min_cnt = 10000;
          }
          else {
               proc->min_cnt = 3;
          }
     }

     proc->label_ts = wssearch_label(type_table, "DATETIME");

     return 1;
}

static inline int report_key_rate(proc_instance_t * proc, wsdata_t * tdata,
                                  key_data_t * kdata) {
     if ((kdata->cnt < proc->min_cnt) || 
         (proc->max_cnt && (kdata->cnt > proc->max_cnt))) {
          return 0;
     }    
     else {
          tuple_member_create_uint64(tdata, kdata->cnt, proc->label_cnt);
          tuple_member_create_uint(tdata, kdata->epoch, proc->label_epoch);
          tuple_member_create_double(tdata,
                                     (double)kdata->cnt / (double)proc->epoch_boundary.increment_ts,
                                     proc->label_rate);
          return 1;
     } 
}

static inline void update_epoch(proc_instance_t * proc, wsdata_t * tuple) {
     //lookup up time.. otherwise poll for time
     time_t tsec = 0;

     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(tuple, proc->label_ts,
                          &mset_len, &mset)) {
          if (mset_len && (mset[0]->dtype == dtype_ts)) {
               //just choose first
               wsdt_ts_t * ts = NULL;
               ts = mset[0]->data;
               tsec = ts->sec;
          }
     }
     if (!tsec) {
          tsec = time(NULL);
     }

     if (sysutil_test_time_boundary(&proc->epoch_boundary, tsec)) {
          proc->epoch++;
          tool_print("epoch %d", proc->epoch);
     }
}

int prockeystate_update(void * vproc, void * vstate, wsdata_t * tuple,
                        wsdata_t *key) {

     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;
     int rtn = 0;

     //lookup up time.. otherwise poll for time
     update_epoch(proc, tuple);

     if (proc->epoch == kdata->epoch) {
          kdata->cnt++;
     }
     else {
          /// report epoch
          rtn = report_key_rate(proc, tuple, kdata);
          //reset epoch
          kdata->epoch = proc->epoch;
          kdata->cnt = 1;
     }

     return rtn;
}

int prockeystate_update_value(void * vproc, void * vstate, wsdata_t * tuple,
                              wsdata_t *key, wsdata_t *value) {

     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;
     int rtn = 0;

     uint64_t v64 = 0;
     if (!dtype_get_uint64(value, &v64)) {
          return 0;
     }

     update_epoch(proc, tuple);
     
     if (proc->epoch == kdata->epoch) {
          kdata->cnt += v64;
     }
     else {
          /// report epoch
          rtn = report_key_rate(proc, tuple, kdata);
          //reset epoch
          kdata->epoch = proc->epoch;
          kdata->cnt = v64;
     }

     return rtn;
}

//return 1 if successful
//return 0 if no..
int prockeystate_destroy(void * vinstance) {
     //proc_instance_t * proc = (proc_instance_t*)vinstance;

     //free dynamic allocations
     //free(proc); // free this in the calling function

     return 1;
}

