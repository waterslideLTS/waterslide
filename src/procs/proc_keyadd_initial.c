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
#define PROC_NAME "keyadd_initial"

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

char proc_version[]     = "1.1";
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "accumulate value, report when limit is reached";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'V',"","",
     "value to add at key",0,0},
     {'l',"","cnt",
     "limit to report value count (default 16)",0,0},
     {'L',"","label",
     "label to apply to accumulated value (default ACC)",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of key to count";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"COUNT", NULL};

typedef struct _key_data_t {
     uint64_t cnt;
     uint64_t value;
} key_data_t;

int prockeystate_state_size = sizeof(key_data_t);

typedef struct _proc_instance_t {
     uint64_t keys;
     wslabel_t * label_cnt;
     wslabel_t * label_value;
     int limit_cnt;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"COUNT",offsetof(proc_instance_t, label_cnt)},
     {"ACC",offsetof(proc_instance_t, label_value)},
     {"",0}
};

char prockeystate_option_str[]    = "l:L:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'l':
          proc->limit_cnt = atoi(str);
          break;
     case 'L':
          proc->label_value = wsregister_label(type_table, str);
          break;
     }
     return 1;
}

int prockeystate_init(void * vproc, void * type_table, int hasvalue) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (!hasvalue) {
          tool_print("must specify value with -V option");
          return 0;
     }

     if (proc->limit_cnt == 0) {
          proc->limit_cnt = 16;
     }
     return 1;
}

int prockeystate_update_value(void * vproc, void * vstate, wsdata_t * tuple,
                              wsdata_t *key, wsdata_t * value) {

     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;


     if (!kdata->cnt) {
          proc->keys++;
     }
     if (kdata->cnt >= proc->limit_cnt) {
          return 0;
     }

     uint64_t v64 = 0;

     if (!dtype_get_uint(value, &v64)) {
          return 0;
     }

     kdata->cnt++;
     kdata->value += v64;

     if (kdata->cnt == proc->limit_cnt) {
          tuple_member_create_uint(tuple, kdata->cnt, proc->label_cnt);
          tuple_member_create_uint(tuple, kdata->value, proc->label_value);
          return 1;
     }

     return 0;
}

int prockeystate_destroy(void * vproc) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     tool_print("keys %"PRIu64, proc->keys); 

     //free dynamic allocations
     //free(proc); // free this in the calling function

     return 1;
}

