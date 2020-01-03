/*
proc_keytrans.c -- time state transitions in value per key

Copyright 2019 Morgan Stanley

THIS SOFTWARE IS CONTRIBUTED SUBJECT TO THE TERMS OF YOU MAY OBTAIN A COPY OF
THE LICENSE AT https://www.apache.org/licenses/LICENSE-2.0.

THIS SOFTWARE IS LICENSED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE AND ANY
WARRANTY OF NON-INFRINGEMENT, ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE. THIS SOFTWARE MAY BE REDISTRIBUTED TO OTHERS ONLY BY
EFFECTIVELY USING THIS OR ANOTHER EQUIVALENT DISCLAIMER IN ADDITION TO ANY
OTHER REQUIRED LICENSE TERMS
 */
#define PROC_NAME "keytrans"

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
#include "evahash64_data.h"
#include "sysutil.h"

int is_prockeystate = 1;

char proc_version[]     = "1.1";
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "time transitions in value per key";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'V',"","data",
     "value to detect transition",0,0},
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
     wsdt_ts_t last;
     uint64_t state;
} key_data_t;

int prockeystate_state_size = sizeof(key_data_t);

typedef struct _proc_instance_t {
     wslabel_t * label_ts;
     wslabel_t * label_timediff;
     uint32_t seed;
     int keepfirst;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"TIMEDIFF",offsetof(proc_instance_t, label_timediff)},
     {"",0}
};

char prockeystate_option_str[]    = "fFL:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'F':
     case 'f':
          proc->keepfirst = 1;
          break;
     case 'L':
          proc->label_timediff = wsregister_label(type_table, str);
          break; 
     }

     return 1;
}

int prockeystate_init(void * vproc, void * type_table, int hasvalue) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     proc->label_ts = wssearch_label(type_table, "DATETIME");
     proc->seed = rand();

     return 1;
}

static wsdt_ts_t * get_datetime(proc_instance_t * proc, wsdata_t * tdata) {
     wsdata_t ** mset;
     int mset_len;
     wsdt_ts_t * ts = NULL;
     if (tuple_find_label(tdata, proc->label_ts,
                          &mset_len, &mset)) {
          if (mset_len && (mset[0]->dtype == dtype_ts)) {
               //just choose first
               ts = mset[0]->data;
          }
     }
     return ts;
}

static void add_timediff(proc_instance_t * proc, wsdata_t * tdata,
                         wsdt_ts_t * ts, key_data_t * kdata) {
     double tnew = (double)ts->sec + (double)ts->usec/1000000;
     double last = (double)kdata->last.sec + (double)kdata->last.usec/1000000;
     double diff = tnew - last;
     tuple_member_create_double(tdata, diff, proc->label_timediff);
}

int prockeystate_update(void * vproc, void * vstate, wsdata_t * tdata,
                        wsdata_t *key) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;
     wsdt_ts_t * ts = get_datetime(proc, tdata);
     if (!ts) {
          return 0;
     }

     if (!kdata->last.sec) {
          kdata->state = 1;
     }
     else {
          add_timediff(proc, tdata, ts, kdata);
     }
     kdata->last.sec = ts->sec;
     kdata->last.usec = ts->usec;
     return 1;
}
int prockeystate_update_value(void * vproc, void * vstate, wsdata_t * tdata,
                              wsdata_t *key, wsdata_t *value) {

     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;

     uint64_t v64 = evahash64_data(value, proc->seed); //hash value


     wsdt_ts_t * ts = get_datetime(proc, tdata);
     if (!ts) {
          return 0;
     }

     if (!kdata->state && !kdata->last.sec) {
          kdata->state = v64;
          kdata->last.sec = ts->sec;
          kdata->last.usec = ts->usec;
     }
     else if (kdata->state != v64) {
          add_timediff(proc, tdata, ts, kdata);
          kdata->state = v64;
          kdata->last.sec = ts->sec;
          kdata->last.usec = ts->usec;
     }
     else if (!proc->keepfirst) {
          kdata->last.sec = ts->sec;
          kdata->last.usec = ts->usec;
     }
     //memcpy(&kdata->last, ts, sizeof(wsdt_ts_t));
     
     return 1;
}


