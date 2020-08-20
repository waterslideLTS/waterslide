/*
Copyright 2020 Morgan Stanley

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
//finds keys acting persistently - acting N of M time windows


#define PROC_NAME "persist"

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_double.h"
#include "datatypes/wsdt_ts.h"
#include "procloader_keystate.h"
#include "evahash64_data.h"
#include "sysutil.h"
#include "bit.h"

int is_prockeystate = 1;

char proc_version[]     = "1.1";
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "detect persistence occurances of keys based on time windows";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'b',"","bins",
     "number of reference bins per key(default: 16, max:64)",0,0},
     {'c',"","count",
     "number of observations before reporting results (default: 4)",0,0},
     {'t',"","milliseconds",
     "time window per bin",0,0},
     {'T',"","milliseconds",
     "overall time span for window",0,0},
     {'L',"","label",
     "record persistence as label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of key to find persistence";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"PERSIST", "PERSIST_COUNT", NULL};

typedef struct _key_data_t {
     time_t last;  //in msec
     uint64_t events;
     uint64_t binbits;
} key_data_t;

int prockeystate_state_size = sizeof(key_data_t);


typedef struct _proc_instance_t {
     wslabel_t * label_ts;
     wslabel_t * label_persist;
     wslabel_t * label_persist_count;

     time_t bintime;
     time_t windowtime;
     uint64_t binbitmask;
     uint8_t bincnt;
     uint8_t minbits;

     uint64_t outoforder;

} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"PERSIST",offsetof(proc_instance_t, label_persist)},
     {"PERSIST_COUNT",offsetof(proc_instance_t, label_persist_count)},
     {"",0}
};

char prockeystate_option_str[]    = "c:C:b:B:t:T:L:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'B':
     case 'b':
          proc->bincnt = atoi(str);
          break;
     case 'C':
     case 'c':
          proc->minbits = atoi(str);
          break;
     case 't':
          proc->bintime = atoi(str);
          break;
     case 'T':
          proc->windowtime = atoi(str);
          break;
     case 'L':
          proc->label_persist = wsregister_label(type_table, str);
          break; 
     }

     return 1;
}


int prockeystate_init(void * vproc, void * type_table, int hasvalue) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (!proc->bincnt) {
          proc->bincnt = 16;
     }
     if (proc->bincnt > 64) {
          proc->bincnt = 64;
     }
     if (!proc->minbits) {
          proc->minbits = 4;
     }

     int i;
     for (i = 0; i < proc->bincnt; i++) {
          proc->binbitmask |= (1 << i);
     }

     if (proc->windowtime) {
          proc->bintime = proc->windowtime/proc->bincnt;
     }
     if (!proc->bintime) {
          proc->bintime = 1000;
     }

     proc->label_ts = wssearch_label(type_table, "DATETIME");

     return 1;
}

static time_t get_datetime_msec(proc_instance_t * proc, wsdata_t * tdata) {
     wsdata_t ** mset;
     int mset_len;
     wsdt_ts_t * ts = NULL;
     if (tuple_find_label(tdata, proc->label_ts,
                          &mset_len, &mset)) {
          if (mset_len && (mset[0]->dtype == dtype_ts)) {
               //just choose first
               ts = mset[0]->data;
               return WSDT_TS_MSEC(ts->sec, ts->usec);
          }
     }
     else {
          //get timestamp from clock
          struct timeval current;
          gettimeofday(&current, NULL);
          return WSDT_TS_MSEC(current.tv_sec, current.tv_usec);
     }
     return 0;

}

int prockeystate_update(void * vproc, void * vstate, wsdata_t * tdata,
                        wsdata_t *key) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;
     time_t current = get_datetime_msec(proc, tdata) / proc->bintime;

     if (!current) {
          return 0;
     }
     if (!kdata->last) {
          kdata->last = current;
          kdata->binbits = 1;
          return 0;
     }
     if (kdata->last > current) {
          proc->outoforder++;
          return 0;
     }

     time_t bindiff = current - kdata->last;

     if (!bindiff) {
          //duplicate hit in time window
          return 0;
     }

     kdata->last = current;

     if (bindiff >= proc->bincnt) {
          //flush bins - start anew
          kdata->binbits = 1;
     }
     else {
          kdata->binbits = ((kdata->binbits << bindiff) + 1) & proc->binbitmask;
     }

     uint32_t hits = uint64_count_bits(kdata->binbits);

     if (hits < proc->minbits) {
          return 0;
     }

     //prepare to output stuff
     tuple_member_create_uint32(tdata, hits,
                                proc->label_persist_count);

     wsdt_string_t * str = tuple_create_string(tdata, proc->label_persist, proc->bincnt);
     if (!str) {
          //unable to create string
          return 1;
     }

     int i;
     for (i = 0; i < proc->bincnt; i++) {
          str->buf[i] = '0' + (uint8_t)((kdata->binbits >> i) & 0x01);
     }

     return 1;
}

int prockeystate_destroy(void * vproc) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     if (proc->outoforder) {
          tool_print("events out of order: %"PRIu64, proc->outoforder);
     }

     return 1;
}
