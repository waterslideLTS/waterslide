/*
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
//finds periodic keys

// test by doing pings
//     ping www.google.com -i 1.35
// "pcap_in -i eth0 | tuplehash -o SRCIP DSTIP -L FOO | periodic FOO | print -TV"


#define PROC_NAME "periodic"

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

int is_prockeystate = 1;

char proc_version[]     = "1.1";
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "detect periodic instances per key, assumes DATETIME is available";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'b',"","bins",
     "number of reference bins per key (default: 4)",0,0},
     {'c',"","count",
     "number of observations before reporting results (default: 4)",0,0},
     {'t',"","milliseconds",
     "minimum time delta (default: 500ms)",0,0},
     {'T',"","milliseconds",
     "maximum time delta",0,0},
     {'x',"","double",
     "threshold scaling factor (default: 0.7)",0,0},
     {'y',"","double",
     "threshold slope factor (default: 0.25)",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'L',"","label",
     "record periodic time difference as label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of key to find periodicity";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"PERIOD", "PERIOD_COUNT", NULL};


/// all times in MSEC
typedef struct _tdelta_bin_t {
     time_t reference;  
     time_t threshold;
     time_t sum; //for averaging
     uint32_t cnt;
     struct _tdelta_bin_t * next;
} tdelta_bin_t;

typedef struct _key_data_t {
     time_t last;  //in msec
     tdelta_bin_t * list;   //lru list
} key_data_t;

int prockeystate_state_size = sizeof(key_data_t);

//indexed by 100 Msec
#define PRECOMPUTE_LEN (10000)
#define PRECOMPUTE_MAX (PRECOMPUTE_LEN * 100)

#define THRESH_X  (0.7)
#define THRESH_Y (0.25)

typedef struct _proc_instance_t {
     wslabel_t * label_ts;
     wslabel_t * label_periodic;
     wslabel_t * label_periodic_count;
     wslabel_t * label_periodic_threshold;
     wslabel_t * label_last_tdiff;
     tdelta_bin_t * freeq;
     uint32_t minimum_observations;
     uint32_t bincnt;
     int min_set;
     time_t minimum_tdiff;
     time_t maximum_tdiff;
     time_t threshold_precompute[PRECOMPUTE_LEN];  //indexed by 100MSEC
     double thresh_x;
     double thresh_y;
     int custom_threshold_factor;
     uint64_t outoforder;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"PERIOD",offsetof(proc_instance_t, label_periodic)},
     {"PERIOD_COUNT",offsetof(proc_instance_t, label_periodic_count)},
     {"PERIOD_THRESHOLD",offsetof(proc_instance_t, label_periodic_threshold)},
     {"LAST_TDIFF",offsetof(proc_instance_t, label_last_tdiff)},
     {"",0}
};

char prockeystate_option_str[]    = "x:y:c:C:b:B:t:T:L:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'x':
          proc->thresh_x = atof(str);
          tool_print("adjusted x threshold factor to %f", proc->thresh_x);
          proc->custom_threshold_factor = 1;
          break;
     case 'y':
          proc->thresh_y = atof(str);
          tool_print("adjusted y threshold factor to %f", proc->thresh_x);
          proc->custom_threshold_factor = 1;
          break;
     case 'B':
     case 'b':
          proc->bincnt = atoi(str);
          break;
     case 'C':
     case 'c':
          proc->minimum_observations = atoi(str);
          break;
     case 't':
          proc->min_set = 1;
          proc->minimum_tdiff = atoi(str);
          break;
     case 'T':
          proc->maximum_tdiff = atoi(str);
          break;
     case 'L':
          proc->label_periodic = wsregister_label(type_table, str);
          break; 
     }

     return 1;
}


time_t get_threshold(proc_instance_t * proc, time_t delta) {
     double td = (double) delta * 0.001;
     double threshold = 1000 * proc->thresh_x * log(proc->thresh_y * (td + 1.0) + 
                                             (1.0 - proc->thresh_y));
     return (time_t)threshold;
}
time_t get_threshold_optimized(proc_instance_t * proc, time_t delta) {
     if (delta < PRECOMPUTE_MAX) {
          time_t dindex = delta / 100;
          return proc->threshold_precompute[dindex];
     }
     return get_threshold(proc, delta);
}


void print_ex_threshold(proc_instance_t * proc, time_t msec) {
     time_t thr = get_threshold_optimized(proc, msec);

     tool_print("when tdelta is %.3f, threshold is %.3f",
                (double)msec/1000,
                (double)thr/1000);
}

void print_example_thresholds(proc_instance_t * proc) {
     print_ex_threshold(proc, 500);
     print_ex_threshold(proc, 2000);
     print_ex_threshold(proc, 30000);
     print_ex_threshold(proc, 600000);
     print_ex_threshold(proc, 1800000);
     print_ex_threshold(proc, 3600000);
     print_ex_threshold(proc, 4*3600000);
     print_ex_threshold(proc, 12*3600000);
     print_ex_threshold(proc, 24*3600000);
}

int prockeystate_init(void * vproc, void * type_table, int hasvalue) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (!proc->min_set) {
          proc->minimum_tdiff = 500;
     }
     if (!proc->minimum_observations) {
          proc->minimum_observations = 4;
     }
     if (!proc->bincnt) {
          proc->bincnt = 4;
     }
     if (!proc->thresh_x) {
          proc->thresh_x = THRESH_X;
     }
     if (!proc->thresh_y) {
          proc->thresh_y = THRESH_Y;
     }
     proc->label_ts = wssearch_label(type_table, "DATETIME");

     time_t i;
     for (i = 0; i < PRECOMPUTE_LEN; i++) {
          proc->threshold_precompute[i] = get_threshold(proc, i * 100 + 50);
     }
     if (proc->custom_threshold_factor) {
          print_example_thresholds(proc);
     }

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
     //get timestamp from clock
     return 0;
     
}

void fill_new_bin(proc_instance_t * proc, key_data_t * kdata,
                  tdelta_bin_t * bin, time_t tdiff) {
     if (!bin) {
          if (proc->freeq) {
               bin = proc->freeq;
               proc->freeq = bin->next;
          }
          else {
               bin = malloc(sizeof(tdelta_bin_t));
               if (!bin) {
                    return;
               }
          }
     }
     //memset(bin, 0, sizeof(tdelta_bin_t));
     bin->next = kdata->list;
     kdata->list = bin;
     bin->reference = tdiff;
     bin->threshold = get_threshold_optimized(proc, tdiff);
     bin->sum = tdiff;
     bin->cnt = 1;
}

int prockeystate_update(void * vproc, void * vstate, wsdata_t * tdata,
                        wsdata_t *key) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;
     time_t current = get_datetime_msec(proc, tdata);

     if (!current) {
          return 0;
     }
     if (!kdata->last) {
          kdata->last = current;
          return 0;
     }
     if (kdata->last > current) {
          proc->outoforder++;
          kdata->last = current;
          return 0;
     }
     time_t tdiff = current - kdata->last;
     kdata->last = current;

     //ignore certain time differentials
     if (tdiff < proc->minimum_tdiff) {
          return 0;
     }
     if (proc->maximum_tdiff && (tdiff > proc->maximum_tdiff)) {
          return 0;
     }

     int list_len = 0;
     tdelta_bin_t * prev = NULL;

     tdelta_bin_t * cursor = kdata->list;

     //walk all bins
     while(cursor) {
          if ((tdiff + cursor->threshold >= cursor->reference) && 
              (tdiff <= cursor->threshold + cursor->reference)) {
               dprint("bin matched");
               cursor->cnt++;
               cursor->sum += tdiff;
               cursor->reference = cursor->sum / (time_t)cursor->cnt;

               //move matched cursor to front of list.. sort most recently used
               if (prev) {
                    prev->next = cursor->next;
                    cursor->next = kdata->list;
                    kdata->list = cursor;
               }

               if (cursor->cnt >= proc->minimum_observations) {
                    tuple_member_create_double(tdata,
                                               (double)cursor->reference/1000.0,
                                               proc->label_periodic);
                    tuple_member_create_uint32(tdata,
                                               cursor->cnt,
                                               proc->label_periodic_count);
                    /*
                    tuple_member_create_double(tdata,
                                               (double)cursor->threshold/1000.0,
                                               proc->label_periodic_threshold);
                    tuple_member_create_double(tdata,
                                               (double)tdiff/1000.0,
                                               proc->label_last_tdiff);
                                               */

                    return 1;
               }
               return 0;
          }
          list_len++;
          if (list_len >= proc->bincnt) {
               dprint("recycling last element");
               //recycle last element in list
               if (prev) {
                    prev->next = NULL;
               }
               else {
                    kdata->list = NULL;
               }
               fill_new_bin(proc, kdata, cursor, tdiff);

               return 0;
          }
          prev = cursor;
          cursor = cursor->next;
     }
     //start new bin
     dprint("starting new bin");
     fill_new_bin(proc, kdata, NULL, tdiff);

     return 0;
}

void prockeystate_expire(void * vproc, void * vdata, ws_doutput_t * dout,
                         ws_outtype_t * outtype_tuple) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     key_data_t * kd = (key_data_t*)vdata;

     dprint("expiring element");

     //walk list of bins -- recycle them.
     if (!kd->list) {
          return;
     }

     tdelta_bin_t * cursor = kd->list;
     //find tail of freeq;
     while (cursor) {
          if (cursor->next) {
               cursor = cursor->next;
          }
          else {
               cursor->next = proc->freeq;
               proc->freeq = kd->list;
               break;
          }
     }
     kd->list = NULL;
}

int prockeystate_destroy(void * vproc) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     if (proc->outoforder) {
          tool_print("events out of order: %"PRIu64, proc->outoforder);
     }

     tool_print("freeing up freeq: START");
     tdelta_bin_t * cursor = proc->freeq;
     tdelta_bin_t * next;
     while(cursor) {
          next = cursor->next;
          free(cursor);
          cursor = next;
     }
     tool_print("freeing up freeq: FINISHED");
     return 1;
}
