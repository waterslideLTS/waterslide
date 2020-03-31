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

//computes the running exponential weighted moving average per <key,value> specified
// also computes variance of this average
#define PROC_NAME "keyEWMA"

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
int prockeystate_multivalue = 1;

char proc_version[]     = "1.5";
char *proc_menus[] = { "Count", NULL };
char *proc_alias[]     = { "keyEMA", "keyewma", "keyema", "exponentialmovingaverage", "keyexpmovavg", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "computes the exponential weighted moving average of each value at a key";
char *proc_synopsis[] = { "keyEWMA <LABEL> [-V <LABEL> -P | -R | -M <size> | -L <LABEL>]", NULL};
char *proc_tags[] = {"key", "count", NULL};
char proc_description[] = {"The keyEWMA computes the exponential weighted moving average of values at a given key"
			""};
proc_example_t proc_examples[] = {
     {"... | keyEWMA LABEL-KEY -V LABEL-VALUE | ...", "will compute EWMA of LABEL-VALUEs within a unique LABEL-KEY"},     
     {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'V',"","label",
     "LABEL of value to compute moving average (can specify multiple)",0,0},
     {'a',"","",
     "weight of new value over old average (decimal between 0 an 1) - defaults to 0.05",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'L',"","LABEL",
     "label the value average as LABEL (in order of values)",0,0},
     {'Q',"","LABEL",
     "label the value variance as LABEL (in order of values)",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_requires[] = "none";
char proc_nonswitch_opts[]    = "LABEL of key";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"COUNT", NULL};
proc_port_t proc_input_ports[] =  {
     {"none","normal operation"},
     {"EXPIRE","trigger gradual expiration of buffered states"},
     {"DELETE","expire specific key, flush state"},
     {"REMOVE","expire specific key, flush state"},
     {NULL, NULL}
};

char *proc_tuple_conditional_container_labels[] = {NULL};

int prockeystate_state_size = 0;  // not state tracked for just the key

typedef struct _value_data_t {
     uint8_t cnt;  //0 = noprior or 1 has prior  
     double prioravg;
     double priorvar;
} value_data_t;

int prockeystate_value_size = sizeof(value_data_t);

typedef struct _proc_instance_t {
     double alpha;
     double one_minus_alpha;
     wslabel_t ** label_value_ewma;
     wslabel_t ** label_value_ewma_variance;
     int max_values;
     int max_values_variance;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

char prockeystate_option_str[]    = "a:L:Q:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'a':
          proc->alpha = atof(str);
          if ((proc->alpha <= 0) || (proc->alpha >= 1)) {
               tool_print("invalid alpha '%f' - must be between 0 and 1",
                          proc->alpha);
               return 0;
          }
          break;
     case 'L':
          if (!proc->label_value_ewma) {
               proc->label_value_ewma = (wslabel_t**)calloc(1, sizeof(wslabel_t*));
          }
          else {
               proc->label_value_ewma = (wslabel_t**)realloc(proc->label_value_ewma,
                                                            (proc->max_values + 1) *
                                                            sizeof(wslabel_t*));
          }
          if (!proc->label_value_ewma) {
               return 0;
          }
          proc->label_value_ewma[proc->max_values] = wsregister_label(type_table, str);
          proc->max_values++;
          break;
     case 'Q':
          if (!proc->label_value_ewma_variance) {
               proc->label_value_ewma_variance = (wslabel_t**)calloc(1, sizeof(wslabel_t*));
          }
          else {
               proc->label_value_ewma_variance = (wslabel_t**)realloc(proc->label_value_ewma_variance,
                                                            (proc->max_values_variance + 1) *
                                                            sizeof(wslabel_t*));
          }
          if (!proc->label_value_ewma_variance) {
               return 0;
          }
          proc->label_value_ewma_variance[proc->max_values_variance] = wsregister_label(type_table, str);
          proc->max_values_variance++;
          break;
     }

     return 1;
}

int prockeystate_init_mvalue(void * vproc, void * type_table, int vcount, wslabel_t ** vlabels) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (!proc->alpha) {
          proc->alpha = 0.05;
          tool_print("alpha not specified, setting to defaulti %f", proc->alpha);
     }
     proc->one_minus_alpha = 1 - proc->alpha;

     if (vcount <= proc->max_values) {
          return 1;
     }
     proc->label_value_ewma = (wslabel_t**)realloc(proc->label_value_ewma,
                                                   vcount * sizeof(wslabel_t*));

     if (!proc->label_value_ewma) {
          tool_print("unable to allocate labels");
          return 0;
     }
     int i;

     for (i = proc->max_values; i < vcount; i++) {
          char * v = rindex(vlabels[i]->name, '.');
          if (v) {
              v++;
          }
          else {
               v = vlabels[i]->name;
          }
          proc->label_value_ewma[i] = wsregister_label_wprefix(type_table,
                                                               v,
                                                               "_EWMA");
     }
     proc->max_values = vcount;

     ///now label variance
     if (vcount <= proc->max_values_variance) {
          return 1;
     }
     proc->label_value_ewma_variance = (wslabel_t**)realloc(proc->label_value_ewma_variance,
                                                   vcount * sizeof(wslabel_t*));

     if (!proc->label_value_ewma_variance) {
          tool_print("unable to allocate labels");
          return 0;
     }

     for (i = proc->max_values_variance; i < vcount; i++) {
          char * v = rindex(vlabels[i]->name, '.');
          if (v) {
              v++;
          }
          else {
               v = vlabels[i]->name;
          }
          proc->label_value_ewma_variance[i] = wsregister_label_wprefix(type_table,
                                                               v,
                                                               "_EWMA_VARIANCE");
     }
     proc->max_values_variance = vcount;

     return 1;
}

// compute ewma for each column of data (labeled value) per key
int prockeystate_update_value_index(void * vproc, void * vstate, wsdata_t * tuple,
                                    wsdata_t *key, wsdata_t * vcurrent, int vindex) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     double value = 0;
     double ewma = 0;
     double var = 0;

     if (!dtype_get_double(vcurrent, &value)) {
          return 1;
     }
     dprint("value %f", value);
     value_data_t * vdata = (value_data_t*)vstate;
     dprint("has prior %d", vdata->cnt);
     dprint("has prioravg %f", vdata->prioravg);
     dprint("has priorvar %f", vdata->priorvar);

     if (!vdata->cnt) {
          vdata->cnt = 1;
          ewma = value;
          var = 0;
     }
     else {
          ewma = proc->alpha * value  + (proc->one_minus_alpha) * vdata->prioravg;
          double diff = value - vdata->prioravg;
          var = (proc->one_minus_alpha) *
               (vdata->priorvar + proc->alpha * diff * diff);
     }

     vdata->prioravg = ewma;
     vdata->priorvar = var;

     tuple_member_create_double(tuple, ewma, proc->label_value_ewma[vindex]);
     tuple_member_create_double(tuple, var, proc->label_value_ewma_variance[vindex]);

     return 1;
}


