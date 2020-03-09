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
//outputs the absolute difference between current value and last value
#define PROC_NAME "keydiff"

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
char *proc_menus[] = { "State", NULL };
char *proc_alias[]     = { "keyabsolutediff", "diff", "absolutediff", "adiff", "keyabsdiff", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "outputs the absolute difference between current and prior value";
char *proc_synopsis[] = { "keydiff <LABEL> [-V <LABEL>] [-V <LABEL2>] [-V <LABEL3>]", NULL};
char *proc_tags[] = {"key", "count", NULL};
char proc_description[] = {"The keydiff kid outputs the absolute difference between current value and prior state value"
			""};
proc_example_t proc_examples[] = {
     {"... | keydiff LABEL-KEY -V LABEL-VALUE | ...", "will diff uthe LABEL-VALUE within a unique LABEL-KEY"},     
     {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'V',"","label",
     "LABEL of value to add at key (can specify multiple)",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'L',"","LABEL",
     "label the value diff as LABEL (in order of values)",0,0},
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
     {"DELETE","expire specific key, flush state"},
     {"REMOVE","expire specific key, flust state"},
     {NULL, NULL}
};

char *proc_tuple_conditional_container_labels[] = {NULL};

/*
typedef struct _key_data_t {
} key_data_t;
*/
int prockeystate_state_size = 0;

typedef struct _value_data_t {
     uint8_t revisit;
     uint64_t prior;
} value_data_t;

int prockeystate_value_size = sizeof(value_data_t);

typedef struct _proc_instance_t {
     uint64_t totalcnt;
     uint64_t outcnt;

     wslabel_t ** label_value_diff;
     int max_values;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

char prockeystate_option_str[]    = "L:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'L':
          if (!proc->label_value_diff) {
               proc->label_value_diff = (wslabel_t**)calloc(1, sizeof(wslabel_t*));
          }
          else {
               proc->label_value_diff = (wslabel_t**)realloc(proc->label_value_diff,
                                                            (proc->max_values + 1) *
                                                            sizeof(wslabel_t*));
          }
          if (!proc->label_value_diff) {
               return 0;
          }
          proc->label_value_diff[proc->max_values] = wsregister_label(type_table, str);
          proc->max_values++;
          break;
     }
     return 1;
}

int prockeystate_init_mvalue(void * vproc, void * type_table, int vcount, wslabel_t ** vlabels) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (vcount <= proc->max_values) {
          return 1;
     }
     proc->label_value_diff = (wslabel_t**)realloc(proc->label_value_diff,
                                                   vcount * sizeof(wslabel_t*));

     if (!proc->label_value_diff) {
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
          proc->label_value_diff[i] = wsregister_label_wprefix(type_table,
                                                               v,
                                                               "_DIFF");
     }
     proc->max_values = vcount;

     return i;
}

// Add values to total counts and count
int prockeystate_update_value_index(void * vproc, void * vstate, wsdata_t * tuple,
                                    wsdata_t *key, wsdata_t * value, int vindex) {
     uint64_t v64 = 0;

     if (!dtype_get_uint(value, &v64)) {
          return 1;
     }
     value_data_t * vdata = (value_data_t*)vstate;

     if (!vdata->revisit) {
          vdata->prior = v64;
          vdata->revisit = 1;
          return 1;
     }

     uint64_t diff;
     if (v64 >= vdata->prior) {
          diff = v64 - vdata->prior;
     }
     else {
          diff = vdata->prior - v64;
     }
     proc_instance_t * proc = (proc_instance_t *)vproc;
     tuple_member_create_uint64(tuple, diff, proc->label_value_diff[vindex]);
     vdata->prior = v64;

     return 1;
}


