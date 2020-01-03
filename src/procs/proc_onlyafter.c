/*

proc_onlyafter.c  - filter out until matched label condition is met

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
#define PROC_NAME "onlyafter"

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
char *proc_alias[]     = { "afteronly", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "pass on events about a key only after detected labels are present";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'A',"","label",
     "label to detect and pass after first detected",0,0},
     {'n',"","",
     "do not include match event in output",0,0},
     {'x',"","",
     "require all conditions to match (default: any match)",0,0},
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
     uint8_t state;
} key_data_t;

int prockeystate_state_size = sizeof(key_data_t);

typedef struct _proc_instance_t {
     int notonmatch;
     int requireall;
     wslabel_set_t lset;
     wslabel_nested_set_t nest;
     int matchcnt;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

char prockeystate_option_str[]    = "Xxa:A:nN";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'X':
     case 'x':
          proc->requireall = 1;
          break;
     case 'N':
     case 'n':
          proc->notonmatch = 1;
          break;
     case 'a':
     case 'A':
          if (index(str,'.') == NULL ) {
               wslabel_set_add(type_table, &proc->lset, str);
               //its not nested -- consider for tuple container label too
          }
          wslabel_nested_search_build(type_table, &proc->nest, str);
          proc->matchcnt++;
          break; 
     }

     return 1;
}

int prockeystate_init(void * vproc, void * type_table, int hasvalue) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     if (proc->nest.cnt == 0) {
          tool_print("must supply -A label to search for");
          return 0;
     }

     return 1;
}

static int nest_search_callback_match(void * vproc, void * vstate,
                                      wsdata_t * tdata, wsdata_t * member) {
     return 1;
}

int container_search(proc_instance_t * proc, wsdata_t * tdata) {
     int i;
     int cnt = 0;
     for (i = 0; i < proc->lset.len; i++) {
          if (wsdata_check_label(tdata, proc->lset.labels[i])) {
               cnt++;
          }
     }
     return cnt;
}

int prockeystate_update(void * vproc, void * vstate, wsdata_t * tdata,
                        wsdata_t *key) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;

     if (kdata->state) {
          return 1;
     }

     int hit = container_search(proc, tdata);

     if (!hit || proc->requireall) {
          //check nested
          hit += tuple_nested_search(tdata, &proc->nest,
                                    nest_search_callback_match,
                                    proc, &kdata);
     }

     if (!hit || (proc->requireall && (hit < proc->matchcnt))) {
          return 0;
     }
     kdata->state = 1;

     if (proc->notonmatch) {
          return 0;
     }
     return 1;
}

