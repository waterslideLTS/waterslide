/*

proc_countfilter.c  - probabilistic counting filter - the more frequent events
occur on a key the less likely they will be emitted

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
#define PROC_NAME "countfilter"

//based on concepts from:
//Robert Morris. 1978. article. Counting Large Numbers of Events in Small Registers.
// Commun. ACM 21, 10 (Oct. 1978), 840–842. https://doi.org/10.1145/359619.359627

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
#include "procloader_keystate.h"
#include "evahash64_data.h"
#include "sysutil.h"

int is_prockeystate = 1;

char proc_version[]     = "1.1";
char *proc_alias[]     = { "freqfilter", "probfilter", "heavyfilter", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "sample events where the more frequently events occur for a key, the less likley they will be selected";
char *proc_synopsis[] = {"countfilter KEY [-P <decimal_value between 1 and 2>]", NULL};

char proc_description[] = 
    "countfilter uses a probabilistic counting method per key to determine "
    "whether to pass on events for a given key. Each time an event is passed, "
    "the exponent of the random value target is increased, thus decreasing the probability "
    "new events will be passed. One can esimate the orignal count by taking the "
    "base value (2 by default) to the power the the observed count. For example " 
    "if 11 events are emitted, the esimate is that 2^11 or 2048 events were observed as input."
    "";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'P',"","decimal",
     "base of the probabilistic counting exponent, typically between 1 and 2 (DEFAULT:2)",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of key to count and track";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"COUNT", NULL};

#define MAX_VALUE (65535)

typedef struct _key_data_t {
     uint16_t value;
} key_data_t;

int prockeystate_state_size = sizeof(key_data_t);

typedef struct _proc_instance_t {
     uint64_t input_cnt;
     uint64_t output_cnt;
     int custom_power;
     double power;
} proc_instance_t;

int prockeystate_instance_size = sizeof(proc_instance_t);

char prockeystate_option_str[]    = "p:P:";

int prockeystate_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'p':
     case 'P':
          proc->power = atof(str);
          proc->custom_power = 1;
          break;
     default:
          return 0;
     }
     return 1;
}
               

int prockeystate_update(void * vproc, void * vstate, wsdata_t * tdata,
                        wsdata_t *key) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *) vstate;
     proc->input_cnt++;

     if (!kdata->value) {
          kdata->value = 1;
          proc->output_cnt++;
          return 1;
     }

     uint32_t prob;
     if (proc->custom_power) {
          prob = (uint32_t)pow(proc->power, kdata->value);
     }
     else {
          prob = (0x1 << kdata->value);
     }

     uint32_t rand = (uint32_t)mrand48();

     // or if doing crypto rand:
     //uint32_t rand = 0;
     //uint8_t * ptrrand = (uint8_t *)&rand;
     //RAND_bytes(ptrrand, sizeof(uint32_t));

     if ((rand % prob) == 0) {
          if (proc->custom_power) {
               if (kdata->value < MAX_VALUE) {
                    kdata->value++;
               }
          }
          else if (kdata->value < 32) {
               kdata->value++;
          }
          proc->output_cnt++;
          return 1;

     }
     return 0;
}
int prockeystate_destroy(void * vproc) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (proc->output_cnt) {
          tool_print("output: %"PRIu64, proc->output_cnt);
          tool_print("output percentage: %.2f%%",
                     ((double)proc->output_cnt * 100)/(double)proc->input_cnt);
     }
     return 1;
}

