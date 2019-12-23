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
#define PROC_NAME "source_keygen"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.5";
char *proc_tags[]     = { "source", "input", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Generate random data for test purposes.";
char *proc_synopsis[] = {"source_keygen [-a <value>] [-f] [-l] [-m <mask>] [-c <count>] [-s <seed>]", NULL};
char proc_description[] = "Generates a stream of tuples containing a random data that can be used to test the code base and some pipelines.  The default is to create a stream of random numbers, however, a finite set of random numbers can also be generated using the '-c' option.  Additional optional arguments control the generation of the stream of numbers including the '-a', '-f', '-l', and '-m' options documented in the options section.";
char proc_requires[] = "";
proc_example_t proc_examples[] = {
	{"source_keygen | ...", "Generates a stream of tuples containing random numbers stored in the KEY label buffer."},
	{"source_keygen -c 20 | ...", "Generates 20 tuples each containing a random number stored in the KEY label buffer."},
	{"source_keygen -l | ...", "Generates a stream of tuples containing uint64-sized random numbers."},
	{NULL, NULL}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'a',"","ascension",
     "ascension rate for key (default is 0, true uniform)",0,0},
     {'f',"","",
     "use rand_r(), a fast but crappy RNG (default is to use the more true RNG, mrand48())",0,0},
     {'l',"","",
     "generate uint64_t random numbers, based on two calls to mrand48())",0,0},
     {'m',"","mask",
     "bit mask for key",0,0},
     {'c',"","count",
     "number of keys to generate", 0, 0},
     {'s',"","seed",
     "seed for the RNG used",0,0},
     {'u',"","usec",
     "usec to sleep",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char *proc_input_types[] = {"None", NULL};
char *proc_output_types[] = {"tuple", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {"KEYGEN", NULL};
char *proc_tuple_conditional_container_labels = {NULL};
char *proc_tuple_member_labels[] = {"KEY", NULL};
char proc_nonswitch_opts[] = "";

//function prototypes for local functions
static int proc_source(void * , wsdata_t*, ws_doutput_t *, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t *outtype_tuple;
     wslabel_t * label_key;
     wslabel_t * label_tup;
     uint64_t max;
     uint64_t mask;
     uint64_t base;
     uint64_t add;
     unsigned int seed;
     unsigned int fast_rand;
     unsigned int u64_rand;
     int usleep_usec;
} proc_instance_t;

uint32_t POW31 = (uint32_t)(1<<31);

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc) {
     int op;

     while ((op = getopt(argc, argv, "u:a:flm:c:s:")) != EOF) {
          switch (op) {
          case 'u':
               proc->usleep_usec = atoi(optarg);
               break;
          case 'a':
               proc->add = (uint64_t)strtoull(optarg, NULL, 0);
               break;
          case 'f':
               proc->fast_rand = 1;
               break;
          case 'l':
               proc->u64_rand = 1;
               break;
          case 'm':
               proc->mask = (uint64_t)strtoull(optarg, NULL, 0);
               break;
          case 'c':
               proc->max = (uint64_t)strtoull(optarg, NULL, 0);
               break;
          case 's':
               proc->seed = strtoul(optarg, NULL, 0);
               break;
          default:
               return 0;
          }
     }

     // If 64-bit random numbers are specified, it supercedes fast_rand.
     if (proc->u64_rand && proc->fast_rand) {
          tool_print("fast_rand superceded by 64-bit random number generation");
          proc->fast_rand = 0;
     }
     
     return 1;
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
//  also register as a source here..
// return 1 if ok
// return 0 if fail
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, ws_sourcev_t * sv,
              void * type_table) {
     
     //allocate proc instance of this processor
     proc_instance_t * proc =
          (proc_instance_t*)calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     // by virtue of calloc above, all values in proc is set to 0 (NULL)
     proc->seed = rand();

     proc->mask = (~(uint64_t)0);

     proc->label_tup = wsregister_label(type_table, "KEYGEN");
     proc->label_key = wsregister_label(type_table, "KEY");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc)) {
          return 0;
     }
 
     // init RNG (random number generator)
     if (!proc->fast_rand) {
          srand48(proc->seed);
     }

     tool_print("attempting to register source");
     proc->outtype_tuple =
          ws_register_source_byname(type_table,
                                    "TUPLE_TYPE", proc_source, sv);

     if (proc->outtype_tuple ==NULL) {
          error_print("Error attempting to register source");
          return 0;
     }

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * dtype,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     // this is a source, so we are expecting no inputs..
     // down the road may expect some sort of status input
     return NULL;
}

//// proc source function assigned to a specific data type
//return 1 if output is available
// return 2 if not output
static int proc_source(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     
     uint64_t key;

     if (proc->usleep_usec) {
          usleep(proc->usleep_usec);
     }

     //figure out which cluster to reference

     if (proc->max != 0 && (proc->meta_process_cnt >= proc->max)) return 0;
     proc->meta_process_cnt++;
     
     if (!proc->fast_rand) {
          if (!proc->u64_rand) {
               key = ((mrand48()+POW31) & proc->mask) ;
          }
          else {
               key = (((((uint64_t)(mrand48()+POW31))<<32) + (mrand48()+POW31)) & proc->mask) ;
          }
     }
     else {
          key = (rand_r(&proc->seed) & proc->mask) ;
     }
     if (proc->add) {
          key += proc->base;
          proc->base += proc->add;
     }
     
     wsdata_add_label(source_data, proc->label_tup);
     tuple_member_create_uint64(source_data, key, proc->label_key);

     // for now this blindly passed all data through
     ws_set_outdata(source_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc);

     return 1;
}

