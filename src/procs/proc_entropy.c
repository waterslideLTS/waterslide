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
#define PROC_NAME "entropy"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.5";
char *proc_tags[]	= {"annotation", NULL};
char *proc_menus[]     = { "Annotate", NULL };
char *proc_alias[]     = { "ent", "chisqr", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "compute entropy about a buffer or buffers";
char proc_description[] = "Compute 8-bit entropy about a buffer or set of buffers";
char proc_requires[] = "none";


proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","LABEL prefix",
     "apply a label prefix to computation labels",0,0},
     {'e',"","",
     "compute for each buffer",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of buffer to compute";
char *proc_input_types[]    = {"any", NULL};
char *proc_output_types[]    = {"any", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};

char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

#define LOCAL_MAX_TYPES 25

typedef struct _entropy_t {
     uint64_t total;
     uint64_t cnt_ascii;
     uint64_t cnt_alphanum;
     uint64_t count[256];
     double log2;          //precomputed value
     double log2precompute[16384]; //precomputed table
} entropy_t;

static entropy_t * entropy_init() {
     //precompute tables
     entropy_t * ent = (entropy_t*)calloc(1, sizeof(entropy_t));
     if (!ent) {
          return NULL;
     }
     ent->log2 = log(2);
     int i;
     for (i = 1; i < 16384; i++) {
          ent->log2precompute[i] = log(i)/(ent->log2);
     }

     return ent;
}

static inline double entropy_log2(entropy_t * ent, uint64_t val) {
     if (val < 16384) {
          return ent->log2precompute[val]; 
     }
     else {
          return (log((double)val)/ent->log2);
     }
} 

static void entropy_reset(entropy_t * ent) {
     ent->total = 0;
     ent->cnt_alphanum = 0;
     ent->cnt_ascii = 0;
     memset(ent->count, 0, sizeof(uint64_t)*256);
}

static void entropy_add_buffer(entropy_t * ent, uint8_t * buf, size_t len) {
     size_t i;
     for (i = 0; i < len; i++) {
          ent->count[buf[i]] += 1;
          ent->cnt_alphanum += (isalnum(buf[i]) != 0);
          ent->cnt_ascii += (isascii(buf[i]) != 0);
     }
     ent->total += len;
}

static double entropy_finalize(entropy_t * ent) {
     if (!ent->total) {
          return 0;
     }
     double output = 0;
     int i;
     for (i = 0; i < 256; i++) {
          if (ent->count[i]) {
               double freq = ((double)ent->count[i])/((double)ent->total);
               output -= 
                    freq * (entropy_log2(ent, ent->count[i]) - entropy_log2(ent, ent->total));
          }
     }
     return output;
}

static double entropy_chisqr(entropy_t * ent) {
     if (!ent->total) {
          return -1;
     }

     double flat = (double)ent->total/256.0;

     double output = 0;
     int i;
     for (i = 0; i < 256; i++) {
          double inner = ent->count[i] - flat;
          output += (inner * inner)/flat; 
     }
     return output;
}

static double entropy_alphanum(entropy_t * ent) {
     if (!ent->total) {
          return -1;
     }
     return (ent->cnt_alphanum * 100)/ent->total;
}

static double entropy_ascii(entropy_t * ent) {
     if (!ent->total) {
          return -1;
     }
     return (ent->cnt_ascii * 100)/ent->total;
}

static void entropy_destroy(entropy_t * ent) {
     if (ent) {
          free(ent);
     }
}

//function prototypes for local functions
static int proc_process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     wslabel_t * label_entropy;
     wslabel_t * label_chisqr;
     wslabel_t * label_alphanum;
     wslabel_t * label_ascii;
     char * label_prefix;
     entropy_t * entropy;
     int eachbuffer;
     int do_monogram;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "eL:")) != EOF) {
          switch (op) {
          case 'e':
               proc->eachbuffer = 1;
               break;
          case 'L':
               proc->label_prefix = strdup(optarg);
          break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
          tool_print("scanning element %s", argv[optind]);
          optind++;
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


     proc->entropy = entropy_init();

     if (!proc->entropy) {
          tool_print("unable to initialize entropy structure");
          return 0;
     }

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
    
     if (!proc->nest.cnt) {
          error_print("need to specify label to seach for");
          return 0;
     } 

     proc->label_entropy = 
          wsregister_label_wprefix(type_table, proc->label_prefix, "ENTROPY");
     proc->label_chisqr = 
          wsregister_label_wprefix(type_table, proc->label_prefix, "CHISQR");
     proc->label_alphanum = 
          wsregister_label_wprefix(type_table, proc->label_prefix, "ALPHANUM");
     proc->label_ascii = 
          wsregister_label_wprefix(type_table, proc->label_prefix, "ASCII");

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * meta_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          return proc_process_tuple;
     }
     return NULL;
}

//callback when searching nested tuple
static int proc_nest_match_callback(void * vproc, void * vignore,
                                    wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     char * buf;
     int blen;
     
     if (proc->eachbuffer) {
          entropy_reset(proc->entropy);
     }

     if (dtype_string_buffer(member, &buf, &blen)) {
          entropy_add_buffer(proc->entropy, (uint8_t *)buf, (uint64_t)blen);
     }
     else {
          return 0;
     }

     if (proc->eachbuffer) {
          tuple_member_create_double(tdata, entropy_finalize(proc->entropy),
                                     proc->label_entropy);
          tuple_member_create_double(tdata, entropy_chisqr(proc->entropy),
                                     proc->label_chisqr);
          tuple_member_create_double(tdata,
                                     entropy_ascii(proc->entropy),
                                     proc->label_ascii);
          tuple_member_create_double(tdata,
                                     entropy_alphanum(proc->entropy),
                                     proc->label_alphanum);
     }


     return 1;
}

static int proc_process_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (!proc->eachbuffer) {
          entropy_reset(proc->entropy);
     }

     //search for specified tuple member
     tuple_nested_search(input_data, &proc->nest,
                         proc_nest_match_callback,
                         proc, NULL);

     if (!proc->eachbuffer && proc->entropy->total) {
          tuple_member_create_double(input_data, entropy_finalize(proc->entropy),
                                     proc->label_entropy);
          tuple_member_create_double(input_data, entropy_chisqr(proc->entropy),
                                     proc->label_chisqr);
          tuple_member_create_double(input_data,
                                     entropy_ascii(proc->entropy),
                                     proc->label_ascii);
          tuple_member_create_double(input_data,
                                     entropy_alphanum(proc->entropy),
                                     proc->label_alphanum);
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     entropy_destroy(proc->entropy);

     //free dynamic allocations
     free(proc);

     return 1;
}

