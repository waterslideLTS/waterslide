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

/* this generates container labels based on hash
   */
#define PROC_NAME "loadbalance"
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "evahash.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "procloader.h"

char proc_version[]     = "1.5";
char *proc_tags[]     = { "Stream manipulation", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "labels data for load balancing";
char *proc_synopsis[] = {"loadbalance [<LABEL>] [-N <count>] [-L <prefix>] [-o]", NULL};
char proc_description[] = "From a single stream, generate labels indicating how to split the stream in N ways to maintain load balance if the streams a parallelized.  The '-N' option is used to specify the number of load-balanced output channels to produce (default is 4).  The '-L' option allows some customization of the output labels (default is 'LB').  The '-o' options computes an ordered hash on the labels specified (useful for coalescing multidirectional traffic).";

proc_example_t proc_examples[] = {
        {"... | loadbalance KEY1 KEY2 -o | ...", "Keeps the ordered hash of the KEY1 and KEY2 in the same load-balanced output channel in an effort to keep flows in the same channel."},
        {"... | loadbalance -N 5 -L CHANNEL | ...", "Sort events into 5 load-balanced channels giving each event a label of CHANNEL[0-4] to help downstream sorting of the events."},
        {NULL, NULL}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'N',"","count",
     "number of output channels",0,0},
     {'L',"","prefix",
     "Label prefix",0,0},
     {'o',"","",
     "do ordered hashing",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_requires[] = "none";
char proc_nonswitch_opts[]    = "LABEL of tuple key";
char *proc_input_types[]    = {"tuple", "any", NULL};
char *proc_output_types[]    = {"any", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {"LB*", NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int process_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _hash_members_t {
     wsdata_t * member[WSDT_TUPLE_MAX];
     int id[WSDT_TUPLE_MAX];
     uint64_t hash;
} hash_members_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     wslabel_set_t lset;
     wslabel_t ** lb_labels;
     int ordered_hash;
     hash_members_t hmembers;
     char * prefix;
     int channels;
     uint32_t seed;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "oN:L:")) != EOF) {
          switch (op) {
          case 'N':
               proc->channels = atoi(optarg);
               break;
          case 'L':
               free(proc->prefix);
               proc->prefix = strdup(optarg);
               break;
          case 'o':
               proc->ordered_hash = 1;
               tool_print("ordered hashing");
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          tool_print("keying on %s", argv[optind]);
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

     proc->prefix = strdup("LB");
     proc->channels = 4;
     proc->seed = rand();


     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //register label buffer
     proc->lb_labels = (wslabel_t**)calloc(proc->channels, sizeof(wslabel_t *));
     if (!proc->lb_labels) {
          error_print("failed calloc of proc->lb_labels");
          return 0;
     }
     int i;
     char lbuf[256];
     for (i = 0; i < proc->channels; i++) {
          snprintf(lbuf, 256, "%s%d", proc->prefix, i);
          proc->lb_labels[i] = wsregister_label(type_table, lbuf);
     }

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

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          tool_print("does not process flusher type");
          return NULL;
     }
     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);


     if (proc->lset.len && wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return process_labeled_tuple; 
     }

     // we are happy.. now set the processor function
     return proc_process_meta; // a function pointer
}

static inline int lb_label_data(proc_instance_t * proc,
                                void * offset, int len) {

     uint32_t hash = evahash((uint8_t*)offset, len, proc->seed);
     return hash % proc->channels;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     uint32_t id = proc->meta_process_cnt % proc->channels;

     wsdata_add_label(input_data, proc->lb_labels[id]);

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}

static inline int get_hashdata(proc_instance_t * proc,
                               wsdata_t * m_tuple,
                               uint8_t ** hstr, uint32_t * hlen) {
     int i,j;
     wsdata_t ** members;
     int mlen;
     int len = 0;
     ws_hashloc_t * hashloc;

     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(m_tuple, proc->lset.labels[i], &mlen, &members)) {
               for (j = 0; j < mlen; j++) {
                    if (members[j]->dtype->hash_func) {
                         proc->hmembers.member[len] = members[j];
                         if (proc->ordered_hash) {
                              proc->hmembers.id[len] = i & proc->seed;
                         }
                         else {
                              proc->hmembers.id[len] = proc->seed;
                         }
                         len++;
                    }
               }
          }
     }

     wsdata_t * member;

     if (len < 1) {
          return 0;
     }
     else if (len == 1) {
          member = proc->hmembers.member[0];
          hashloc = member->dtype->hash_func(member);
          if (hashloc->offset) {
               *hstr = hashloc->offset;
               *hlen = hashloc->len;
          }
          return 1;
     }
     //else
     uint64_t hash = 0;
     for (i = 0; i < len; i++) {
          //if multiple copies of a member exist, choose one 
          member = proc->hmembers.member[i];
          hashloc =
               member->dtype->hash_func(member);
          if (hashloc->offset) {
               hash += evahash64(hashloc->offset,
                                 hashloc->len,
                                 proc->hmembers.id[i]);
          }
     }
     proc->hmembers.hash = hash;
     *hstr = (uint8_t*)&proc->hmembers.hash;
     *hlen = sizeof(uint64_t);
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_labeled_tuple(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     uint8_t * hstr = NULL;
     uint32_t hlen = 0;

     int id;
     if (!get_hashdata(proc, input_data, &hstr, &hlen)) {
          id = (int)proc->meta_process_cnt % proc->channels;
     }
     else {
          id = lb_label_data(proc, hstr, hlen); 
     }

     wsdata_add_label(input_data, proc->lb_labels[id]);

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
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
     free(proc->prefix);
     free(proc->lb_labels);
     free(proc);

     return 1;
}

