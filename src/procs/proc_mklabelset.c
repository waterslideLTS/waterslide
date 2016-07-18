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
#define PROC_NAME "mklabelset"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "waterslide_io.h"
#include "procloader.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_labelset.h"

char proc_version[] = "1.5";
char *proc_alias[] = { NULL};
char proc_name[] = PROC_NAME;
char proc_purpose[] = "create a label set from labels of tuple members";
char *proc_tags[] = { "stream", NULL };

char proc_requires[] = "none";

char *proc_synopsis[] = { "mklabelset [-l] [-L <LABEL>] [-I <LABEL>] [-S] [-C] <LABEL>", NULL };

char proc_description[] = \
"The mklabelset kid creates a label set from the labels of a given tuple "
"member. For example, if a tuple member contains the labels SPACE, "
"VERB, and NOUN labels, using mklabelset on the SPACE " 
"label would yield a new label set containing the labels VERB and "
"NOUN.\n\n"
"If the -C flag is used, mklabelset generates a label set from the container "
"set labels of a tuple. " 
"The -l flag forces mklabelset to only add the last label to the new "
"label set. "
"The -I flag is used to prevent a specific label from being added to the "
"new label set. The -I flag can be set more than once to prevent multiple "
"labels from being added to the label set. "
"If the -S flag is set, the outputted label set is a single, colon-delimited "
"string (instead of the default label set type). This is useful for kids "
"which require string type inputs.";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'l',"","",
     "last label per member only",0,0},
     {'L',"","LABEL",
     "output tuple label",0,0},
     {'I',"","LABEL",
     "label to ignore",0,0},
     {'S',"","",
     "output as a string buffer",0,0},
     {'C',"","",
     "use container labels",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

proc_example_t proc_examples[] = {
     {"... | mklabelset WORD | ...", "create a new label set from the labels at the WORD member"},
     {"... | mklabelset -I VERB WORD | ...", "create a new label set from the labels at the WORD member, but don't add the VERB label to the new label set"},
     {NULL,""}
};

char proc_nonswitch_opts[] = "list of LABELS";
char *proc_input_types[] = {"tuple", NULL};
char *proc_output_types[] = {"tuple", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};

char *proc_tuple_member_labels[] = {"LABELSET", NULL};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};


#define MAX_LABELS 32

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int process_container(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     ws_tuplemember_t * tmember_labelset;
     wslabel_t * label_out;
     wslabel_set_t lset;
     wslabel_set_t ignore;
     int last_only;
     int out_string;
     int container_labels;
     //wslabel_t * labels[MAX_LABELS];
     //uint32_t numlabels;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "CSI:lL:")) != EOF) {
          switch (op) {
          case 'C':
               proc->container_labels = 1;
               break;
          case 'S':
               proc->out_string = 1;
               break;
          case 'I':
               wslabel_set_add_noindex(type_table, &proc->ignore, optarg);
               tool_print("ignoring label %s", optarg);
               break;
          case 'l':
               proc->last_only = 1;
               break;
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               tool_print("adding label %s to output tuple", optarg);
               break;
          default:
               return 0;
          }
     }

     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
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

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //if (!proc->label_out) {
     //     printf("setting label_out to MKLABELSET\n"); 
     //     proc->label_out = wsregister_label(type_table, "MKLABELSET");
     //}

     proc->tmember_labelset =
          register_tuple_member_type(type_table, "LABELSET_TYPE",
                                     "LABELSET");

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

          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, meta_type, proc->label_out);
          }
          if (proc->container_labels) {
               return process_container;
          }
          return process_tuple;
     }
     return NULL;
}

static inline int label_ignore(proc_instance_t * proc, wslabel_t * label) {
     int i;

     for (i = 0; i < proc->ignore.len; i++) {
          if (label == proc->ignore.labels[i]) {
               return 1;
          }
     }
     return 0;
}

static inline int add_to_labelset(proc_instance_t * proc,
                                  wsdt_labelset_t * mlset, wsdata_t * member,
                                   wslabel_t * ignore) {
     int i;
     int added = 0;
     if (!member->label_len) {
          return 0;
     }
     for (i = (member->label_len - 1); i >= 0; i--) {
          //printf("i: %d\n", i);
          //printf("i: %d\n",  (member->labels[i] != ignore));
          //printf("i: %d\n",  !label_ignore(proc, member->labels[i]));
          //printf(":: %s\n",  member->labels[i]->name);

          if ((member->labels[i] != ignore) &&
              !label_ignore(proc, member->labels[i])) {

               //printf("added++\n");
               wsdt_labelset_add(mlset, member->labels[i]);
               added++;
               if (proc->last_only) {
                    //printf("last only\n");
                    return 1;
               }
          }

          //printf("-----\n");
     }
     return added;
}

static inline void local_set_label_string(proc_instance_t * proc,
                                          wsdata_t * tdata,
                                          ws_doutput_t * dout) {
     wslabel_t * label_set[MAX_LABELS];
     int lcnt = 0;
     int slen = 0;
     wsdata_t ** mset;
     int mset_len;
     int i;
     int j;
     int k;
     dprint("mklabelstring here");
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(tdata, proc->lset.labels[i], &mset_len,
                               &mset)) {
               wslabel_t * ignore = proc->lset.labels[i];

               for (j = 0; j < mset_len; j++ ) {
                    wsdata_t * member = mset[j];

                    dprint("mklabelstring here found member");
                   // for (k = 0; k < member->label_len; k++) {
                    for (k = (member->label_len - 1); k >= 0; k--) {
                         if ((member->labels[k] != ignore) &&
                             !label_ignore(proc, member->labels[k])) {
                              if (lcnt < MAX_LABELS) {
                                   dprint("mklabelstring here found label");
                                   label_set[lcnt] = member->labels[k];
                                   lcnt++;
                                   slen += strlen(member->labels[k]->name) + 1;
                              }
                              if (proc->last_only) {
                                   // only want last label (that not ignoring)
                                   break;
                              }
                         }
                    }
               }
          }
     }
     if (slen) {
          wsdt_string_t * str = tuple_create_string(tdata, proc->label_out,
                                                    slen);
          dprint("mklabelstring here2");
          if (str) {
               dprint("mklabelstring here3");
               int offset = 0;
               for (i = 0; i < lcnt; i++) {
                    if (i) {
                         str->buf[offset] = ':';
                         offset++;
                    }
                    int llen = strlen(label_set[i]->name);
                    memcpy(str->buf + offset, label_set[i]->name, llen);
                    offset += llen;
               }
               str->len = offset;
          }
     }
     ws_set_outdata(tdata, proc->outtype_tuple, dout);
     proc->outcnt++;
     return;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int member_cnt = 0;
     wsdata_t ** mset;
     int mset_len;

     if (proc->out_string) { 
          local_set_label_string(proc, input_data, dout);
          return 1;
     }

     // allocate a labelset, add the label_out label to the set 
     wsdt_labelset_t * mlset =
          tuple_member_alloc_label(input_data,
                                   proc->tmember_labelset,
                                   proc->label_out);
     if (!mlset) {
          printf("mlset is null");
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
          return 0;
     }

     int i;
     int j;
     // iterate through all the labels given on the command line
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(input_data, proc->lset.labels[i], &mset_len,
                               &mset)) {
               //printf("mset_len: %d\n", mset_len);
               for (j = 0; j < mset_len; j++ ) {
                    member_cnt += add_to_labelset(proc, mlset,
                                                  mset[j], proc->lset.labels[i]);
               }
          }
     }

     //printf("member_cnt: %d\n", member_cnt);
     //printf("=====\n");
    
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

static inline void local_set_container_string(proc_instance_t * proc,
                                              wsdata_t * tdata) {
     int slen = 0;
     int i;

     if (proc->last_only) {
          slen = strlen(tdata->labels[tdata->label_len -1]->name);
          tuple_dupe_string(tdata, proc->label_out, 
                            tdata->labels[tdata->label_len -1]->name,
                            slen);
          return;
     }

     for (i = 0; i < tdata->label_len; i++) {
          slen += strlen(tdata->labels[i]->name) + 1;
     }


     if (slen) {
          wsdt_string_t * str = tuple_create_string(tdata, proc->label_out,
                                                    slen);
          dprint("mklabelstring here2");
          if (str) {
               int offset = 0;
               for (i = 0; i < tdata->label_len; i++) {
                    if (i) {
                         str->buf[offset] = ':';
                         offset++;
                    }
                    int llen = strlen(tdata->labels[i]->name);
                    memcpy(str->buf + offset, tdata->labels[i]->name, llen);
                    offset += llen;
               }
               str->len = offset;
          }
     }
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_container(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (!input_data->label_len) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
          return 1;
     }

     if (proc->out_string) { 
          local_set_container_string(proc, input_data);
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
          return 1;
     }

     wsdt_labelset_t * mlset =
          tuple_member_alloc_label(input_data,
                                   proc->tmember_labelset,
                                   proc->label_out);
     if (!mlset) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
          return 0;
     }


     
     if (proc->last_only) {
          wsdt_labelset_add(mlset, input_data->labels[input_data->label_len -1]);
     }
     else {
          int i;
          for (i = 0; i < input_data->label_len; i++) {
               wsdt_labelset_add(mlset, input_data->labels[i]);
          }
     }
    
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

     //free dynamic allocations
     free(proc);

     return 1;
}

