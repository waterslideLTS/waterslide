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
#define PROC_NAME "combinestrings"

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <netinet/in.h>
#include <time.h>
#include <assert.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_bigstring.h"
#include "datatypes/wsdt_mediumstring.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_ftype.h"
#include "procloader.h"
#include "wstypes.h"

char proc_version[]     = "1.2";
char *proc_menus[]     = { NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "combine strings from members of the same tuple";
char proc_requires[]     = "";
char *proc_tags[]      = { "stream", NULL };
char *proc_synopsis[]    = {"combinestrings <LABEL> [-c <value>] [-L <value>]", NULL};
char proc_description[] = "Combine strings of multiple label members "
                          "of the same tuple into a new label member";

proc_example_t proc_examples[]    = {
          {"... | combinestrings DATA1 DATA2 | ...","combine the DATA1 and DATA2 label members into a new label member."},
          {"... | combinestrings -c ':' DATA1 DATA2 DATA3 | ...","combine the DATA1, DATA2, and DATA3 label members, but separate them with a colon in the new label member"},
               {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'c',"","string",
     "insert string between combined label members",0,0},
     {'L',"","",
     "label of combined strings",0,0},
     {'S',"","",
     "force output to be a string",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of tuple string members to combine";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};

proc_port_t proc_input_ports[]     = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};
char *proc_tuple_member_labels[] = {"COMBINE", NULL};

//function prototypes for local functions
static int proc_process_label(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t cmb_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_combine;
     wslabel_set_t lset;
     char * delim;
     int delimlen;
     int forcestring;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc,
                             void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "Sc:L:")) != EOF) {
          switch (op) {
          case 'S':
               proc->forcestring = 1;
               break;
          case 'c':
               proc->delim = strdup(optarg);
               proc->delimlen = strlen(optarg);
               break;
          case 'L':
               proc->label_combine = wsregister_label(type_table, optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          tool_print("searching for string with label %s",
                     argv[optind]);
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

     proc->label_combine = wsregister_label(type_table, "COMBINE");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->lset.len) {
          tool_print("must specify labels of strings to combine");
          return 0;
     }
     return 1; 
}


// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;  // not matching expected type
     }

     proc->outtype_tuple =
          ws_add_outtype(olist, input_type, NULL);

     if (proc->lset.len) {
          return proc_process_label; // a function pointer
     }
     return NULL;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 2 if not output

// Commented out code represents in-place copy, but uses more memory
// The current version deflates to a persistent buffer and copies out (more computation)

static int proc_process_label(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     int id;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     tuple_init_labelset_iter(&iter, input_data,
                              &proc->lset);
     int olen = 0;
     char * buf;
     int blen;
     int isbinary = 0;

     while (tuple_search_labelset(&iter, &member, &label, &id)) {
          if (dtype_string_buffer(member, &buf, &blen)) {
               if (blen > 0) {
                    if (member->dtype != dtype_string) {
                         isbinary = 1;
                    }
                    if (proc->delim && olen) {
                         olen += proc->delimlen;
                    }
                    olen += blen;
               }
          }
     }

     //override binary detection
     isbinary = (!proc->forcestring) && isbinary;

     //allocate new string
     if (olen) {
          proc->cmb_cnt++;
          char * obuf;
          int obuf_len = 0;
          if (isbinary) {
               wsdt_binary_t * bin = tuple_create_binary(input_data, proc->label_combine, olen);
               if (!bin) {
                    ws_set_outdata(input_data, proc->outtype_tuple, dout);
                    proc->outcnt++;
                    return 1;
               }
               obuf = bin->buf;
          }
          else {
               wsdt_string_t * str = tuple_create_string(input_data, proc->label_combine, olen);
               if (!str) {
                    ws_set_outdata(input_data, proc->outtype_tuple, dout);
                    proc->outcnt++;
                    return 1;
               }
               obuf = str->buf;
          }
          tuple_init_labelset_iter(&iter, input_data, &proc->lset);
          while (tuple_search_labelset(&iter, &member, &label, &id)) {
               if (obuf_len && proc->delim) {
                    memcpy(obuf + obuf_len, proc->delim, proc->delimlen);
                    obuf_len += proc->delimlen;
               }
               if (dtype_string_buffer(member, &buf, &blen)) {
                    if (blen >0) {
                         memcpy(obuf + obuf_len, buf, blen);
                         obuf_len += blen;
                    }
               }
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
     tool_print("tuple cnt %" PRIu64, proc->cmb_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc->delim);
     free(proc);

     return 1;
}

