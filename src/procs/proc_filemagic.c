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
// uses the libmagic library that is used in the unix "file" command to annotate
// the content of a buffer in a tuple
#define PROC_NAME "filemagic"

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
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_ftype.h"
#include "procloader.h"
#include "wstypes.h"
#include <magic.h>

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Decoder", "Annotation", "Detection", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "annotate buffers with filetypes";
char proc_description[] = "Detects file and MIME type in buffers and adds a new member containing the type.  Default detection is for file type, MIME type detection is accessed with the '-m' option.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'f',"","path to magic file",
     "set location of the magic file default (default is /usr/share/file/magic",0,0},
     {'m',"","",
     "output mime type instead of description",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL of tuple string member to decode";
char *proc_input_types[]       =  {"tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {"FILEMAGIC", NULL};
char *proc_synopsis[]          =  {"filemagic [-m] <LABEL>", NULL};
proc_example_t proc_examples[] =  {
	{"... | base64 BUFFER -L CONTENT | filemagic CONTENT | ...", "Adds a FILEMAGIC label to the tuple containing the filemagic-detected filetype."},
	{"... | base64 BUFFER -L CONTENT | filemagic -m CONTENT | ...", "Adds a FILEMAGIC label to the tuple containing the filemagic-detected MIME-type."},
	{NULL, NULL}
};

//function prototypes for local functions
static int proc_process_label(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_unlabeled(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t bs_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     ws_tuplemember_t * tmember_magic;
     wslabel_t * label_magic;
     wslabel_set_t lset;
     magic_t magic_cookie;
     char* magic_file;
     int do_mime;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc,
                             void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "mf:")) != EOF) {
          switch (op) {
          case 'f':
               proc->magic_file = strdup(optarg);
               break;
          case 'm':
               proc->do_mime = 1;
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

#ifdef MAGIC_MIME_TYPE
#define PROC_MIME_TYPE MAGIC_MIME_TYPE
#else
#define PROC_MIME_TYPE MAGIC_MIME
#endif

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

     proc->label_magic = wsregister_label(type_table, "FILEMAGIC");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if(!proc->magic_file)
          proc->magic_file = "/usr/share/file/magic";

     if (proc->do_mime) {
          proc->magic_cookie = magic_open(PROC_MIME_TYPE);
          if (magic_load(proc->magic_cookie, proc->magic_file)) {
               error_print("unable to load magic file");
               return 0;
          }
     }
     else {
          proc->magic_cookie = magic_open(MAGIC_NONE);
          if (magic_load(proc->magic_cookie, proc->magic_file)) {
               error_print("unable to load magic file");
               return 0;
          }
     }


     proc->tmember_magic = register_tuple_member_type(type_table,
                                                      "FIXEDSTRING_TYPE",
                                                      "FILEMAGIC");
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
     else {
          return proc_process_unlabeled; // a function pointer
     }
}

static inline void decode_str(proc_instance_t * proc, wsdata_t * tdata,
                                   uint8_t * buf, int buflen) {
     if (!buf || (buflen <= 0)) {
          return;
     }
     proc->bs_cnt++;
     //allocate output buffer..
     const char * resp = 
          magic_buffer(proc->magic_cookie, (const void *)buf, buflen);

     if (!resp) {
          error_print("bad buffer");
          return;
     }
     int resp_len = strlen(resp);

     wsdata_t * wsd_fstr = tuple_member_alloc_wsdata(tdata, proc->tmember_magic);
     if (wsd_fstr) {
          wsdt_fstr_t * fstr = wsd_fstr->data;
          fixedstring_copy(fstr, resp, resp_len);
     }
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

     while (tuple_search_labelset(&iter, &member, &label, &id)) {
          char * buf;
          int len;
          if (dtype_string_buffer(member, &buf, &len)) {
               decode_str(proc, input_data, (uint8_t*)buf, len);
          }
     }
     
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

static int proc_process_unlabeled(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     wsdt_tuple_t * tuple = input_data->data;
     int i;
     wsdata_t * member;
     int tlen = tuple->len;

     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          if (member->label_len && member->labels[0] != proc->label_magic) {
               char * buf;
               int len;
               if (dtype_string_buffer(member, &buf, &len)) {
                    decode_str(proc, input_data, (uint8_t*)buf, len);
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
     tool_print("file characterize      %" PRIu64, proc->bs_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     magic_close(proc->magic_cookie);

     //free dynamic allocations
     free(proc);

     return 1;
}

