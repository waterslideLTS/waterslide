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
/*
   writes a labeled buffer to a file..
*/

#define PROC_NAME "writebuffer"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "output", "summarizer", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "writes string buffer to file";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'w',"","file_prefix",
      "prefix for every file",0,0},
     {'l',"","",
      "append buffer labels to filename",0,0},
     {'L',"","LABEL",
      "label of member values for appending to filename",0,0},
     {'o',"","",
      "write as a stdout stream",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "list of buffers to write";
char *proc_input_types[]    = {"tuple", NULL};

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_tuple_stdout(void *, wsdata_t*, ws_doutput_t*, int);

#define LOCAL_FILE_MAX 2000

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     char filename[LOCAL_FILE_MAX];
     int fnlen; //length of file
     uint64_t files_written;
     wslabel_set_t lset_buffer;
     wslabel_set_t lset_name;
     char * prefix;
     int prefix_len;
     char * suffix;
     int suffix_len;
     int generation;
     int add_labels;
     int write_stdout;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "olL:w:")) != EOF) {
          switch (op) {
          case 'l':
               proc->add_labels = 1;
               break;
          case 'L':
               wslabel_set_add(type_table, &proc->lset_name, optarg);
               break;
          case 'w':
               proc->prefix = strdup(optarg);
               proc->prefix_len = strlen(optarg);
               break;
          case 'o':
               proc->write_stdout = 1;
               break;

          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset_buffer, argv[optind]);
          optind++;
     }

     return 1;
}

static inline void reset_filename(proc_instance_t * proc) {
     proc->fnlen = 0;
     proc->generation = 0;

     if (proc->prefix) {
          memcpy(proc->filename, proc->prefix, proc->prefix_len);
          proc->fnlen += proc->prefix_len;
     }
}

static inline void add_to_filename(proc_instance_t * proc, wsdata_t * wsd) {
     proc->filename[proc->fnlen] = '_';
     proc->fnlen++;

     int fnmax = LOCAL_FILE_MAX - proc->fnlen;
     uint64_t u64;

     if (wsd->dtype == dtype_string) {
          wsdt_string_t * str = (wsdt_string_t *) wsd->data;
          if ((proc->fnlen + str->len) < LOCAL_FILE_MAX) {
               memcpy(proc->filename + proc->fnlen, str->buf, str->len);
               proc->fnlen += str->len;
          }
     }
     else if (wsd->dtype == dtype_ts) {
          wsdt_ts_t * ts = (wsdt_ts_t *) wsd->data;
          int tslen;
          tslen = sysutil_snprintts_sec2(proc->filename + proc->fnlen, fnmax, ts->sec);
          proc->fnlen += tslen;
     }
     else if (dtype_get_uint(wsd, &u64)) {
          int ulen = snprintf(proc->filename + proc->fnlen, fnmax, "%"PRIu64, u64);
          proc->fnlen += ulen;
     } 
}

static inline void add_labels_to_filename(proc_instance_t * proc, wsdata_t * wsd) {
     int i;
     for (i = 0; i < wsd->label_len; i++) {
          int fnmax = LOCAL_FILE_MAX - proc->fnlen;

          int len = snprintf(proc->filename + proc->fnlen, fnmax,
                             "_%s", wsd->labels[i]->name);
          if (len > 0) {
               proc->fnlen += len;
          }
     }
}

static inline FILE * open_buffer_file(proc_instance_t * proc) {
     int fnmax = LOCAL_FILE_MAX - proc->fnlen;


     struct stat statbuffer;

     dprint("open buffer file");
     //try generation
     while (proc->generation < 100000) {
          snprintf(proc->filename + proc->fnlen, fnmax,
                   "_%05d%.*s",
                   proc->generation, proc->suffix_len,
                   proc->suffix);
          //test filename
          if (!stat(proc->filename, &statbuffer)) {
               //file already exists
               dprint("open buffer %d", proc->generation);
               proc->generation++;

               dprint("open buffer %s", proc->filename);
          }
          else {
               FILE * fp = fopen(proc->filename, "w");
               tool_print("opening %s for writing", proc->filename);
               return fp;
          }
     }
     return NULL;
}

static inline int writebuffer(proc_instance_t * proc, wsdata_t * member) {
     dprint("writebuffer");
     char * buf = NULL;
     int blen = 0;
     if (!dtype_string_buffer(member, &buf, &blen)) {
          return 0;
     }

     dprint("writebuffer_ has buffer");
     if (!blen) {
          return 0;
     }

     if (proc->add_labels) {
          add_labels_to_filename(proc, member);
     }

     //we have something to write
     FILE * fp = open_buffer_file(proc);
     if (!fp) {
          return 0;
     }

     dprint("writebuffer_ actual buffer");
     fwrite(buf, blen, 1, fp);
     proc->files_written++;
     fclose(fp);

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
          (proc_instance_t*)calloc(1, sizeof(proc_instance_t));
     *vinstance = proc;

     proc->suffix = ".buffer";
     proc->suffix_len = strlen(proc->suffix);

     //number of files written
     proc->files_written = 0;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (proc->write_stdout) {
          if (isatty(fileno(stdout)) != 0) {
               tool_print("ERROR - will not write binary to your terminal, please redirect");
               return 0;
          }
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
     if (meta_type == dtype_tuple) {
          if (proc->write_stdout) {
               return proc_tuple_stdout;
          }
          return proc_tuple;
     }

     return NULL;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     dprint("reset file");
     reset_filename(proc);

     wsdata_t ** mset;
     int mset_len;
     int i;
     int j;

     dprint("get filename parameters");
     //get filenames
     for (i = 0; i < proc->lset_name.len; i++) {
          if (tuple_find_label(input_data,
                               proc->lset_name.labels[i], &mset_len,
                               &mset)) {
               for (j = 0; j < mset_len; j++) {
                    add_to_filename(proc, mset[j]);
               }
          }
     }

     dprint("finding buffers");
     //write buffers
     for (i = 0; i < proc->lset_buffer.len; i++) {
          if (tuple_find_label(input_data,
                               proc->lset_buffer.labels[i], &mset_len,
                               &mset)) {
               for (j = 0; j < mset_len; j++) {
                    writebuffer(proc, mset[j]); 
               }
          }
     }

     return 0;
}

static int proc_tuple_stdout(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     dprint("finding buffers");
     //write buffers
     int i;
     int j;
     wsdata_t ** mset;
     int mset_len;
     for (i = 0; i < proc->lset_buffer.len; i++) {
          if (tuple_find_label(input_data,
                               proc->lset_buffer.labels[i], &mset_len,
                               &mset)) {
               for (j = 0; j < mset_len; j++) {
                    char * buf;
                    int blen;
                    if (dtype_string_buffer(mset[j], &buf, &blen)) {
                         fwrite(buf, blen, 1, stdout);
                         fflush(stdout);
                         proc->files_written++;
                    }
               }
          }
     }

     return 0;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("files written %" PRIu64, proc->files_written);

     //free dynamic allocations
     free(proc->prefix);
     free(proc);

     return 1;
}

