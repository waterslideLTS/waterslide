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
#define PROC_NAME "file_in"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_mmap.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_uint64.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "source", "input", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "reads in files specified in stdin, creates metadata";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'F',"","input file",
     "Specify an input file",0,0},
     {'m',"","",
     "output related file metadata in tuple",0,0},
     {'L',"","specify output label",
     "Specify an output label default DATA",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

//function prototypes for local functions
static int data_source(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     char filename[WSDT_MMAP_MAX_FILENAME_LEN + 1];
     int singlefile;
     wslabel_t * label_buf;
     wslabel_t * label_tuple;
     wslabel_t * label_filename;
     wslabel_t * label_len;
     wslabel_t * label_lmod;
     wslabel_t * label_dtime;
     struct stat statbuf;
     char * olabel;
     int extra_meta;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "mF:L:")) != EOF) {
          switch (op) {
          case 'm':
               proc->extra_meta = 1;
               tool_print("adding extra metadata");
               break;
          case 'F':
               strncpy(proc->filename, optarg, WSDT_MMAP_MAX_FILENAME_LEN);
               proc->filename[WSDT_MMAP_MAX_FILENAME_LEN] = '\0';
               proc->singlefile = 1;
               break;
          case 'L':
               free(proc->olabel);
               proc->olabel = strdup(optarg);
               break;
          default:
               return 0;
          }
     }

     return 1;
}

static inline int create_mmap(proc_instance_t * proc, wsdt_mmap_t * mm, const char * filename) {
     int rStat = 0;
     int fd;

     dprint( "%s (%d): filename = %s", __FILE__, __LINE__, filename);

     // Open the file
     if ((fd = open(filename, O_RDONLY)) == -1) {
          tool_print("unable to open file %s", filename);
     }
     else {
          mm->srcfd = fd;

	     // Next, get the length of the file
	     if (fstat(fd, &proc->statbuf) < 0) {
		     tool_print("unable to stat file %s", filename);
		     rStat = 0;
               close(fd);
	     }
	     else {
		     char * memblock;
		     // map the file, specified by srcfd into memory.
		     mm->len = (int) proc->statbuf.st_size;
		     if ((memblock = (char *)mmap(0,
						 mm->len,
						 PROT_READ,
						 MAP_SHARED,
						 fd,
						 0)) == MAP_FAILED) {
		          tool_print("unable to mmap file %s", filename);
		          rStat = 0;
		     }
		     else {
		          mm->buf = memblock;
                    strncpy(mm->filename, filename, WSDT_MMAP_MAX_FILENAME_LEN);
                    mm->filename[WSDT_MMAP_MAX_FILENAME_LEN] = '\0';

		          // life is good
		          rStat = 1;

		     }// end else mmapped file successfully. 

          }//end did fstat successfully 
     }

     return rStat;
}

static int local_scour_stdin(proc_instance_t * proc) {
     int len;
     char * buf = proc->filename;  //use pre-allocated buffer
     dprint("scour stdin");

     //read from stdin.. list of files..
     while (fgets(buf, WSDT_MMAP_MAX_FILENAME_LEN, stdin)) {
          //strip return
          len = strlen(buf);
          if (buf[len - 1] == '\n') {
               buf[len - 1] = '\0';
               len--;
          }

          return 1;
     }
     tool_print("no more files");
     return 0;
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

     proc->olabel = strdup("DATA");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->filename[0] && !local_scour_stdin(proc)) {
               error_print("no input file to start with");
     }

     proc->label_buf = wsregister_label(type_table, proc->olabel);
     proc->label_tuple = wsregister_label(type_table, "FILE");
     proc->label_filename = wsregister_label(type_table, "FILENAME");
     proc->label_len = wsregister_label(type_table, "LENGTH");
     proc->label_lmod = wsregister_label(type_table, "LAST_MODIFICATION");
     proc->label_dtime = wsregister_label(type_table, "DATETIME");

     proc->outtype_tuple =
          ws_register_source_byname(type_table, "TUPLE_TYPE", data_source, sv);

     if (proc->outtype_tuple == NULL) {
          fprintf(stderr, "registration failed\n");
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
     return NULL;
}

static inline void add_extra_metadata(proc_instance_t * proc, wsdata_t * tdata) {
     if (!proc->extra_meta) {
          return;
     }
     int flen = strlen(proc->filename);

     if (flen > 0) {
          wsdt_string_t * str = tuple_create_string(tdata, proc->label_filename, flen);
          if (str) {
               memcpy(str->buf, proc->filename, flen);
          }
     }

     if (proc->statbuf.st_size) {
          wsdt_uint64_t * u64 = tuple_member_create(tdata, dtype_uint64,
                                                    proc->label_len);
          if (u64) {
               *u64 = (uint64_t)proc->statbuf.st_size;
          }
     }
     wsdt_ts_t * ts = tuple_member_create_mlabel(tdata, dtype_ts,
                                                 proc->label_dtime,
                                                 proc->label_lmod);
     if (ts) {
          ts->sec = proc->statbuf.st_mtime;
          ts->usec = 0;
     }

}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
//
static int data_source(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     wsdata_t * wsmm = wsdata_alloc(dtype_mmap);

     int do_output = 0;
     int hasfiles = 1;

     if (!wsmm) {
          return 0;
     }

     wsdt_mmap_t * mm = (wsdt_mmap_t*)wsmm->data;

     if (proc->filename[0] || (!proc->singlefile && local_scour_stdin(proc))) {
          if (create_mmap(proc, mm, proc->filename)) {
               wsdt_binary_t * bstr = tuple_member_create_wdep(source_data,
                                                               dtype_binary,
                                                               proc->label_buf,
                                                               wsmm);
               if (bstr) {
                    bstr->buf = mm->buf;
                    bstr->len = mm->len;

                    wsdata_add_label(source_data, proc->label_tuple);

                    add_extra_metadata(proc, source_data);

                    proc->outcnt++;
                    ws_set_outdata(source_data, proc->outtype_tuple, dout);
                    
                    do_output = 1;
               }
          }
          proc->filename[0] = '\0';
     }
     else {
          //nofiles
          hasfiles = 0;
     }

     if (!do_output && wsmm) {
          wsdata_delete(wsmm);
     }
     
     return hasfiles;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc->olabel);
     free(proc);

     return 1;
}

