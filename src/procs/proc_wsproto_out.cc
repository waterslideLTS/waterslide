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
#define PROC_NAME "wsproto_out"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <zlib.h>
#include <netinet/in.h>
#include <time.h>
#include "waterslide.h"  
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "procloader.h"
#include "sysutil.h"
#include "zutil.h"
#include "listhash.h"
#include "protobuf/wsproto_lib.h"
#include "protobuf/wsproto_lib_gz.h"
#ifdef __cplusplus
CPP_OPEN
#endif
#include "fileout.h"

char proc_version[]     = "1.5.1";
const char *proc_tags[]     = { "wsproto","output", NULL };
const char *proc_alias[]     = { "pb2out", "pb2_fileout", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "write out metadata in Protocol Buffer format";
const char *proc_synopsis[]   = {"wsproto_out [-O <filename> | -s]",
                         "wsproto_out -B",  
                         "wsproto_out [-t <period>] [-v <moveprefix>] "
                         "[-w <prefix>] [-E <ENVIRONMENT>] "
                         "[-b <bytes] [-m <count] [-z] [-h]", NULL }; 
char proc_description[] ="Writes metadata to files or stdout in the wsproto"
                         " Protocol Buffer (protobuf) format. This format is specified in"
                         " 'procs/protobuf/wsproto.proto'. This format differs"
                         " from the wsserial.proto in that it includes "
                         "labels with each "
                         "tuple in string format instead of at the beginning "
                         "of each file, data elements are stored in the "
                         "closest matching Protocol Buffer datatype instead "
                         "of in raw bytes, and an enum specifies the type of "
                         "data being stored. \n"
                         "The supported datatypes of wsproto are: dtype_tuple, "
                         "dtype_string, dtype_binary, dtype_fixedstring, "
                         "dtype_mediumstring, dtype_double, dtype_uint, "
                         "dtype_uint64, dtype_uint16, dtype_uint8, "
                         "dtype_label, dtype_ts, and dtype_int.\n"
                         "The file metadata (-m option) attaches the a tuple's "
                         "source file name in the tuple under the label "
                         "PBFILENAME.  This label is not appended when reading "
                         "content from stdin.\n"
                         "The binary dtype (-B option) is used to pass protobuf "
                         "through the wsproto_out kid to the next kid in the "
                         "pipeline. This pass-through mechanism is useful for "
                         "tasks such as generating network traffic containing "
                         "the protobuf.";
proc_example_t proc_examples[]    = {
     {"... | wsproto_out","writes output to YYYYMMDD.HH.wsproto"},
     {"... | wsproto_out -z","writes output to YYYYMMDD.HH.wsproto.gz"},
     {"... | wsproto_out -O out.wsproto","writes output to out.wsproto"},
     {"... | wsproto_out -O out.test.wsproto","writes output to out.test (truncates extension) - this is a bug)"},
     {"... | wsproto_out -O out.wsproto -z","writes nothing (-O does not work with compression)"},
     {"... | wsproto_out -t 1h -z","writes output to YYYYMMDD.HH.wsproto.gz and starts a new file each hour"},
     {"... | wsproto_out -t 15m -z","writes output to YYYYMMDD.HHMM.wsproto.gz and starts a new file every 15 minutes"},
     {"... | wsproto_out -v 'test.' -t 15m -z","writes output to test.YYYYMMDD.HHMM.wsproto.gz and starts a new file every 15 minutes"},
     {"... | wsproto_out -w 'test.' -v 'data/finished.' -t 15m -z","writes output to test.YYYYMMDD.HHMM.wsproto.gz, moves the file to data/finished.YYYYMMDD.HHMM.wsproto.gz when it is done, and starts a new file every 15 minutes"},
     {"... | wsproto_out -w 'test.' -v 'data/finished.' -E 'MY_ENV' -t 15m -z","if MY_ENV is 'SAMPLE', writes output to test.SAMPLE.YYYYMMDD.HHMM.wsproto.gz, moves the file to data/finished.SAMPLE.YYYYMMDD.HHMM.wsproto.gz when it is done, and starts a new file every 15 minutes"},
     {NULL,""}
};
char proc_requires[]    = "";
const char *proc_input_types[]        = {"tuple", NULL};
const char *proc_output_types[]   = {NULL};
proc_port_t proc_input_ports[]  = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'t',"","period",
     "period to create new file (default unit is sec.; use m for minutes, h for hours)",0,0},
     {'v',"","moveprefix",
     "destination to move file to after write completes ",0,0},
     {'O',"","filename",
     "write a single file out",0,0},
     {'B',"","",
     "send out binary dtypes",0,0},
     {'w',"","prefix",
     "output file prefix to write metadata ",0,0},
     {'E',"","ENVIRONMENT",
     "environment variable to append to filename",0,0},
     {'b',"","bytes",
      "max bytes per file (accepts M and K bytes)",0,0},
     {'m',"","count",
      "max records per file",0,0},
     {'z',"","",
      "gzip the output into a .wsproto.gz file",0,0},
     {'s',"","",
      "write output to stdout",0,0},
     {'D',"","",
       "prevent overwrites when creating and moving files",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[] = "";

#define MAX_SPLITFILENAME 256

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     filespec_t *fs;
     char * writefile;
     char * fname;
     char * env;
     wsproto::wsdata * wsproto; 
     uint16_t protocolversion;
     uint16_t protocolid;
     uint8_t sendbinary;
     ws_outtype_t * outtype_bstr; 
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, proc_instance_t * proc) {
     int op;

     while ((op = getopt(argc, argv, "sb:O:BE:m:t:v:w:zM:D")) != EOF) {
          switch (op) { 
          case 'B':
               proc->sendbinary = 1;  
               break;
          case 'b':
	  case 'M':
               proc->fs->bytemax = sysutil_get_strbytes(optarg);
               break;
          case 'm':
               proc->fs->recordmax = strtoul(optarg, NULL, 0);
               tool_print("using %" PRIu64 " as max record per file", proc->fs->recordmax);
               break;
          case 't':
	       proc->fs->timeslice = sysutil_get_duration_ts(optarg);
               if (proc->fs->timeslice) {
                    dprint("timeslice = %d", proc->fs->timeslice);
               }
               else {
                    error_print("time specified (%d) is invalid", (int)proc->fs->timeslice);
                    return 0;
               }
               break;
          case 'v':
	       proc->fs->moveprefix = (char*)fileout_parse_filespec(optarg, NULL, 1);
               tool_print("moving file to prefix %s after closing", 
			  proc->fs->moveprefix);
               break;
          case 'w':
               proc->fs->fileprefix = (char*)fileout_parse_filespec(optarg, NULL, 1);
               tool_print("writing metadata to prefix %s", 
			  proc->fs->fileprefix);
               break;
          case 'E':
               proc->env = getenv(optarg);
               break;
          case 's':
               proc->fs->outfp = stdout;
               break;
          case 'z':
	       proc->fs->extension = (char*)".wsproto.gz";
               proc->fs->use_gzip = 1;
               break;
	  case 'O':
	       proc->fname = strdup(optarg);
	       break;
	  case 'D':
	       proc->fs->safename = 1;
	       break;
          default:
               return 0;
          }
     }

     if((proc->fs->outfp == stdout) && proc->fs->use_gzip) {
          tool_print("cannot write to stdout with gzip");
          return 0;
     }

     if (proc->fs->outfp != stdout) {
	  // make the filename
	  if (proc->fname) {
	       fileout_parse_filespec(proc->fname, proc->fs, 0);
	       proc->fs->mode = 'w';
	  } else { 
	       // build a filespec from the other options
	       char filestring[400] = "";
//	       if (proc->writefile) {
//		    strcpy(filestring, proc->writefile);
//	       } 
	       if (proc->env) {
		    strcat(filestring, proc->env);
		    strcat(filestring, ".");
	       }
	       strcat(filestring, "<%Y%m%d.%H%M>");
	       proc->fs->mode = 'w';
	       fileout_parse_filespec(filestring, proc->fs, 0);
	  }
     }
     return 1;
}

// the following is a function to take in command arguments  and initalize
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

     proc->fs = (filespec_t*)calloc(1,sizeof(filespec_t));
     proc->fs->extension = (char*)".wsproto";

     //read in command options
     if (!proc_cmd_options(argc, argv, proc)) {
          return 0;
     }

     proc->wsproto = wsproto_init();

     // initialize the fields for the header
     proc->protocolid = WSPROTO_FORMAT_ID;
     proc->protocolversion = WSPROTO_FORMAT_VERSION;

     if(!fileout_initialize(proc->fs, type_table)) {
          // problem in initializing fileout
          return 0;
     }

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
// return a function pointer for processing tuples if it is a tuple type 
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * meta_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     if (meta_type != dtype_tuple) {
          return NULL;
     }

     proc_instance_t * proc = (proc_instance_t *)vinstance;
     if (proc->sendbinary && !proc->outtype_bstr) {
          proc->outtype_bstr = ws_add_outtype_byname(type_table, olist, "BINARY_TYPE", NULL);
     }

     return process_tuple; // a function pointer
}


static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     time_t ts=0;
     fpdata_t *fpd;
     proc->meta_process_cnt++;

     if(proc->sendbinary) {
          //get allocated data buffer
          int send_labels = 1; // can be made an option later
          wsdata_t * wsd_bin = wsproto_fill_buffer_alloc(proc->wsproto, input_data, send_labels);
          if (wsd_bin) {
               ws_set_outdata(wsd_bin, proc->outtype_bstr, dout);
               proc->outcnt++;
          }
          return 1;
     } 
     
     fpd = fileout_select_file(input_data, proc->fs, ts);
     if (fpd && fpd->bytecount == 0) {
	  // new file, add header
	  if (proc->fs->use_gzip) {
	       fpd->bytecount += wsproto_header_writefp_gz(proc->protocolid, proc->protocolversion, (gzFile)fpd->fp);
	  } else {
	       fpd->bytecount += wsproto_header_writefp(proc->protocolid, proc->protocolversion, (FILE*)fpd->fp);
	  }
     }
     if( proc->fs->use_gzip ) {
	  fpd->bytecount += wsproto_tuple_writefp_gz(proc->wsproto, input_data, (gzFile) fpd->fp);
     } else {
	  fpd->bytecount += wsproto_tuple_writefp(proc->wsproto, input_data, (FILE*) fpd->fp);
     }
     return 0;
}


//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("input cnt  %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     wsproto_destroy(proc->wsproto);

     fileout_filespec_cleanup(proc->fs);

     //free dynamic allocations
     free(proc->fs->fileprefix);
     free(proc->fs->moveprefix);
     free(proc->fs);
     free(proc);

     return 1;
}

#ifdef __cplusplus
CPP_CLOSE
#endif

