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
#define PROC_NAME "wsproto_in"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <poll.h>
#include <zlib.h>
#include <glob.h>
#if defined(__FreeBSD__)
#include <sys/endian.h>
#elif defined(__APPLE__)
#include <machine/endian.h>
#else
#include <endian.h>
#endif
#include <string>
#include <deque>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_double.h"
#include "protobuf/wsproto_lib.h"
#include "protobuf/ws_protobuf.h"

#ifdef __cplusplus
CPP_OPEN
#endif

// TODO: as part of file metadata, include file offset and record length

char proc_version[]     = "1.5";
const char *proc_tags[]     = { "wsproto","input","pbmeta", NULL };
const char *proc_alias[]     = { "pb2in", "pb2_filein", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "reads in data or files in stdin, creates metadata";
const char *proc_synopsis[]   = { "wsproto_in [-i | -m] [-s]", NULL };
char proc_description[] ="Reads in metadata from files or stdin in both the "
                         "wsproto and pbmeta Protocol Buffer formats. "
                         "WARNING: when reading from stdin, do not switch "
                         "between pbmeta and wsproto.  Mingling formats is "
                         "otherwise okay as long as the format doesn't change "
                         "in the middle of a file.  "
                         "The wsproto format is specified in "
                         "'procs/protobuf/wsproto.proto'. This format differs "
                         "from the wsserial.proto in that it includes "
                         "labels with each "
                         "tuple in string format instead of at the beginning "
                         "of each file, data elements are stored in the "
                         "closest matching Protocol Buffer datatype instead "
                         "of in raw bytes, and an enum specifies the type of "
                         "data being stored.  "
                         "The supported datatypes of wsproto are: dtype_tuple, "
                         "dtype_string, dtype_binary, dtype_fixedstring, "
                         "dtype_mediumstring, dtype_double, dtype_uint, "
                         "dtype_uint64, dtype_uint16, dtype_uint8, "
                         "dtype_label, dtype_ts, and dtype_int. "
                         "The file metadata (-m option) attaches the a tuple's "
                         "source file name in the tuple under the label "
                         "PBFILENAME.  This label is not appended when reading "
                         "content from stdin.";
proc_example_t proc_examples[]    = {
     {"wsproto_in | ...","process the list of files whose names were passed to stdin"},
     {"wsproto_in -i | ...","process the wsproto content passed to stdin"},
     {"wsproto_in -m | ...","include the file name of the tuple's source file in the tuple"},
     {"wsproto_in -r '/tmp/*.wsproto.gz' | ...", "Process all files in /tmp which match *.wsproto.gz"},
     {NULL,""}
};
char proc_requires[]    = "";
const char *proc_input_types[]        = {NULL};
const char *proc_output_types[]   = {"tuple", NULL};
proc_port_t proc_input_ports[]  = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
const char *proc_tuple_member_labels[] = {"PBFILENAME",NULL};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {'i',"","",
     "read file from stdin",0,0},
     {'m',"","",
     "pass on file metadata",0,0},
     {'s',"","",
     "suppress output about file opening",0,0},
     {'B',"","",
     "receive binary dtypes directly from wsproto_out",0,0},
     {'r',"","Filepath",
     "expand passed glob as a list of files to process",1,0},
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[] = "";

#define LOCAL_FILENAME_MAX 2048
#define MAXBUF 65536

//function prototypes for local functions

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t badfile_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     std::deque<std::string> *filenames;
     gzFile fp;
     FILE * in;
     int stdin_data;
     int used_glob;
     void * type_table;
     char * buf;
     uint64_t maxbuf;
     uint16_t formatid;
     uint16_t formatversion;
     uint16_t earliestsupportedformatversion;
     wsproto::wsdata * wsproto;
     ws_protobuf_t * pbuf;
     int done;
     wsdata_t * file_wsd;
     wslabel_t * label_file;
     int pass_file_meta;
     int suppress_output;
     uint8_t receivebinary;
} proc_instance_t;


static int read_names_glob(proc_instance_t *proc, const char *pattern);
static int read_names_file(proc_instance_t *proc);
static int get_next_file(proc_instance_t * proc);
static int proc_binary(void *, wsdata_t*, ws_doutput_t*, int);
static int data_source(void *, wsdata_t*, ws_doutput_t*, int);

static int proc_cmd_options(int argc, char ** argv,
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "ir:smB")) != EOF) {
          switch (op) {
          case 'i':
               proc->stdin_data = 1;
               break;
          case 'm':
               proc->pass_file_meta = 1;
               break;
          case 's':
               proc->suppress_output = 1;
               break;
          case 'B':
               proc->receivebinary = 1;
               break;
          case 'r':
               read_names_glob(proc, optarg);
               proc->used_glob++;
               break;
          default:
               return 0;
          }
     }

     if(proc->stdin_data && proc->pass_file_meta) {
          error_print("cannot pass file metadata (-m) if reading content from stdin (-i)");
          return 0;
     }

     return 1;
}


static int read_names_file(proc_instance_t *proc) {
     int count = 0;
     char buf[LOCAL_FILENAME_MAX+1];
     while ( fgets(buf, LOCAL_FILENAME_MAX, proc->in) ) {
          size_t len = strlen(buf);
          char *p = buf;
          while ( len && isspace(*p) ) {
               p++;
               len--;
          }
          while ( len && isspace(p[len-1]) ) {
               p[len-1] = '\0';
               len--;
          }
          struct stat sbuf;
          if ( !stat(p, &sbuf) ) { /* Check if file exists */
               proc->filenames->push_back(p);
               count++;
          } else {
               error_print("File '%s' cannot be read", p);
               proc->badfile_cnt++;
          }
     }
     return count;
}


static int read_names_glob(proc_instance_t *proc, const char *pattern) {
     int count = 0;
     glob_t globbuf;
     int opts = 0;
#ifdef GLOB_BRACE
     opts |= GLOB_BRACE;
#endif
#ifdef GLOB_TILDE
     opts |= GLOB_TILDE;
#endif

     int ret = glob(pattern, opts, NULL, &globbuf);
     if ( !ret ) {
          for ( size_t i = 0 ; i < globbuf.gl_pathc ; i++ ) {
               count++;
               proc->filenames->push_back(globbuf.gl_pathv[i]);
          }
     }
     globfree(&globbuf);

     return count;
}


static int get_next_file(proc_instance_t * proc) {

     if (proc->done) {
          return 0;
     }

     //close old capture if needed
     if (proc->fp) {
          gzclose(proc->fp);
          proc->fp = NULL;
     }

     if ( proc->filenames->empty() ) {
          return 0;
     }

     while ( !proc->filenames->empty() ) {
          std::string filename = proc->filenames->front();
          proc->filenames->pop_front();
          char *buf = (char*)filename.c_str();

          proc->fp = gzopen(buf, "r");
          if (proc->fp) {
               if (proc->pass_file_meta) {
                    char * fn = basename(buf);
                    int flen = strlen(fn);

                    if (proc->file_wsd) {
                         wsdata_delete(proc->file_wsd);
                    }
                    proc->file_wsd = wsdata_create_string(fn, flen);
                    if (proc->file_wsd) {
                         wsdata_add_reference(proc->file_wsd);
                         wsdata_add_label(proc->file_wsd, proc->label_file);
                    }
               }
               if (!proc->suppress_output) {
                    tool_print("opened file %s", buf);
               }
               return 1;
          }
     }
     proc->done = 1;
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

     proc->in = stdin;
     proc->filenames = new std::deque<std::string>();
     proc->type_table = type_table; // for inline label lookup

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if(0 == proc->receivebinary) {
          if (proc->pass_file_meta) {
               proc->label_file = wsregister_label(type_table, "PBFILENAME");
          }

          if (proc->stdin_data) {
               int fd = fileno(stdin);
               proc->fp = gzdopen(fd, "r");
          }
          else {
               /* Read from stdin if user didn't specify with -g */
               if ( !proc->used_glob && proc->filenames->empty() )
                    read_names_file(proc);
               if ( proc->filenames->empty() ) {
                    error_print("No files to process");
               }
          }

          proc->outtype_tuple =
               ws_register_source_byname(type_table, "TUPLE_TYPE", data_source, sv);

          // set the buffer
          proc->maxbuf = MAXBUF;
          proc->buf = (char *)malloc(MAXBUF);
          if (!proc->buf) {
               error_print("failed malloc of proc->buf");
               return 0;
          }
     }


     // init wsproto and pbmeta specifics
     proc->pbuf = protobuf_init(type_table);

     proc->formatid = 0; // set that there is no current format

     proc->wsproto = wsproto_init();
     proc->type_table = type_table;

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

     // then we are a source kid
     if(proc->receivebinary) {
          // a filter kid
          proc->outtype_tuple = ws_add_outtype_byname(type_table, olist, "TUPLE_TYPE", NULL);
     }
     // else.. must have been registered as source in proc_init

     if (!proc->outtype_tuple) {
          fprintf(stderr, "registration failed\n");
          return NULL;
     }


     if (proc->receivebinary && wsdatatype_match(type_table, input_type, "BINARY_TYPE")) {
          return proc_binary;
     }

     return NULL;
}

static inline int fail_read(proc_instance_t *proc) {
     gzclose(proc->fp);
     proc->badfile_cnt++;
     proc->fp = NULL;
     return 1;
}

static inline int read_next_record(proc_instance_t * proc, wsdata_t * tdata) {
     if (!proc->fp) {
          // since we are closing the file, mark the format as unknown.
          proc->formatid = 0;

          if (proc->stdin_data) {
               return 0;
          }
          get_next_file(proc);
          if (!proc->fp) {
               dprint("nofile");
               return 0;
          }
     }

     uint8_t read32bits = 0; // whether or not we've read in the first record length
     uint32_t init_mlen = 0; // the value of the record length (32 bits)
     // if we haven't
     if(proc->formatid == 0) {
          if (gzread(proc->fp, &init_mlen, sizeof(uint32_t)) != sizeof(uint32_t)) {
               gzclose(proc->fp);
               proc->fp = NULL;
               return 1;
          }
          read32bits = 1;

          // the length of a wsproto header is 4 bytes.  Because of the
          // ordering of the bytes, both 32 and 64 bit reads on the header
          // record length will always be 4.
          if(init_mlen == sizeof(uint16_t)*2) {
               // set the supported format id and version for wsproto
               proc->formatid = WSPROTO_FORMAT_ID;
               proc->formatversion = WSPROTO_FORMAT_VERSION;
               proc->earliestsupportedformatversion = WSPROTO_EARLIEST_SUPPORTED_FORMAT_VERSION;
               if (!proc->suppress_output) {
                    tool_print("format: wsproto");
               }
          }
          // pbmeta doesn't have a header and all records should be well over
          // 4 bytes.  Hence, if we got something bigger it should be a pbmeta
          // record
          else if(init_mlen > sizeof(uint16_t)*2) {
               // set the supported format id and version for pbmeta
               proc->formatid = PBMETA_FORMAT_ID;
               proc->formatversion = PBMETA_FORMAT_VERSION;
               if (!proc->suppress_output) {
                    tool_print("format: pbmeta");
               }
          }
          else {
               tool_print("Unsupported record length (%d)", init_mlen);
               return fail_read(proc);
          }
     }

     if(proc->formatid == WSPROTO_FORMAT_ID) {
          uint64_t mlen;

          // read the length of the next record
          // if we already read 32 bits while detecting the file type, only read 32.
          if(read32bits == 1) {
               // if we've read 32 bits already, it was while trying to figure
               // out the format id.  Since the header comes first, this
               // should be a header file and the remaining 32-bits should be
               // 0's.  let's verify that.
               uint32_t init_mlen2;
               if (gzread(proc->fp, &init_mlen2, sizeof(uint32_t)) != sizeof(uint32_t)) {
                    gzclose(proc->fp);
                    proc->fp = NULL;
                    return 1;
               }

               if(init_mlen != sizeof(uint16_t)*2 || init_mlen2 != 0) {
                    tool_print("not a wsproto file header as expected");
                    return fail_read(proc);
               }
               mlen = (uint64_t)init_mlen;
          }
          // otherwise, read the full 64 bits
          else {
               if (gzread(proc->fp, &mlen, sizeof(uint64_t)) != sizeof(uint64_t)) {
                    gzclose(proc->fp);
                    proc->fp = NULL;
                    return 1;
               }
          }

          if (mlen == sizeof(uint16_t) * 2) {
               uint16_t formatID;
               uint16_t formatVersion;
               if(gzread(proc->fp, &formatID, sizeof(uint16_t)) != sizeof(uint16_t)) {
                    return fail_read(proc);
               }
               if(gzread(proc->fp, &formatVersion, sizeof(uint16_t)) != sizeof(uint16_t)) {
                    return fail_read(proc);
               }
               if (!proc->suppress_output) {
                    tool_print("format id: %d, format version: %d", formatID, formatVersion);
               }

               if(proc->formatid != formatID) {
                    tool_print("unsupported format: %d", formatID);
                    return fail_read(proc);
               }
               if(proc->formatversion < formatVersion) {
                    tool_print("the format version of the file (%d) is higher than that of the parser (%d)", formatVersion, proc->formatversion);
               }
               if(proc->earliestsupportedformatversion > formatVersion) {
                    tool_print("unsupported format version: %d", formatVersion);
                    return fail_read(proc);
               }
          }
          else {
               if (mlen > proc->maxbuf) {
                    proc->buf = (char *)realloc(proc->buf, mlen);
                    if (proc->buf) {
                         proc->maxbuf = mlen;
                    }
                    else {
                         error_print("failed realloc of proc->buf");
                         return fail_read(proc);
                    }
               }

               // TODO: segfault occurs on gzread call below if last file was a pbmeta file
               //if(proc->buf) {
               //     tool_print("buf is good");
               //}
               //if(proc->fp) {
               //     tool_print("fp is good");
               //}

               if (gzread(proc->fp, proc->buf, mlen) != (int)mlen) {
                    return fail_read(proc);
               }
               if (!wsproto_tuple_readbuf(proc->wsproto, tdata, proc->type_table, proc->buf, mlen)) {
                    return fail_read(proc);
               }
          }
     }
     else if(proc->formatid == PBMETA_FORMAT_ID) {
          uint32_t mlen;
          // read the length of the next record
          // if we already read in the length of the first record, use it here.
          if(read32bits == 1) {
               mlen = init_mlen;
          }
          else if (gzread(proc->fp, &mlen, sizeof(uint32_t)) != sizeof(uint32_t)) {
               gzclose(proc->fp);
               proc->fp = NULL;
               return 1;
          }

          if (mlen > proc->maxbuf) {
               proc->buf = (char *)realloc(proc->buf, mlen);
               if (proc->buf) {
                    proc->maxbuf = mlen;
               }
               else {
                    error_print("failed realloc of proc->buf");
                    return fail_read(proc);
               }
          }
          if (gzread(proc->fp, proc->buf, mlen) != (int)mlen) {
               return fail_read(proc);
          }
          if (!protobuf_tuple_readbuf(proc->pbuf, tdata, proc->buf, mlen)) {
               return fail_read(proc);
          }
     }
     else {
       tool_print("Unknown format id");
       return 1;
     }

     return 1;
}

// get the size of the typetable
static int32_t get_current_index_size(void * type_table) {
       mimo_datalists_t * mdl = (mimo_datalists_t *)type_table;
       return mdl->index_len;
}

static int proc_binary(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;


     wsdata_t * tdata = ws_get_outdata(proc->outtype_tuple);
     if (!tdata) {
          return 0;
     }

     wsdt_binary_t * bin = (wsdt_binary_t *)source_data->data;

     uint32_t start_index_size = get_current_index_size(proc->type_table);
     int rtn = wsproto_tuple_readbuf(proc->wsproto, tdata, proc->type_table, bin->buf, bin->len);
     if (rtn == 1) {
          // get the size of the index so we can know later if it has changed
          wsdt_tuple_t * tuple = (wsdt_tuple_t*)tdata->data;
          if (tuple->len) {
               // get the end index size
               uint32_t end_index_size = get_current_index_size(proc->type_table);

               if(start_index_size == end_index_size) {
                    // if the index size remained the same, write out the tuple
                    ws_set_outdata(tdata, proc->outtype_tuple, dout);
                    proc->outcnt++;
               }
               else {
                    // if the index size changed, dupe the tuple to reindex the labels in the tuple
                    dprint("cloning tuple to add new labels into label index");
                    wsdata_t * tdata_copy = wsdata_alloc(dtype_tuple);
                    if(!tdata_copy) {
                         tool_print("unable to allocate tuple for copy");
                         wsdata_delete(tdata);
                         return 0;
                    }
                    if(!tuple_deep_copy(tdata, tdata_copy)) {
                         tool_print("unable to duplicate tuple");
                         wsdata_delete(tdata);
                         wsdata_delete(tdata_copy);
                         return 0;
                    }
                    ws_set_outdata(tdata_copy, proc->outtype_tuple, dout);
                    proc->outcnt++;
                    wsdata_delete(tdata);
               }
          }
     }
     else {
          tool_print("unable to parse binary to tuple data");
          wsdata_delete(tdata);
     }

     return 1;
}
//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
//
static int data_source(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (proc->done) {
          return 0;
     }

     // get the size of the index so we can know later if it has changed
     uint32_t start_index_size = get_current_index_size(proc->type_table);
     if (read_next_record(proc, source_data)) {
          wsdt_tuple_t * tuple = (wsdt_tuple_t*)source_data->data;
          if (tuple->len) {
               proc->meta_process_cnt++;

               if (proc->pass_file_meta && proc->file_wsd) {
                    add_tuple_member(source_data, proc->file_wsd);
               }

               // get the end index size
               uint32_t end_index_size = get_current_index_size(proc->type_table);

               if(start_index_size == end_index_size) {
                    // if the index size remained the same, write out the tuple
                    ws_set_outdata(source_data, proc->outtype_tuple, dout);
                    proc->outcnt++;
               }
               else {
                    // if the index size changed, dupe the tuple to reindex the labels in the tuple
                    dprint("cloning tuple to add new labels into label index");
                    wsdata_t * source_data_copy = wsdata_alloc(dtype_tuple);
                    if(!source_data_copy) {
                         return 0;
                    }
                    if(!tuple_deep_copy(source_data, source_data_copy)) {
                         wsdata_delete(source_data_copy);
                         return 0;
                    }
                    ws_set_outdata(source_data_copy, proc->outtype_tuple, dout);
                    proc->outcnt++;
               }
          }
          return 1;
     }
     return 0;

}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);
     if (proc->badfile_cnt) {
          tool_print("badfile cnt %" PRIu64, proc->badfile_cnt);
     }

     if (!proc->stdin_data && proc->fp) {
          gzclose(proc->fp);
     }
     if (proc->in && proc->in != stdin) {
          fclose(proc->in);
     }
     if (proc->file_wsd) {
          wsdata_delete(proc->file_wsd);
     }

     if ( proc->filenames ) {
          delete proc->filenames;
     }

     //free dynamic allocations
     protobuf_destroy(proc->pbuf);
     wsproto_destroy(proc->wsproto);
     free(proc->buf);
     free(proc);

     return 1;
}

#ifdef __cplusplus
CPP_CLOSE
#endif

