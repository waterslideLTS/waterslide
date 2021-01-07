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
//keeps track of the last n samples of data at keys..   on query appends these
//items

#define PROC_NAME "lastn"

//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "procloader.h"
#ifdef PBMETA
#include "zlib.h"
#include "protobuf/wsproto_lib.h"
//#include "protobuf/ws_protobuf.h"
#endif // PBMETA

#ifdef __cplusplus
CPP_OPEN
#endif

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
const char *proc_tags[]        =  { "Stream Manipulation", "Filtering", "Detection", "State Tracking", NULL };
const char *proc_alias[]       =  { "appendlastn", "lastn_shared", "appendlastn_shared", NULL };
char proc_purpose[]            =  "appends the last n items for each query key";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'F',"","filename",
     "load pbmeta or wsproto records from file and inserts into lastn table",0,0},
     {'L',"","LABELprefix",
     "output label prefix appended to last n items",0,0},
     {'l',"","",
     "output labels with original labels",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'N',"","count",
     "number of records per key",0,0},
     {'n',"","count",
     "number of records per key",0,0},
     {'V',"","LABEL",
     "item to track if last at key",0,0},
     {'E',"","filename",
     "load existing database (key table) from file",0,0},
     {'O',"","filename",
     "write database (key table) to file",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "LABEL of single key to index on, values";
const char *proc_input_types[] =  {"tuple", NULL};
// (Potential) Output types: tuple
const char *proc_output_types[] =  {"tuple", NULL};
char proc_requires[]           =  ""; 
// Ports: none, QUERY, ADDQUERY, QUERYADD, DELETE, REMOVE
proc_port_t proc_input_ports[] =  {
     {"none","Check and sets last n data at key; passes no tuples"},
     {"QUERY","Appends last n items at key; passes output unless key does not exist"},
     {"FILTER","Appends last n items at key; only passes output if key has been stored"},
     {"ADDQUERY","Appends last n items at key; also set values in table; pass all tuples"},
     {"QUERYADD","Appends last n items at key; also set values in table; pass all tuples"},
     {"DELETE","delete all items at key"},
     {"REMOVE","delete all items at key"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL}; 
char *proc_tuple_conditional_container_labels[] =  {NULL}; 
const char *proc_tuple_member_labels[] =  {"LASTN_0_BAR", "LASTN_1_BAR", "LASTN_0_BAZ", "LASTN_1_BAZ", NULL}; 
const char *proc_synopsis[]  =  { "lastn [-J <SHT5 LABEL>] <LABEL OF KEY> -V <VALUE> [-V <VALUE>...] [-N <NUMBER> -M <NUMBER>] [-L <prefix> | -l] [-F <file.pb>]", NULL }; 

proc_example_t proc_examples[] =  {
	{"... | ADDQUERY:lastn KEY -V VALUE1 -V VALUE2 -N 1 | ...","Append last VALUE1 and VALUE2 for each KEY to the lastn table; output last VALUE1 and VALUE2 seen for a particular KEY in new tuples LASTN_0_VALUE1 and LASTN_0_VALUE2"},
	{"... | $stream1, $stream2:QUERY | lastn -J TABLE0 FOO -V BAR -V BAZ -N 2 -L 'FIDDLE_' | ...","Set last 2 values of BAR and BAZ seen for each FOO label in $stream1; check label FOO on $stream2 and pass last 2 values in FIDDLE_0_BAR, FIDDLE_1_BAR, FIDDLE_0_BAZ, and FIDDLE_1_BAZ.  The table TABLE0 is shareable across threads."},
	{"... | $stream1, $stream2:FILTER | lastn FOO -V BAR | ...","Record most recently seen value of BAR seen for each FOO label in $stream1; if label FOO on $stream2 has been set, pass previous value as LASTN_0_BAR"},
	{"... | QUERY:lastn -F 'inputfile.wsproto' FOO -lV BAR | ...","Read in records from inputfile and use them to seed the lastn table; then if FOO is seen in input, append last value of BAR to the tuples and pass output in new tuple labeled FOO."},
     {NULL,""}
}; 

char proc_description[] = "buffer the last n items for a specific key."
			  "It appends last n values for each item for each key.  The default"
                          "value of n is 1.  The max is 20."
			  "lastn has several ports that can be used to add "
			  "and query data, as well as delete items in the table. See proc examples"
			  "for some examples of using the various ports"; 

typedef struct _key_data_t {
     uint16_t next;
     uint16_t len;
} key_data_t; 

//function prototypes for local functions
static int proc_insert(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_query(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_filter(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_delete(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_addquery(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

#define MAX_VALUES 20
#define MAX_DEPTH 20

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t query_cnt;
     uint64_t outcnt;

     stringhash5_t * last_table;
     uint32_t buflen;
     int n;
     ws_outtype_t * outtype_tuple;
     int num_val;
     wslabel_t * label_value[MAX_VALUES];
     wslabel_t * label_out[MAX_DEPTH][MAX_VALUES];
     wslabel_t * label_key;
     char * label_prefix;
     uint64_t val_delete;
     uint64_t val_add;
     char * input_protobuf;
     int original_labels;
     char * outfile;
     char * open_table;

     char * sharelabel;
     int sharer_id;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:lF:n:L:V:N:M:E:O:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'l':
               proc->original_labels = 1;
               break;
          case 'F':
               if (!proc->input_protobuf) {
                    proc->input_protobuf = strdup(optarg);
               }
               else {
                    tool_print("only one input file\n");
                    return 0;
               }
               break;
          case 'n':
          case 'N':
               proc->n = atoi(optarg);
               if (proc->n > MAX_DEPTH) {
                    tool_print("n too big, max is %d", MAX_DEPTH);
                    proc->n = MAX_DEPTH;
                    return 0;
               }
               break;
          case 'V':
               if (proc->num_val >= MAX_VALUES) {
                    tool_print("too many values");
                    return 0;
               }
               proc->label_value[proc->num_val] = wssearch_label(type_table, optarg);
               proc->num_val++;
               tool_print("using value %s", optarg);
               break;
          case 'L':
               proc->label_prefix = optarg;
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          case 'E':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash5_create_shared_sht
               // call in proc_init
               break;
          case 'O':
               proc->outfile = strdup(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          if (!proc->label_key) {
               proc->label_key = wssearch_label(type_table, argv[optind]);
               tool_print("using key %s", argv[optind]);
          }
          else {
               if (proc->num_val >= MAX_VALUES) {
                    tool_print("too many values");
                    return 0;
               }
               proc->label_value[proc->num_val] = wssearch_label(type_table,
                                                                 argv[optind]);
               proc->num_val++;
          }
          optind++;
     }
     
     return 1;
}

static void last_destroy(void * vdata, void * vproc) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     key_data_t * kdata = (key_data_t*)vdata;
     if (kdata->len) {
          wsdata_t ** wsdp = (wsdata_t **)((uint8_t*)vdata + sizeof(key_data_t));
          int i;
          int j;
          int offset;
          for (i = 0; i < kdata->len; i++) {
               offset = i * proc->num_val;
               for (j = 0; j < proc->num_val; j++) {
                    wsdata_delete(wsdp[offset + j]);
                    wsdp[offset + j] = NULL;
                    proc->val_delete++;
               }
          }
     }
     kdata->len = 0;
     kdata->next = 0;
}

static void set_outlabels(proc_instance_t * proc, void * type_table) {
     char buf[100];
     int i,j;
     for (i = 0; i < proc->n; i++) {
          for (j = 0; j < proc->num_val; j++) {
               if (proc->original_labels) { 
                    proc->label_out[i][j] = proc->label_value[j];
               }
               else {
                    snprintf(buf, 100, "%s%d_%s", proc->label_prefix, i,
                             proc->label_value[j]->name);
                    proc->label_out[i][j] = 
                         wsregister_label(type_table, buf);
               }
          }
     }
}

#ifdef PBMETA
static int read_input_protobuf(proc_instance_t * proc, void * type_table) {
     // this function is adapted from read_next_record() in proc_wsproto_in.c

     uint16_t format_id = 0;
     uint16_t format_version = 0;
     uint16_t earliest_supported_format_version = 0;

     char * buf = NULL;

     int ret = 1;

     //if (!pbuf) {
     //     tool_print("unable to initialize protocol buffer parser");
     //     return 0;
     //}

     gzFile fp = gzopen(proc->input_protobuf, "r");
     if (!fp) {
          tool_print("unable to open protocol buffer file");
          return 0;
     }

     uint8_t read32bits = 0; // whether or not we've read in the first record length
     uint32_t init_mlen = 0; // the value of the record length (32 bits)

     // if we haven't
     if (gzread(fp, &init_mlen, sizeof(uint32_t)) != sizeof(uint32_t)) {
          tool_print("unable to initialize protocol buffer parser");
          gzclose(fp);
          return 0;
     }
     read32bits = 1;

     // the length of a wsproto header is 4 bytes.  Because of the
     // ordering of the bytes, both 32 and 64 bit reads on the header
     // record length will always be 4.
     if (init_mlen == sizeof(uint16_t)*2) {
          // set the supported format id and version for wsproto
          format_id = WSPROTO_FORMAT_ID;
          format_version = WSPROTO_FORMAT_VERSION;
          earliest_supported_format_version = WSPROTO_EARLIEST_SUPPORTED_FORMAT_VERSION;
     }
     // pbmeta doesn't have a header and all records should be well over
     // 4 bytes.  Hence, if we got something bigger it should be a pbmeta
     // record
     //else if (init_mlen > sizeof(uint16_t)*2) {
     //     // set the supported format id and version for pbmeta
     //     format_id = PBMETA_FORMAT_ID;
     //     format_version = PBMETA_FORMAT_VERSION;
     //}
     else {
          error_print("Unsupported protocol buffer record length (%d)", init_mlen);
          gzclose(fp);
          return 0;
     }

     if (format_id == WSPROTO_FORMAT_ID) {
          wsproto::wsdata * wsproto = wsproto_init();

          uint64_t mlen;
          uint64_t maxbuf = 0;
          //char * buf = NULL;
          wsdata_t * tdata;

          while (1) {
               // read the length of the next record
               // if we already read 32 bits while detecting the file type, only read 32.
               if (read32bits == 1) {
                    read32bits = 0;
                    // if we've read 32 bits already, it was while trying to figure
                    // out the format id.  Since the header comes first, this
                    // should be a header file and the remaining 32-bits should be
                    // 0's.  let's verify that.
                    uint32_t init_mlen2;
                    if (gzread(fp, &init_mlen2, sizeof(uint32_t)) != sizeof(uint32_t)) {
                         error_print("not a wsproto file header as expected");
                         gzclose(fp);
                         fp = NULL;
                         ret = 0;
                         break;
                    }

                    if (init_mlen != sizeof(uint16_t)*2 || init_mlen2 != 0) {
                         error_print("not a wsproto file header as expected");
                         ret = 0;
                         break;
                    }
                    mlen = (uint64_t) init_mlen;
               }
               // otherwise, read the full 64 bits
               else {
                    if (gzread(fp, &mlen, sizeof(uint64_t)) != sizeof(uint64_t)) {
                         gzclose(fp);
                         fp = NULL;
                         // don't set the fail return value because we've
                         // reached the end of the file
                         //ret = 0;
                         break;
                    }
               }

               if (mlen == sizeof(uint16_t) * 2) {
                    uint16_t formatID;
                    uint16_t formatVersion;
                    if (gzread(fp, &formatID, sizeof(uint16_t)) != sizeof(uint16_t)) {
                         error_print("unsupported wsproto format: %d", formatID);
                         ret = 0;
                         break;
                    }
                    if (gzread(fp, &formatVersion, sizeof(uint16_t)) != sizeof(uint16_t)) {
                         ret = 0;
                         break;
                    }

                    if (format_id != formatID) {
                         error_print("unsupported format: %d", formatID);
                         ret = 0;
                         break;
                    }
                    if (format_version < formatVersion) {
                         error_print("the format version of the file (%d) is higher than that of the parser (%d)", formatVersion, format_version);
                    }
                    if (earliest_supported_format_version > formatVersion) {
                         error_print("unsupported format version: %d", formatVersion);
                         ret = 0;
                         break;
                    }
               }
               else {
                    if (mlen > maxbuf) {
                         buf = (char *)realloc(buf, mlen);
                         if (buf) {
                              maxbuf = mlen;
                         }
                         else {
                              error_print("failed realloc of buf");
                              ret = 0;
                              break;
                         }
                    }

                    if (gzread(fp, buf, mlen) != (int) mlen) {
                        ret = 0;
                        break;
                    }

                    tdata = wsdata_alloc(dtype_tuple);
                    if (!tdata) {
                         error_print("unable to allocate read tuple for protocol buffer");
                         ret = 0;
                         break;
                    }

                    wsdata_add_reference(tdata); //ref count = 1

                    if (!wsproto_tuple_readbuf(wsproto, tdata, type_table, buf, mlen)) {
                         ret = 0;
                         break;
                    }
                    //parse tuple for keys and values
                    //append to table
                    proc_insert(proc, tdata, NULL, 0);

                    wsdata_delete(tdata);
                    tdata = NULL;
               }
          }
          wsproto_destroy(wsproto);
     }
     else {
          error_print("Unknown protocol buffer format id");
          return 0;
     }

     free(buf);
     gzclose(fp);

     //return 1;
     return ret;
}
#else
static int read_input_protobuf(proc_instance_t * proc, void * type_table) {
     tool_print("input not implemented -- must be compiled in");
     return 0;
}
#endif // PBMETA

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

     ws_default_statestore(&proc->buflen); 

     // default value of n and prefix:
     proc->n = 1;
     proc->label_prefix = (char *)"LASTN_";

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->label_key || !proc->label_value) {
          error_print("need to specify a key and value");
          return 0;
     }
     set_outlabels(proc, type_table);

     size_t kdtemp = ((size_t)proc->n * (size_t)proc->num_val *
                   sizeof(wsdata_t*)) + sizeof(key_data_t);
     if (kdtemp > INT_MAX) {
          error_print("value sizes exceed limits");
          return 0;
     }
     int kdata_len = kdtemp;

     //other init - init the stringhash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->sh_callback = last_destroy;
     sh5_sh_opts->proc = proc; 
     sh5_sh_opts->open_table = proc->open_table;

     if (proc->sharelabel) {
          int ret;

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->last_table, 
                                              proc->sharelabel, proc->buflen, kdata_len, 
                                              &proc->sharer_id, sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash5_open_sht_table(&proc->last_table, proc, proc->buflen, 
                                                kdata_len, sh5_sh_opts); 
          }
          // create the stringhash5 table from scratch
          if (!ret) {
              proc->last_table = stringhash5_create(0, proc->buflen, kdata_len);
              if (!proc->last_table) {
                   return 0;
              }
          }
          stringhash5_set_callback(proc->last_table, last_destroy, proc);
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->last_table->max_records;

     free(proc->open_table);

     if (proc->input_protobuf) {
          if (!read_input_protobuf(proc, type_table)) return 0;
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

     if (meta_type == dtype_flush) {
          return proc_flush;
     }
     if (meta_type == dtype_tuple) {
          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
          }
          if (wslabel_match(type_table, port, "QUERY")) {
               return proc_query;
          }
          if (wslabel_match(type_table, port, "FILTER")) {
               return proc_filter;
          }
          if (wslabel_match(type_table, port, "ADDQUERY")) {
               return proc_addquery;
          }
          if (wslabel_match(type_table, port, "QUERYADD")) {
               return proc_addquery;
          }
          if (wslabel_match(type_table, port, "DELETE")) {
               return proc_delete;
          }
          if (wslabel_match(type_table, port, "REMOVE")) {
               return proc_delete;
          }
	  // the "none" port: 
          return proc_insert;
     }

     return NULL; // a function pointer
}

static inline int do_insert(proc_instance_t * proc,
                            wsdata_t * key,
                            wsdata_t ** values) {
     void * vdata;
     vdata = stringhash5_find_attach_wsdata(proc->last_table, key);
     if (vdata == NULL) {
          //error with hashtable
          return 0;
     }

     key_data_t * kdata;

     kdata = (key_data_t*)vdata;
     wsdata_t ** wsdp = (wsdata_t**)((uint8_t*)vdata + sizeof(key_data_t));


     int i;
     int offset = kdata->next * proc->num_val;
     //check if old data is around..
     if (wsdp[offset]) {
          for (i = 0; i < proc->num_val; i++) {
               wsdata_delete(wsdp[offset + i]);
          }
     }

     for (i = 0; i < proc->num_val; i++) {
          wsdp[offset + i] = values[i];
          wsdata_add_reference(values[i]);
          proc->val_add++;
     }

     kdata->next++;
     if (kdata->next == proc->n) {
          kdata->next = 0;
     }
     if (kdata->len < proc->n) {
          kdata->len++;
     }
     stringhash5_unlock(proc->last_table);

     return 1;
}

// append data to the tuple before passing it
static inline int do_append(proc_instance_t * proc,
                            wsdata_t * tdata,
                            wsdata_t * key) {
     void * vdata;
     vdata = stringhash5_find_wsdata(proc->last_table, key);
     if (vdata == NULL) {
          //error with hashtable
          return 0;
     }

     key_data_t * kdata;

     kdata = (key_data_t*)vdata;
     wsdata_t ** wsdp = (wsdata_t**)((uint8_t*)vdata + sizeof(key_data_t));

     int i;
     int j = 0;
     int k;
     int offset;
     int p;
     if (kdata->len < proc->n) {
         for (i = kdata->len - 1; i >= 0; i--) {
              offset = i * proc->num_val;
              for (k = 0; k < proc->num_val; k++) {
                   tuple_member_add_ptr(tdata, wsdp[offset + k],
                                        proc->label_out[j][k]);
              }
              j++;
         } 
     }
     else {
         for (i = proc->n - 1; i >= 0; i--) {
              p = (kdata->next + i) % proc->n;
              offset = p * proc->num_val;
              for (k = 0; k < proc->num_val; k++) {
                   tuple_member_add_ptr(tdata, wsdp[offset + k],
                                        proc->label_out[j][k]);
              }
              j++;
         } 
     }
     stringhash5_unlock(proc->last_table);

     return 1;
}


//insert values at key
static int proc_insert(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     wsdata_t * key = NULL;

     dprint("proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
          dprint("found key");
     }
     if (!key) {
          return 0;
     }

     wsdata_t * value[MAX_VALUES];

     int i;
     for (i = 0; i < proc->num_val; i++) {
          if (tuple_find_label(input_data, proc->label_value[i],
                               &mset_len, &mset)) {
               value[i] = mset[0];
               dprint("found value");
          }
          else {
               return 0;
          }
     }

     dprint("found key");

     do_insert(proc, key, value);

     //always return 1 since we don't know if table will flush old data
     return 1;
}

// append values at key if key is found - and pass the output
// if key is not found, pass nothing
static int proc_query(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->query_cnt++;

     wsdata_t * key = NULL;

     dprint("proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
          dprint("found key");
     }

     if (key) {
          do_append(proc, input_data, key);
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

// append values at key if key has been previously stored - and pass the output
// if key is not found, pass nothing
static int proc_filter(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->query_cnt++;

     wsdata_t * key = NULL;

     dprint("proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
          dprint("found key");
     }

     if (key) {
          if (do_append(proc, input_data, key)) {
	       ws_set_outdata(input_data, proc->outtype_tuple, dout);
               proc->outcnt++;
	  }
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

//append values at key if key is present; also insert values at key if present in curr record
//pass all records through either way
static int proc_addquery(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->query_cnt++;

     wsdata_t * key = NULL;

     dprint("proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
          dprint("found key");
     }
 
     // if KEY is not present in the current record, pass data through
     if (!key) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
          return 0;
     }

     wsdata_t * value[MAX_VALUES];
     int nv = 0;
     int i;
     for (i = 0; i < proc->num_val; i++) {
          if (tuple_find_label(input_data, proc->label_value[i],
                               &mset_len, &mset)) {
               value[i] = mset[0];
               nv++;
               dprint("found value");
          }
          else {
               break;
          }
     }

     // else if KEY is present, append the values to the lastn table
     // and pass output data
     do_append(proc, input_data, key);
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     // also insert values at key
     if (nv == proc->num_val) {
          do_insert(proc, key, value);
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int proc_delete(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->query_cnt++;

     wsdata_t * key = NULL;

     dprint("proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
          dprint("found key");
     }

     if (!key) {
          return 0;
     }
     
     void * vdata;
     vdata = stringhash5_find_wsdata(proc->last_table, key);
     if (vdata != NULL) {
          last_destroy(vdata, proc);
          stringhash5_delete_wsdata(proc->last_table, key);
          stringhash5_unlock(proc->last_table);
     }

     return 1;
}

static void serialize_table(proc_instance_t * proc) {
     if (proc->outfile && (!proc->sharelabel || !proc->sharer_id)) {
	  tool_print("Writing data table to %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
	  if (fp) {
	       stringhash5_dump(proc->last_table, fp);
	       fclose(fp);
	  }
          else {
               perror("failed writing data table");
               tool_print("unable to write to file %s", proc->outfile);
          }
     }
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     serialize_table(proc);
     stringhash5_scour(proc->last_table, last_destroy, proc);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("query cnt %" PRIu64, proc->query_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_destroy(proc->last_table);

     //free dynamic allocations
     if (proc->input_protobuf) {
          free(proc->input_protobuf);
     }
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->outfile) {
          free(proc->outfile);
     }
     free(proc);

     return 1;
}

#ifdef __cplusplus
CPP_CLOSE
#endif

