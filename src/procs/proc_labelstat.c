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
//compute stats about labels

#define PROC_NAME "labelstat"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_string.h"
#include "stringhash5.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
//char *proc_menus[]             =  { "Filters", NULL };
char *proc_tags[]              =  {"Stream maniputlation", "Statistics", "State tracking", NULL};
char *proc_alias[]             =  { "statlabel", NULL };
char proc_purpose[]            =  "Compute stats about labels";
char proc_description[] = "Compute the list and counts of all of the labels seen since a flush or interrupt has been detected.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'F',"","filename",
     "load existing database (key table) from file",0,0},
     {'O',"","filename",
     "write database (key table) to file",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'o',"","",
     "track outer labels and tuple member labels",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "";
char *proc_input_types[]       =  {"tuple", "any", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {"LABELSTAT", NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {"LABEL", "COUNT", NULL};
char *proc_synopsis[]          =  {"labelstat [<LABEL>] [-M <value>] [-o]", NULL};
proc_example_t proc_examples[] =  {
	{"... | labelstat | print -V", "prints all of the member labels found in the stream with a count defining the number of times the given label was present"},
	{"... | labelstat LABEL | print -V", "prints the number of times the label, LABEL, was found"},
	{"... | labelstat -o | print -V", "prints all member and container labels with counts on number of times each label was present"},
	{NULL, NULL} 
};

#define LOCAL_MAX_SH5_TABLE 10000

typedef struct _label_data_t {
     wslabel_t * label;
     uint32_t cnt;
} label_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_wsdata(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_tuple_search(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * key_table;
     uint32_t buflen;
     wslabel_set_t lset;
     ws_tuplemember_t * tmember_cnt;
     ws_tuplemember_t * tmember_labelstr;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;
     int all_labels;
     char * outfile;
     char * open_table;

     char * sharelabel;
     int sharer_id;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:F:O:oM:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'F':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash5_create_shared_sht
               // call in proc_init
               break;
          case 'O':
               proc->outfile = strdup(optarg);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          case 'o':
               proc->all_labels = 1;
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          tool_print("using key %s", argv[optind]);
          optind++;
     }
     
     return 1;
}

static void last_destroy(void * vdata, void * vproc) {
     dprint("last destroy");
     proc_instance_t * proc = (proc_instance_t *)vproc;
     label_data_t * kd = (label_data_t *)vdata;
     if (kd && kd->label && kd->cnt) {
          wsdata_t * tdata = ws_get_outdata(proc->outtype_tuple);
          if (!tdata) {
               //no subscribers
               tool_print("%s %u", kd->label->name, kd->cnt);
               return;
          }
          wsdt_string_t * str = tuple_member_alloc(tdata, proc->tmember_labelstr);
          str->buf = kd->label->name;
          str->len = strlen(kd->label->name);
          wsdt_uint_t * cntp = tuple_member_alloc(tdata, proc->tmember_cnt);
          if (cntp) {
               *cntp = kd->cnt;
          }
          //wsdata_add_label(tdata, kd->label);
          ws_set_outdata(tdata, proc->outtype_tuple, proc->dout);
          proc->outcnt++;
     }
}

// dump an wslabel_t to a saved hash table
static inline uint32_t wslabel_t_dump(void * vdata, uint32_t max_records, 
                                      uint32_t data_alloc, FILE * fp) {
     uint32_t i, cnt = 0, len, bytes = 0;
     size_t rtn;
     uint8_t * data = (uint8_t *)vdata;

     //determine the number of wslabel_t items that will be dumped
     for (i = 0; i < max_records; i++) {
          label_data_t * kd = (label_data_t *)(data + (size_t)i * data_alloc);
          if (kd->label) {
               cnt++;
          }
     }
     rtn = fwrite(&cnt, sizeof(uint32_t), 1, fp);

     //dump the index, label, name length, and name for each table entry
     for (i = 0; i < max_records; i++) {
          label_data_t * kd = (label_data_t *)(data + (size_t)i * data_alloc);
          if (kd->label) {
               rtn += fwrite(&i, sizeof(uint32_t), 1, fp);
               rtn += fwrite(kd->label, sizeof(wslabel_t), 1, fp);
               len = strlen(kd->label->name);
               rtn += fwrite(&len, sizeof(uint32_t), 1, fp);
               rtn += fwrite(kd->label->name, len, 1, fp);
               bytes += sizeof(wslabel_t) + 2*sizeof(uint32_t) + len;
          }
     }

     return bytes;
}

// read an wslabel_t from a saved hash table
static inline uint32_t wslabel_t_read(void * vdata, uint32_t data_alloc, FILE * fp) {
     uint32_t i, j, cnt = 0, len, bytes = 0;
     size_t rtn;
     uint8_t * data = (uint8_t *)vdata;

     rtn = fread(&cnt, sizeof(uint32_t), 1, fp);

     for (i = 0; i < cnt; i++) {
          rtn += fread(&j, sizeof(uint32_t), 1, fp);
          label_data_t * kd = (label_data_t *)(data + (size_t)j * data_alloc);
          kd->label = (wslabel_t *)calloc(sizeof(wslabel_t), 1);
          rtn = fread(kd->label, sizeof(wslabel_t), 1, fp);
          rtn += fread(&len, sizeof(int), 1, fp);
          if (len < 255) {
               kd->label->name = (char *)calloc(len, 1);
               rtn += fread(kd->label->name, len, 1, fp);
          }
          bytes += sizeof(wslabel_t) + 2*sizeof(uint32_t) + len;
     }

     return bytes;
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

     proc->buflen = LOCAL_MAX_SH5_TABLE; 

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     proc->tmember_cnt = register_tuple_member_type(type_table,
                                                    "UINT_TYPE",
                                                    "COUNT");
     proc->tmember_labelstr = register_tuple_member_type(type_table,
                                                    "STRING_TYPE",
                                                    "LABEL");

     //other init - init the stringhash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->sh_callback = last_destroy;
     sh5_sh_opts->proc = proc; 
     sh5_sh_opts->open_table = proc->open_table;
     sh5_sh_opts->sh5_dataread_cb = wslabel_t_read;

     if (proc->sharelabel) {
          int ret;

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->key_table, 
                                              proc->sharelabel, proc->buflen,
                                              sizeof(label_data_t), &proc->sharer_id,
                                              sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash5_open_sht_table(&proc->key_table, proc, 
                                                proc->buflen, sizeof(label_data_t), 
                                                sh5_sh_opts);
          }
          // create the stringhash5 table from scratch
          if (!ret) {
               proc->key_table = stringhash5_create(0, proc->buflen, sizeof(label_data_t));
               if (!proc->key_table) {
                    return 0;
               }
          }
          stringhash5_set_callback(proc->key_table, last_destroy, proc);
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->key_table->max_records;

     free(proc->open_table);

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

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype_byname(type_table, olist, "TUPLE_TYPE",
                                                      "LABELSTAT");
     }
     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return proc_flush;
     }
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          if (proc->lset.len) {
               return proc_tuple_search;
          }
          else {
               return proc_tuple;
          }
     }
     else {
          return proc_wsdata;
     }

     return NULL; // a function pointer
}

static inline void add_member(proc_instance_t * proc, wsdata_t * member) {
     label_data_t * kdata = NULL;
     int i;

     for (i = 0; i < member->label_len; i++) {
          int len = strlen(member->labels[i]->name);
          kdata = (label_data_t *) stringhash5_find_attach(proc->key_table,
                                                           (uint8_t*)member->labels[i]->name,
                                                           len);
          if (kdata) {
               if (!kdata->label) {
                    kdata->label = member->labels[i];
               }
               kdata->cnt++;
               stringhash5_unlock(proc->key_table);
          }
     }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_wsdata(void * vinstance, wsdata_t* input_data,
                       ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;
     
     add_member(proc, input_data);

     //always return 1 since we don't know if table will flush old data
     return 1;
}



//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;
     
     if (proc->all_labels) {
          add_member(proc, input_data);
     }

     wsdt_tuple_t * tuple = input_data->data;
     dprint("proc_tuple");

     int i;
     for (i = 0; i < tuple->len; i++) {
          add_member(proc, tuple->member[i]);
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple_search(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;
     dprint("proc_tuple");

     int id;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     tuple_init_labelset_iter(&iter, input_data,
                              &proc->lset);

     while (tuple_search_labelset(&iter, &member,
                                  &label, &id)) {
          add_member(proc, member);
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

uint32_t labelstat_dump_table = 0;

static void dump_table_during_flush(proc_instance_t * proc) {
     if (!labelstat_dump_table) {
	  tool_print("Writing data table to %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
	  if (fp) {
	       stringhash5_dump_with_ptrs(proc->key_table, fp, wslabel_t_dump);
	       fclose(fp);
	  }
          else {
               perror("failed writing data table");
               tool_print("unable to write to file %s", proc->outfile);
          }
          labelstat_dump_table = 1;
     }
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     //dump table before scour and flush
     if (proc->outfile) {
          dump_table_during_flush(proc);
     }
     stringhash5_scour_and_flush(proc->key_table, last_destroy, proc);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_destroy(proc->key_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->outfile) {
          free(proc->outfile);
     }
     free(proc);

     return 1;
}

