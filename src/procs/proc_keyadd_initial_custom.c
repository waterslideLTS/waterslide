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
//tracks biased keys

//NOTE: both serial and parallel versions of this kid are built from this source file
#define PROC_NAME "keyadd_initial_custom"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_double.h"
#include "stringhash5.h"
#include "procloader.h"

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "trackbkey", "keyadd_initial_custom_shared", "trackbkey_shared", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "tracks biased keys";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'j',"","sharename5",
     "shared table with other sh5 kids",0,0},
     {'2',"","",
     "perform two level benchmark",0,0},
     {'m',"","count",
     "threshold for checking value",0,0},
     {'b',"","count",
     "threshold for anomalous value",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'F',"","filename",
     "load existing database (key table) from file",0,0},
     {'O',"","filename",
     "write database (key table) to file",0,0},
     {'L',"","filename",
     "load existing database (outer table) from file",0,0},
     {'P',"","filename",
     "write database (outer table) to file",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "";
char *proc_input_types[]    = {"string", NULL};
const char *proc_synopsis[]  =  { "keyadd_initial_custom [-J <SHT LABEL>] [-j <SHT5 LABEL>] [-2] [-m <NUMBER>] [-b <NUMBER>] [-M <NUMBER>]", NULL }; 

#define LOCAL_MAX_SH5_TABLE 8000000

typedef struct _key_data_t {
     uint32_t cnt;
     uint32_t value;
} key_data_t;

typedef struct _outer_data_t {
     uint8_t pos;  //1 through 5
     uint64_t key;
} outer_data_t;

//function prototypes for local functions
static int proc_string(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_string_twolevel(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     stringhash5_t * key_table;
     stringhash5_t * outer_table;
     uint32_t target_cnt;
     uint32_t threshold_value;
     uint64_t ibufs;
     uint64_t keys;
     uint64_t positive;
     uint64_t negative;
     uint64_t fp;
     uint64_t fn;
     uint64_t tp;
     uint64_t tn;
     uint64_t errors;
     uint32_t buflen;
     uint64_t outerkeys;
     uint64_t datums;
     int twolevel;
     char * open_table;
     char * open_table5;

     char * sharelabel;
     char * sharelabel5;
     int sharer_id;
     int sharer_id5;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:j:2m:b:M:F:O:L:P:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'j':
               proc->sharelabel5 = strdup(optarg);
               break;
          case '2':
               proc->twolevel = 1;
               break;
          case 'm':
               proc->target_cnt = atoi(optarg);
               break;
          case 'b':
               proc->threshold_value = atoi(optarg);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          case 'F':
               proc->open_table = strdup(optarg);
               // loading of the key table is postponed to the stringhash5_create_shared_sht
               // call in proc_init
               break;
          case 'L':
               proc->open_table5 = strdup(optarg);
               // loading of the outer table is postponed to the stringhash5_create_shared_sht
               // call in proc_init
               break;
          default:
               return 0;
          }
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

     proc->buflen = LOCAL_MAX_SH5_TABLE;
     proc->threshold_value = 5;
     proc->target_cnt = 24;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //other init - init the stringhash tables

     //init the first hash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->open_table = proc->open_table;

     if (proc->sharelabel) {
          int ret;

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->key_table, 
                                              proc->sharelabel, proc->buflen, 
                                              sizeof(key_data_t), &proc->sharer_id, 
                                              sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash5_open_sht_table(&proc->key_table, proc, proc->buflen, 
                                            sizeof(key_data_t), sh5_sh_opts);
               if (!ret && errno) return 0;
          }
          // create the stringhash5 table from scratch
          if (!ret) {
               proc->key_table = stringhash5_create(0, proc->buflen, 
                                                    sizeof(key_data_t));
               if (!proc->key_table) {
                    return 0;
               }
          }

          free(proc->open_table);
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->key_table->max_records;

     //init the second hash table

     if (proc->twolevel) {

          //calloc shared sh5 option struct
          stringhash5_sh_opts_t * sh5_sh_opts;
          stringhash5_sh_opts_alloc(&sh5_sh_opts);

          //set shared sh5 option fields
          sh5_sh_opts->open_table = proc->open_table5;

          if (proc->sharelabel5) {
               int ret;

               ret = stringhash5_create_shared_sht(type_table, (void **)&proc->outer_table, 
                                                   proc->sharelabel5, proc->buflen, 
                                                   sizeof(outer_data_t), &proc->sharer_id5, 
                                                   sh5_sh_opts);

               if (!ret) return 0;
          }
          else {
               // read the stringhash5 table from the open_table5 file
               uint32_t ret = 0;
               if (proc->open_table5) {
                    ret = stringhash5_open_sht_table(&proc->outer_table, proc, proc->buflen, 
                                                     sizeof(outer_data_t), sh5_sh_opts);
                    if (!ret && errno) return 0;
               }
               // create the stringhash5 table from scratch
               if (!ret) {
                    proc->outer_table = stringhash5_create(0, proc->buflen, 
                                                           sizeof(outer_data_t));
                    if (!proc->outer_table) {
                         return 0;
                    }
               }
          }

          //free shared sh5 option struct
          stringhash5_sh_opts_free(sh5_sh_opts);

          free(proc->open_table5);
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

     if (meta_type == dtype_string) {
          if (proc->twolevel) {
               return proc_string_twolevel;
          }
          return proc_string;
     }

     return NULL; // a function pointer
}

static int proc_string(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->ibufs++;

     wsdt_string_t * str = (wsdt_string_t*)input_data->data;

     char * buf = str->buf;

     char * sep;
     //skip frame header
     sep = strsep(&buf, "\n");

     if (!sep) {
          proc->errors++;
          return 1;
     }
     while (((sep = strsep(&buf, "\n")) != NULL) && (sep[0] != 0)) {
          char * pbuf = sep;
          char * key = strsep(&pbuf, ",");
          char * value = strsep(&pbuf, ",");
          char * bias = pbuf;

          if (!key || !value) {
               proc->errors++;
               return 1;
          }
          proc->datums++;
          key_data_t * kd = stringhash5_find_attach(proc->key_table, key, strlen(key));
          kd->cnt++;
          kd->value += (value[0] == '1');

          if (kd->cnt == 1) {
               proc->keys++;
          }
          else if (kd->cnt == proc->target_cnt) {
               if (kd->value < proc->threshold_value) {
                    proc->positive++;
                    if (bias && (bias[0] == '1')) {
                         fprintf(stderr, "%s %u positive anomaly\n", key, kd->value);
                         proc->tp++;
                    }
                    else {
                         fprintf(stderr, "%s %u false anomaly\n", key, kd->value);
                         proc->fp++;
                    }
               }
               else {
                    proc->negative++;
                    if (bias && (bias[0] == '1')) {
                         //printf("%s %u missed anomaly\n", key, kd->value);
                         proc->fn++;
                    }
                    else {
                         proc->tn++;
                    }
               }
          }
          stringhash5_unlock(proc->key_table);
          
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}


static inline int parse_two_level_datum(proc_instance_t * proc, char * key,
                                        char * value, char * pos) {
     if (!key || !value || !pos) {
          proc->errors++;
          return 0;
     }

     proc->datums++;
     outer_data_t * od = stringhash5_find_attach(proc->outer_table, key, strlen(key));
     if (!od) {
          return 1;
     }

     switch(pos[0]) {
     case '1':
          proc->outerkeys++;
          od->pos = pos[0];
          od->key = atoi(value); 
          stringhash5_unlock(proc->outer_table);
          return 1;
     case '2':
          if (od->pos != '1') {
               od->pos = 0;
               stringhash5_unlock(proc->outer_table);
               return 1;
          }
          od->pos = pos[0];
          od->key |= (((uint64_t)strtoull(value, NULL, 10) & 0xFFFF) << 16); 
          stringhash5_unlock(proc->outer_table);
          return 1;
     case '3':
          if (od->pos != '2') {
               od->pos = 0;
               stringhash5_unlock(proc->outer_table);
               return 1;
          }
          od->pos = pos[0];
          od->key |= (((uint64_t)strtoull(value, NULL, 10) & 0xFFFF) << 32); 
          stringhash5_unlock(proc->outer_table);
          return 1;
     case '4':
          if (od->pos != '3') {
               od->pos = 0;
               stringhash5_unlock(proc->outer_table);
               return 1;
          }
          od->pos = pos[0];
          od->key |= (((uint64_t)strtoull(value, NULL, 10) & 0xFFFF) << 48); 
          stringhash5_unlock(proc->outer_table);
          return 1;
     case '5':
          if (od->pos != '4' ||
              ((value[0] != '1') && (value[0] != '0'))) {
               od->pos = 0;
               stringhash5_unlock(proc->outer_table);
               return 1;
          }

          break; //fall through
     default:
          tool_print("unexpected position");
     }
     stringhash5_unlock(proc->outer_table);

     key_data_t * kd = stringhash5_find_attach(proc->key_table,
                                               &od->key,
                                               sizeof(uint64_t));

     kd->cnt++;
     kd->value += (value[1] == '1');
     int bias = (value[0] == '1');

     if (kd->cnt == 1) {
          proc->keys++;
     }
     else if (kd->cnt == proc->target_cnt) {
          if (kd->value < proc->threshold_value) {
               proc->positive++;
               if (bias) {
                    fprintf(stderr,"%"PRIu64" %u positive anomaly\n", od->key, kd->value);
                    proc->tp++;
               }
               else {
                    fprintf(stderr,"%"PRIu64" %u false anomaly\n", od->key, kd->value);
                    proc->fp++;
               }
          }
          else {
               proc->negative++;
               if (bias) {
                    //printf("%s %u missed anomaly\n", key, kd->value);
                    proc->fn++;
               }
               else {
                    proc->tn++;
               }
          }
     }

     stringhash5_delete(proc->outer_table, key, strlen(key));
     stringhash5_unlock(proc->key_table);

     return 1;
}

static int proc_string_twolevel(void * vinstance, wsdata_t* input_data,
                                ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->ibufs++;

     wsdt_string_t * str = (wsdt_string_t*)input_data->data;

     char * buf = str->buf;

     char * sep;
     //skip frame header
     sep = strsep(&buf, "\n");

     if (!sep) {
          proc->errors++;
          return 1;
     }
     while (((sep = strsep(&buf, "\n")) != NULL) && (sep[0] != 0)) {
          char * pbuf = sep;
          char * key = strsep(&pbuf, ",");
          char * value = strsep(&pbuf, ",");
          char * pos = pbuf;

          if (!parse_two_level_datum(proc, key, value, pos)) {
               break;
          }
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("%" PRIu64" input buffers", proc->ibufs);
     tool_print("%" PRIu64" datums", proc->datums);
     if (proc->twolevel) {
          tool_print("%" PRIu64" outer keys", proc->outerkeys);
          tool_print("%" PRIu64" inner keys", proc->keys);
     }
     else {
          tool_print("%" PRIu64" uniq keys", proc->keys);
     }
     tool_print("%" PRIu64" Total Positives", proc->positive);
     tool_print("%" PRIu64" Total Negatives", proc->negative);
     tool_print("---- anomaly detection");
     tool_print("%" PRIu64" True Positives", proc->tp);
     tool_print("%" PRIu64" False Negatives", proc->fn);
     tool_print("%" PRIu64" False Positives", proc->fp);
     tool_print("%" PRIu64" True Negatives", proc->tn);
     if (proc->errors) {
          tool_print("%" PRIu64" Parsing Errors", proc->errors);
     }

     //destroy tables
     if (proc->sharer_id == 0) {
          stringhash5_destroy(proc->key_table);
     }

     if (proc->twolevel) {
          if (proc->sharer_id5 == 0) {
          stringhash5_destroy(proc->outer_table);
          }
     }

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->sharelabel5) {
          free(proc->sharelabel5);
     }
     free(proc);

     return 1;
}

