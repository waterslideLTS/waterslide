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
//computes an variance... keep a representative tuple for each key..
#define PROC_NAME "keyvariance"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_int.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_double.h"
#include "stringhash5.h"
#include "procloader.h"
#include "variance.h"

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "varkey", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "computes variance of a value at keys - must be consistant datatype";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'K',"","label",
     "set key label",0,0},
     {'V',"","label",
     "set label of value to average",0,0},
     {'L',"","label",
     "set label of new accumulated value",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'F',"","",
     "emit mean and count information as well",0,0},
     {'W',"","window size",
      "the window size to keep track of",0,0},
     {'S',"","",
      "emit skewness information as well",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of key to average";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};

typedef struct _key_data_t {
     wsdata_t * wsd;
     variance var;
} key_data_t;


//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t add_member_cnt;
     uint64_t outcnt;

     stringhash5_t * key_table;
     uint32_t buflen;
     uint32_t full;
     wslabel_t * label_key;
     wslabel_t * label_value;
     ws_tuplemember_t * tmember_uint64;
     ws_tuplemember_t * tmember_dbl;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;
     wslabel_t * label_var;
     wslabel_t * label_avg;
     wslabel_t * label_cnt;
     wslabel_t * label_skew;
     uint32_t window_size;

     char * sharelabel;
} proc_instance_t;


static void last_destroy(void * vdata, void * vproc) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     key_data_t * kd = (key_data_t *)vdata;
     if (kd && kd->wsd) {
          dprint("output wsdata 2");

          wsdata_t * var_tm = tuple_member_alloc_wsdata(kd->wsd, proc->tmember_dbl);
          if (var_tm) {
               wsdt_double_t* var_data = var_tm->data;
               *var_data = var_getVariance(&kd->var);
               tuple_add_member_label(kd->wsd, var_tm, proc->label_var);

               if (proc->full) {
                    wsdata_t * avg_tm = tuple_member_alloc_wsdata(kd->wsd,
                                                        proc->tmember_dbl);
                    wsdata_t * cnt_tm = tuple_member_alloc_wsdata(kd->wsd,
                                                        proc->tmember_dbl);

                    if (avg_tm && cnt_tm) {
                         wsdt_double_t* avg_data = avg_tm->data;
                         wsdt_double_t* cnt_data = cnt_tm->data;

                         *cnt_data = kd->var.count;
                         *avg_data= var_getMean(&kd->var);

                         tuple_add_member_label(kd->wsd, avg_tm, proc->label_avg);
                         tuple_add_member_label(kd->wsd, cnt_tm, proc->label_cnt);
                    }
               }
	       if( proc->label_skew ) {
                    wsdata_t * skew_tm = tuple_member_alloc_wsdata(kd->wsd,
                                                        proc->tmember_dbl);
		    if( skew_tm ) {
			 wsdt_double_t* skew_data = skew_tm->data;
			 *skew_data = var_getSkewness(&kd->var);
                         tuple_add_member_label(kd->wsd, skew_tm, proc->label_skew);
		    }
		 
	       }


               ws_set_outdata(kd->wsd, proc->outtype_tuple, proc->dout);
               proc->outcnt++;
               wsdata_delete(kd->wsd);
          }
     }
}

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     proc->window_size=0;

     while ((op = getopt(argc, argv, "J:W:FSL:K:V:M:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'L':
               proc->label_var = wsregister_label(type_table, optarg);
               tool_print("result label %s", optarg);
               break;
          case 'K':
               proc->label_key = wssearch_label(type_table, optarg);
               tool_print("using key %s", optarg);
               break;
          case 'V':
               proc->label_value = wssearch_label(type_table, optarg);
               tool_print("using value %s", optarg);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          case 'F':
               proc->full=1;
               break;
	  case 'S':
	       proc->label_skew= wsregister_label(type_table, "SKEWNESS");
	       tool_print( "outputting skewness" );
	       break;
          case 'W':
               proc->window_size=atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          proc->label_key = wssearch_label(type_table, argv[optind]);
          tool_print("using key %s", argv[optind]);
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

     ws_default_statestore(&proc->buflen);

     proc->label_var = wsregister_label(type_table, "VARIANCE");
     proc->label_avg = wsregister_label(type_table, "AVERAGE");
     proc->label_cnt = wsregister_label(type_table, "CNT");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->label_key || !proc->label_value) {
          tool_print("must specify key and value labels");
          return 0;
     }
     proc->tmember_uint64 = register_tuple_member_type(type_table,
                                                       "UINT64_TYPE",
                                                       NULL);
     proc->tmember_dbl = register_tuple_member_type(type_table,
                                                    "DOUBLE_TYPE",
                                                    NULL);

     //other init - init the stringhash table
     if (proc->sharelabel) {
          stringhash5_sh_opts_t * sh5_sh_opts;
          int ret;

          //calloc shared sh5 option struct
          stringhash5_sh_opts_alloc(&sh5_sh_opts);

          //set shared sh5 option fields
          sh5_sh_opts->sh_callback = last_destroy;
          sh5_sh_opts->proc = proc; 

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->key_table, 
                                              proc->sharelabel, proc->buflen, 
                                              sizeof(key_data_t)+(sizeof(double)*proc->window_size), 
                                              NULL, sh5_sh_opts);

          //free shared sh5 option struct
          stringhash5_sh_opts_free(sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          proc->key_table = stringhash5_create(0, proc->buflen, sizeof(key_data_t)+
                                               (sizeof(double)*proc->window_size));
          if (!proc->key_table) {
               return 0;
          }
          stringhash5_set_callback(proc->key_table, last_destroy, proc);
     }

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->key_table->max_records;

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

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return proc_flush;
     }

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static inline void add_member(proc_instance_t * proc, wsdata_t * tdata,
                              wsdata_t * key, wsdata_t * value) {
     key_data_t * kdata = NULL;
     if (key->dtype == dtype_uint64) {
          wsdt_uint64_t * tag = key->data;
          kdata = (key_data_t *) stringhash5_find_attach(proc->key_table,
                                                         (uint8_t*)tag,
                                                         sizeof(wsdt_uint64_t));
     }
     else {
          ws_hashloc_t * hashloc = key->dtype->hash_func(key);
          if (hashloc && hashloc->len) {
               kdata = (key_data_t *) stringhash5_find_attach(proc->key_table,
                                                               (uint8_t*)hashloc->offset,
                                                               hashloc->len);
          }
     }
     if (kdata) {

          if (proc->window_size!=kdata->var.window_size) {
               kdata->var.previous_items=(double*)(kdata+1);
               kdata->var.window_size=proc->window_size;
          }
          
          if (!kdata->wsd) {
               kdata->wsd = tdata;
               wsdata_add_reference(tdata);
          }

          if (value->dtype == dtype_uint) { 
               wsdt_uint_t * p32 = value->data;
               var_hit(&kdata->var, *p32);
          }
          else if (value->dtype == dtype_int) {
               wsdt_int_t * p64 = value->data;
               var_hit(&kdata->var, *p64);
          }
          else if (value->dtype == dtype_uint64) {
               wsdt_uint64_t * p64 = value->data;
               var_hit(&kdata->var, *p64);
          }
          else if (value->dtype == dtype_double) {
               wsdt_double_t * pdbl = value->data;
               var_hit(&kdata->var, *pdbl);
          }
          stringhash5_unlock(proc->key_table);
     }
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;

     //search key
     wsdata_t * key = NULL;
     wsdata_t * value = NULL;

     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
     }
     if (tuple_find_label(input_data, proc->label_value,
                          &mset_len, &mset)) {
          value = mset[0];
     }

     if (key && value) {
          proc->add_member_cnt++;
          add_member(proc, input_data, key, value);
     }

     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                                              ws_doutput_t * dout, int
                                              type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     stringhash5_scour_and_flush(proc->key_table, last_destroy, proc);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("add_member cnt %" PRIu64, proc->add_member_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_destroy(proc->key_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     free(proc);

     return 1;
}

