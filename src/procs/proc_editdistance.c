/*
Copyright 2019 Morgan Stanley
  
THIS SOFTWARE IS CONTRIBUTED SUBJECT TO THE TERMS OF YOU MAY OBTAIN A COPY OF
THE LICENSE AT https://www.apache.org/licenses/LICENSE-2.0.

THIS SOFTWARE IS LICENSED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE AND ANY
WARRANTY OF NON-INFRINGEMENT, ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE. THIS SOFTWARE MAY BE REDISTRIBUTED TO OTHERS ONLY BY
EFFECTIVELY USING THIS OR ANOTHER EQUIVALENT DISCLAIMER IN ADDITION TO ANY
OTHER REQUIRED LICENSE TERMS
*/
#define PROC_NAME "editdistance"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "stringhash5.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.5";
char *proc_tags[]	= {"annotation", NULL};
char *proc_menus[]     = { "Annotate", NULL };
char *proc_alias[]     = { "levenshtein", "editdist", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "compair strings and measure edit distance";
char proc_description[] = "Utilize the levenshtein algorithm to measure edit distance between two strings";
char proc_requires[] = "none";


proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'S',"","string",
     "reference string to compair",0,0},
     {'K',"","LABEL",
     "compair values at a given key",0,0},
     {'L',"","LABEL prefix",
     "apply a label prefix to computation labels",0,0},
     {'B',"","size",
     "maximum buffer size for edit distance (2047 default)",0,0},
     {'M',"","records",
     "maximum keystate table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of buffer to compair";
char *proc_input_types[]    = {"any", NULL};
char *proc_output_types[]    = {"any", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};

char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};


typedef struct _editdistance_t {
     uint32_t *v0;
     uint32_t *v1;
     uint32_t maxbuf;
} editdistance_t;

//allocated memory for doing edit distance computations
static editdistance_t * editdistance_init(uint32_t maxbuf) {
     if ((maxbuf < 4) | (maxbuf > 0x8FFFFF00)) {
          return NULL;
     }
     //precompute tables
     editdistance_t * ed = (editdistance_t*)calloc(1, sizeof(editdistance_t));
     if (!ed) {
          return NULL;
     }
     ed->maxbuf = maxbuf;
     maxbuf +=1;
     ed->v0 = (uint32_t *)calloc(1, sizeof(uint32_t) * maxbuf);
     if (!ed->v0) {
          free(ed);
          return NULL;
     }
     ed->v1 = (uint32_t *)calloc(1, sizeof(uint32_t) * maxbuf);
     if (!ed->v1) {
          free(ed->v0);
          free(ed);
          return NULL;
     }

     return ed;
}

//compute levenshtein distance using two-matrix rows method
static uint32_t editdistance_compair(editdistance_t * ed,
                                 uint8_t * buf0, uint32_t len0,
                                 uint8_t * buf1, uint32_t len1 ) {

     uint32_t * v0 = ed->v0;
     uint32_t * v1 = ed->v1;

     if ((len1+1) > ed->maxbuf) {
          len1 = ed->maxbuf;
     }

     uint32_t i;
     uint32_t j;
     //inialize base matrix score
     for (i = 0; i <= len1; i++) {
          v0[i] = i;
     }

     for (i = 0; i < len0; i++) {
          v1[0] = i + 1;

          for (j = 0; j < len1; j++) {
               uint32_t scost;
               uint32_t dcost = v0[j + 1] + 1;
               uint32_t icost = v1[j] + 1;
               if (buf0[i] == buf1[j]) {
                    scost = v0[j];
               }
               else {
                    scost = v0[j] + 1;
               }

               //get minimum of (dcost, icost and scost)
               uint32_t mcost = dcost;
               if (mcost > icost) {
                    mcost = icost;
               }
               if (mcost > scost) {
                    mcost = scost;
               }
               
               v1[j + 1] = mcost; 
          }
          //swap v0 and v1
          uint32_t * tmp = v0;
          v0 = v1;
          v1 = tmp;
     }
     return v0[len1];
}

//free up allocated memory for doing edit distance computations
static void editdistance_destroy(editdistance_t * ed) {
     if (ed) {
          if (ed->v0) {
               free(ed->v0);
          }
          if (ed->v1) {
               free(ed->v1);
          }
          free(ed);
     }
}

typedef struct _key_data_t {
     wsdata_t * prior;
} key_data_t;

static void evict_state(void * vdata, void * vproc) {
     key_data_t * kdata = (key_data_t *)vdata;
     if (kdata->prior) {
          wsdata_delete(kdata->prior);
     }
}

//function prototypes for local functions
static int proc_process_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;

     wslabel_nested_set_t keys;
     int haskey;
     uint32_t buflen;
     stringhash5_t * keytable;

     wslabel_nested_set_t nest;

     wslabel_t * label_edist;
     wslabel_t * label_enorm;

     uint8_t * reference;
     uint32_t reference_len;
     editdistance_t * edist;


     uint32_t maxbuf;  // for edit distance
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "B:M:k:K:s:S:L:")) != EOF) {
          switch (op) {
          case 'B':
               proc->maxbuf = atoi(optarg);
               break;
          case 'k':
          case 'K':
               wslabel_nested_search_build(type_table, &proc->keys, optarg);
               proc->haskey = 1;
               break;
          case 's':
          case 'S':
               proc->reference_len = strlen(optarg);
               proc->reference = (uint8_t *)strdup(optarg);
               break;
          case 'L':
               proc->label_edist = 
                    wsregister_label(type_table, optarg);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
          tool_print("scanning element %s", argv[optind]);
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

     proc->maxbuf = 2047;


     ws_default_statestore(&proc->buflen);


     proc->label_edist = wsregister_label(type_table, "EDITDISTANCE");
     proc->label_enorm = wsregister_label(type_table, "EDITNORM");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     proc->edist = editdistance_init(proc->maxbuf);
     if (!proc->edist) {
          tool_print("unable to allocate edit distance buffer %d", proc->maxbuf);
          return 0;
     }

     if (proc->haskey) {
          if (!proc->keys.cnt) {
               error_print("need to specify label for key to seach for");
               return 0;
          }
          proc->keytable = stringhash5_create(0, proc->buflen,
                                              sizeof(key_data_t));
          if (!proc->keytable) {
               return 0;
          }
          stringhash5_set_callback(proc->keytable, evict_state, proc);
     }
     else if (!proc->reference || !proc->reference_len) {
          tool_print("must have a reference string");
          return 0;
     }
    
     if (!proc->nest.cnt) {
          error_print("need to specify label to seach for");
          return 0;
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

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          return proc_process_tuple;
     }
     if (proc->keytable && wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return proc_flush;
     }

     return NULL;
}

//callback when searching nested tuple
static int proc_nest_match_callback(void * vproc, void * vignore,
                                    wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     char * buf;
     int blen;

     if (dtype_string_buffer(member, &buf, &blen)) {
          uint32_t distance = editdistance_compair(proc->edist,
                                                   proc->reference,
                                                   proc->reference_len,
                                                   (uint8_t*)buf, blen);
          tuple_member_create_uint(tdata, distance,
                                     proc->label_edist);
          int max = (blen > proc->reference_len) ? blen : proc->reference_len;
          if (max) {
               tuple_member_create_double(tdata, (double)distance/(double)max,
                                          proc->label_enorm);
          }
     }
     else {
          return 0;
     }

     return 1;
}




//only select first key found as key to use
static int proc_nest_key_callback(void * vproc, void * vkdata,
                                  wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t ** pkdata = (key_data_t**)vkdata;
     key_data_t * kdata = *pkdata;

     if (kdata) {
          return 0;
     }
     *pkdata = (key_data_t*)stringhash5_find_attach_wsdata(proc->keytable, member);
     
     return 0;
}


static int proc_nest_key_value_callback(void * vproc, void * vkdata,
                                  wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t*)vkdata;
     char * buf;
     int blen = 0;

     if (!dtype_string_buffer(member, &buf, &blen) || (blen == 0)) {
          return 0;
     }

     if (kdata->prior) {
          char * ref;
          int rlen = 0;
          
          if (dtype_string_buffer(kdata->prior, &ref, &rlen)) {
               uint32_t distance = editdistance_compair(proc->edist,
                                                        (uint8_t*)ref,
                                                        rlen,
                                                        (uint8_t*)buf, blen);
               tuple_member_create_uint(tdata, distance,
                                        proc->label_edist);

               int max = (blen > rlen) ? blen : rlen;
               if (max) {
                    tuple_member_create_double(tdata, (double)distance/(double)max,
                                               proc->label_enorm);
               }

          }
          wsdata_delete(kdata->prior);
     }


     kdata->prior = member;
     wsdata_add_reference(member);

     return 1;

}

static int proc_process_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (proc->haskey) {
          key_data_t * kdata = NULL;
          tuple_nested_search(input_data, &proc->keys,
                              proc_nest_key_callback,
                              proc, &kdata);
          if (kdata) {
               tuple_nested_search(input_data, &proc->nest,
                                   proc_nest_key_value_callback,
                                   proc, kdata);
          }
     }
     else {
          //search for specified tuple member
          tuple_nested_search(input_data, &proc->nest,
                              proc_nest_match_callback,
                              proc, NULL);
     }


     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}


static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (proc->keytable) {
          stringhash5_scour_and_flush(proc->keytable, evict_state, proc);
     }

     return 1;
}


//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     editdistance_destroy(proc->edist);
     if (proc->keytable) {
          stringhash5_destroy(proc->keytable);
     }

     //free dynamic allocations
     free(proc);

     return 1;
}

