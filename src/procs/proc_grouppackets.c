/*

proc_grouppackets.c -- group content carrying packets around the same session key

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

#define PROC_NAME "grouppackets"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
//#include <netinet/tcp.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "procloader.h"

//defines from netinet/tcp.h
# define TH_FIN     0x01
# define TH_SYN     0x02
# define TH_RST     0x04
# define TH_PUSH    0x08
# define TH_ACK     0x10
# define TH_URG     0x20

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Sessionization", "State tracking", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "group packets of the same TCP flow and direction";
char proc_description[] = "attempts to partially sessionize TCP packets";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'f',"","",
     "keep first n elements at key",0,0},
     {'c',"","",
     "emit count of stored elements",0,0},
     {'n',"","count",
     "number of each member to store at key",0,0},
     {'N',"","count",
     "emit only when count reaches limit",0,0},
     {'V',"","label",
     "label of member to store at key (can be multiple)",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL of key to index on";
char *proc_input_types[]       =  {"any","tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {
     {"none","Store value at key"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {"KEEPCOUNT", NULL};
char *proc_synopsis[]          =  {"grouppackets <LABEL>", NULL};
proc_example_t proc_examples[] =  {
	{NULL, NULL}
};

typedef struct _key_data_t {
     uint32_t nextseq;
     uint32_t ack;  //should be fixed
     uint32_t contentcnt;
     uint32_t contentlen;  //sum length of stored content buffers
     wsdata_t * reference;  //first tuple member of this set
     wsdata_t * content[0]; //content buffers to glue together
} key_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * session_table;
     uint32_t buflen;
     wslabel_t * label_flow;
     wslabel_t * label_seq;
     wslabel_t * label_ack;
     wslabel_t * label_content;
     wslabel_t * label_session;
     wslabel_t * label_packetcount;
     wslabel_t * label_flags;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;
     int cnt;
     size_t key_struct_size;

     char * sharelabel;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:n:M:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'n':
               proc->cnt = atoi(optarg);
               tool_print("storing %d objects at key", proc->cnt);
               if(proc->cnt <= 0) {
                    error_print("invalid count of objects ... specified %d", proc->cnt);
                    return 0;
               }
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          proc->label_flow = wssearch_label(type_table, argv[optind]);
          tool_print("using key %s", argv[optind]);
          optind++;
     }
     
     return 1;
}


static void emit_state(void * vdata, void * vproc) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     key_data_t * kdata = (key_data_t *)vdata;
     uint32_t i;
     dprint("emit state");

     if (!kdata->reference) {
          return;
     }

     if (kdata->contentcnt == 1) {
          dprint("single content");
          tuple_member_add_ptr(kdata->reference, kdata->content[0],
                               proc->label_session);

          tuple_member_create_uint(kdata->reference, 1, proc->label_packetcount);
          ws_set_outdata(kdata->reference, proc->outtype_tuple,
                         proc->dout);
          wsdata_delete(kdata->reference);
     }
     else {
          dprint("merge content");
          size_t offset = 0;
          wsdt_binary_t * bin = 
               tuple_create_binary(kdata->reference, proc->label_session,
                                   kdata->contentlen);
          if (!bin) {
               wsdata_delete(kdata->reference);
          }
          else {
               char * buffer;
               int len;
               for (i = 0; i < kdata->contentcnt; i++) {
                    if (dtype_string_buffer(kdata->content[i],&buffer, &len) && 
                        ((offset + len) <= bin->len)) {
                         memcpy(bin->buf + offset, buffer, len);
                         offset += len;
                    }
               }
               if (offset != bin->len) {
                    dprint("unexpected output length %d %d", (int)offset,
                           bin->len);
               }

               tuple_member_create_uint(kdata->reference, kdata->contentcnt, proc->label_packetcount);
               //output
               ws_set_outdata(kdata->reference, proc->outtype_tuple,
                              proc->dout);
               wsdata_delete(kdata->reference);
          }
     }

     dprint("clean up key_data");
     //flush all stored content
     for (i = 0; i < kdata->contentcnt; i++) {
          wsdata_delete(kdata->content[i]);
     }

     memset(kdata, 0, proc->key_struct_size);
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

     proc->cnt = 10;
     proc->label_session = wsregister_label(type_table, "SESSION");
     proc->label_packetcount = wsregister_label(type_table, "PACKETCOUNT");

     proc->label_flow = wssearch_label(type_table, "BIFLOW");
     proc->label_seq = wssearch_label(type_table, "TCPSEQ");
     proc->label_ack = wssearch_label(type_table, "TCPACK");
     proc->label_content = wssearch_label(type_table, "CONTENT");
     proc->label_flags = wssearch_label(type_table, "FLAGS");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (proc->cnt <= 1) {
          tool_print("invalid session count");
          return 0;
     }

     proc->key_struct_size = sizeof(key_data_t) + sizeof(wsdata_t*) * proc->cnt;

     //other init - init the stringhash table
     if (proc->sharelabel) {
          // Note:  the following seems backwards.  But, if hitemit is set, we only
          // want full sets of elements, so rec_erase is used to throw away all of 
          // the partials at expire time.  But, if hitemit is not set, then we are
          // expecting the partials, hence the use of emit_state here.
          stringhash5_sh_opts_t * sh5_sh_opts;
          int ret;

          //calloc shared sh5 option struct
          stringhash5_sh_opts_alloc(&sh5_sh_opts);

          //set shared sh5 option fields
          sh5_sh_opts->sh_callback = emit_state;
          sh5_sh_opts->proc = proc; 

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->session_table, 
                                              proc->sharelabel, proc->buflen, 
                                              proc->key_struct_size, 
                                              NULL, sh5_sh_opts);

          //free shared sh5 option struct
          stringhash5_sh_opts_free(sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          proc->session_table = stringhash5_create(0, proc->buflen,
                                                   proc->key_struct_size);
          if (!proc->session_table) {
               return 0;
          }
          stringhash5_set_callback(proc->session_table, emit_state, proc);
     }

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->session_table->max_records;

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
          proc->outtype_tuple = ws_add_outtype(olist, wsdatatype_get(type_table,
                                                                     "TUPLE_TYPE"), NULL);
          return proc_flush;
     }
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static void check_for_flush(proc_instance_t * proc, key_data_t * kdata,
                            uint32_t seqnum, uint32_t acknum, int contentlen) {

     dprint("check for flush");
     if ((kdata->ack == acknum) && (kdata->nextseq == seqnum)) {
          dprint("seq number match");
          return;
     }
     if (contentlen) {
          dprint("added content emit %u %u %u %u", seqnum, kdata->nextseq,
                     acknum, kdata->ack);
          emit_state(kdata, proc);
     }
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     dprint("proc_tuple");
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;

     //get FLOW KEY
     wsdata_t ** mset;
     int mset_len;
     dprint("find key");
     if (!tuple_find_label(input_data, proc->label_flow,
                          &mset_len, &mset)) {
          return 0;
     }
     wsdata_t * key = mset[0];

     //get SEQ number
     dprint("find seq");
     if (!tuple_find_label(input_data, proc->label_seq,
                          &mset_len, &mset)) {
          return 0;
     }
     wsdata_t * seq = mset[0];
     uint32_t seqnum = 0;
     if (!seq->dtype->to_uint32 || !seq->dtype->to_uint32(seq, &seqnum)) {
          return 0;
     }

     //get ACK number
     dprint("find ack");
     if (!tuple_find_label(input_data, proc->label_ack,
                          &mset_len, &mset)) {
          return 0;
     }
     wsdata_t * ack = mset[0];
     uint32_t acknum;
     if (!ack->dtype->to_uint32 || !ack->dtype->to_uint32(ack, &acknum)) {
          return 0;
     }

     dprint("find flags");
     if (!tuple_find_label(input_data, proc->label_flags,
                          &mset_len, &mset)) {
          return 0;
     }
     wsdata_t * flags = mset[0];
     uint32_t flagval;
     if (!flags->dtype->to_uint32 || !flags->dtype->to_uint32(flags, &flagval)) {
          return 0;
     }

     key_data_t * kdata = 
          (key_data_t *) stringhash5_find_attach_wsdata(proc->session_table, key);

     if (!kdata) {
          return 0;
     }


     int contentlen = 0;
     wsdata_t * content = NULL;
     dprint("find content");
     if (tuple_find_label(input_data, proc->label_content,
                          &mset_len, &mset)) {
          content = mset[0];
          char * buffer = NULL;
          dtype_string_buffer(content, &buffer, &contentlen);
     }

     dprint("we have seq %u %u, content len %u, nextseq %u", seqnum, acknum,
                contentlen, seqnum + (uint32_t)contentlen);
     check_for_flush(proc, kdata, seqnum, acknum, contentlen); 
     
     if (!contentlen) {
          dprint("no content");
          if ((flagval & (TH_FIN || TH_SYN)) != 0) {
               dprint("FYN emit %u %u", seqnum, acknum);
               emit_state(kdata, proc);
          }
          return 0;
     } 


     dprint("adding content to keydata");
     if (!kdata->reference) {
          dprint("new reference");
          kdata->reference = input_data;
          wsdata_add_reference(input_data);
     }
     kdata->nextseq = seqnum + contentlen;
     dprint("looking for seq %u %u", kdata->nextseq, acknum);
     dprint("adding nextseq = %u", kdata->nextseq);
     kdata->ack = acknum;
     kdata->content[kdata->contentcnt] = content;
     wsdata_add_reference(content);
     kdata->contentlen += contentlen;
     kdata->contentcnt++;
     dprint("content added. cnt %u, len %u", kdata->contentcnt,
            kdata->contentlen);

     if ((kdata->contentcnt >= proc->cnt) ||
         ((flagval & TH_FIN) != 0)) {
          emit_state(kdata, proc);
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     // Note:  the following seems backwards.  But, if hitemit is set, we only
     // want full sets of elements, so rec_erase is used to throw away all of 
     // the partials at flush time.  But, if hitemit is not set, then we are
     // expecting the partials, hence the use of emit_state here.
     stringhash5_scour_and_flush(proc->session_table, emit_state, proc);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_destroy(proc->session_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     free(proc);

     return 1;
}

