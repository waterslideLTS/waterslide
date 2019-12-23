/*  

proc_tuple2json - module for waterslide

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
//#define DEBUG 1
#define PROC_NAME "tuple2json"
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <signal.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_binary.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "output", NULL };
char *proc_alias[]     = { "tojson", "outputjson", "tupletojson", "mkjson", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "serialize tuple data to a string buffer";
char proc_nonswitch_opts[] = "serialize tuple datato a string buffer, append to tuple";
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'1',"","(one)",
     "print only the first label found",0,0},
     {'2',"","",
     "print only the last label found",0,0},
     {'L',"","LABEL",
     "LABEL of output json buffer",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};


//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t out;
     uint64_t toolong;
     uint64_t writefail;
     uint64_t resize;

     int print_only_first_label;
     int print_only_last_label;


     wslabel_t * label_json;

     ws_outtype_t * outtype_tuple;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "12L:")) != EOF) {
          switch (op) {
          case '2':
               proc->print_only_last_label = 1;
               break;
          case '1':
               proc->print_only_first_label = 1;
               break;
          case 'L':
               proc->label_json = wsregister_label(type_table, optarg);
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

     proc->label_json = wsregister_label(type_table, "JSON");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
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
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }
     if (input_type == dtype_tuple) {
          return proc_tuple;
     }

     return NULL;
}

//return number of bytes copied
//fall through fail 
static int copy_check_fail(proc_instance_t * proc, char * outbuf, int outlen, 
                           int offset, const char * inbuf, int inlen, int * fail) {
     dprint("ccf %d %d", offset, *fail);
     if (!fail || *fail) {
          return 0;
     }
     if ((offset + inlen) > outlen) {
          *fail = 1;
          return 0;
     }
     memcpy(outbuf + offset, inbuf, inlen);

     return inlen;
}

//return current offset if success
//return 0 on fail -- overwite of buffer
static int print_json_label(proc_instance_t * proc,
                            char * outbuf, int outlen, int offset,
                            wsdata_t * member, int * fail) {
     dprint("print_json_label %d %d", offset, *fail);
     char * name = NULL;
     if (!fail || *fail) {
          return 0;
     }
     if (!member->label_len) {
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "\"NULL\":", 7, fail);
     }
     else if (proc->print_only_last_label || proc->print_only_first_label) {
          if (proc->print_only_last_label) {
               name = member->labels[member->label_len-1]->name;
          }
          else {
               name = member->labels[0]->name;
          }
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "\"", 1, fail);
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    name, strlen(name), fail);
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "\":", 2, fail);
     }
     else {
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "\"", 1, fail);
          int i;
          for (i = 0; i < member->label_len; i++) {
               name = member->labels[i]->name;
               offset += copy_check_fail(proc, outbuf, outlen, offset,
                                         name, strlen(name), fail);
               if (i>0) {
                    offset += copy_check_fail(proc, outbuf, outlen, offset,
                                              ":", 1, fail);
               }
          }
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "\":", 2, fail);
     }
     return offset;
}

//return current offset if success
//return 0 on fail -- overwite of buffer
static int print_json_string(proc_instance_t * proc, char * outbuf, int outlen,
                              int offset, char * str, int slen, int *fail) {
     dprint("print_json_string %d %d", offset, *fail);

     if (!fail || *fail) {
          return 0;
     }
     offset += copy_check_fail(proc, outbuf, outlen, offset,
                               "\"", 1, fail);
     int prior = 0;
     int i;
     char * jstr;
     for (i = 0; i < slen; i++) {
          jstr = NULL;
          switch(str[i]) {
          case '\"':
               jstr = "\\\"";
               break;
          case '\r':
               jstr = "\\r";
               break;
          case '\b':
               jstr = "\\b";
               break;
          case '/':
               jstr = "\\/";
               break;
          case '\f':
               jstr = "\\f";
               break;
          case '\n':
               jstr = "\\n";
               break;
          case '\t':
               jstr = "\\t";
               break;
          case '\\':
               jstr = "\\\\";
               break;
          }
          if (jstr) {
               if (prior < i) {
                    offset += copy_check_fail(proc, outbuf, outlen, offset,
                                              str + prior, i - prior, fail);
               }
               offset += copy_check_fail(proc, outbuf, outlen, offset,
                                         jstr, 2, fail);
               prior = i + 1;
          }
     }
     if (prior < slen) {
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    str + prior, slen - prior, fail);
     }
     offset += copy_check_fail(proc, outbuf, outlen, offset,
                               "\"", 1, fail);

     return offset;
}


static int print_json_tuple(proc_instance_t * proc, char * outbuf, int outlen,
                             int offset, wsdata_t * tdata, int * fail);

static int print_json_member(proc_instance_t * proc, char * outbuf, int outlen,
                              int offset, wsdata_t * member, int * fail) {
     dprint("print_json_member %d %d", offset, *fail);
     if (!fail || *fail) {
          return 0;
     }

     if (member->dtype == dtype_tuple) {
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                               "{", 1, fail);
          offset = print_json_tuple(proc, outbuf, outlen, offset, member, fail);
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                               "}", 1, fail);
          return offset;
     }
     else if (member->dtype == dtype_binary) {
          wsdt_binary_t * bin = (wsdt_binary_t*)member->data;
          int total = offset + 2 + (2* bin->len);
          if (total >= outlen) {
               *fail = 1;
               return 0;
          }
          outbuf[offset] = '\"';
          offset++;
          int b;
          for (b = 0; b < bin->len; b++) {
               if (snprintf(outbuf + offset, outlen - offset, "%02x",
                            (uint8_t)bin->buf[b]) != 2) {
                    *fail = 1;
                    return 0;
               }
               offset +=2;
          }
          outbuf[offset] = '\"';
          offset++;
          return offset;
     }
     else {
          char * buf = NULL;
          int len = 0;
          if (!dtype_string_buffer(member, &buf, &len) || (len == 0)) {

               offset += copy_check_fail(proc, outbuf, outlen, offset,
                               "\"\"", 2, fail);
               return offset;
          }

          return print_json_string(proc, outbuf, outlen, offset, buf, len, fail);
     }
}

//return offset if successful
//return 0 if fail
static int print_json_tuple(proc_instance_t * proc, char * outbuf, int outlen,
                             int offset, wsdata_t * tdata, int *fail) {
     dprint("print_json_tuple %d %d", offset, *fail);
     wsdt_tuple_t * tuple = (wsdt_tuple_t*)tdata->data;
     wsdata_t * member;
     int i;
     int out = 0;

     if (!fail || *fail) {
          return 0;
     }

     for (i = 0; i < tuple->len; i++) {
          member = tuple->member[i];
          if ((member->dtype!=dtype_tuple) && !member->dtype->to_string) {
               continue;
          }

          if (out) {
               offset += copy_check_fail(proc, outbuf, outlen, offset,
                                         ",", 1, fail);
          }

          offset = print_json_label(proc, outbuf, outlen, offset, member, fail);

          //check if list
          int listlen = 0;
          int nolist = 0;
          int j;
          for (j = (i+1); j < tuple->len; j++) {
               wsdata_t * nextmember = tuple->member[j];
               if ((nextmember->dtype!=dtype_tuple) && !nextmember->dtype->to_string) {
                    break;
               }
               if (member->label_len != nextmember->label_len) {
                    break;
               }
               int l;
               for (l = 0; l < member->label_len; l++) {
                    if (member->labels[l] != nextmember->labels[l]) {
                         nolist = 1;
                         break;
                    }
               }
               if (nolist) {
                    break;
               }
               listlen++;
          }
          
          if (listlen) {
               offset += copy_check_fail(proc, outbuf, outlen, offset,
                                         "[", 1, fail);
               
               for (j = 0; j <= listlen; j++) {
                    member = tuple->member[i+j];
                    if (j > 0) {
                         offset += copy_check_fail(proc, outbuf, outlen, offset,
                                                   ",", 1, fail);
                    }
                    offset = print_json_member(proc, outbuf, outlen, offset,
                                               member, fail);
               }
               offset += copy_check_fail(proc, outbuf, outlen, offset,
                                         "]", 1, fail);
               i += listlen;
          }
          else {
               offset = print_json_member(proc, outbuf, outlen, offset, member, fail);
          }
          out++;
     }
     return offset;
}


//main tuple process for writing strings
//return offset length
//return 0 on fail -- overwite of buffer
static int write_json(proc_instance_t * proc,
                      char * outbuf, int outlen,
                      wsdata_t * tuple) {
     dprint("write_json");
     int fail = 0;
     int offset = 0;

     if (tuple->label_len) {
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "{", 1, &fail);
          offset = print_json_label(proc, outbuf, outlen, offset, tuple, &fail);
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "{", 1, &fail);
          offset = print_json_tuple(proc, outbuf, outlen, offset, tuple, &fail);
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "}}", 2, &fail);
     }
     else {
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "{", 1, &fail);
          offset = print_json_tuple(proc, outbuf, outlen, offset, tuple, &fail);
          offset += copy_check_fail(proc, outbuf, outlen, offset,
                                    "}", 1, &fail);
     }
     dprint("write_json - finish %d %d", offset, fail);
     if (fail) {
          proc->writefail++;
          return 0;
     }
     else {
          return offset;
     }
}


//used to estimate buffer size of resulting json
static int est_json_label(proc_instance_t * proc, wsdata_t * member) {
     if (!member->label_len) {
          return 7;
     }
     else if (proc->print_only_last_label) {
          return strlen(member->labels[member->label_len - 1]->name) + 3;
     }
     else if (proc->print_only_first_label) {
          return strlen(member->labels[0]->name) + 3;
     }
     else {
          int est = 3;
          int i;
          for (i = 0; i < member->label_len; i++) {
               est += strlen(member->labels[i]->name);
               if (i > 0) {
                    est ++;;
               }
          }
          return est;
     }
}
static int est_json_tuple(proc_instance_t * proc, wsdata_t * tdata);

static int est_json_string(proc_instance_t * proc, char * str, int slen) {
     int est = 2 + slen;
     int i;
     for (i = 0; i < slen; i++) {
          switch(str[i]) {
          case '\"':
          case '\r':
          case '\b':
          case '/':
          case '\f':
          case '\n':
          case '\t':
          case '\\':
               est += 1;
          }
     }
     return est;
}

//used to estimate buffer size of resulting json
static int est_json_member(proc_instance_t * proc, wsdata_t * member) {
     if (member->dtype == dtype_tuple) {
          return 2 + est_json_tuple(proc, member);
     }
     else if (member->dtype == dtype_binary) {
          wsdt_binary_t * bin = (wsdt_binary_t*)member->data;
          return 2 + (2 * bin->len);
     }
     else {
          char * buf = NULL;
          int len = 0;
          if (!dtype_string_buffer(member, &buf, &len) || (len == 0)) {
               return 2;
          }
          else {
               return est_json_string(proc, buf, len);
          }
     }
}

//used to estimate buffer size of resulting json
static int est_json_tuple(proc_instance_t * proc, wsdata_t * tdata) {
     wsdt_tuple_t * tuple = (wsdt_tuple_t*)tdata->data;
     wsdata_t * member;
     int i;
     int out = 0;
     int est = 0;

     for (i = 0; i < tuple->len; i++) {
          member = tuple->member[i];
          if ((member->dtype!=dtype_tuple) && !member->dtype->to_string) {
               continue;
          }

          if (out) {
               est++;
          }

          est += est_json_label(proc, member);

          //check if list
          int listlen = 0;
          int nolist = 0;
          int j;
          for (j = (i+1); j < tuple->len; j++) {
               wsdata_t * nextmember = tuple->member[j];
               if ((nextmember->dtype!=dtype_tuple) && !nextmember->dtype->to_string) {
                    break;
               }
               if (member->label_len != nextmember->label_len) {
                    break;
               }
               int l;
               for (l = 0; l < member->label_len; l++) {
                    if (member->labels[l] != nextmember->labels[l]) {
                         nolist = 1;
                         break;
                    }
               }
               if (nolist) {
                    break;
               }
               listlen++;
          }
          
          if (listlen) {
               est +=2;
               
               for (j = 0; j <= listlen; j++) {
                    member = tuple->member[i+j];
                    if (j > 0) {
                         est++;
                    }
                    est += est_json_member(proc, member);
               }
               i += listlen;
          }
          else {
               est += est_json_member(proc, member);
          }
          out++;
     }
     return est;
}

//used to estimate buffer size of resulting json
static int estimate_json_buffer(proc_instance_t * proc, wsdata_t * tuple) {
     int est = 0;

     if (tuple->label_len) {
          est += 4;
          est += est_json_label(proc, tuple);
          est += est_json_tuple(proc, tuple);
     }
     else {
          est += 2;
          est += est_json_tuple(proc, tuple);
     }

     return est;
}

//// proc processing function assigned to a specific data type in proc_io_init
//
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     char * outbuf;
     int outlen;
     //start by allocating a reasonable size
     wsdata_t * wsb = wsdata_create_buffer(16376, &outbuf, &outlen);
     //wsdata_t * wsb = wsdata_create_buffer(35, &outbuf, &outlen);
     if (!wsb) {
          return 0;
     }
     wsdata_add_reference(wsb);


     //write to buffer == return 0 if fail
     int jlen = write_json(proc, outbuf, outlen, input_data);
     if (!jlen) {
          dprint("buffer not sized correctly, trying again");
          wsdata_delete(wsb);

          int resize = estimate_json_buffer(proc, input_data);
          dprint("estimate resize %d", resize);

          if (resize) {
               proc->resize++;
               wsb = wsdata_create_buffer(resize+1024, &outbuf, &outlen);
               wsdata_add_reference(wsb);
               jlen = write_json(proc, outbuf, outlen, input_data);
               if (!jlen) {
                    wsdata_delete(wsb);
                    dprint("buffer not sized correctly, failing");
                    proc->toolong++;
                    return 0;
               }
               dprint("final size %d", jlen);
          }
     }

     //create a string from buffer, -- attach to existing tuple
     wsdata_t * wsstr = wsdata_alloc(dtype_string);
     if (!wsstr) {
          wsdata_delete(wsb);
          return 0;
     }
     wsdata_add_label(wsstr, proc->label_json);
     wsdata_assign_dependency(wsb, wsstr);
     wsdt_string_t * str = (wsdt_string_t *) wsstr->data;
     str->buf = outbuf;
     str->len = jlen;

     //add buffer to tuple 
     add_tuple_member(input_data, wsstr);

     wsdata_delete(wsb); //clean up initial reference

     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->out++;
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt   %" PRIu64, proc->meta_process_cnt);
     tool_print("tuples out %" PRIu64, proc->out);
     if (proc->writefail) {
          tool_print("write fails %" PRIu64, proc->writefail);
          tool_print("resizes     %" PRIu64, proc->resize);
     }
     if (proc->toolong) {
          tool_print("tuples too big for serialization %" PRIu64, proc->toolong);
     }


     //free dynamic allocations
     free(proc);
     return 1;
}

