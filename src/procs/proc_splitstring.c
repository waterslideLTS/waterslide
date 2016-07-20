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
//make substrings off of named-labeled strings
#define PROC_NAME "splitstring"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include "waterslide.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_fixedstring.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "wstypes.h"
#include "ahocorasick.h"
#include "sysutil.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Stream Manipulation", "Matching", NULL}; 
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "split strings based on match";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'T',"","",
     "split into separate tuple",0,0},
     {'C',"","LABEL",
     "carry over member for separate tuples",0,0},
     {'R',"","string",
     "string to split on",0,0},
     {'N',"","cnt",
     "split the first N occurances",0,0},
     {'s',"","cnt",
     "skip the first N substrings",0,0},
     {'E',"","",
     "only emit last substring",0,0},
     {'L',"","LABEL",
     "label of new substring",0,0},
	{'F',"","", "force to create substrings as strings", 0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_description[]        = "Splits strings from labeled members based on a substring to search for."
				 " There are various options to split out multiple strings or skip the first "
				 " n occurrences of the substring.  You can choose to emit only the last or first "
				 "match and can carry over a member into the new tuple, optionally.";
char proc_nonswitch_opts[]     =  "LABEL of string to split";
char *proc_input_types[]       =  {"tuple", NULL}; 
char *proc_output_types[]      =  {"tuple", NULL}; 
char proc_requires[]           =  ""; 
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}}; 
char *proc_tuple_container_labels[] =  {NULL}; 
char *proc_tuple_conditional_container_labels[] =  {NULL}; 
char *proc_tuple_member_labels[] =  {"Defined by command line options", NULL}; 
char *proc_synopsis[]          =  {"splitstring <LABEL> -R <substring to split on> [-T | -C <LABEL>] [-N <cnt> | -s <cnt> | -E | -F] -L <NEWLABEL>"}; 
proc_example_t proc_examples[] =  {
{"... | splitstring -R CRNLCRNL -N 1 -s 1 DOCUMENT -L LINE | ...","Splits string in label DOCUMENT by the string '\\r\\n\\r\\n'; only splits the first occurrence of the string and then skips that occurrence emitting nothing in the label LINE. To emit results when using '-N' and -s' together the value given to '-N' for the number of occurrences to split must be larger than the value given to '-s' for the number of occurrences to skip."},
{"... | splitstring -R CRNLCRNL -N 2 -s 1 DOCUMENT -L LINE | ...","Splits string in label DOCUMENT by the string '\\r\\n\\r\\n'; split the first two occurrences of the string and skip the first occurrence. This will emit only the second occurrence of the string."},
{NULL, ""}
}; 


//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_splittuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_splittuple_allstr(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr(void *, wsdata_t*, ws_doutput_t*, int);

#define MAX_SUBSTRING_LABELS 20

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_set_t lset;
     ahoc_t * aho;
     int stringcnt;
     int occurances;
     int skip;
     wslabel_set_t lset_carry;
     ws_doutput_t * dout;
     wsdata_t * carry[WSDT_TUPLE_MAX];
     int splittuple;
     int onlylast;
	int treat_string;

     wslabel_t * label_substring[MAX_SUBSTRING_LABELS];
     int label_cnt;
} proc_instance_t;

#define DELIM ":,; "
static void set_substring_labels(proc_instance_t * proc, char * str, void * type_table) {
     char * ptok = NULL;

     int cnt = proc->label_cnt;
     char * rtok = strtok_r(str, DELIM, &ptok);
     while (rtok) {
          if (cnt >= MAX_SUBSTRING_LABELS) {
               return;
          }
          dprint("rtok %s", rtok);
          if (strlen(rtok)) {
               dprint("range %s", rtok);
               proc->label_substring[cnt] = wsregister_label(type_table, rtok);
               cnt++;
               tool_print("setting substring label %d to %s", cnt, rtok);
          }
          rtok = strtok_r(NULL, DELIM, &ptok);
     }
     proc->label_cnt = cnt;
}


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "ETFC:s:N:R:L:")) != EOF) {
          switch (op) {
          case 'E':
               proc->onlylast = 1;
               break;
          case 'T':
               proc->splittuple = 1;
               break;
          case 'C':
               wslabel_set_add(type_table, &proc->lset_carry, optarg);
               proc->splittuple = 1;
               break;
          case 's':
               proc->skip = atoi(optarg);
               tool_print("skipping the first %d substrings", proc->skip);
               break;
          case 'N':
               proc->occurances = atoi(optarg);
               tool_print("splitting on the first %d occurances", 
                          proc->occurances);
               break;
          case 'R':
               if (strcmp(optarg,"CRNLCRNL") == 0) {
                    ac_loadkeyword(proc->aho, "\r\n\r\n", 4, 4);
                    tool_print("splitting on string '\\r\\n\\r\\n' with length 4");
               }
               else if (strcmp(optarg,"CRNL") == 0) {
                    ac_loadkeyword(proc->aho, "\r\n", 2, 2);
                    tool_print("splitting on string '\\r\\n' with length 2");
               }
               else {
  		    char * buf = strdup(optarg);
		    int len = strlen(optarg);
		    sysutil_decode_hex_escapes(buf, &len);
                    ac_loadkeyword(proc->aho, buf, len, len);
                    free(buf);
               }
               proc->stringcnt++;
               break;
          case 'L':
               set_substring_labels(proc, optarg, type_table);
               break;
		case 'F':
			proc->treat_string = 1;
			break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          tool_print("searching for string with label %s", argv[optind]);
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

     proc->aho = ac_init();


     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->stringcnt) {
          ac_loadkeyword(proc->aho, "\r\n", 2, 2);
          proc->stringcnt++;
     }

     ac_finalize(proc->aho);

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

     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;  // not matching expected type
     }

     proc->outtype_tuple = ws_add_outtype(olist, input_type, NULL);

     if (!proc->lset.len) {
          if (proc->splittuple) {
               return proc_process_splittuple_allstr; // a function pointer
          }
          return proc_process_allstr;
     }

     // we are happy.. now set the processor function
     if (proc->splittuple) {
          return proc_process_splittuple; // a function pointer
     }
     return proc_process_meta; // a function pointer
}

// lookup user supplied label.. cnt from 1-N
static inline wslabel_t * get_substring_label(proc_instance_t * proc, int cnt) {
     if (!cnt) {
          return NULL;
     }
     if (cnt > proc->label_cnt) {
          if (!proc->label_cnt) {
               return NULL;
          }
          else {
               //return last label
               return proc->label_substring[proc->label_cnt - 1];
          } 
     }
     else {
          return proc->label_substring[cnt - 1];
     }
}

static inline int add_string(proc_instance_t * proc, wsdata_t * tdata, 
                             uint8_t * str, int len, int isbinary, wsdata_t *dep,
                             int cnt) {

     char * buf = (char *)str;
     if (proc->skip && (cnt <= proc->skip)) {
          return 0;
     }
     if (len <= 0) {
          return 0;
     }
     if (isbinary) {
          wsdt_binary_t * bin = tuple_member_create_wdep(tdata, dtype_binary,
                                                         get_substring_label(proc, cnt), dep);
          if (bin) {
               bin->buf = buf;
               bin->len = len;
          }
     }
     else {
          wsdt_string_t * str = tuple_member_create_wdep(tdata, dtype_string,
                                                         get_substring_label(proc, cnt), dep);
          if (str) {
               str->buf = buf;
               str->len = len;
          }
     }
     return 1;
}

static inline int add_substring(proc_instance_t * proc, wsdata_t * tdata, 
                                char * str, int len, int isbinary, wsdata_t *dep) {

     int cnt = 0;
     int mval;
     uint8_t * prev;
     uint32_t plen;
     uint8_t * buf = (uint8_t*)str;
     uint32_t buflen = len;
     ahoc_state_t ac_ptr = proc->aho->root;
     do {
          prev = buf;
          plen = buflen;
          mval = ac_singlesearch(proc->aho, &ac_ptr,
                                 buf, buflen, &buf, &buflen);

          dprint("here");
          if (mval < 0) {
               cnt++;
               add_string(proc, tdata, prev, plen, isbinary, dep, cnt);
               break;
          }
          else if (buflen == 0) {
               if ((plen - mval > 0)) {
                    dprint("here1 %.*s %d %d", plen - mval, prev, mval, buflen);
                    cnt++;
                    add_string(proc, tdata, prev, plen - mval, isbinary, dep, cnt);
               }
               break;
          }
          dprint("here2 %.*s", buflen, buf);

          if (!proc->onlylast) {
               int end_offset = buf - mval - prev;
               if (end_offset) {
                    cnt++;
                    add_string(proc, tdata, prev, end_offset, isbinary, dep, cnt);
               }
               //get next split offset
               if (proc->occurances && (len > 0) && (cnt >= proc->occurances)) {
                    //cnt++;
                    //add_string(proc, tdata, buf, buflen, isbinary, dep, cnt);
                    break;
               }
          }
     } while (buflen > 0);

     return 1;
}

static inline int add_string_tup(proc_instance_t * proc, wsdata_t * tdata, 
                                 uint8_t * str, int len, int isbinary, wsdata_t *dep,
                                 int cnt, int carrylen) {
     char * buf = (char *)str;
     if (proc->skip && (cnt <= proc->skip)) {
          return 0;
     }
     if (len <= 0) {
          return 0;
     }
     wsdata_t * newtup = ws_get_outdata(proc->outtype_tuple);
     if (!newtup) {
          return 0;
     }

     int i;

     for (i = 0; i < carrylen; i++) {
          add_tuple_member(newtup, proc->carry[i]);
     }
     
     if (isbinary) {
          wsdt_binary_t * bin =
               tuple_member_create_wdep(newtup, dtype_binary,
                                        get_substring_label(proc, cnt), dep);
          if (bin) {
               bin->buf = buf;
               bin->len = len;
          }
     }
     else {
          wsdt_string_t * str =
               tuple_member_create_wdep(newtup, dtype_string,
                                        get_substring_label(proc, cnt), dep);
          if (str) {
               str->buf = buf;
               str->len = len;
          }
     }
     ws_set_outdata(newtup, proc->outtype_tuple, proc->dout);
     proc->outcnt++;
     return 1;
}


static inline int add_substring_tup(proc_instance_t * proc, wsdata_t * tdata, 
                                    char * str, int len, int isbinary,
                                    wsdata_t *dep, int carrylen) {

     int cnt = 0;
     int mval;
     uint8_t * prev;
     uint32_t plen;
     uint8_t * buf = (uint8_t*)str;
     uint32_t buflen = len;
     ahoc_state_t ac_ptr = proc->aho->root;
     do {
          prev = buf;
          plen = buflen;
          mval = ac_singlesearch(proc->aho, &ac_ptr,
                                 buf, buflen, &buf, &buflen);
          dprint("here");
          if (mval < 0) {
               cnt++;
               add_string_tup(proc, tdata, prev, plen, isbinary, dep, cnt,
                              carrylen);
               break;
          }
          else if (buflen == 0) {
               if ((plen - mval > 0)) {
                    dprint("here1 %.*s %d %d", plen - mval, prev, mval, buflen);
                    cnt++;
                    add_string_tup(proc, tdata, prev, plen - mval, isbinary,
                                   dep, cnt, carrylen);
               }
               break;
          }
          dprint("here2 %.*s", buflen, buf);

          if (!proc->onlylast) {
               int end_offset = buf - mval - prev;
               if (end_offset) {
                    cnt++;
                    add_string_tup(proc, tdata, prev, end_offset, isbinary, dep, cnt,
                                   carrylen);
               }
               //get next split offset
               if (proc->occurances && (len > 0) && (cnt >= proc->occurances)) {
                    cnt++;
                    add_string_tup(proc, tdata, buf, buflen, isbinary, dep, cnt,
                                   carrylen);
                    break;
               }
          }
     } while (len > 0);

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int id;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     tuple_init_labelset_iter(&iter, input_data, &proc->lset);

     while (tuple_search_labelset(&iter, &member, &label, &id)) {
          char * buf;
          int blen;
          if (dtype_string_buffer(member, &buf, &blen)) {
               add_substring(proc, input_data, buf, blen, 
                             (proc->treat_string || member->dtype == dtype_string) ? 0 : 1, member);
          }
     }
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

static int proc_process_splittuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int id;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     tuple_init_labelset_iter(&iter, input_data, &proc->lset);
     proc->dout = dout;


     wsdata_t ** mset;
     int mset_len;

     int i;
     int j;
     int carrylen = 0;
     for (i = 0; i < proc->lset_carry.len; i++) {
          if (tuple_find_label(input_data, proc->lset_carry.labels[i],
                               &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    if (carrylen < WSDT_TUPLE_MAX) {
                         proc->carry[carrylen] = mset[j];
                         carrylen++;
                    }
               }
          }
     }

     while (tuple_search_labelset(&iter, &member, &label, &id)) {
          char * buf;
          int blen;
          if (dtype_string_buffer(member, &buf, &blen)) {
               add_substring_tup(proc, input_data, buf, blen, 
                                 (proc->treat_string || member->dtype == dtype_string) ? 0 : 1,
                                 member, carrylen);
          }
     }

     return 1;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_allstr(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wsdt_tuple_t * tuple = input_data->data;

     proc->meta_process_cnt++;

     int i;
     int tlen = tuple->len; //use this length because we are going to grow tuple
     wsdata_t * member;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          char * buf;
          int blen;
          if (dtype_string_buffer(member, &buf, &blen)) {
               add_substring(proc, input_data, buf, blen, 
                             (proc->treat_string || member->dtype == dtype_string) ? 0 : 1, member);
          }
     }
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

static int proc_process_splittuple_allstr(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     wsdata_t * member;
     proc->dout = dout;


     wsdata_t ** mset;
     int mset_len;

     int i;
     int j;
     int carrylen = 0;
     for (i = 0; i < proc->lset_carry.len; i++) {
          if (tuple_find_label(input_data, proc->lset_carry.labels[i],
                               &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    if (carrylen < WSDT_TUPLE_MAX) {
                         proc->carry[carrylen] = mset[j];
                         carrylen++;
                    }
               }
          }
     }
     wsdt_tuple_t * tuple = input_data->data;
     int tlen = tuple->len; //use this length because we are going to grow tuple

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          char * buf;
          int blen;
          if (dtype_string_buffer(member, &buf, &blen)) {
               add_substring_tup(proc, input_data, buf, blen, 
                                 member->dtype == dtype_string ? 0 : 1,
                                 member, carrylen);
          }
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free aho-corasick
     ac_free(proc->aho);

     //free dynamic allocations
     free(proc);

     return 1;
}


