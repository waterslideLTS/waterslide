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

//Match strings
#define PROC_NAME "match"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include "waterslide.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "ahocorasick.h"
#include "sysutil.h"
#include "label_match.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "match", NULL };
char *proc_alias[]     = { "stringmatch", "aho", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "string matches using aho corasick";
char *proc_synopsis[] = {
     "match [-I] [-L <label>] [-F <file>] [-R <string>] <label of string member to match>",
     NULL };
char proc_description[] =
     "match uses the aho-corasick algorithm to find and label matching buffers."
     "  The matches can originate at any offset in the buffer.  "
     "The strings to match come from a file or individually using -R option "
     "(the -R option can be used more than once during kid specification to "
     "match multiple strings.)"
     "\n"
     "When using the dictionary file option (-F), formatting for the file "
     "should have the match string in quotes, whitespace, and then a "
     "label to apply in parentheses.  When using hex, these characters should "
     "be placed between pipe characters. A sample file is as follows: \n"
     "  \"astring\"          (ASTRING_LABEL)\n"
     "  \"bstring\"          (BSTRING_LABEL)\n"
     "  \"|00 00 00 00 00|\" (HEXSTRING_LABEL)\n";

proc_example_t proc_examples[] = {
     {NULL, NULL}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'I',"","",
     "case insensitive, must be done prior to match files or arguments",0,0},
     {'L',"","label",
     "label to add to matched value",0,0},
     {'U',"","label",
     "label to apply to unmatched values (not supported in NOT mode)",0,0},
     {'F',"","file",
     "file with items to search",0,0},
     {'R',"","string",
     "string to match",0,0},
     {'t',"","",
     "tag parent tuple of member match",0,0},
     {'B',"","label",
     "tag root/base tuple if any match found",0,0},
     {'S',"","label",
     "place match label in tuple string",0,0},
     {'1',"","",
     "[the number one] label only first match",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of string member to match";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"MATCH", NULL};
char proc_requires[]    = "";
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};

proc_port_t proc_input_ports[] = {
     {"none","pass if match"},
     {"TAG","pass all, tag tuple if match"},
     {"INVERSE","pass if no match"},
     {"NOT","pass if no match"},
     {NULL, NULL}
};

#define LOCAL_MAX_SH5_TABLE 3000000

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_meta_inverse(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr_inverse(void *, wsdata_t*, ws_doutput_t*, int);

#define LOCAL_MAX_TYPES 25

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t meta_flow_cnt;
     uint64_t hits;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_match;
     wslabel_t * label_unmatch;
     wslabel_t * label_base;
     label_match_t * lmatch;
     ahoc_t * ac_struct;
     int do_tag[LOCAL_MAX_TYPES];
     wslabel_nested_set_t nest;
     int tag_parent_tuple;

     wslabel_t * label_stringlabel;
     int match_only_one;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;
     int lid;

     while ((op = getopt(argc, argv, "1S:s:U:u:M:tIR:F:L:B:b:")) != EOF) {
          switch (op) {
          case '1':
               proc->match_only_one = 1;
               break;
          case 'S':
          case 's':
               proc->label_stringlabel = wsregister_label(type_table, optarg);
               break;
          case 'b':
          case 'B':
               proc->label_base = wsregister_label(type_table, optarg);
               break;
          case 'u':
          case 'U':
               proc->label_unmatch = wsregister_label(type_table, optarg);
               break;
          case 't':
               proc->tag_parent_tuple = 1;
               break;
          case 'I':
               proc->lmatch->ac_struct->case_insensitive = 1;
               break;
          case 'F':
               if (label_match_loadfile(proc->lmatch, optarg)) {
                    tool_print("reading file %s", optarg);
               }
               break;
          case 'L':
               proc->label_match = wsregister_label(type_table, optarg);
               tool_print("setting label to %s", optarg);
               break;
          case 'R':
               {
                    lid =
                         label_match_make_label(proc->lmatch,
                                                proc->label_match->name);
                    char *buf = strdup(optarg);
                    int len = strlen(buf);
                    sysutil_decode_hex_escapes(buf, &len);
                    ac_loadkeyword(proc->lmatch->ac_struct, buf, len, lid);
                    tool_print("added match string '%s'", optarg);
                    free(buf);
               }
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
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

     proc->label_match = wsregister_label(type_table, "MATCH");

     proc->lmatch = label_match_create(type_table);

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     proc->ac_struct = label_match_finalize(proc->lmatch);

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

     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }
     if (wslabel_match(type_table, port, "TAG")) {
          proc->do_tag[type_index] = 1;
     }

     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;  // not matching expected type
     }
     proc->outtype_tuple = ws_add_outtype(olist, input_type, NULL);

     if (wslabel_match(type_table, port, "INVERSE") ||
         wslabel_match(type_table, port, "NOT")) {
          if (!proc->nest.cnt) {
               return proc_process_allstr_inverse;
          }
          else {
               return proc_process_meta_inverse; // a function pointer
          }
     }
     else {
          if (!proc->nest.cnt) {
               return proc_process_allstr;
          }
          else {
               return proc_process_meta; // a function pointer
          }
     }
}

static inline void add_match_string_label(proc_instance_t * proc, wsdata_t * tuple,
                                          wslabel_t * label) {
     wslabel_t ** tlabel = (wslabel_t **) tuple_member_create(tuple, dtype_label,
                                                              proc->label_stringlabel);

     if (tlabel) {
          *tlabel = label;
     }
}

static inline int find_match(proc_instance_t * proc, wsdata_t * wsd, char * content,
                             int len, wsdata_t * tdata, wsdata_t * tparent) {
     if (len <= 0) {
          return 0;
     }
     ahoc_state_t ac_ptr = proc->ac_struct->root;
     int mval;
     u_char * buf = (u_char *)content;
     uint32_t buflen = (uint32_t)len;
     int matches = 0;

     while ((mval = ac_singlesearch(proc->ac_struct, &ac_ptr,
                                 buf, buflen, &buf, &buflen)) >= 0) {
          if (wsd) {
               wslabel_t * mlabel = label_match_get_label(proc->lmatch, mval);
               mlabel = mlabel ? mlabel : proc->label_match;
               if (proc->label_stringlabel) {
                    add_match_string_label(proc, tdata, mlabel);
               }
               else if (!wsdata_check_label(wsd, mlabel)) {
                   tuple_add_member_label(tdata, wsd, mlabel);
               }
               if (proc->tag_parent_tuple && !wsdata_check_label(tdata, mlabel)) {
                    if (tparent) {
                         tuple_add_member_label(tparent, tdata, mlabel);
                    }
                    else {
                         wsdata_add_label(tdata, mlabel);
                    }
               }
          }
          matches++;
          if (proc->match_only_one) {
               break;
          }
     }
     if (!matches && proc->label_unmatch) {
          if (proc->label_stringlabel) {
               add_match_string_label(proc, tdata, proc->label_unmatch);
          }
          tuple_add_member_label(tdata, wsd, proc->label_unmatch);
          if (proc->tag_parent_tuple && 
              !wsdata_check_label(tdata, proc->label_unmatch)) {
               if (tparent) {
                    tuple_add_member_label(tparent, tdata,
                                           proc->label_unmatch);
               }
               else {
                    wsdata_add_label(tdata, proc->label_unmatch);
               }
          }
     }
     return matches ? 1 : 0;
}

static inline int member_match(proc_instance_t *proc, wsdata_t *member,
                               wsdata_t * wsd_label, wsdata_t * tdata,
                               wsdata_t * tparent) {
     int found = 0;
     char * buf;
     int len;
     if (dtype_string_buffer(member, &buf, &len)) {
          found = find_match(proc, wsd_label, buf, len, tdata, tparent);
     }

     if (found) {
          proc->hits++;
     }

     return found;
}

static int proc_nest_match_callback(void * vinstance, void * ignore,
                              wsdata_t * tdata, wsdata_t * member,
                              wsdata_t * tparent) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     return member_match(proc, member, member, tdata, tparent);
}

static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int found = tuple_nested_search2(input_data, &proc->nest,
                                     proc_nest_match_callback,
                                     proc, NULL);

     if (proc->label_base && found && 
         !wsdata_check_label(input_data, proc->label_base)) {
          wsdata_add_label(input_data, proc->label_base);
     }
     if (found || proc->do_tag[type_index]) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

static int proc_nest_notmatch_callback(void * vinstance, void * ignore,
                              wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     return member_match(proc, member, NULL, NULL, NULL);
}

static int proc_process_meta_inverse(void * vinstance, wsdata_t* input_data,
                                     ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int found = tuple_nested_search(input_data, &proc->nest,
                                     proc_nest_notmatch_callback,
                                     proc, NULL);

     if (proc->label_base && !found && 
         !wsdata_check_label(input_data, proc->label_base)) {
          wsdata_add_label(input_data, proc->label_base);
     }
     if (!found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
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
     int found = 0;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          found += member_match(proc, member, member, input_data, NULL);
     }

     if (proc->label_base && found && 
         !wsdata_check_label(input_data, proc->label_base)) {
          wsdata_add_label(input_data, proc->label_base);
     }
     if (found || proc->do_tag[type_index]) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_allstr_inverse(void * vinstance, wsdata_t* input_data,
                                       ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wsdt_tuple_t * tuple = input_data->data;

     proc->meta_process_cnt++;

     int i;
     int tlen = tuple->len; //use this length because we are going to grow tuple
     wsdata_t * member;
     int found = 0;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          found += member_match(proc, member, NULL, NULL, NULL);
     }
     if (proc->label_base && !found && 
         !wsdata_check_label(input_data, proc->label_base)) {
          wsdata_add_label(input_data, proc->label_base);
     }
     if (!found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("meta_flow cnt %" PRIu64, proc->meta_flow_cnt);
     tool_print("matched tuples cnt %" PRIu64, proc->hits);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free label_match data and aho-corasick
     label_match_destroy(proc->lmatch);

     //free dynamic allocations
     free(proc);

     return 1;
}

