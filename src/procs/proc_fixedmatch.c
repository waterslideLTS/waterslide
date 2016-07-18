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

#define PROC_NAME "fixedmatch"

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
#include "sysutil.h"
#include "fixed_match.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "match", NULL };
char *proc_alias[]     = { "fmatch", "fixedmatch_shared", "fmatch_shared", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "finds strings at fixed locations in character buffers";
char *proc_synopsis[] = {
     "fixedmatch [-J <SHT5 LABEL>] [-L <label>] [-F <file>] [-R <string>] [-T] [-Q] [-e] <label of string member to match>",
     NULL };
char proc_description[] = 
     "fixedmatch matches strings at fixed locations in character buffers. "
     "The strings to match come from a file or as one of the option arguments."
     "The fixed locations are the beginning of the buffer (default), the end of the "
     "buffer, or some offset within the buffer. For the input file, on each line "
     "include the string to search for (in quotes), white space, and then the "
     "label to apply (in parentheses). For example, \"astring\" (ASTRING_"
     "LABEL). For hex strings, use curley braces: {00 00 00 00 00} "
     " (HEXSTRING_LABEL).  When using hex strings, be sure to pay heed to "
     "endianness. Special criteria can be applied to each search string "
     "by placing the comma separated criteria between the search string "
     "and label. The valid criteria are offset (the offset into the "
     "buffer to match), atend (match at the end of the buffer), clen_eq "
     "(buffer length), clen_lt (max buffer length - 1), clen_gt (min buffer "
     "length + 1), nocase (case insensitive). For example: \"bstring\",atend "
     "(BSTRING_LABEL) and \"cstring\",offset=100 (CSTRING_LABEL).";
proc_example_t proc_examples[] = {
     {"... | fixedmatch -F 'first_words.wscfg' | print", 
     "read in data, use fixed match to detect the starts of words in a sentence, then print"},
     { NULL, "" }
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'R',"","string",
     "match fixed position string",0,0},
     {'E',"","string",
     "match fixed position string at end of input",0,0},
     {'F',"","file",
     "file with items to search",0,0},
     {'e',"","",
     "match end of string for cmd line matches -- ONLY APPLIES TO STRINGS AFTER INVOCATION;  use -E <string> for less ambiguous results",0,0},
     {'l',"","length",
     "only match strings less than or equal to length",0,0},
     {'L',"","label",
     "label to apply to next cmd line matches",0,0},
     {'B',"","num",
     "apply label to previous n members of tuple",0,0},
     {'A',"","num",
     "apply label to next n members of tuple",0,0},
     {'t',"","",
     "apply label to tuple container",0,0},
     {'U',"","label",
     "apply label to unmatched data",0,0},
     {'o',"","offset",
     "offset match for cmd line matches",0,0},
     {'s',"","",
     "output flow label event",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
proc_port_t proc_input_ports[] = {
     {"none","pass if match"},
     {"TAG","pass all, tag tuple if match"},
     {"NOT","pass if no match"},
     {"INVERSE","pass if no match"},
     {NULL, NULL}
};
char proc_nonswitch_opts[]    = "LABELs of string buffers to search";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"FMATCH", NULL};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char proc_requires[]    = "";

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_meta_inverse(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_meta_tag(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr_inverse(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr_tag(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _flow_data_t {
     wslabel_t * label;
} flow_data_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t meta_flow_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     int string_cnt;
     fixed_match_t * fmatch;
     int atend;
     int cmd_offset;
     int apply_label_prior;
     int apply_label_after;
     int apply_labels;
     int tuple_container_label;
     char * cmd_label;
     wslabel_t * label_unmatch;
     int maxlength;
     int output_flow_label;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "sl:U:tL:A:B:o:eR:E:F:")) != EOF) {
          switch (op) {
          case 's':
               proc->output_flow_label = 1;
               break;
          case 'l':
               proc->maxlength = atoi(optarg);
               break;
          case 'U':
               proc->label_unmatch = wsregister_label(type_table, optarg);
               break;
          case 't':
               proc->tuple_container_label = 1;
               break;
          case 'B':
               proc->apply_label_prior = atoi(optarg);
               break;
          case 'A':
               proc->apply_label_after = atoi(optarg);
               break;
          case 'e':
               tool_print("Note: -e only applies to -R strings after this point; use -E <string> for less ambiguous results");
               proc->atend = 1;
               break;
          case 'o':
               proc->cmd_offset = atoi(optarg);
               break;
          case 'F':
               fixed_match_loadfile(proc->fmatch, optarg);
               break;
          case 'L':
               //Note that -L can be invoked more than once, so free a previous
               //invocation if it involved strdup
               free(proc->cmd_label);
               proc->cmd_label = strdup(optarg);
               break;
          case 'R':
               {
               char * buf = strdup(optarg);
               int len = strlen(optarg);
               sysutil_decode_hex_escapes(buf, &len);
               fixed_match_load_single(proc->fmatch, buf, len,
                                       proc->cmd_offset,
                                       proc->cmd_label, proc->atend);
               free(buf);
               }
               break;
          case 'E':
               { // search for a string from the end
               char * buf = strdup(optarg);
               int len = strlen(optarg);
               sysutil_decode_hex_escapes(buf, &len);
               fixed_match_load_single(proc->fmatch, buf, len,
                                       proc->cmd_offset,
                                       proc->cmd_label, 1);
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

// dump an wslabel_t to a saved hash table
static inline uint32_t wslabel_t_dump(void * vdata, uint32_t max_records, 
                                      uint32_t data_alloc, FILE * fp) {
     uint32_t i, cnt = 0, len, bytes = 0;
     size_t rtn;
     uint8_t * data = (uint8_t *)vdata;

     //determine the number of wslabel_t items that will be dumped
     for (i = 0; i < max_records; i++) {
          flow_data_t * kd = (flow_data_t *)(data + (size_t)i * data_alloc);
          if (kd->label) {
               cnt++;
          }
     }
     rtn = fwrite(&cnt, sizeof(uint32_t), 1, fp);

     //dump the index, label, name length, and name for each table entry
     for (i = 0; i < max_records; i++) {
          flow_data_t * kd = (flow_data_t *)(data + (size_t)i * data_alloc);
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
          flow_data_t * kd = (flow_data_t *)(data + (size_t)j * data_alloc);
          kd->label = (wslabel_t *)calloc(sizeof(wslabel_t), 1);
          rtn = fread(kd->label, sizeof(wslabel_t), 1, fp);
          rtn += fread(&len, sizeof(int), 1, fp);
          kd->label->name = (char *)calloc(len, 1);
          rtn += fread(kd->label->name, len, 1, fp);
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

     proc->fmatch = fixed_match_create_default(type_table);
     proc->cmd_label = strdup("FMATCH");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     proc->apply_labels = proc->apply_label_prior + proc->apply_label_after;

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

     if (wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, input_type, NULL);
          }

          if (wslabel_match(type_table, port, "INVERSE") ||
              wslabel_match(type_table, port, "NOT")) {
               if (!proc->nest.cnt) {
                    return proc_process_allstr_inverse;
               }
               else {
                    return proc_process_meta_inverse; // a function pointer
               }
          }
          else if (wslabel_match(type_table, port, "TAG")) {
               if (!proc->nest.cnt) {
                    return proc_process_allstr_tag;
               }
               else {
                    return proc_process_meta_tag; // a function pointer
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
     return NULL;
}

static inline void local_apply_labels(wsdata_t * tdata, wsdata_t * ref,
                                      wslabel_t * label,
                                      int prior, int after) {
     dprint("here");
     wsdt_tuple_t * tuple = tdata->data;
     int i, j;
     for (i = 0; i < tuple->len; i++) {
          if (tuple->member[i] == ref) {
               dprint("found %d %d", i, prior);
               if (prior) {
                    if (prior > i) {
                         prior = i;
                    }
                    for (j = 1; j <= prior; j++) {
                         tuple_add_member_label(tdata, tuple->member[i-j], label);
                    }
               }
               if (after) {
                    if (tuple->len <= (i + after)) {
                         after = tuple->len - i - 1;
                    }
                    for (j = 1; j <= after; j++) {
                         tuple_add_member_label(tdata, tuple->member[i+j], label);
                    }
               }
               return;
          }
     } 
}

static inline wslabel_t * find_match(proc_instance_t * proc,
                                     wsdata_t * wsd, char * content,
                                     int len, wsdata_t * tdata,
                                     wsdata_t * tparent) {
     if (len <= 0) {
          return 0;
     }

     wslabel_t * label;

     if ((label = fixed_match_search(proc->fmatch,
                                     (uint8_t*)content, len)) != NULL) {
          if (wsd) {
               if (tdata) {
                    tuple_add_member_label(tdata, wsd, label);
               }
               else {
                    wsdata_add_label(wsd, label);
               }
               if (proc->apply_labels) {
                    local_apply_labels(tdata, wsd, label,
                                       proc->apply_label_prior,
                                       proc->apply_label_after);
               }
          }
          if (proc->tuple_container_label && tdata &&
              !wsdata_check_label(tdata, label)) {
               if (tparent) {
                    tuple_add_member_label(tparent, tdata, label);
               }
               else {
                    wsdata_add_label(tdata, label);
               }
          }
          return label;
     }
     else if (proc->label_unmatch) {
          if (wsd) {
               if (tdata) {
                    tuple_add_member_label(tdata, wsd, proc->label_unmatch);
               }
               else {
                    wsdata_add_label(wsd, proc->label_unmatch);
               }
          }
          if (proc->tuple_container_label && !wsdata_check_label(tdata,
                                                                 proc->label_unmatch)) {
               if (tparent) {
                    tuple_add_member_label(tparent, tdata, label);
               }
               else {
                    wsdata_add_label(tdata, proc->label_unmatch);
               }
          }
          return proc->label_unmatch;
     }
     return NULL;
}

static inline wslabel_t * member_match(proc_instance_t *proc, wsdata_t *member,
                                       wsdata_t * wsd_label, wsdata_t * tdata,
                                       wsdata_t * tparent) {
     char * buf;
     int len;
     if (dtype_string_buffer(member, &buf, &len)) {
          if (proc->maxlength && (len > proc->maxlength)) {
               return NULL;
          }
          return find_match(proc, wsd_label, buf, len, tdata, tparent);
     }

     return NULL;
}

static int proc_nest_match_callback(void * vinstance, void * ignore,
                                    wsdata_t * tdata, wsdata_t * member,
                                    wsdata_t * tparent) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     return (member_match(proc, member, member, tdata, tparent) != NULL);
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     int found = tuple_nested_search2(input_data, &proc->nest,
                                      proc_nest_match_callback,
                                      proc, NULL);

     if (found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

static int proc_nest_tag_callback(void * vinstance, void * vll,
                                    wsdata_t * tdata, wsdata_t * member,
                                    wsdata_t * tparent) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wslabel_t ** lastlabel = (wslabel_t**)vll;
     wslabel_t * rlabel;
     rlabel = member_match(proc, member, member, tdata, tparent);

     if (rlabel) {
          *lastlabel = rlabel;
          return 1;
     }
     else {
          return 0;
     }
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta_tag(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     wslabel_t * lastlabel = NULL;
     int found = tuple_nested_search2(input_data, &proc->nest,
                                     proc_nest_tag_callback,
                                     proc, &lastlabel);
     if (found && lastlabel && !wsdata_check_label(input_data, lastlabel)) {
          wsdata_add_label(input_data, lastlabel);
     }
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

static int proc_nest_inverse_callback(void * vinstance, void * ignore,
                                    wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     return (member_match(proc, member, NULL, NULL, NULL) != NULL);
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta_inverse(void * vinstance, wsdata_t* input_data,
                                     ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     int found = tuple_nested_search(input_data, &proc->nest,
                                     proc_nest_inverse_callback,
                                     proc, NULL);
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
          if (member_match(proc, member, member, input_data, NULL)) {
               found++;
          }
     }
     if (found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_allstr_tag(void * vinstance, wsdata_t* input_data,
                                   ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wsdt_tuple_t * tuple = input_data->data;

     proc->meta_process_cnt++;

     int i;
     int tlen = tuple->len; //use this length because we are going to grow tuple
     wsdata_t * member;
     int found = 0;
     wslabel_t * label;
     wslabel_t * lastlabel = NULL;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          if ((label = member_match(proc, member, member, input_data, NULL)) != NULL) {
               found++;
               lastlabel = label;
          }
     }
     if (found && lastlabel && !wsdata_check_label(input_data, lastlabel)) {
          wsdata_add_label(input_data, lastlabel);
     }
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

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
          if (member_match(proc, member, NULL, NULL, NULL)) {
               found++;
          }
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
     tool_print("output cnt %" PRIu64, proc->outcnt);

     fixed_match_delete(proc->fmatch);

     //free dynamic allocations
     free(proc->cmd_label);
     free(proc);

     return 1;
}

