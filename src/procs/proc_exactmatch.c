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

#define PROC_NAME "exactmatch"
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
#include "stringhash9a.h"
#include "procloader.h"
#include "sysutil.h"

char proc_name[]    = PROC_NAME;
char *proc_tags[]   = { "Matching", NULL };
char proc_purpose[] = "Matches strings for exact content payloads."; // Fix this wording
char *proc_synopsis[]    = {"exactmatch <LABEL> ... -R <string> [-F <filename>][-L <LABEL>][-U <LABEL>]", NULL};
char proc_description[]  = "The exactmatch module finds buffers that exactly "
     "match the specified reference string(s) (-R). Multiple reference strings "
     "can be provided via a text file and the -F option. If the user does not "
     "indicate a buffer <LABEL> with the default port, all buffers in the tuple "
     "will be searched; all other ports require a buffer to be specified by the "
     "user. The EXACTMATCH label is applied to matching buffers; the user can "
     "designate a different label with the -L option. Additionally, with the -U "
     "option and the TAG port, the user can specify a label to be applied to "
     "buffers that contain data that does not match the reference string. Labels "
     "cannot be applied using the NOT or INVERSE ports.";

proc_example_t proc_examples[]     = {
     {"... | exactmatch WORD -R 'she' -L NOUN | ...", "Passes only "
          "tuples with the specified reference string in the WORD buffer; "
               "labels as NOUN."},
     {"... | TAG:exactmatch WORD -R 'kitty' -R 'cat' -U NOCATS | ...", "Finds "
          "all tuples with one of the specified reference strings in the "
               "WORD buffer; passes all tuples, and labels those that match "
               "as EXACTMATCH and those that don't match as NOCATS."},
     {"... | NOT:exactmatch PATH -F common_paths.exactmatch | ...", "Passes only tuples "
          "without one of the reference strings specified in 'common_paths.exactmatch' "
               "in the PATH buffer."},
     {NULL, NULL}
};

char *proc_alias[]  = {"ematch", NULL};
char proc_version[] = "1.5"; // updated documentation 03 Feb 2014
char proc_requires[]     = "";
char *proc_input_types[] = {"tuple", NULL};
char *proc_output_types[]     = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {"none","pass if match"},
     {"TAG","pass all, tag tuple if match"},
     {"NOT","pass if no match"}, // what's the difference between NOT and INVERSE?
     {"INVERSE","pass if no match"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};
char *proc_tuple_member_labels[] = {"EXACTMATCH", NULL};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'F',"","filename", 
     "file with reference strings",0,0},
     {'L',"","LABEL-M",
     "label to apply to matched data ",0,0},
     {'R',"","string",
     "string to exactly match",0,0},
     {'U',"","LABEL-U",
     "label to apply to unmatched data",0,0}, // needs to be used with a port?
     {'M',"","records",
     "maximum table size",0,0},
     {'O',"","filename",
     "write out exactmatch table to file",0,0},
     {'E',"","filename",
     "load in exactmatch table from file",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABELs of string buffers to search";

#define LOCAL_MAX_SH9A_TABLE 20000

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_meta_inverse(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_meta_tag(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr_inverse(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr_tag(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     stringhash9a_t * exactmatch_table;
     uint32_t max_table;
     wslabel_t * label_unmatch;
     wslabel_t * label_match;
     int items;
     char * dump_file;
     char * open_table;

     char * sharelabel;
     int sharer_id;
} proc_instance_t;


#define LOCAL_INPUT_BUF 1024
static inline void exact_match_loadfile(proc_instance_t * proc, char * filename) {
     
     FILE* fp;
     char line[LOCAL_INPUT_BUF+1];
     int len;
     char* linep;
     
     // try to open the file
     if ((fp = sysutil_config_fopen(filename, "r")) == NULL) {
          perror("Match file not found");
          return;
     }
     dprint("opened the match file: %s", filename);
     // read all the junk at the beginning of the file
     while (fgets(line, LOCAL_INPUT_BUF, fp)) { // returns <1024 + '/0'
          len = strlen(line); // no null terminator
          // strip return
          if (line[len - 1] == '\n') {
               line[len - 1] = '\0';
               len--;
          }
          if (line[len - 1] == '\r') {
               line[len - 1] = '\0';
               len--;
          }
          // ignore empty lines and comments
          if ((len <= 0) || (line[0] == '#')) {
               continue;
          }

          linep = line; // includes null terminator
          dprint("Got this stripped line: %s", linep);

          if (len > 0) {
               sysutil_decode_hex_escapes(linep, &len);
               if ((len > 0) && !stringhash9a_set(proc->exactmatch_table, linep, len)) {
                    proc->items++;
               }
          }
     }
     sysutil_config_fclose(fp);
}

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     // Note: unusual handling of hash table creation in this kid.
     // If -F and/or -R options are used, then the exactmatch table
     // must be created here.  If -the -M option is invoked first,
     // the the table size can be reset before table creation, otherwise
     // the max_table set by -M will be bypassed.
     //
     // In addition, any -J, -E and -O options must all be read before
     // -F or -R in order to get correct behavior.
     while ((op = getopt(argc, argv, "J:U:L:R:F:M:O:E:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'U': // register label for unmatched string
               proc->label_unmatch = wsregister_label(type_table, optarg);
               break;
          case 'F': // read user-provided match file
               if (!proc->exactmatch_table) {
                    stringhash9a_sh_opts_t * sh9a_sh_opts;

                    //calloc shared sh9a option struct
                    stringhash9a_sh_opts_alloc(&sh9a_sh_opts);

                    //set shared sh9a option fields
                    sh9a_sh_opts->open_table = proc->open_table;

                    //proc->exactmatch_table = stringhash9a_create(0, proc->max_table);
                    if (proc->sharelabel) {
                         if (!stringhash9a_create_shared_sht(type_table, (void **)&proc->exactmatch_table, 
                                                         proc->sharelabel, proc->max_table, 
                                                         &proc->sharer_id, sh9a_sh_opts)) {
                              return 0;
                         }
                    }
                    else {
                         // read the stringhash9a table from the open_table file
                         uint32_t ret = 0;
                         if (proc->open_table) {
                              ret = stringhash9a_open_sht_table(&proc->exactmatch_table, 
                                                                proc->max_table, sh9a_sh_opts);
                         }
                         // create the stringhash9a table from scratch
                         if (!ret) {
                              proc->exactmatch_table = stringhash9a_create(0, proc->max_table);
                              if (NULL == proc->exactmatch_table) {
                                   tool_print("unable to create a proper stringhash9a table");
                                   return 0;
                              }
                         }
                    }

                    //free shared sh9a option struct
                    stringhash9a_sh_opts_free(sh9a_sh_opts);

                    //use the stringhash9a-adjusted value of max_records to reset max_table
                    proc->max_table = proc->exactmatch_table->max_records;

                    free(proc->open_table);
               }
               exact_match_loadfile(proc, optarg);
               break;
          case 'L': // register label for matched string
               proc->label_match = wsregister_label(type_table, optarg);
               break;
          case 'R': // reference string
               {
                    char * buf = strdup(optarg);
                    int len = strlen(optarg);
                    sysutil_decode_hex_escapes(buf, &len);
                    if (!proc->exactmatch_table) {
                         stringhash9a_sh_opts_t * sh9a_sh_opts;

                         //calloc shared sh9a option struct
                         stringhash9a_sh_opts_alloc(&sh9a_sh_opts);

                         //set shared sh9a option fields
                         sh9a_sh_opts->open_table = proc->open_table;

                         //proc->exactmatch_table = stringhash9a_create(0, proc->max_table);
                         if (proc->sharelabel) {
                              if (!stringhash9a_create_shared_sht(type_table, (void **)&proc->exactmatch_table, 
                                                              proc->sharelabel, proc->max_table, 
                                                              &proc->sharer_id, sh9a_sh_opts)) {
                                   return 0;
                              }
                         }
                         else {
                              // read the stringhash9a table from the open_table file
                              uint32_t ret = 0;
                              if (proc->open_table) {
                                   ret = stringhash9a_open_sht_table(&proc->exactmatch_table, 
                                                                     proc->max_table, sh9a_sh_opts);
                              }
                              // create the stringhash9a table from scratch
                              if (!ret) {
                                   proc->exactmatch_table = stringhash9a_create(0, proc->max_table);
                                   if (NULL == proc->exactmatch_table) {
                                        tool_print("unable to create a proper stringhash9a table");
                                        return 0;
                                   }
                              }
                         }

                         //free shared sh9a option struct
                         stringhash9a_sh_opts_free(sh9a_sh_opts);

                         //use the stringhash9a-adjusted value of max_records to reset max_table
                         proc->max_table = proc->exactmatch_table->max_records;

                         free(proc->open_table);
                    }
                    if (!stringhash9a_set(proc->exactmatch_table, buf, len)) {
                         // stringhash9a_set returns 0 on success
                         tool_print("searching for the reference string: \"%s\"", buf);
                         proc->items++;
                    }
                    free(buf);
               }
               break;
          case 'M':
               if (!proc->exactmatch_table) {
                    proc->max_table = atoi(optarg);
               }
               else {
                    tool_print("WARNING: the -M option to exactmatch kid was specified, but must appear before -F and -R to take effect.");
               }
               break;
          case 'O':
               proc->dump_file = strdup(optarg);
               break;
          case 'E':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash9a_create
               // call in proc_init
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

/* Take in command arguments and initialize this processor's instance. Also
 * register as source here.
 * Return 1 if OK.
 * Return 0 if fail.
 */
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, 
              ws_sourcev_t * sv, void * type_table) {
     
     // allocate proc instance of this processor
     proc_instance_t * proc = (proc_instance_t*)calloc(1, sizeof(proc_instance_t));
     *vinstance = proc;

     proc->max_table = LOCAL_MAX_SH9A_TABLE;

     // register labels
     proc->label_match = wsregister_label(type_table, "EXACTMATCH");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //other init - init the stringhash table, if not done already in 
     //proc_cmd_options above

     // initialize table for matching
     if (!proc->exactmatch_table) {
          stringhash9a_sh_opts_t * sh9a_sh_opts;

          //calloc shared sh9a option struct
          stringhash9a_sh_opts_alloc(&sh9a_sh_opts);

          //set shared sh9a option fields
          sh9a_sh_opts->open_table = proc->open_table;

          if (proc->sharelabel) {
               if (!stringhash9a_create_shared_sht(type_table, (void **)&proc->exactmatch_table, 
                                               proc->sharelabel, proc->max_table, 
                                               &proc->sharer_id, sh9a_sh_opts)) {
                    return 0;
               }
          }
          else {
               // read the stringhash9a table from the open_table file
               uint32_t ret = 0;
               if (proc->open_table) {
                    ret = stringhash9a_open_sht_table(&proc->exactmatch_table, 
                                                      proc->max_table, sh9a_sh_opts);
               }
               // create the stringhash9a table from scratch
               if (!ret) {
                    proc->exactmatch_table = stringhash9a_create(0, proc->max_table);
                    if (NULL == proc->exactmatch_table) {
                         tool_print("unable to create a proper stringhash9a table");
                         return 0;
                    }
               }
          }

          //free shared sh9a option struct
          stringhash9a_sh_opts_free(sh9a_sh_opts);

          //use the stringhash9a-adjusted value of max_records to reset max_table
          proc->max_table = proc->exactmatch_table->max_records;

          free(proc->open_table);
     }
     
     tool_print("matching items %d", proc->items);

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
//          tool_print("in proc_input_set: tuple type");
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
//               tool_print("default port");
//               tool_print("nest.cnt: %d", proc->nest.cnt);
               if (!proc->nest.cnt) {
//                    tool_print("no nesting; calling proc_process_allstr");
                    return proc_process_allstr;
               }
               else {
//                    tool_print("nesting; calling proc_process_meta");
                    return proc_process_meta; // a function pointer
               }
          }
     }
     return NULL;
}

static inline int member_match(proc_instance_t *proc, wsdata_t *member,
                               wsdata_t * tdata,
                               wsdata_t * tparent) {

//     tool_print("in member_match");
     char * buf;
     int len;
     int match = 0;
     if (dtype_string_buffer(member, &buf, &len)) {
//          tool_print("what's in buf? %s", buf);
          if (stringhash9a_check(proc->exactmatch_table, buf, len)) {
               match = 1;
          }
     }
     if (match) {
          tuple_add_member_label(tdata, member, proc->label_match);
          if (tparent) {
               tuple_add_member_label(tparent, tdata, proc->label_match);
          }
          else {
               if (!wsdata_check_label(tdata, proc->label_match)) {
                    wsdata_add_label(tdata, proc->label_match);
               }
          }
     }
     else if (proc->label_unmatch) {
          tuple_add_member_label(tdata, member, proc->label_unmatch);
          if (tparent) {
               tuple_add_member_label(tparent, tdata, proc->label_unmatch);
          }
          else {
               if (!wsdata_check_label(tdata, proc->label_unmatch)) {
                    wsdata_add_label(tdata, proc->label_unmatch);
               }
          }
          match = 1;
     }

     return match;
}

static int proc_nest_match_callback(void * vinstance, void * ignore,
                                    wsdata_t * tdata, wsdata_t * member,
                                    wsdata_t * tparent) {

//     tool_print("in proc_nest_match_callback");
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     return member_match(proc, member, tdata, tparent);
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

//     tool_print("in proc_process_meta");
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

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta_tag(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     tuple_nested_search2(input_data, &proc->nest,
                          proc_nest_match_callback,
                          proc, NULL);
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

static int proc_nest_inverse_callback(void * vinstance, void * ignore,
                                    wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     char * buf;
     int len;
     if (dtype_string_buffer(member, &buf, &len)) {
          if (stringhash9a_check(proc->exactmatch_table, buf, len)) {
               return 1;
          }
     }

     return 0;
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
          found += member_match(proc, member, input_data, NULL);
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

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          found += member_match(proc, member, input_data, NULL);
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
          found += member_match(proc, member, input_data, NULL);
     }
     if (!found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

static inline void proc_dump_existence_table(proc_instance_t * proc) {
     if (proc->dump_file && (!proc->sharelabel || !proc->sharer_id)) {
          tool_print("dumping existence table to file %s", proc->dump_file);
          FILE * fp = fopen(proc->dump_file, "w");
          if (fp) {
               stringhash9a_dump(proc->exactmatch_table, fp);
               fclose(fp);
          }
          else {
               perror("failed writing existence table");
               tool_print("unable to write to file %s", proc->dump_file);
          }
     }
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     proc_dump_existence_table(proc);
     stringhash9a_destroy(proc->exactmatch_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->dump_file) {
          free(proc->dump_file);
     }
     free(proc);

     return 1;
}

