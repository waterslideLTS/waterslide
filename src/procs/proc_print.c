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
#define PROC_NAME "print"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_binary.h"

#include "fileout.h"

char *proc_tags[]     = { "output", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Prints metadata associated with stream data to file or screen";
char *proc_synopsis[] = { "print [-s <separator>] [-S | -X | -b] [-O <outfile> | -A <outfile>] -w <destination> [-L -H] [-V[V]]", NULL};
char proc_description[] = 
     "Prints out information about the items/tuples/data stream. "
     "By default, prints to stdout, but can print to a specified "
     "output file instead.  Can specify a separator to use between "
     "fields; the default separator is ':'.  The '-V' option is commonly "
     "used to print verbose information including field labels. \n\n"
     "The outfile is a full filespec, which can include environment variable "
     "components, a label value, and an arbitrary timespec; see examples for "
     "details.  If an output filename includes a timespec, the filename will "
     "change once per timeslice [-t; defaults to 1 hour).";

proc_example_t proc_examples[] = {
	{"... | print -V", "prints all of the fields to stdout with verbose information (field labels as well as data)"},
	{"... | print -VV", "prints all of the fields to stdout with extra verbose information (field labels as well as data as well as field data type)"},
	{"... | print -s ',' -O outputfile.txt", "prints fields as comma separated values to outputfile.txt in current directory"},
	{"... | print -X", "prints all fields to stdout, but convert binary strings to hex before printing"},
	{"... | print -O {USER}-[WORD]__<%Y%m%d.%H%M>.out", "results stored in files named \"jsmith-foo__20160320.1145\"(e.g.)"},
	{"... | print -O foo<%H%M> -t 5 -v bar", "result files moved to directory bar every 5 minutes"},
	{NULL,""}
};
char *proc_alias[]     = { "pmeta", "p", NULL };
char proc_version[]     = "1.5.1";
char proc_requires[]	= "";

char *proc_input_types[] = {"tuple", "any", NULL};
char *proc_output_types[] = {"none", NULL};

// there aren't any ports:
proc_port_t proc_input_ports[]  = {{NULL, NULL}};

// there aren't any tuple labels:
char *proc_tuple_container_labels[] = {NULL};

// there aren't any conditional labels
char *proc_tuple_conditional_container_labels[] = {NULL};

// there aren't any added labels
char *proc_tuple_member_labels[] = {NULL};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'s',"","separator",
     "set separator between fields",0,0},
     {'S',"","",
     "print binary strings as ascii",0,0},
     {'X',"","",
     "print binary strings as hex",0,0},
     {'O',"","outfile",
     "set output file (stdout default)",0,0},
     {'w',"","prefix",
     "output file prefix (e.g., path)",0,0},
     {'A',"","outfile",
      "append to output file outfile",0,0},
     {'L',"","",
     "print only field labels",0,0},
     {'H',"","",
     "add line of labels prior to first record",0,0},
     {'V',"","",
     "verbose: print labels and data (if more than one V, e.g., VV, then print data's datatype as well)",0,0},
     {'T',"","",
     "tree: print labels and data in a tree-type format.  Only useful with -V", 0,0},
     {'J',"","",
     "print as json", 0,0},
     {'1',"","",
     "print only first label (JSON only)", 0,0},
     {'2',"","",
     "print only last label (JSON only)", 0,0},
     {'b',"","",
     "print binary format",0,0},
     {'E',"","",
     "print output to STDERR",0,0},
     {'t',"","period",
     "timecycle period (default unit is sec., or use m for minutes, h for hours)",0,0},
     {'v',"","moveprefix",
     "destination after write completes",0,0},
     {'D',"","", 
      "avoid overwriting file contents by creating versioned files",0,0},
     {'m',"","count",
     "max records per file",0,0},
     // TODO: incorporate byte count support through the codebase
//     {'M',"","count",
//     "max bytes per file",0,0},
     {'d',"","drop",
      "don't include label used in filespec",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[] = "";

#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_label_only(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_verbose(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_json(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     filespec_t *fs;
     int dropfslabel;
     fpdata_t *outfpdata;
     FILE * outfp;
     uint8_t binary;
     uint8_t label_only;
     int verbose;
     int tree_verbose;
     int tree_indent;
     char sep;
     int do_strings;
     int do_hex;
     int line_labels;
     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     int do_json;
     int print_only_first_label;
     int print_only_last_label;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc) {
     int op;
     int A_opt = 0, O_opt = 0;

     while ((op = getopt(argc, argv, "21EJRXHw:VSs:LbA:O:t:v:m:TdD")) != EOF) {
          switch (op) {
          case '2':
               proc->print_only_last_label = 1;
               break;
          case '1':
               proc->print_only_first_label = 1;
               break;
          case 'E':
               proc->fs->outfp = stderr;
               break;
          case 'H':
               proc->line_labels = 1;
               break;
          case 'V':
               proc->verbose++;
               break;
          case 'T':
               proc->tree_verbose = 1;
               proc->sep = '\n';
               break;
          case 'S':
               proc->do_strings = 1;
               break;
          case 'X':
               proc->do_hex = 1;
               break;
          case 's':
               if (strcmp(optarg, "\\n") == 0) {
                    proc->sep = '\n';
                    tool_print("using [newline] as record separator");
               }
               else if (strcmp(optarg, "\\t") == 0) {
                    proc->sep = '\t';
                    tool_print("using [tab] as record separator");
               }
               else {
                    proc->sep = optarg[0];
                    tool_print("using '%c' as record separator", proc->sep);
               }
               break;
          case 'L':
               proc->label_only = 1;
               tool_print("printing only labels");
               break;
          case 'b':
               proc->binary = 1;
               tool_print("printing in binary");
               break;
          case 'J':
               proc->do_json = 1;
               break;
          case 'A':
               A_opt++;
               if (!fileout_parse_filespec(optarg, proc->fs, 0)) {
                    return 0;
               }
               proc->fs->mode = 'a';
               break;
          case 'w':
               proc->fs->fileprefix = fileout_parse_filespec(optarg, NULL, 1);
               break;
          case 'O':
               O_opt++;
               if (!fileout_parse_filespec(optarg, proc->fs, 0)) {
                    return 0;
               }
               proc->fs->mode = 'w';
               break;
          case 't': 
               proc->fs->timeslice = sysutil_get_duration_ts(optarg);
               break;
          case 'v':
               proc->fs->moveprefix = fileout_parse_filespec(optarg, NULL, 1);
               break;
          case 'm':
               proc->fs->recordmax = strtoul(optarg, NULL, 0);
               break;
               //	  case 'M':
               //	       proc->fs->bytemax = sysutil_get_strbytes(optarg);
               //	       break;
          case 'd':
               proc->dropfslabel = 1;
               break;
          case 'D':
               proc->fs->safename = 1;
               break;
          default:
               return 0;
          }
     }

     if (A_opt && O_opt) {
          error_print("-O and -A options are mutually exclusive.");
          return 0;
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

     proc->fs = calloc(1, sizeof(filespec_t));
     proc->fs->outfp = stdout;
     proc->sep = ':';

     //read in command options
     if (!proc_cmd_options(argc, argv, proc)) {
          return 0;
     }

     proc->outfpdata = fileout_initialize(proc->fs, type_table);
     if (proc->outfpdata == 0) 
	  return 0;

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

     if (meta_type == dtype_monitor) {
          return NULL;
     }

     //check if datatype has print function
     if (!meta_type->print_func){
          return NULL;
     }

     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     if ((meta_type == dtype_tuple) && proc->do_json) {
          return proc_json;
     }

     // we are happy.. now set the processor function
     if (proc->label_only) {
          return proc_process_label_only;
     }
     else if (proc->verbose) {
          return proc_process_verbose;
     }
     return proc_process_meta; // a function pointer
}

static void print_indent(proc_instance_t * proc ) {
     if ( proc->tree_verbose ) {
          int i;
          for ( i = 0 ; i < proc->tree_indent ; i++ )
               proc->outfpdata->bytecount += fprintf(proc->outfp, "    ");
     }
}

static inline void print_label_v1(proc_instance_t * proc, wsdata_t * input_data) {
     fprintf(proc->outfp, "[");
     int i; 
     for (i = 0; i < input_data->label_len; i++) {
          if (i > 0) {
               proc->outfpdata->bytecount += fprintf(proc->outfp, ":");
          }
          proc->outfpdata->bytecount += fprintf(proc->outfp, "%s", input_data->labels[i]->name);
     }
     proc->outfpdata->bytecount += fprintf(proc->outfp, "]");
     
     if (proc->verbose > 1) {
          proc->outfpdata->bytecount += fprintf(proc->outfp,"{%s}", input_data->dtype->name);
     }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_label_only(void * vinstance, wsdata_t* input_data,
                                   ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     int outputgenerated = 0;

     proc->outfpdata = fileout_select_file(input_data, proc->fs, 0);
     proc->outfp = (FILE *)proc->outfpdata->fp;

     proc->meta_process_cnt++;

     if (input_data->dtype == dtype_tuple) {
          print_label_v1(proc, input_data);
          proc->outfpdata->bytecount += fprintf(proc->outfp, ":");
	  
          int i;
          wsdt_tuple_t * tuple = input_data->data;
         
          for (i = 0; i < tuple->len; i++) {
	       if (proc->dropfslabel && 
		   tuple->member[i]->label_len == 1 &&
		   tuple->member[i]->labels[0] == proc->fs->label) {
		    continue;
	       }
               if (outputgenerated) {
                    proc->outfpdata->bytecount += fprintf(proc->outfp, ",");
               }
	       outputgenerated = 1;
               print_label_v1(proc, tuple->member[i]);
          } 
     }
     else {
	  if (!(proc->dropfslabel && 
		input_data->label_len == 1 &&
		input_data->labels[0] == proc->fs->label)) {
	       print_label_v1(proc, input_data);
	       outputgenerated = 1;
	  }
     }
     if (outputgenerated) { 
	  proc->outfpdata->bytecount += fprintf(proc->outfp, "\n"); 
     }
     fflush(proc->outfp);

     //handle passthrough of data through print function
     if (ws_check_subscribers(proc->outtype_meta[type_index])) {
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
     }
     return 0;
}

// TODO: either the sysutil wrappers need to return the byte count, or 
//   fileout has to broker fprintf.   Grrr....

static inline void local_print_strings(proc_instance_t * proc, wsdata_t * member) {
     wsdt_binary_t * bin = member->data;
     sysutil_print_content_strings(proc->outfp, (uint8_t*)bin->buf, bin->len, 4);
}

static inline void local_print_hex(proc_instance_t * proc, wsdata_t * member) {
     wsdt_binary_t * bin = member->data;
     sysutil_print_content_hex(proc->outfp, (uint8_t*)bin->buf, bin->len);
}


static inline void print_label_v2(proc_instance_t * proc, wsdata_t * member) {
     int printed_something = 0;
 
     if (proc->verbose > 1) {
          proc->outfpdata->bytecount += fprintf(proc->outfp,"{%s}", member->dtype->name);
          printed_something = 1;
     }
     if (member->label_len) {
          int i;
          for (i = 0; i < member->label_len; i++) {
               if (i > 0) {
                    proc->outfpdata->bytecount += fprintf(proc->outfp, ":");
               }
               proc->outfpdata->bytecount += fprintf(proc->outfp, "%s", member->labels[i]->name);
               printed_something = 1;
          }
     }
     if (printed_something) {
          proc->outfpdata->bytecount += fprintf(proc->outfp, "%c", proc->sep);
	  proc->tree_indent++;
     }
}

static inline void print_line_labels(proc_instance_t * proc, wsdt_tuple_t * tup) {
     int i;
     int outputgenerated=0;

     for (i = 0; i < tup->len; i++) {
	  if (proc->dropfslabel && 
	      tup->member[i]->labels[0] == proc->fs->label &&
	      tup->member[i]->label_len == 1) {
	       continue;
	  } 
          if (outputgenerated) {
               proc->outfpdata->bytecount += fprintf(proc->outfp, "%c", proc->sep);
          }
	  outputgenerated = 1;
          print_label_v1(proc, tup->member[i]); 
     }
     if (outputgenerated) {
	  proc->outfpdata->bytecount += fprintf(proc->outfp, "\n");
     }
     fflush(proc->outfp);
}

static inline void print_subtuple(proc_instance_t * proc, wsdata_t * tdata) {
     int i;
     int outputgenerated = 0;
     wsdt_tuple_t * tuple = tdata->data;
     wsdata_t * member;
     if (proc->line_labels) {
          print_line_labels(proc, tuple);
          proc->line_labels = 0;
     }
     for (i = 0; i < tuple->len; i++) {
          member = tuple->member[i];
	  if (proc->dropfslabel && 
	      member->label_len == 1 &&
	      member->labels[0] == proc->fs->label) {
	       continue;
	  }
          if (outputgenerated) {
               proc->outfpdata->bytecount += fprintf(proc->outfp, "%c", proc->sep);
          }
	  outputgenerated = 1;
          if (proc->do_strings) {
               if (member->dtype == dtype_binary) {
                    local_print_strings(proc, member);
               }
               else {
                    if (member->dtype->print_func) {
                         member->dtype->print_func(proc->outfp, member,
                                                   WS_PRINTTYPE_TEXT);
                    }
               }
          }
          else if (proc->do_hex) {
               if (member->dtype == dtype_binary) {
                    local_print_hex(proc, member);
               }
               else {
                    if (member->dtype->print_func) {
                         member->dtype->print_func(proc->outfp, member,
                                                   WS_PRINTTYPE_TEXT);
                    }
               }
          }
          else {
               if (member->dtype == dtype_tuple) {
                    print_subtuple(proc, member);
               }
               else if (member->dtype->print_func) {
                    member->dtype->print_func(proc->outfp, member,
                                              WS_PRINTTYPE_TEXT);
               }
          }
     }
}

static inline void print_subtuple_verbose(proc_instance_t * proc,
                                          wsdata_t * tdata) {
     wsdt_tuple_t * tuple = tdata->data;
     wsdata_t * member;
     int i;
     int outputgenerated = 0;
     for (i = 0; i < tuple->len; i++) {
          member = tuple->member[i];
	  if (proc->dropfslabel && 
	      member->label_len == 1 &&
	      member->labels[0] == proc->fs->label) {
	       // just one label, the one we should ignore
	       continue; 
	  }
          if (outputgenerated) {
               proc->outfpdata->bytecount += fprintf(proc->outfp, "%c", proc->sep);
          }
          print_indent(proc);
          outputgenerated=1;
          print_label_v1(proc, member);
          if (proc->do_strings) {
               if (member->dtype == dtype_binary) {
                    local_print_strings(proc, member);
               }
               else {
                    if (member->dtype->print_func) {
                         member->dtype->print_func(proc->outfp, member,
                                                   WS_PRINTTYPE_TEXT);
                    }
               }

          }
          else if (proc->do_hex) {
               if (member->dtype == dtype_binary) {
                    local_print_hex(proc, member);
               }
               else {
                    if (member->dtype->print_func) {
                         member->dtype->print_func(proc->outfp, member,
                                                   WS_PRINTTYPE_TEXT);
                    }
               }

          }
          else {
               if (member->dtype == dtype_tuple) {
                    proc->outfpdata->bytecount += fprintf(proc->outfp, "{tuplebegin}%c", proc->sep);
                    proc->tree_indent++;
                    print_subtuple_verbose(proc, member);
                    proc->tree_indent--;
                    proc->outfpdata->bytecount += fprintf(proc->outfp, "%c", proc->sep);
                    print_indent(proc);
                    proc->outfpdata->bytecount += fprintf(proc->outfp, "{tupleend}");
               }
               else if (member->dtype->print_func) {
                    member->dtype->print_func(proc->outfp, member,
                                              WS_PRINTTYPE_TEXT);
               }
          }
     }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     dprint("print proc_process_meta");

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (!input_data->dtype->print_func) {
	  fprintf(stderr,"no print\n");
          return 0;
     }

     proc->outfpdata = fileout_select_file(input_data, proc->fs, 0);
     proc->outfp = (FILE *)proc->outfpdata->fp;
//     fprintf(proc->outfp,"rc: %" PRIu64 "\n",proc->meta_process_cnt);

     if (proc->binary) {
          input_data->dtype->print_func(proc->outfp, input_data, 
                                        WS_PRINTTYPE_BINARY);
	  fprintf(stderr, "binary\n");
          return 0;
     }

     proc->tree_indent = 0;
     print_label_v2(proc, input_data);

     //handle tuple in a special way to allow for user-defined
     // field separator
     if (input_data->dtype == dtype_tuple) {
          print_subtuple(proc, input_data);
     }
     else {
	  print_indent(proc);
          input_data->dtype->print_func(proc->outfp, input_data, 
                                        WS_PRINTTYPE_TEXT);
     }
     proc->outfpdata->bytecount += fprintf(proc->outfp, "\n");
     // In tree mode, print an extra newline between items
     if ( proc->tree_verbose ) fprintf(proc->outfp, "-----------------\n");
     fflush(proc->outfp);
     proc->tree_indent = 0;

     //handle passthrough of data through print function
     if (ws_check_subscribers(proc->outtype_meta[type_index])) {
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
     }
     return 0;
}

static void print_json_label(proc_instance_t * proc, wsdata_t * member) {

     if (!member->label_len) {
          fprintf(proc->outfp, "\"NULL\":");
     }
     else if (proc->print_only_last_label) {
          fprintf(proc->outfp,"\"%s\":", member->labels[member->label_len-1]->name);
     }
     else if (proc->print_only_first_label) {
          fprintf(proc->outfp,"\"%s\":", member->labels[0]->name);
     }
     else {
          fprintf(proc->outfp,"\"");
          int i;
          for (i = 0; i < member->label_len; i++) {
               fprintf(proc->outfp, "%s%s", (i>0) ? ":":"", member->labels[i]->name);
          }
          fprintf(proc->outfp, "\":");
     }
}

static void print_json_string(proc_instance_t * proc, wsdata_t * member) {
     fprintf(proc->outfp, "\"");
     wsdt_string_t * str = (wsdt_string_t *)member->data;
     int prior = 0;
     int i;
     char * jstr;
     for (i = 0; i < str->len; i++) {
          jstr = NULL;
          switch(str->buf[i]) {
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
                    fprintf(proc->outfp, "%.*s", i - prior, str->buf + prior);
               }
               fprintf(proc->outfp, "%s", jstr);
               prior = i + 1;
          }
     }
     if (prior < str->len) {
          fprintf(proc->outfp, "%.*s", str->len - prior, str->buf + prior);
     }
     fprintf(proc->outfp, "\"");
}

static void print_json_tuple(proc_instance_t * proc, wsdata_t * tdata);

static void print_json_member(proc_instance_t * proc, wsdata_t * member) {
     if (member->dtype == dtype_tuple) {
          fprintf(proc->outfp, "{");
          print_json_tuple(proc, member);
          fprintf(proc->outfp, "}");
          return;
     }
     else if (!member->dtype->print_func) {
          return;
     }

     if (member->dtype == dtype_binary) {
          //print binary as basic hex
          fprintf(proc->outfp, "\"");
          wsdt_binary_t * bin = (wsdt_binary_t*)member->data;
          int b;
          for (b = 0; b < bin->len; b++) {
               fprintf(proc->outfp, "%02u", (uint8_t)bin->buf[b]);
          }
          fprintf(proc->outfp, "\"");
     }
     else if (member->dtype == dtype_string) {
          print_json_string(proc, member);
     }
     else {
          fprintf(proc->outfp, "\"");
          member->dtype->print_func(proc->outfp, member, WS_PRINTTYPE_TEXT);
          fprintf(proc->outfp, "\"");
     }
     return;

}

static void print_json_tuple(proc_instance_t * proc, wsdata_t * tdata) {
     wsdt_tuple_t * tuple = (wsdt_tuple_t*)tdata->data;
     wsdata_t * member;
     int i;
     int out = 0;

     for (i = 0; i < tuple->len; i++) {
          member = tuple->member[i];

          if ((member->dtype!=dtype_tuple) && !member->dtype->print_func) {
               continue;
          }
          if (out) {
               fprintf(proc->outfp, ",");
          }

          print_json_label(proc, member);

          //check if list
          int listlen = 0;
          int nolist = 0;
          int j;
          for (j = (i+1); j < tuple->len; j++) {
               wsdata_t * nextmember = tuple->member[j];
               if ((nextmember->dtype!=dtype_tuple) && !nextmember->dtype->print_func) {
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
               fprintf(proc->outfp, "[");
               
               for (j = 0; j <= listlen; j++) {
                    member = tuple->member[i+j]; 
                    if (j > 0) {
                         fprintf(proc->outfp, ",");
                    }
                    print_json_member(proc, member);
               }
               fprintf(proc->outfp, "]");
               i += listlen;
          } 
          else {
               print_json_member(proc, member);
          }
          out++;
     }
}

static int proc_json(void * vinstance, wsdata_t* input_data,
                                ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     proc->outfpdata = fileout_select_file(input_data, proc->fs, 0);
     proc->outfp = (FILE *)proc->outfpdata->fp;

     if (input_data->label_len) {
          fprintf(proc->outfp, "{");
          print_json_label(proc, input_data);
          fprintf(proc->outfp, "{");
          print_json_tuple(proc, input_data);
          fprintf(proc->outfp, "}}\n");
     }
     else {
          fprintf(proc->outfp, "{");
          print_json_tuple(proc, input_data);
          fprintf(proc->outfp, "}\n");
     }

     //implement as passthrough
     if (ws_check_subscribers(proc->outtype_meta[type_index])) {
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
     }
     return 0;
}



//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_verbose(void * vinstance, wsdata_t* input_data,
                                ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (!input_data->dtype->print_func) {

          //handle passthrough of data through print function
          if (ws_check_subscribers(proc->outtype_meta[type_index])) {
               ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
               proc->outcnt++;
          }
          return 0;
     }

     proc->outfpdata = fileout_select_file(input_data, proc->fs, 0);
     proc->outfp = (FILE *)proc->outfpdata->fp;
//     fprintf(proc->outfp,"rc: %" PRIu64 "\n",proc->meta_process_cnt);

     print_label_v2(proc, input_data);

     //handle tuple in a special way to allow for user-defined
     // field separator
     if (input_data->dtype == dtype_tuple) {
          print_subtuple_verbose(proc, input_data);
     }
     else {
          input_data->dtype->print_func(proc->outfp, input_data, 
                                        WS_PRINTTYPE_TEXT);
     }
     proc->outfpdata->bytecount += fprintf(proc->outfp, "\n");
     // In tree mode, print an extra newline between items
     if ( proc->tree_verbose ) fprintf(proc->outfp, "-----------------\n");
     fflush(proc->outfp);
     proc->tree_indent = 0;
     
     //handle passthrough of data through print function
     if (ws_check_subscribers(proc->outtype_meta[type_index])) {
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
     }
     return 0;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     if (proc->outcnt) {
          tool_print("output cnt %" PRIu64, proc->outcnt);
     }

     //destroy outfile(s) 
     fileout_filespec_cleanup(proc->fs);

     //free dynamic allocations
     free(proc->fs);
     free(proc);

     return 1;
}

