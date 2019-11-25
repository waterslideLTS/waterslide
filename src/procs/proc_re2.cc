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
/* use google's regular expression library re2 to do matching and extraction*/
/* this requires a shared library version of re2.. to get this working you need
 * to modify the re2 makefile to include:
CXXFLAGS+= -fPIC

obj/libre2.so: $(OFILES)
     @mkdir -p obj
     $(CC) -shared -Wl,-soname,/usr/local/lib/libre2.so -o obj/libre2.so $(OFILES)
*/

#define PROC_NAME "re2"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <re2/re2.h>
#include "waterslide.h"
#include "waterslidedata.h"
//#include "waterslide_io.h"
#include "procloader_buffer.h"
#include "mimo.h"
#include "wstypes.h"
#include "datatypes/wsdt_tuple.h"

#ifdef __cplusplus
CPP_OPEN
#endif

int is_procbuffer = 1;

int procbuffer_pass_not_found = 0;

char proc_name[]       = PROC_NAME;
char proc_version[]     = "1.5";
const char *proc_tag[]     = { "match", NULL };
const char *proc_alias[]     = { "rex", NULL };
char proc_purpose[]    = "performs regular expression search in buffers";
const char *proc_synopsis[] = {
     "re2 -R <regex> [-L <label>] [-T] <LABEL of buffer to search>",
     NULL };
char proc_description[] = 
     "performs a regular expression search in the buffers.  If no "
     "buffer is specified, all string fields are searched.\n"
     "\n"
     "Uses Google's re2 library.  The format of the regular expessions "
     "is perl compatable and is defined in re2/re2.h\n"
     "\n"
     "Parts of the expression included in parenthesis are extracted and "
     "saved under the label specified by the -L option";

proc_example_t proc_examples[] = {
     {NULL, ""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {'R',"","regex",
     "query buffer",0,0},
     {'L',"","labels",
     "labels for extracted output",0,0},
     {'T',"","",
      "pass all, tag if match",0,0},
     {'S',"","",
      "output as strings rather than binary",0,0},
     {'N',"","LABEL",
      "output results in nested tuple per input",0,0},
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of buffer to search";
// proc_input_types and proc_output_types automatically set for procbuffer kids
char *proc_tuple_member_labels[] = {NULL};
char proc_requires[] = "";
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};

proc_port_t proc_input_ports[] = {
     {NULL, NULL}
};

#define MAX_RES_ARG 10

typedef struct _proc_instance_t {
     RE2 * re;
     wslabel_t * label_res[MAX_RES_ARG];

     int lrescnt;

     RE2::Arg* res_args[MAX_RES_ARG];
     re2::StringPiece *res_str[MAX_RES_ARG];
     int num_args;
     int output_strings;
     wslabel_t * label_nest;

   int tag_output;

} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

#define DELIM ":,; "
static void set_res_labels(proc_instance_t * proc, char * str, void * type_table) {
     char * ptok = NULL;

     if (proc->lrescnt >= MAX_RES_ARG) {
          return;
     }
     char * rtok = strtok_r(str, DELIM, &ptok);
     while (rtok) {
          dprint("rtok %s", rtok);
          if (strlen(rtok)) {
               dprint("range %s", rtok);
               proc->label_res[proc->lrescnt] = wsregister_label(type_table, rtok);
               proc->lrescnt++;
               if (proc->lrescnt >= MAX_RES_ARG) {
                    return;
               }
          }
          rtok = strtok_r(NULL, DELIM, &ptok);
     }
}

char procbuffer_option_str[]    = "L:R:TSN:";

int procbuffer_option(void * vproc, void * type_table,
                      int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'L':
          set_res_labels(proc, optarg, type_table);
          break;
     case 'R':
          proc->re = new re2::RE2(optarg);
          if (!proc->re->ok()) {
               return 0;
          }
          tool_print("setting regular expression %s", optarg);
          proc->num_args = proc->re->NumberOfCapturingGroups();
          if (proc->num_args > MAX_RES_ARG) {
               proc->num_args = MAX_RES_ARG;
          }
          break;
     case 'S':
          proc->output_strings = 1;
          break;
     case 'T':
          proc->tag_output = 1;
          break;
     case 'N':
          proc->label_nest =  
               wsregister_label(type_table, optarg);
          break;

     }
     return 1;
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
//  also register as a source here..
// return 1 if ok
// return 0 if fail
int procbuffer_init(void * vproc, void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (!proc->re) {
          tool_print("must specify a regex with -R");
          return 0;
     }

     int i;
     for (i = 0; i < MAX_RES_ARG; i++) {
          RE2::Arg * av = new RE2::Arg;
          proc->res_str[i] = new re2::StringPiece;
          *av = proc->res_str[i];
          proc->res_args[i] = av;
     }

     //other init 
     return 1; 
}

int procbuffer_decode(void * vproc, wsdata_t * tdata, wsdata_t * dep,
                      uint8_t * buf, int len) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     re2::StringPiece str((const char *)buf, len);
     /*RE2::Arg argv[2];
     const RE2::Arg* const args[2] = {&argv[0], &argv[1]};
     re2::StringPiece res;
     argv[0] = &res;
     */

     wsdata_t * nest = NULL;
     
     if (RE2::PartialMatchN(str, *proc->re, proc->res_args, proc->num_args)) {
          int i;
          for (i = 0; i < proc->num_args; i++) {
               if (proc->res_str[i]->length()) {
                    if (proc->label_nest && !nest) {
                         nest = tuple_member_create_wsdata(tdata, dtype_tuple,
                                                           proc->label_nest);
                         tdata = nest;
                    }
                    if (proc->output_strings) {
                         tuple_member_create_dep_string(tdata, dep,
                                                        proc->label_res[i], 
                                                        (char *)proc->res_str[i]->data(),
                                                        proc->res_str[i]->length());
                    }
                    else {
                         tuple_member_create_dep_binary(tdata, dep,
                                                        proc->label_res[i],
                                                        (char *)proc->res_str[i]->data(),
                                                        proc->res_str[i]->length());
                    }
                    proc->res_str[i]->clear();
               }
          }
          return 1;
     }

     if (proc->tag_output) {
	return 1;
     }
     else {
	return 0;
     }
}

//return 1 if successful
//return 0 if no..
int procbuffer_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     //free dynamic allocations
     delete(proc->re);
     int i;
     for (i = 0; i < MAX_RES_ARG; i++) {
          delete proc->res_str[i];
          delete proc->res_args[i];
     }
     //free(proc); // free this in the calling function

     return 1;
}

#ifdef __cplusplus
CPP_CLOSE
#endif

