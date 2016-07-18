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
#define PROC_NAME "substring"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
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

char proc_version[]     = "1.5";
char *proc_tags[] = {"Stream Manipulation", NULL};
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Create substrings from labeled strings";
char *proc_synopsis[] = { "substring LABEL_TO_SUBSTRING [-o <value>] [-e <value>] [-l <value>] [-L <LABEL>]", NULL};
char proc_description[] = "From a given label, create a substring from the label contents.  For instance, from the string '12345xxtextwewantyyyy', utilizing the substring processor with the options '-e 4 -o 7 -L NEWLABEL' would create a label, NEWLABEL in the tuple with the value 'textwewant'.  The '-l' option can be used to create fixed length substrings (e.g., using the example above with '-l 5' option would produce a substring with a value of '12345'.";
proc_example_t proc_examples[] = {
     {"... | substring -l 12 CONTENT -L FOO | ...","Pull the first 12 characters out of the CONTENT buffer and place in the new FOO buffer."},
     {"... | substring -e 4 -o 7 -L ROOT WORD | ...","Skip the first four characters and then pull the next seven characters out of the WORD buffer and place in the new ROOT buffer."},
     {NULL,""}
};
char proc_requires[] = "";
// proc_input_types and proc_output_types automatically set for procbuffer kids
proc_port_t proc_input_ports[] = {{NULL,NULL}};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};
char proc_nonswitch_opts[] = "";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'o',"","offset",
     "fixed offset into string",0,0},
     {'l',"","length",
     "length of substring",0,0},
     {'e',"","",
     "offset from end of string",0,0},
     {'L',"","",
     "label of new substring",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

int is_procbuffer = 1;

int procbuffer_pass_not_found = 1;

typedef struct _proc_instance_t {
     int offset;
     int len;
     wslabel_t * label_substring;
     int at_end;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"SUBSTRING", offsetof(proc_instance_t, label_substring)},
     {"",0}
};

char procbuffer_option_str[]    = "el:o:L:";

int procbuffer_option(void * vproc, void * type_table,
                      int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
          case 'e':
               proc->at_end = 1;
               tool_print("offset at end");
               break;
          case 'o':
               proc->offset = atoi(str);
               tool_print("offset set to %d", proc->offset);
               break;
          case 'l':
               proc->len = atoi(str);
               tool_print("length set to %d", proc->len);
               break;
          case 'L':
               proc->label_substring = wsregister_label(type_table, str);
               tool_print("setting substring label to %s", str);
               break;
     }
     return 1;
}

int procbuffer_decode(void * vproc, wsdata_t * tuple,
                      wsdata_t * member,
                      uint8_t * ubuf, int len) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     char * buf = (char *)ubuf;


     int trimlen = proc->len;

     if ((len <= 0) || (proc->offset >= len)) {
          return 1;
     }
     dprint("add_substr %d %d %d", len, proc->offset, proc->len);
     if ((len - proc->offset) < trimlen) {
          trimlen = len - proc->offset;
     }
     if (proc->at_end) {
          buf += len - trimlen - proc->offset; 
     }
     else {
          buf += proc->offset;
     }

     if (member->dtype == dtype_binary) {
          tuple_member_create_dep_binary(tuple, member, proc->label_substring,
                                         buf, trimlen);
     }
     else {
          tuple_member_create_dep_string(tuple, member, proc->label_substring,
                                         buf, trimlen);
     }
     return 1;
}

//return 1 if successful
//return 0 if no..
int procbuffer_destroy(void * vinstance) {
     //proc_instance_t * proc = (proc_instance_t*)vinstance;

     //free dynamic allocations
     //free(proc); // free this in the calling function

     return 1;
}

