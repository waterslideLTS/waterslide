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
#define PROC_NAME "strlen"

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "procloader_buffer.h"
#include "datatypes/wsdt_tuple.h"

int is_procbuffer = 1;
int procbuffer_pass_not_found = 1;

char proc_version[]     = "1.5";
char proc_requires[]     = "";
char *proc_tags[]        = { "stream", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Find the length of a tuple member.";
char *proc_alias[]     = { NULL };
char *proc_synopsis[]   = { "strlen <LABEL> [-e <value | -g <value>| -l <value>] [-L <value>]", NULL };
char proc_description[] = "Appends a new a tuple member that is the length of the specified label. Multiple labels may be specified.";

proc_example_t proc_examples[] = {
     {"... | strlen CONTENT | ...", "append a tuple member with the label 'STRLEN' that contains the length of the CONTENT field"},
     {"... | strlen -g 1000 CONTENT | ...", "if the CONTENT field is longer than 1000 characters, append a tuple member with the label 'STRLEN' that contains the length of the CONTENT field, otherwise no tuple members will be passed along"},
     {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     {'e',"","length",
     "Only pass if length is equal to value",0,0},
     {'g',"","length",
     "Only pass if length is greater than value",0,0},
     {'l',"","length",
     "Only pass if length is less than value",0,0},
     {'L',"","LABEL",
     "New label name of length",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char *proc_tuple_member_labels[] = {"STRLEN", NULL};
char proc_nonswitch_opts[]    = "LABEL of tuple string member to decode";
// proc_input_types and proc_output_types automatically set for procbuffer kids

proc_port_t proc_input_ports[]     = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};

#define LOCAL_GT    1
#define LOCAL_LT    2
#define LOCAL_EQ    3

//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_strlen;
     uint32_t condition;
     int value;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"STRLEN",offsetof(proc_instance_t, label_strlen)},
     {"",0}
};


char procbuffer_option_str[]    = "L:l:g:e:";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'L':
          proc->label_strlen = wsregister_label(type_table, str);
          break;
     case 'l':
          proc->condition = LOCAL_LT;
          proc->value = atoi(str);
          break;
     case 'g':
          proc->condition = LOCAL_GT;
          proc->value = atoi(str);
          break;
     case 'e':
          proc->condition = LOCAL_EQ;
          proc->value = atoi(str);
          break;
     }
     return 1;
}

int procbuffer_decode(void * vproc, wsdata_t * tdata,
                                     wsdata_t * member,
                                     uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     switch (proc->condition) {
     case LOCAL_LT:
          if (buflen >= proc->value) {
               return 0;
          }
          break;
     case LOCAL_GT:
          if (buflen <= proc->value) {
               return 0;
          }
          break;
     case LOCAL_EQ:
          if (buflen != proc->value) {
               return 0;
          }
          break;
     }
     tuple_member_create_uint(tdata, (uint32_t)buflen, proc->label_strlen);

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

