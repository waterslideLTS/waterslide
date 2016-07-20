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
#define PROC_NAME "encodebase64"

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "wsbase64.h"
#include "procloader_buffer.h"

int is_procbuffer = 1;
//int procbuffer_pass_not_found = 1;

char proc_version[]     = "1.2";
char proc_requires[]     = "";
char *proc_tags[]      = { "decoder", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "encode a label member as base64";
char *proc_alias[]     = { NULL };
char *proc_synopsis[]    = {"encodebase64 <LABEL> [-L <value>] [-p]", NULL};
char proc_description[] = "Encode a tuple label member as a base64 string. "
                          "For example, encode  \"ABCD\" to \"QUJDRA==\".\n"
                          "\n"
                          "For the reverse conversion (i.e., base64 to ASCII), see base64.";

proc_example_t proc_examples[]    = {
          {"... | encodebase64 DATA | ...","encode the tuple member label DATA as base64"},
               {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     {'L',"","LABEL",
     "label of decoded data",0,0},
     //{'p',"","",
     //"pass along all non hex (default behavior: drop non hex characters)",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char *proc_tuple_member_labels[] = {"BASE64", NULL};
char proc_nonswitch_opts[]    = "LABEL of tuple string member to encode";
// proc_input_types and proc_output_types automatically set for procbuffer kids
proc_port_t proc_input_ports[]     = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};

//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_encode;
     //int pass_all;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"BASE64",offsetof(proc_instance_t, label_encode)},
     {"",0}
};


char procbuffer_option_str[]    = "L:p";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'L':
          proc->label_encode = wsregister_label(type_table, str);
          break;
     }
     return 1;
}

int procbuffer_decode(void * vproc, wsdata_t * tdata,
                                     wsdata_t * member,
                                     uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     int outlen = buflen * 2 + 10;
     wsdt_string_t * bin = tuple_create_string(tdata, proc->label_encode,
                                               outlen);
     if (!bin) {
          return 1; // tuple is full
     }
     int outresult = wsbase64_encode(buf, buflen, (uint8_t*)bin->buf, outlen);

     if (outresult <= outlen) {
          bin->len = outresult;
     }
     else {
          bin->len = 0;
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

