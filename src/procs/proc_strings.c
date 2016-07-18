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
#define PROC_NAME "strings"

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "procloader_buffer.h"

int is_procbuffer = 1;
int procbuffer_pass_not_found = 1;

char proc_name[]        = PROC_NAME;
char *proc_tags[]	= {"Statistics", NULL };
char proc_purpose[]     = "take binary buffer, extract out continuous printable strings";
char *proc_synopsis[]	= { "strings [-r] [-s] [-L <LABEL>] <LABEL>", NULL };
char proc_description[]	= "Take a binary buffer and extract out continuous printable strings from it. The strings found within the same buffer are delimited by a carriage return by default unless the user specifies the delimiter using the '-s' option. The default minimum run of characters to pass as a printable string is 4. The user can specify a longer or shorter minumum run of characters by using the '-r' option. A new label named 'STRINGS' is created where the outputted strings are placed. The user can specify a custom label by using the '-L' option.";  	
proc_example_t proc_examples[]  = {
	{"... | strings DOCUMENT | ...", "extract the strings that are 4 characters in length or longer out of the binary DOCUMENT buffer, put the strings in a new tuple member named 'STRINGS' using a carriage return as the delimiter"},
	{"... | strings -r 6 -L MYSTRINGS DOCUMENT | ...", "extract the strings that are 6 characters in length or longer out of the binary 'DOCUMENT' buffer, put the strings in a new tuple member with the custom label name 'MYSTRINGS' using a carriage return as the delimiter"},
	{"... | strings -r 6 -s ':' DOCUMENT | ...", "extract the strings that are 6 characters in length or longer out of the binary 'DOCUMENT' buffer,  put the strings in a new tuple member named 'STRINGS' using a colon as a custom delimiter"},
	{NULL,""} 
};
char *proc_alias[]      = { NULL };
char proc_version[]     = "1.5";
char proc_requires[]	= "";
// proc_input_types and proc_output_types automatically set for procbuffer kids
proc_port_t proc_input_ports[]  = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {"STRINGS", NULL};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     {'r',"","count",
     "minimum run of characters",0,0},
     {'s',"","delim",
     "separator string",0,0},
     {'L',"","LABEL",
     "label of string buffer",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of tuple binary buffer to evaluate";

//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_strings;
     char * sep;
     int sep_len;
     int minrun;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"STRINGS",offsetof(proc_instance_t, label_strings)},
     {"",0}
};


char procbuffer_option_str[]    = "s:L:r:";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 's':
          free(proc->sep);
          proc->sep = strdup(str);
          proc->sep_len = strlen(proc->sep);
          break;
     case 'L':
          proc->label_strings = wsregister_label(type_table, str);
          break;
     case 'r':
          proc->minrun = atoi(optarg);
          break;
     }
     return 1;
}

int procbuffer_init(void * vproc, void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (!proc->sep) {
          proc->sep = strdup("\n");
          proc->sep_len = 1;
     }
     if (!proc->minrun) {
          proc->minrun = 4;
     }

     return 1;
}

int procbuffer_decode(void * vproc, wsdata_t * tdata,
                                     wsdata_t * member,
                                     uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     wsdt_string_t * str = tuple_create_string(tdata, proc->label_strings, buflen);

     if (!str) {
          return 1; // tuple is full
     }

     int blen = 0;
     int i;
     int crun = 0;

     uint8_t * startp = NULL;

     int seplen = 0;

     for (i = 0; i < buflen; i++) {
          if (isprint(buf[i])) {
               if (!crun) {
                    startp = &buf[i];
               }
               crun++;
          }
          else {
               if (startp && (crun >= proc->minrun) && 
                   (buflen >= (blen + crun + seplen))) {
                    if (seplen) {
                         memcpy(str->buf + blen, proc->sep, seplen);
                         blen += seplen; 
                    }
                    else {
                         seplen = proc->sep_len;
                    }
                    memcpy(str->buf + blen, startp, crun);
                    blen += crun;
               }
               crun = 0;
               startp = NULL;
          }
     }
     if (startp && (crun >= proc->minrun) && 
         (buflen >= (blen + crun + seplen))) {
          if (seplen) {
               memcpy(str->buf + blen, proc->sep, seplen);
               blen += seplen; 
          }
          memcpy(str->buf + blen, startp, crun);
          blen += crun;
     }
     str->len = blen;

     return 1;
}

int procbuffer_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     //free dynamic allocations
     free(proc->sep);
     //free(proc); // free this in the calling function

     return 1;
}

