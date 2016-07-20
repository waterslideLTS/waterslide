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
#define PROC_NAME "charcnt"

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include "procloader_buffer.h"
#include "bit.h"

int is_procbuffer = 1;
int procbuffer_pass_not_found = 1;

char proc_version[]     = "1.1";
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "count specific characters in a buffer";
char *proc_alias[]     = { NULL };

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {'X',"","hex string",
     "ascii hex of characters to count",0,0},
     {'L',"","label",
     "output label of count",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of tuple member to validate";
// proc_input_types and proc_output_types automatically set for procbuffer kids

//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_charcnt;
     wslabel_t * label_extra;
     uint8_t cset[256];
     int char_cnt;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"CHARCNT",offsetof(proc_instance_t, label_charcnt)},
     {"",0}
};

#define DELIM ":,; "
static void set_characters(proc_instance_t * proc, const char * str) {
     char * dup = strdup(str);
     char * buf = dup;

     char * rtok = strsep(&buf, DELIM);

     uint8_t c;
     while (rtok) {
          dprint("rtok %s", rtok);
          if (isxdigit(rtok[0])) {
               c = (uint8_t)(0xFF & strtol(rtok, NULL, 16));
               proc->cset[c] = 1;
               proc->char_cnt++;
          }
          rtok = strsep(&buf, DELIM);
          //rtok = strtok_r(NULL, DELIM, &ptok);
     }

     free(dup);
}

char procbuffer_option_str[]    = "L:X:";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'L':
          proc->label_extra = wsregister_label(type_table, str);
          break;
     case 'X':
          set_characters(proc, str);
          break;
     }
     return 1;
}

int procbuffer_init(void * vproc, void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     
     if (!proc->char_cnt) {
          tool_print("dictionary not set, using default of '='");
          proc->cset['='] = 1;
     }
     return 1;
}




int procbuffer_decode(void * vproc, wsdata_t * tdata,
                                     wsdata_t * member,
                                     uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     uint32_t cnt = 0;
     int i;
     for (i = 0; i < buflen; i++) {
          if (proc->cset[buf[i]]) {
               cnt++;
          }
     }
     wsdata_t * wsd = tuple_member_create_uint(tdata, cnt,
                                               proc->label_charcnt);

     if (wsd && proc->label_extra) {
          tuple_add_member_label(tdata, wsd, proc->label_extra);
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

