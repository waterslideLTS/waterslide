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
#define PROC_NAME "asciihex"

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "procloader_buffer.h"

int is_procbuffer = 1;
int procbuffer_pass_not_found = 1;

char proc_version[] = "1.2";
char proc_requires[]     = "";
const char *proc_tags[] = { "decoder", NULL };
char proc_name[] = PROC_NAME;
char proc_purpose[] = "decode asciihex strings";
const char *proc_synopsis[] = 
     { "asciihex <LABEL> [-s <value>] [-L <value>] [-p]", NULL };
char proc_description[] = "Decode ASCII hex strings into their ASCII " 
                          "string equivalent. For example, convert the ASCII"
                          " hex string \"41424344\" to \"ABCD\".\n";
char *proc_alias[] = { NULL };

proc_example_t proc_examples[]    = {
     {"... | asciihex DATA | ...","decode the tuple member with the label DATA to its ASCII representation"},
     {"... | asciihex DATA -s : | ...","decode the tuple member with the label DATA to ASCII, using a colon as a delimiter"},
     {"... | asciihex DATA -p | ...","decode the tuple member with label DATA, but don't drop non hex characters in the output tuple member"},
     {NULL,""}
};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     {'s',"","delim",
     "delimiter string to ignore (default: no delimiter)",0,0},
     {'L',"","LABEL",
     "label of newly decoded data",0,0},
     {'X',"","",
     "decode only if entire string is hex or delimiter",0,0},
     {'A',"","character",
     "decode into string, non-printable will be replaced with character",0,0},
     {'p',"","",
     "pass along all non hex (default behavior: drop non hex characters)",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char *proc_tuple_member_labels[] = {"HEXDECODE", NULL};
char proc_nonswitch_opts[]    = "LABEL of tuple string member to decode";
// proc_input_types and proc_output_types automatically set for procbuffer kids
proc_port_t proc_input_ports[]     = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};

//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_decode;
     int pass_all;
     char * ignore;
     int ignore_len;
     int allhex;
     int string_only;
     char nonprint_char;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"HEXDECODE",offsetof(proc_instance_t, label_decode)},
     {"",0}
};


char procbuffer_option_str[]    = "A:Xs:L:p";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'X':
          proc->allhex = 1;
          break;
     case 's':
          proc->ignore = strdup(str);
          proc->ignore_len = strlen(proc->ignore);
          break;
     case 'L':
          proc->label_decode = wsregister_label(type_table, str);
          break;
     case 'p':
          proc->pass_all = 1;
          break;
     case 'A':
          proc->string_only = 1;
          proc->nonprint_char = str[0];
          break;
     }
     return 1;
}

static inline uint8_t get_xdigit(uint8_t c) {
     if (isdigit(c)) {
          return (c - 0x30);
     }
     else if (isupper(c)) {
          return (c - 0x37);
     }
     else {
          return (c - 0x57);
     }
}

static int check_allhex(proc_instance_t * proc, uint8_t * buf, int buflen) {
     int i;
     for (i = 0; i < buflen; i++) {
          if (proc->ignore_len && (i < (buflen - proc->ignore_len + 1)) &&
              (strncmp((char*)buf + i, proc->ignore, proc->ignore_len) == 0)) {
               i += proc->ignore_len - 1;
               continue;
          }
          if (i < (buflen - 1)) {
               if (isxdigit(buf[i]) && isxdigit(buf[i+1])) {
                    i++;
               }
               else {
                    return 0;
               }
          }
          else {
               return 0;
          }
     }
     return 1;
}

int procbuffer_decode(void * vproc, wsdata_t * tdata,
                                     wsdata_t * member,
                                     uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     if (proc->allhex && !check_allhex(proc, buf, buflen)) {
          return 1;
     }

     wsdt_binary_t * bin = NULL;
     if (proc->string_only) {
          wsdt_string_t * str = tuple_create_string(tdata, proc->label_decode, buflen);
          bin = (wsdt_binary_t*)str;
     }
     else {
          bin = tuple_create_binary(tdata, proc->label_decode, buflen);
     }
     if (!bin) {
          return 1; // tuple is full
     }
     int blen = 0;
     int i;
     for (i = 0; i < buflen; i++) {
          if (proc->ignore_len && (i < (buflen - proc->ignore_len + 1)) &&
              (strncmp((char*)buf + i, proc->ignore, proc->ignore_len) == 0)) {
               i += proc->ignore_len - 1;
               continue;
          }
          if (i < (buflen - 1)) {
               if (isxdigit(buf[i]) && isxdigit(buf[i+1])) {
                    bin->buf[blen] = (get_xdigit(buf[i]) << 4) + get_xdigit(buf[i+1]);
                    if (proc->string_only && !isprint(bin->buf[blen])) {
                         bin->buf[blen] = proc->nonprint_char;
                    }
                    i++;
                    blen++;
                    continue;
               }
          }
          if (proc->pass_all) {
               bin->buf[blen] = buf[i];
               blen++;
          }
     }
     bin->len = blen;

     return 1;
}

int procbuffer_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (proc->ignore) {
          free(proc->ignore);
     }
     //free(proc); // free this in the calling function

     return 1;
}

