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
#define PROC_NAME "tr"

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "procloader_buffer.h"
#include "sysutil.h"

int is_procbuffer = 1;
int procbuffer_pass_not_found = 1;

char proc_version[] = "1.5";
char proc_name[] = PROC_NAME;
char proc_purpose[] = "translate characters into other characters - like unix tr command";
char *proc_tags[] = { "decoder", NULL };
char proc_requires[] = "";
char *proc_alias[] = { NULL };
char proc_description[] = "Translate characters into other characters like the tr unix command. The -f flag specifies the characters to be translated and the -t specifies the single character they will translate into. Characters that cannot easily be typed on a keyboard can be specified in hexadecimal with the -x flag (instead of -f). The -U flag is a shortcut to convert uppercase characters to lowercase.";

char *proc_synopsis[]   = { "tr [-U] <LABEL>", NULL };

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     {'U',"","",
     "convert uppercase letters to lowercase",0,0},
     {'f',"","chars",
     "characters to translate",0,0},
     {'x',"","chars",
      "hex characters to translate (example: '|0a 0d|')",0,0},
     {'t',"","char",
     "character to output",0,0},
     {'L',"","LABEL",
     "specify new label name",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

proc_example_t proc_examples[] = {
     {"... | tr -U UPPERCASE | ...","convert the uppercase characters in UPPERCASE member to lowercase, store result in TRBUFFER member"},
     {"... | tr -U LABEL -L LOWERCASE | ...","convert the uppercase characters in LABEL member to lowercase, store result in member named LOWERCASE"},
     {"... | tr -f AEIOU -t '_' LABEL | ...","replace vowels in the LABEL member with an underscore"},
     {"... | tr -x '|0a|' -f '' LABEL | ...","remove newlines from the LABEL member (0a is a newline in hex, replaced with nothing)"},
     {NULL,""}
};

char *proc_tuple_member_labels[] = {"TRBUFFER", NULL};
char proc_nonswitch_opts[] = "LABEL of tuple string member to decode";
// proc_input_types and proc_output_types automatically set for procbuffer kids
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};

//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_decode;
     int pass_all;
     char *fromStr;
     char *toStr;
     char *hexFromStr;
     uint8_t translate[256];
     uint8_t upper2lower;
     uint8_t tr_is_remove;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"TRBUFFER",offsetof(proc_instance_t, label_decode)},
     {"",0}
};


char procbuffer_option_str[]    = "x:Uf:t:s:L:";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'f':
          {
               if ( proc->fromStr != NULL ) {
                    error_print("Only one \"from\" string should be specified");
                    return 0;
               }
               proc->fromStr = strdup(str);
          }
          break;
     case 'x':
          {
               if ( proc->hexFromStr != NULL ) {
                    error_print("Only one hex \"from\" string should be specified");
                    return 0;
               }
               proc->hexFromStr = strdup(str);
	  }
	  break;
     case 't':
          {
               if ( proc->toStr != NULL ) {
                    error_print("Only one \"to\" string should be specified");
                    return 0;
               }
               proc->toStr = strdup(str);
          }
          break;
     case 'U':
          proc->upper2lower = 1;
          break;
     case 'L':
          proc->label_decode = wsregister_label(type_table, str);
          break;
     }
     return 1;
}

int procbuffer_init(void *vproc, void *type_table) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     size_t i;
     for ( i = 0 ; i < 256 ; i++ ) {
          proc->translate[i] = i;
     }

     if ( proc->upper2lower ) return 1;

     char *fromString = NULL;
     size_t fromLen = 0;
     if ( proc->hexFromStr ) {
          fromLen = strlen(proc->hexFromStr);
          fromString = proc->hexFromStr;
          int x = fromLen;
          sysutil_decode_hex_escapes(fromString, &x);
          fromLen = x;
     } else if ( proc->fromStr ) {
          fromLen = strlen(proc->fromStr);
          fromString = proc->fromStr;
     } else {
          error_print("At least one of -U, -x or -f MUST be specified");
          return 0;
     }

     if ( !proc->toStr ) {
          proc->tr_is_remove = 1;
          for ( i = 0 ; i < fromLen ; i++ ) {
               proc->translate[(uint8_t)fromString[i]] = '\0';
          }
     } else {
          size_t toLen = strlen(proc->toStr);
          if ( toLen == 1 ) {
               for ( i = 0 ; i < fromLen ; i++ ) {
                    proc->translate[(uint8_t)fromString[i]] = proc->toStr[0];
               }
          } else if ( toLen == fromLen ) {
               for ( i = 0 ; i < fromLen ; i++ ) {
                    proc->translate[(uint8_t)fromString[i]] = proc->toStr[i];
               }
          } else {
               error_print("To length must be 0, 1 or equal to the from length\n");
               return 0;
          }
     }

     if ( proc->hexFromStr ) free(proc->hexFromStr);
     if ( proc->fromStr ) free(proc->fromStr);
     if ( proc->toStr ) free(proc->toStr);

     return 1;
}

int procbuffer_decode(void * vproc, wsdata_t * tdata,
                                     wsdata_t * member,
                                     uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     wsdt_binary_t * bin;
     if (member->dtype == dtype_string) {
          bin = (wsdt_binary_t *)tuple_create_string(tdata, proc->label_decode, buflen);
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
          if ( proc->upper2lower ) {
               bin->buf[blen] = tolower(buf[i]);
               blen++;
          } else {
               bin->buf[blen] = proc->translate[buf[i]];
               if ( !(proc->tr_is_remove && bin->buf[blen] == '\0') )
                    blen++;
          }
     }
     bin->len = blen;

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

