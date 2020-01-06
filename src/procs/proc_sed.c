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
#define PROC_NAME "sed"

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

char proc_version[]     = "1.5";
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "perform (a subset of) UNIX sed operations";
char *proc_synopsis[]  = { "sed -e 's/str1/string2/g' FOO", NULL };
char *proc_alias[]     = { NULL };

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     {'e', "", "string", 
      "sed-formatted directive (s,y)",0,1},
     {'L',"","LABEL",
     "output label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_description[] = "Performs a subset of sed operations [currently "
  "s (substitute) and y (translate)] on the specified labeled tuple (or the "
  "first tuple encountered).  The g (global) flag is handled for "
  "substitution.  Note that the sed script must be quoted to avoid shell "
  "interpretation 'help'.";

proc_example_t proc_examples[]  = {
  {"... | sed -e 's/any/all/' LABEL | ...", "replace first occurrence "
   "of the string 'any' with the string 'all' in the tuple labeled LABEL "
   "(e.g. 'anyone? bueller? anyone' -> 'allone? bueller? anyone?')"},
  {"... | sed -e 's/bogus/fake/g' FOO | ...", "replace all strings of 'bogus' "
   " with 'fake'"},
  {"... | sed -e 'y/abc/xyz/' BAR | ...", "replace all 'a' characters with "
   "'x', all 'b's with 'y's, and 'c's with 'z's."},
  {NULL,""} 
};


#define DEFAULTLABEL "SEDBUFFER"
char *proc_tuple_member_labels[] = {DEFAULTLABEL, NULL};
char proc_nonswitch_opts[]    = "LABEL of tuple string member to decode";
// proc_input_types and proc_output_types automatically set for procbuffer kids

#define MAXSTRINGSIZE 256
//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_decode;
     char sedop;
     uint8_t xlate[256];
     char original[MAXSTRINGSIZE];
     char replace[MAXSTRINGSIZE];
     int global;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {DEFAULTLABEL, offsetof(proc_instance_t, label_decode)},
     {"",0}
};


int extract_parameters(const char *instring, char *param1, char *param2, char *flags, int paramlimit)
{
  char *ptr;
  char *paramstart;
  char delim;

  delim = instring[1];
  paramstart = (char*)&instring[2];

  ptr = strchr(paramstart,delim);
  if (!ptr) {
    return 0;
  }
  if ((ptr-paramstart) >= paramlimit) {
    tool_print("parameter too large");
    return 0;
  }
  strncpy(param1, paramstart, ptr-paramstart);
  param1[ptr-paramstart] = 0;

  paramstart = ptr + 1;
  ptr = strchr(paramstart,delim);
  if (!ptr) {
    return 0;
  }
  if ((ptr-paramstart) >= paramlimit) {
    tool_print("parameter too large");
    return 0;
  }
  strncpy(param2, paramstart, ptr-paramstart);
  param2[ptr-paramstart] = 0;

  paramstart = ptr + 1;
  strcpy(flags, paramstart);
  
  //  printf("instring: %s\n delim: %c param1: %s param2: %s flags: %s\n",
  //	 instring, delim, param1, param2, flags);

  return 1;
}

char procbuffer_option_str[]    = "e:L:";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     char flags[10];

     switch(c) {
     case 'e':
          {
	    proc->sedop = str[0];

	    switch (proc->sedop) {
	    case 's':
	      {
		if (1 != extract_parameters(str, proc->original, 
					    proc->replace, flags, MAXSTRINGSIZE)) {
		  tool_print("Parameter issue with %s", str);
		  return 0;
		}

		if (flags[0] == 'g') {
		  proc->global = 1;
		}

	      }
	      break;
	    case 'y':
	      {
		char orig[257];
		char dest[257];
		int i;

		if (1 != extract_parameters(str, orig, dest, flags, 256)) {
		  tool_print("Parameter issue with %s", str);
		  return 0;
		}
		if (strlen(dest) != strlen(orig)) {
		  tool_print("transform character sets must be the same size");
		  return 0;
		}
		// initialize values
		for (i=0; i<256; i++) {
		  proc->xlate[i]=i;
		}
		// set up replacements
                int len = strlen(orig);
                for (i=0; i < len; i++) {
		  proc->xlate[(uint8_t)orig[i]] = dest[i];
		}
		  
	      }
	      break;
	    default:
	      { 
		tool_print("Don't know how to handle directive '%c'", proc->sedop);
		return 0;
	      }
	    }
	    break;
	  }
     case 'L':
       proc->label_decode = wsregister_label(type_table, str);
       break;
     }
     return 1;
}

int procbuffer_decode(void * vproc, wsdata_t * tdata,
                                     wsdata_t * member,
                                     uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     int count = 0;
     int delta;
     int i;
     uint8_t *rear;
     uint8_t *front;
     uint8_t *dest;
     uint8_t *firstmatch;
     int bufremaining;
     int resultlen;
     int offset = 0;

     wsdt_binary_t * bin;

     if (proc->sedop == 0) {
       tool_print("No SED script specified (-e)");
       return 0;
     }

     switch(proc->sedop) {
     case 's':
       { // Handling SED s(ubstitute) function....
	 firstmatch = memmem(buf, buflen, proc->original, 
			     strlen(proc->original));
	 
	 if (firstmatch == NULL) { // nothing to be replaced
	   if (member->dtype == dtype_string) {
	     bin = (wsdt_binary_t *)tuple_create_string(tdata, 
							proc->label_decode, 
							buflen);
	   }
	   else {
	     bin = tuple_create_binary(tdata, proc->label_decode, buflen);
	   }
	   if (!bin) {
	     return 1; // tuple is full
	   }
	   memcpy(bin->buf, buf, buflen);
	   bin->len = buflen;
	   
	   return 1;
	 }
	 
	 // otherwise, let's see how many to replace
	 front = firstmatch;
	 rear = buf;
	 do {
	   count++;
	   offset += front - rear;
	   rear = front + strlen(proc->original);
	 } while ((front = memmem(rear, buflen - offset, 
				proc->original, strlen(proc->original))));
	 
	 
	 
	 // Make enough space
	 delta = (strlen(proc->replace) - strlen(proc->original)) * count;
	 resultlen = buflen + delta;
	 
	 if (member->dtype == dtype_string) {
	   bin = (wsdt_binary_t *)tuple_create_string(tdata, proc->label_decode, resultlen);
	 }
	 else {
	   bin = tuple_create_binary(tdata, proc->label_decode, resultlen);
	 }
	 if (!bin) {
	   return 1; // tuple is full
	 }
	 
	 dest = (uint8_t*)bin->buf;
	 front = firstmatch;
	 rear = buf;
	 bufremaining = buflen;
	 
	 do {
	   offset = front - rear;
	   memcpy(dest, rear, offset);
	   dest += offset;
	   bufremaining -= offset;
	   memcpy(dest, proc->replace, strlen(proc->replace));
	   dest += strlen(proc->replace);
	   bufremaining -= strlen(proc->original);
	   rear = front + strlen(proc->original);
	 } while (proc->global && (front = memmem(rear, bufremaining, 
				proc->original, strlen(proc->original))));
	 
	 memcpy(dest, rear, bufremaining);
	 
	 bin->len = resultlen;
	 
	 return 1;
	 break;
       }
     case 'y':
       { // Handle SED 'y' (translate) operation
	 if (member->dtype == dtype_string) {
	   bin = (wsdt_binary_t *)tuple_create_string(tdata, proc->label_decode, buflen);
	 }
	 else {
	   bin = tuple_create_binary(tdata, proc->label_decode, buflen);
	 }
	 if (!bin) {
	   return 1; // tuple is full
	 }
	 // do the translation 
	 for (i=0; i<buflen; i++) {
	   bin->buf[i] = proc->xlate[buf[i]];
	 }

	 bin->len = buflen;
	 
	 return 1;
	 break;
       }
     default:
       {
	 tool_print("No support for SED operation '%c'", proc->sedop);
	 return 0;
       }
     }
     return 0;
}

//return 1 if successful
//return 0 if no..
int procbuffer_destroy(void * vinstance) {
     //proc_instance_t * proc = (proc_instance_t*)vinstance;

     //free dynamic allocations
     //free(proc); // free this in the calling function

     return 1;
}

