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
//read in config files for matching with aho-corasick --- labels data upon match
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include "ahocorasick.h"
#include "label_match.h"
#include "waterslide.h"
#include "sysutil.h"

//change hex string into character string
static int process_hex_string(char * matchstr, int matchlen) {
     int i;
     char matchscratchpad[1500];
     int soffset = 0;
     
     for (i = 0; i < matchlen; i++) {
          if (isxdigit(matchstr[i])) {
               if (isxdigit(matchstr[i + 1])) {
                    matchscratchpad[soffset] = (char)strtol(matchstr + i, 
                                                              NULL, 16);
                    soffset++;
                    i++;
               }
          }
     }
     
     if (soffset) {
          //overwrite hex-decoded string on top of origional string..
          memcpy(matchstr,matchscratchpad, soffset);
     }
     
     return soffset;
}

int label_match_make_label(label_match_t * lm, const char * str) { 
     int rtn = 0;
     wslabel_t * newlabel = wsregister_label(lm->type_table, str);

     //find label
     if (!newlabel) {
          return 0;
     }
     int i;
     for (i = 1; i < lm->label_cnt; i++) {
          if (lm->label[i] == newlabel) {
               return i;
          }
     }
     //label not found.. create new one
     if (lm->label_cnt < LABEL_MATCH_MAX_LABELS) {
          lm->label[lm->label_cnt] = wsregister_label(lm->type_table, str);
          rtn = lm->label_cnt;
          lm->label_cnt++;
     }
     else {
          fprintf(stderr, "Max labels reached in match_label loading");
     }
     return rtn;
}

//read input match strings from input file
int label_match_loadfile(label_match_t * task, char * thefile) {
     FILE * fp;
     char line [2001];
     int linelen;
     char * linep;
     char * matchstr;
     int matchlen;
     char * endofstring;
     char * labelstr;
     int label;

     if ((fp = sysutil_config_fopen(thefile,"r")) == NULL) {
          error_print("label_match_loadfile input file %s could not be located\n", thefile);
          error_print("label_match Loadfile not found.");
          return 0;
     } 

     while (fgets(line, 2000, fp)) {
          //strip return
          linelen = strlen(line);
          if (line[linelen - 1] == '\n') {
               line[linelen - 1] = '\0';
               linelen--;
          }
          if ((linelen <= 0) || (line[0] == '#')) {
               continue;
          }

          linep = line;
          matchstr = NULL;
          labelstr = NULL;

          // read line - exact seq
          if (linep[0] == '"') {
               linep++;
               endofstring = (char *)strrchr(linep, '"');
               if (endofstring == NULL) {
                    continue;
               }
               endofstring[0] = '\0';
               matchstr = linep;
               matchlen = strlen(matchstr);
               sysutil_decode_hex_escapes(matchstr, &matchlen);
               linep = endofstring + 1;
          }

          else if (linep[0] == '{') {
               linep++;
               endofstring = (char *)strrchr(linep, '}');
               if (endofstring == NULL) {
                    continue;
               }
               endofstring[0] = '\0';
               matchstr = linep;

               matchlen = process_hex_string(matchstr, strlen(matchstr));
               if (!matchlen) {
                    continue;
               }

               linep = endofstring + 1;
          }
          else {
               continue;
          }

         
          if (matchstr) { 
               //find (PROTO)
               labelstr = (char *) strchr(linep,'(');
               endofstring = (char *) strrchr(linep,')');

               if (labelstr && endofstring && (labelstr < endofstring)) {
                    labelstr++;
                    endofstring[0] = '\0';

                    //turn protostring into label
                    label = label_match_make_label(task, labelstr);
                    ac_loadkeyword(task->ac_struct, matchstr, matchlen, label);
               }
               else  {
                    ac_loadkeyword(task->ac_struct, matchstr, matchlen, 0);
               }
          }
     }
     sysutil_config_fclose(fp);

     return 1;
}

wslabel_t * label_match_get_label(label_match_t * task, int labelid) {
     if ((labelid >= 0) && (labelid < LABEL_MATCH_MAX_LABELS)) {
          return task->label[labelid];
     }
     else {
          return NULL;
     }
}

//initialize variables
label_match_t* label_match_create(void * type_table) {
     label_match_t * task = NULL;
     task = (label_match_t *)calloc(1,sizeof(label_match_t));
     if (!task) {
          error_print("failed label_match_create calloc of task");
          return NULL;
     }

     task->ac_struct = ac_init();
     task->type_table = type_table;
     task->label_cnt = 1;

     return task;
}

ahoc_t * label_match_finalize(label_match_t * task) {
     ac_finalize(task->ac_struct);
     return task->ac_struct;
}

void label_match_destroy(label_match_t * task) {
     if (task) {
          ac_free(task->ac_struct);

          free(task);
     }
}

