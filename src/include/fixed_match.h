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

#ifndef _FIXED_MATCH_H
#define _FIXED_MATCH_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "stringhash3.h"

#define FM_CTYPE_IGNORE      0
#define FM_CTYPE_EQUAL       1
#define FM_CTYPE_LESS        2
#define FM_CTYPE_GREATER     3

//#define FM_CAM_TABLE_SIZE 65536 
#define FM_CAM_TABLE_SIZE 1024

/* maximum allowable permutations of upper- or lowercase */
#define FM_MAX_PERM_ALPHA 10

//for CAM based search- hashtable lookups
typedef struct _search_cam_t {
     uint8_t clen_type; //content equal, less, greater than
     uint16_t clen;  //content length...
     int len;    //length of strings to match
     int offset; //offset into content
     uint8_t matchatend;
     stringhashtable3_t * matchtable;  //hashtable to match
     struct _search_cam_t * next;
} search_cam_t;

typedef struct _cam_element_t {
     void * label;
     int isprintable;
} cam_element_t;

typedef void * (*fm_make_label)(const char *, void *);

// instance data structure
typedef struct _fixed_match_t {
     int cam_count;
     int matchstr_count;
     int nocase;
     search_cam_t * searchcams;  //head of link list of cams
     fm_make_label make_label;
     void * label_data;
} fixed_match_t;

     //initialize variables
fixed_match_t* fixed_match_create(fm_make_label, void * );

fixed_match_t* fixed_match_create_default(void * /*type_table*/);

//read input match strings from input file
int fixed_match_loadfile(fixed_match_t *, char * /*thefile*/);

void fixed_match_delete(fixed_match_t * fm);

void fixed_match_load_single(fixed_match_t *, char *, int, int, char *, int);

//test to see if item length is valid for a particular cam
static inline int fm_test_cam_len(search_cam_t * cam, int len) {
     switch(cam->clen_type) {
     case FM_CTYPE_IGNORE:
          return 1;

     case FM_CTYPE_EQUAL:
          if (len == cam->clen) {
               return 1;
          }
          return 0;

     case FM_CTYPE_LESS:
          if (len < cam->clen) {
               return 1;
          }
          return 0;
     case FM_CTYPE_GREATER:
          if (len > cam->clen) {
               return 1;
          }
          return 0;
     default:
          return 1;
     } 
}

static inline void * fixed_match_search(fixed_match_t * task, uint8_t * content, int clen) {
     search_cam_t * cam;
     stringhash3_t * sh_element;
     cam_element_t * cam_element;

     for (cam = task->searchcams; cam; cam = cam->next) {
          if ((clen >= (cam->offset + cam->len)) &&
              fm_test_cam_len(cam, clen)) {

               /*
                  print_search_string(source + cam->offset, cam->len,
                  cam->offset,"");
                  fprintf(stderr,"clen %d\n", len);
                */

               if (cam->matchatend) {
                    if ((sh_element = stringhash3_find(cam->matchtable,
                                                       content
                                                       + clen
                                                       - cam->len
                                                       - cam->offset)) != NULL) {

                         cam_element = (cam_element_t *) sh_element->data;
                         return cam_element->label;
                    }
               }
               else if ((sh_element = stringhash3_find(cam->matchtable,
                                                       content +
                                                       cam->offset)) != NULL) {
                    cam_element = (cam_element_t *) sh_element->data;
                    return cam_element->label;
               }
          }
     }
     return NULL;
}

#endif // _FIXED_MATCH_H
