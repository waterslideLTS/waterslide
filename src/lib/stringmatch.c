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
/* The following code was taken from 
   http://www-igm.univ-mlv.fr/~lecroq/string/
   to implement the Boyer-Moore-Horspool algorithm for string matching

   it was redesigned for speed and for better return values..
*/
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include "stringmatch.h"

//alphabet size...

// HOW-TO do case-insensitive boyer-moore matchin
//
// come up with an all lower-case and all upper-case version of your string of interest
// make a bmshift register
// memset() the shift register to 0
// call bmh_initstring_no_case() over the lower-case version of the string
// call bmh_initstring_no_case() over the upper-case version of the string
// 
// use bmh_match_string_no_case() to look for the string, passing in both
// the upper- and lower-case versions of the string
//


//initialize array offsets for matching - use in Boyer-Moore & Horspool..
//call this function before matching to initialize an offset array
// be sure to call a memset() over your shift array BEFORE using this function
// as the function will not clear the array for you.
// 

stringmatch_t * stringmatch_init(uint8_t * str, int len) {
     int i;
     stringmatch_t * sm = (stringmatch_t *)calloc(1, sizeof(stringmatch_t));
     if (!sm) {
          error_print("failed stringmatch_init calloc of sm");
          return 0;
     }
     sm->str = (uint8_t *)malloc(len);
     if (!sm->str) {
          error_print("failed stringmatch_init malloc of sm->str");
          return 0;
     }
     memcpy(sm->str, str, len);
     sm->len = len;
     for (i = 0; i < BMH_CHARSET_SIZE; ++i) {
          sm->bmshift[i] = len;
     }
     for (i = 0; i < (len - 1); ++i) {
          sm->bmshift[str[i]] = len - i - 1;
     }
     return sm;
}

stringmatch_t * stringmatch_init_nocase(uint8_t * str, int len) {
     int i;
     stringmatch_t * sm = (stringmatch_t *)calloc(1, sizeof(stringmatch_t));
     if (!sm) {
          error_print("failed stringmatch_init_nocase calloc of sm");
          return 0;
     }
     sm->str = (uint8_t *)malloc(len);
     if (!sm->str) {
          error_print("failed stringmatch_init_nocase malloc of sm->str");
          return 0;
     }
     sm->str2 = (uint8_t *)malloc(len);
     if (!sm->str2) {
          error_print("failed stringmatch_init_nocase malloc of sm->str2");
          return 0;
     }
     sm->nocase = 1;
     sm->len = len;
     for (i = 0; i < len; ++i) {
          sm->str[i] = tolower(str[i]);
          sm->str2[i] = toupper(str[i]);
     }
     for (i = 0; i < BMH_CHARSET_SIZE; ++i) {
          sm->bmshift[i] = len;
     }
     for (i = 0; i < (len - 1); ++i) {
          sm->bmshift[sm->str[i]] = len - i - 1;
          sm->bmshift[sm->str2[i]] = len - i - 1;
     }
     return sm;
}

void stringmatch_free(stringmatch_t * sm) {
     if (sm->str) {
          free(sm->str);
     }
     if (sm->str2) {
          free(sm->str2);
     }
     free(sm);
}

uint8_t * stringmatch(stringmatch_t * sm, uint8_t *haystack, int haylen) {
     int j;
     uint8_t c;
     /* Searching */
     j = 0;
     if (!haystack) {
          return NULL;
     }
     if (haylen < sm->len) {
          return NULL;
     }

     while (j <= (haylen - sm->len)) {
          c = haystack[j + sm->len - 1];
          if ((sm->str[sm->len - 1] == c) && 
              (memcmp(sm->str, haystack + j, sm->len - 1) == 0)) {
               return haystack + j;
          }
          j += sm->bmshift[c];
     }

     return NULL;
}


uint8_t * stringmatch_nocase(stringmatch_t * sm, uint8_t *haystack, int haylen) {
     int j,i;
     uint8_t c;
     /* Searching */
     j = 0;
     if (!haystack) {
          return NULL;
     }
     if (haylen < sm->len) {
          return NULL;
     }

     while (j <= (haylen - sm->len)) {
          c = haystack[j + sm->len - 1];
          if ((sm->str[sm->len - 1] == c) || (sm->str2[sm->len - 1] == c)) {
               for (i = 0; i < (sm->len - 1); i++) {
                    if ((sm->str[i] != (haystack+j)[i]) &&
                        (sm->str2[i] != (haystack+j)[i])) {
                         break;
                    }
               }
               if (i == (sm->len -1)) {
                    return haystack + j;
               }
          }
          j += sm->bmshift[c];
     }

     return NULL;
}

int stringmatch_offset_nocase(stringmatch_t * sm, uint8_t *haystack, int haylen) {
     uint8_t * match;

     match = stringmatch_nocase(sm, haystack, haylen);  
     if (!match) {
          return -1;
     }
     return (match - haystack) + sm->len;
}

int stringmatch_offset(stringmatch_t * sm, uint8_t *haystack, int haylen) {
     uint8_t * match;

     match = stringmatch(sm, haystack, haylen);  
     if (!match) {
          return -1;
     }
     return (match - haystack) + sm->len;
}

