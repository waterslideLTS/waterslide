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
#ifndef _TIMEPARSE_H
#define _TIMEPARSE_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <time.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "dprint.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define STR2TIME_DEFAULT_FORMAT "%Y-%m-%dT%H:%M:%SZ"
#define STR2TIME_WSSTD_FORMAT "%Y-%m-%dT%H:%M:%SZ"
#if !defined(__USE_XOPEN) && !defined(__FreeBSD__)
#define _TIMEPARSE_UNUSABLE
#endif

#ifdef _TIMEPARSE_UNUSABLE
static inline time_t timeparse_str2time(char * timestr, const char * format) {
     return 0;
}
#else
static inline time_t timeparse_str2time(char * timestr, const char * format) {
     if (format == NULL) {
          format = STR2TIME_DEFAULT_FORMAT;
     }
     struct tm tm;
     if (strptime(timestr, format, &tm) == NULL) {
          perror("strptime");
     }
     return timegm(&tm);
     //return mktime(&tm);
}
#endif

#define MAX_FTIME 100
static inline time_t timeparse_file_time(const char *filename) {
     char buffer[MAX_FTIME];
     char * basename = strrchr((char *)filename, '/');
     if (!basename) {
          basename = (char *)filename;
     }
     else {
          basename++;
     }
     char * prefix_end = strrchr(basename, '.');

     if (!prefix_end) {
          return 0;
     }

     //search until number is found after period
     while (!isdigit(basename[0])) {
          char *newbase = strchr(basename,'.');
          if (!newbase) {
               break;
          }
          basename = newbase + 1;
     }

     int plen = prefix_end - basename;

     if (MAX_FTIME < plen ) {
          return 0;
     }
     memcpy((void * __restrict__)buffer, (void * __restrict__)basename, plen);
     buffer[plen] = '0';
     buffer[plen+1] = '0';
     buffer[plen+2] = '\0';
     return timeparse_str2time(buffer,"%Y%m%d.%H%M%S");
}

#define MAX_FTIME 100
static inline time_t timeparse_file_time2(const char *filename) {
     char buffer[MAX_FTIME+4];
     char * basename = strrchr((char *)filename, '/');
     if (!basename) {
          basename = (char *)filename;
     }
     else {
          basename++;
     }
     char * prefix_end = strrchr(basename, '.');

     if (!prefix_end) {
          return 0;
     }

     //search until number is found after period
     while (!isdigit(basename[0])) {
          char *newbase = strchr(basename,'.');
          if (!newbase) {
               break;
          }
          basename = newbase + 1;
     }

     int plen = prefix_end - basename;

     if (MAX_FTIME < plen ) {
          return 0;
     }
     memcpy(buffer, basename, plen);
     buffer[plen] = '0';
     buffer[plen+1] = '0';
     buffer[plen+2] = '\0';
     return timeparse_str2time(buffer,"%Y%m%d%H%M%S");
}

#ifdef _TIMEPARSE_UNUSABLE
static inline int timeparse_detect_time(char * buf, int len) {
     return 0;
}
static inline int timeparse_detect_date(char * buf, int len) {
     return 0;
}
#else
//return 1 if time detected but no usec time..
//return 2 if time has usec
//return 0 if time was not detected
static inline int timeparse_detect_time(char * buf, int len) {
     if ((len < 8) || (len > 15)) {
          dprint("invalid time - incorrect length");
          return 0;
     }
     //check periods
     if ((buf[2] != ':') || (buf[5] != ':')) {
          dprint("invalid time - nocolon");
          return 0;
     }
     int i;
     if (len > 8) {
          if (buf[8] != '.') {
               return 0;
          }
          for (i = 0; i < len; i++) {
               if (!isdigit(buf[i]) && (i != 2) && (i != 5) && (i != 8)) {
                    dprint("invalid time - non digit");
                    return 0;
               }
          }
          return 2;
     }
     else {
          for (i = 0; i < 8; i++) {
               if (!isdigit(buf[i]) && (i != 2) && (i != 5)) {
                    dprint("invalid time - non digit");
                    return 0;
               }
          }
          return 1;
     }
}

//return 1 if only date was detected
//return 2 if date and time was detected
//return 0 if date was not detected
static inline int timeparse_detect_date(char * buf, int len) {
     if ((len < 10) || (len > 26)) {
          dprint("invalid date - incorrect length");
          return 0;
     }
     int rtn = 1;
     if (len > 10) {
          rtn = 2;
          if ((buf[10] != ' ') || !timeparse_detect_time(buf + 11, len - 11)) {
               return 0;
          }
     }

     //check periods
     if ((buf[4] != '.') || (buf[7] != '.')) {
          dprint("invalid date - no periods");
          return 0;
     }
     int i;
     for (i = 0; i < 10; i++) {
          if (!isdigit(buf[i]) && (i != 4) && (i != 7)) {
               dprint("invalid date - not digit");
               return 0;
          }
     }
     return rtn;
}
#endif

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _TIMEPARSE_H
