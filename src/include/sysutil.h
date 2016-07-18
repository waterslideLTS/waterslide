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

#ifndef _SYSUTIL_H
#define _SYSUTIL_H

#include <stdint.h>
#include <time.h>
#include <netinet/in.h>
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/*
 *    structure for determining when a time boundary is reached
 *       .. useful in creating files every N minutes
 */
typedef struct _time_boundary_t {
     time_t current_boundary;   //timestamp of current block of time
     time_t prev_boundary;      //timestamp of previous block of time
     time_t boundary_ts;        //timestamp of next time boundary
     time_t increment_ts;       //how much to increment for each boundary
} time_boundary_t;

time_t sysutil_get_duration_ts(char * optarg);

static inline void sysutil_print_webchars(FILE * stream, int len, uint8_t * str) {
     int i;
     for (i = 0; i < len; i++) {
          switch(str[i]) {
          case '<':
               fprintf(stream, "&lt;");
               break;
          case '>':
               fprintf(stream, "&gt;");
               break;
          case '&':
               fprintf(stream, "&amp;");
               break;
          default:
               fprintf(stream, "%c", str[i]);
          }
     }
     fprintf(stream, "\n");
}

void sysutil_print_content_ascii(FILE *, uint8_t * /*content*/,
                                 int /*clen*/);
void sysutil_print_content(FILE * , uint8_t * , int );
void sysutil_print_content_web(FILE * , uint8_t * , int );
void sysutil_print_content_hex(FILE * , uint8_t * , int );
void sysutil_print_content_strings(FILE * , uint8_t * /*buf*/, 
                                   int /*buflen*/, int /*runlen*/);
void sysutil_print_content_strings_web(FILE * , uint8_t * /*buf*/, 
                                       int /*buflen*/, int /*runlen*/);

void sysutil_rename_file(char * /*source file*/, char * /*destination file*/);

int sysutil_name_timedfile(char * /*basefile*/, char * /*extension*/,
                              time_t /*filetime*/, time_t /*increment*/,
                              char * /*outfilename*/, int /*outfilename_len*/);



FILE * sysutil_open_timedfile(char * /*basefile*/, char * /*extension*/,
                              time_t /*filetime*/, time_t /*increment*/,
                              char * /*outfilename*/, int /*outfilename_len*/);

uint64_t sysutil_get_strbytes(char * optarg);
time_t sysutil_get_duration_ts(char * optarg);

time_t sysutil_get_hourly_increment_ts(char * optarg);
int sysutil_test_time_boundary(time_boundary_t * tb, time_t current_time);
void sysutil_print_time_interval(FILE * fp, time_t t);

void sysutil_printts(FILE * stream, time_t sec, time_t usec);
void sysutil_printts_sec(FILE * stream, time_t sec);
int sysutil_snprintts(char * buf, int len, time_t sec, time_t usec);
int sysutil_snprintts_sec(char * buf, int len, time_t sec);
int sysutil_snprintts_sec2(char * buf, int len, time_t sec);

//return 1 if file exists and has length > 0
int sysutil_file_exists(char * /*filename*/);

FILE * sysutil_config_fopen(const char *, const char *);
void sysutil_config_fclose(FILE *);
int set_sysutil_pConfigPath(const char *);
int get_sysutil_pConfigPath(uint32_t, char **, uint *);
void free_sysutil_pConfigPath(void);
int sysutil_prepend_config_path(char**);

int sysutil_decode_hex_escapes(char * /*str*/, int * /*len*/);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _SYSUTIL_H
