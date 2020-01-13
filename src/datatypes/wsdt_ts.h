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
#ifndef _WSDT_TS_H
#define _WSDT_TS_H
/* datatypes.h
 * here are common input/output datatypes for metadata processing
 */
#include <time.h>
#include <stdio.h>
#define WSDT_TS_MSEC(s,u) (((s) * 1000) + (((u) / 1000) % 1000))

static inline int wsdt_print_ts_sec_usec(FILE * stream, time_t tsec,
                                         unsigned int usec) {
     int s;
     struct tm tdata;
     struct tm *tp;
     time_t stime;

          
     /* these variables are not currently in use
     static unsigned b_sec;
     static unsigned b_usec;
     */

     s = tsec % 86400;
     stime = tsec - s;
     tp = gmtime_r(&stime, &tdata);

     if (usec) {
          return fprintf(stream,"%04d-%02d-%02dT%02d:%02d:%02d.%06uZ",
                         tp->tm_year+1900,
                         tp->tm_mon+1, tp->tm_mday,
                         s / 3600, (s % 3600) / 60,
                         s % 60,
                         usec);
     }
     else {
          return fprintf(stream,"%04d-%02d-%02dT%02d:%02d:%02dZ",
                         tp->tm_year+1900,
                         tp->tm_mon+1, tp->tm_mday,
                         s / 3600, (s % 3600) / 60,
                         s % 60);
     }
}


static inline int wsdt_print_ts_sec(FILE * stream, time_t tsec) {
     return wsdt_print_ts_sec_usec(stream, tsec, 0);
}

static inline int wsdt_print_ts(FILE * stream, wsdt_ts_t * ts) {
     return wsdt_print_ts_sec_usec(stream, ts->sec, (unsigned int)ts->usec);
}

static inline int wsdt_snprint_ts_sec_usec(char * buf, int len, time_t tsec,
                                           unsigned int usec) {
     int s;
     struct tm tdata;
     struct tm *tp;
     time_t stime;

          
     /* these variables are not currently in use
     static unsigned b_sec;
     static unsigned b_usec;
     */

     s = tsec % 86400;
     stime = tsec - s;
     tp = gmtime_r(&stime, &tdata);
     if (usec) {
          return snprintf(buf, len,"%04d-%02d-%02dT%02d:%02d:%02d.%06uZ",
                          tp->tm_year+1900,
                          tp->tm_mon+1, tp->tm_mday,
                          s / 3600, (s % 3600) / 60,
                          s % 60,
                          usec);
     }
     else {
          return snprintf(buf, len, "%04d-%02d-%02dT%02d:%02d:%02dZ",
                          tp->tm_year+1900,
                          tp->tm_mon+1, tp->tm_mday,
                          s / 3600, (s % 3600) / 60,
                          s % 60);
     }

}
static inline int wsdt_snprint_ts_sec(char * buf, int len, time_t tsec) {
     return wsdt_snprint_ts_sec_usec(buf, len, tsec, 0);
}

static inline int wsdt_snprint_ts(char * buf, int len, wsdt_ts_t * ts) {
     return wsdt_snprint_ts_sec_usec(buf, len, ts->sec, (unsigned int) ts->usec);
}



#endif
