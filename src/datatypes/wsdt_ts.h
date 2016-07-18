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

static inline int wsdt_print_ts_sec(FILE * stream, time_t tsec)
{
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
     return fprintf(stream,"%04d.%02d.%02d %02d:%02d:%02d",
                    tp->tm_year+1900,
                    tp->tm_mon+1, tp->tm_mday,
                    s / 3600, (s % 3600) / 60,
                    s % 60);
}

static inline int wsdt_print_ts(FILE * stream, wsdt_ts_t * ts) {
     int rtn = 0;
     rtn += wsdt_print_ts_sec(stream,ts->sec);
     if (ts->usec) {
          rtn += fprintf(stream, ".%06u", (unsigned int)ts->usec);
     }
     return rtn;
}

static inline int wsdt_snprint_ts_sec(char * buf, int len, time_t tsec)
{
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
     return snprintf(buf, len, "%04d.%02d.%02d %02d:%02d:%02d",
                    tp->tm_year+1900,
                    tp->tm_mon+1, tp->tm_mday,
                    s / 3600, (s % 3600) / 60,
                    s % 60);
}

static inline int wsdt_snprint_ts(char * buf, int len, wsdt_ts_t * ts) {
     int rtn;
     rtn = wsdt_snprint_ts_sec(buf, len, ts->sec);
     if ((rtn > 0) && ts->usec && (rtn < len)) {
          int l;
          l = snprintf(buf + rtn, len - rtn, ".%06u", (unsigned int)ts->usec);
          if (l > 0) {
               rtn += l;
          }
     }

     return rtn;
}



#endif
