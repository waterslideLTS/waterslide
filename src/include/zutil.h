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

#ifndef _ZUTIL_H
#define _ZUTIL_H

#include <stdint.h>
#include <zlib.h>
#include "sysutil.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

static inline gzFile sysutil_open_timedfile_gz(char * basefile, char * extension,
                              time_t filetime, time_t increment,
                              char * outfilename, int outfilename_len) {
     char workingfile[2000];
     gzFile rfp;

     if (sysutil_name_timedfile(basefile, extension, filetime, increment, workingfile, 2000) == 0) {
          return NULL;
     }

     //open file
     if ((rfp =gzopen(workingfile, "wb9")) == NULL) {
          perror("opening gzipped stats file for writing");
          error_print("could not open gzip output stats file %s",workingfile);
          return NULL;
     }

     if (outfilename) {
          int len = strlen(workingfile);
          //truncate somehow..
          if (len > outfilename_len) {
               len = outfilename_len;
          }
          memcpy(outfilename, workingfile, len);
          outfilename[len] = 0;
     }

     return rfp;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _ZUTIL_H
