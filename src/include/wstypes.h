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

#ifndef _WSTYPES_H
#define _WSTYPES_H

#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_massivestring.h"
#include "datatypes/wsdt_hugeblock.h"
#include "datatypes/wsdt_largestring.h"
#include "datatypes/wsdt_bigstring.h"
#include "datatypes/wsdt_mediumstring.h"
#include "datatypes/wsdt_smallstring.h"
#include "datatypes/wsdt_tinystring.h"
#include "datatypes/wsdt_flush.h"
#include "datatypes/wsdt_mmap.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_uint16.h"
#include "datatypes/wsdt_uint8.h"
#include "datatypes/wsdt_int.h"
#include "datatypes/wsdt_int64.h"
#include "datatypes/wsdt_double.h"
#include "datatypes/wsdt_ts.h"
#include "datatypes/wsdt_monitor.h"
#include "datatypes/wsdt_vector_double.h"
#include "datatypes/wsdt_vector_uint32.h"
#include "timeparse.h"
#include <unistd.h> // for _SC_PAGESIZE
#include <errno.h>
#include "cppwrap.h"

#ifndef _WSUTIL
#define EXT extern
#else
#define EXT
#endif

// Globals
EXT wsdatatype_t * dtype_uint;
EXT wsdatatype_t * dtype_uint8;
EXT wsdatatype_t * dtype_uint16;
EXT wsdatatype_t * dtype_uint64;
EXT wsdatatype_t * dtype_double;
EXT wsdatatype_t * dtype_int;
EXT wsdatatype_t * dtype_int64;
EXT wsdatatype_t * dtype_string;
EXT wsdatatype_t * dtype_str;
EXT wsdatatype_t * dtype_binary;
EXT wsdatatype_t * dtype_fstr;
EXT wsdatatype_t * dtype_fixedstring;
EXT wsdatatype_t * dtype_bigstr;
EXT wsdatatype_t * dtype_bigstring;
EXT wsdatatype_t * dtype_largestring;
EXT wsdatatype_t * dtype_mediumstring;
EXT wsdatatype_t * dtype_massivestring;
EXT wsdatatype_t * dtype_hugeblock;
EXT wsdatatype_t * dtype_ts;
EXT wsdatatype_t * dtype_tuple;
EXT wsdatatype_t * dtype_monitor;
EXT wsdatatype_t * dtype_labelset;
EXT wsdatatype_t * dtype_label;
EXT wsdatatype_t * dtype_mmap;
EXT wsdatatype_t * dtype_flush;
EXT wsdatatype_t * dtype_array_uint;
EXT wsdatatype_t * dtype_array_double;
EXT wsdatatype_t * dtype_smallstring;
EXT wsdatatype_t * dtype_tinystring;
EXT wsdatatype_t * dtype_vector_double;
EXT wsdatatype_t * dtype_vector_uint32;

//get a string buffer from a data type
static inline int dtype_string_buffer(wsdata_t * member,
                                      char ** buf, int * len) {
     return member->dtype->to_string(member, buf, len);
}

static inline int dtype_get_uint(wsdata_t * member, uint64_t * o64) {
     return member->dtype->to_uint64(member, o64);
}

static inline int dtype_get_uint64(wsdata_t * member, uint64_t * o64) {
     return member->dtype->to_uint64(member, o64);
}

static inline int dtype_get_uint32(wsdata_t * member, uint32_t * o32) {
     return member->dtype->to_uint32(member, o32);
}

static inline int dtype_get_int(wsdata_t * member, int64_t * i64) {
     return member->dtype->to_int64(member, i64);
}

static inline int dtype_get_int64(wsdata_t * member, int64_t * i64) {
     return member->dtype->to_int64(member, i64);
}

static inline int dtype_get_int32(wsdata_t * member, int32_t * i32) {
     return member->dtype->to_int32(member, i32);
}

static inline int dtype_get_double(wsdata_t * member, double * dbl) {
     return member->dtype->to_double(member, dbl);
}

static inline wsdata_t * wsdata_create_hugebuffer(uint64_t, char**, uint64_t*);

//create a wsdata type with a buffer big enough to hold the specified length
static inline wsdata_t * wsdata_create_buffer(int len, char ** pbuf,
                                              int * plen) {
     wsdata_t * dep = NULL;
     if (len <= WSDT_TINYSTRING_LEN) {
          dep = wsdata_alloc(dtype_tinystring);
          if (dep) {
               wsdt_tinystring_t * tstr = (wsdt_tinystring_t*)dep->data;
               *plen = len;
               *pbuf = tstr->buf;
          }
     }
     else if (len <= WSDT_FIXEDSTRING_LEN) {
          dep = wsdata_alloc(dtype_fstr);
          if (dep) {
               wsdt_fixedstring_t * fstr = (wsdt_fixedstring_t*)dep->data;
               *plen = len;
               *pbuf = fstr->buf;
          }
     }
     else if (len <= WSDT_SMALLSTRING_LEN) {
          dep = wsdata_alloc(dtype_smallstring);
          if (dep) {
               wsdt_smallstring_t * smlstr = (wsdt_smallstring_t*)dep->data;
               *plen = len;
               *pbuf = smlstr->buf;
          }
     }
     else if (len <= WSDT_MEDIUMSTRING_LEN) {
          dep = wsdata_alloc(dtype_mediumstring);
          if (dep) {
               wsdt_mediumstring_t * mstr = (wsdt_mediumstring_t*)dep->data;
               *plen = len;
               *pbuf = mstr->buf;
          }
     }
     else if (len <= WSDT_BIGSTRING_LEN) {
          dep = wsdata_alloc(dtype_bigstring);
          if (dep) {
               wsdt_bigstring_t * bstr = (wsdt_bigstring_t*)dep->data;
               *plen = len;
               *pbuf = bstr->buf;
          }
     }
     else if (len <= WSDT_LARGESTRING_LEN) {
          dep = wsdata_alloc(dtype_largestring);
          if (dep) {
               wsdt_largestring_t * lrgstr = (wsdt_largestring_t*)dep->data;
               *plen = len;
               *pbuf = lrgstr->buf;
          }
     }
     else if (len <= WSDT_MASSIVESTRING_LEN) {
          dep = wsdata_alloc(dtype_massivestring);
          if (dep) {
               // char * buf = (char *)malloc(len);
               void * buf;
               long mem_page_sz = sysconf(_SC_PAGESIZE);
               int verify = posix_memalign(&buf, mem_page_sz, len);
               if(0 != verify)
               {
                    // show reason for error
                    fprintf(stderr, "wsdata_create_buffer: ERROR! posix_memalign failed...");
                    if(EINVAL == verify)
                    {
                         fprintf(stderr, "mem_page_sz = %ld is not a power of two or not a multiple of sizeof(void*)", mem_page_sz);
                    }
                    else if(ENOMEM == verify)
                    {
                         fprintf(stderr, "available memory is insufficient for len = %d bytes", len);
                    }
                    fprintf(stderr, "\n");

                    wsdata_delete(dep);
                    return NULL;
               }
               if (buf) {
                    wsdt_massivestring_t * str = (wsdt_massivestring_t *)dep->data;
                    str->buf = (char *)buf;
                    str->len = len;
                    *plen = len;
                    *pbuf = (char *)buf;
               }
               else {
                    wsdata_delete(dep);
                    return NULL;
               }
          }
     }
     else {
          fprintf(stderr, "len = %d exceeds WSDT_MASSIVESTRING_LEN...returning NULL\n", len);
          fprintf(stderr, "consider using wsdata_create_hugebuffer instead\n");
          return NULL;
     }

     return dep;
}

//create a wsdata type with a HUGE buffer; we envision using this function
// when size is bigger than we have with wsdt_massivestring_t in the latter
// part of wsdata_create_buffer.
static inline wsdata_t * wsdata_create_hugebuffer(uint64_t len, char ** pbuf,
                                              uint64_t * plen) {
     wsdata_t * dep = wsdata_alloc(dtype_hugeblock);
     if (dep) {
          wsdt_hugeblock_t * hb = (wsdt_hugeblock_t *)dep->data;
          if(hb->buf) {
               if(hb->len == len) {
                    *plen = len;
                    *pbuf = hb->buf;
                    return dep;
               }

               // free the existing (unlucky) buf that came to us
               // and proceed with creating a new one
               free(hb->buf);
               hb->buf = NULL;
          }

          void * buf;
          int verify = posix_memalign(&buf, sysconf(_SC_PAGESIZE), len);
          if (0 != verify) {
               // show reason for error
               fprintf(stderr, "wsdata_create_hugebuffer: ERROR! posix_memalign failed...");
               if(EINVAL == verify)
               {
                    fprintf(stderr, "alignment field = %ld is not a power of two or not a multiple of sizeof(void*)", 
                                    sysconf(_SC_PAGESIZE));
               }
               else if(ENOMEM == verify)
               {
                    fprintf(stderr, "available memory is insufficient for len = %" PRIu64 " bytes", len);
               }
               fprintf(stderr, "\n");

               wsdata_delete(dep);
               return NULL;
          }

          if (buf) {
               hb->buf = (char *)buf;
               hb->len = len;
               *plen = len;
               *pbuf = (char *)buf;
          }
          else {
               wsdata_delete(dep);
               return NULL;
          }
     }

     return dep;
}

static inline wsdata_t * wsdata_create_string(char * cpy_buffer, int len) {
     int plen;
     char * pbuf;

     wsdata_t * wsbuf = wsdata_create_buffer(len, &pbuf, &plen);
     if (!wsbuf) {
               return NULL;
     }
     if (plen < len) {
          wsdata_delete(wsbuf);
          return NULL;
     }
     wsdata_t * wsstr = wsdata_alloc(dtype_string);
     if (!wsstr) {
          wsdata_delete(wsbuf);
          return NULL;
     }
     wsdata_assign_dependency(wsbuf, wsstr);
     wsdt_string_t * str = (wsdt_string_t *)wsstr->data;
     str->buf = pbuf; 
     str->len = len; 

     memcpy(pbuf, cpy_buffer, len);
     return wsstr;
}

static inline wsdata_t * dtype_alloc_binary(int len) {
     int plen;
     char * pbuf;
     wsdata_t * wsbuf = wsdata_create_buffer(len, &pbuf, &plen);
     if (!wsbuf) {
          return NULL;
     }
     if (plen < len) {
          wsdata_delete(wsbuf);
          return NULL;
     }
     wsdata_t * wsstr = wsdata_alloc(dtype_binary);
     if (!wsstr) {
          wsdata_delete(wsbuf);
          return NULL;
     }
     wsdata_assign_dependency(wsbuf, wsstr);
     wsdt_binary_t * str = (wsdt_binary_t *)wsstr->data;
     str->buf = pbuf; 
     str->len = len; 

     return wsstr;
}


static inline wsdata_t * dtype_str2ts(char * str, int len) {
     if (timeparse_detect_date(str, len) == 2) {
          wsdata_t * wsd = wsdata_alloc(dtype_ts);
          if (wsd) {
               wsdt_ts_t * ts = (wsdt_ts_t*)wsd->data;
               if (len > 19) {
                    str[19] = '\0';
                    if (len > 20) {
                         char * ustr = str + 20;
                         ts->usec = atoi(ustr);
                    }
               }
               ts->sec = timeparse_str2time(str, STR2TIME_WSSTD_FORMAT);
          }
          return wsd;
     }
     return NULL;
}

static inline wsdata_t * dtype_detect_strtype(char * str, int len) {

     if (!str || !len) {
          return NULL;
     }

     if (str[0] == ' ') {
          str++;
          len--;
          if (len == 0) {
               return NULL;
          }
     }

     int offset = 0;
     int is_neg = 0;
     if (str[0] == '-') {
         is_neg = 1; 
         offset++;
         if (len <= offset) {
              return NULL;
         }
     }

     if (!isxdigit(str[offset])) {
          return NULL;
     }


     wsdata_t * wsd = dtype_str2ts(str, len);

     if (wsd) {
          return wsd;
     }
     
     int i;
     int is_int = 1;
     int dots = 0, colons = 0, ees = 0;
     int is_double = 1;


     //test for pure uints and doubles
     for (i = offset; i < len; i++) {
          if (!isdigit(str[i])) {
               is_int = 0;
               if (str[i] == '.') {
                    dots++;
                    if (dots > 1) {
                         is_double = 0;
                         if ((i < (len - 1)) &&
                             !isdigit(str[i+1])) {
                              break;
                         }
                    }
               }
               else if (str[i] == ':') {
                    is_double = 0;
                    colons++;
                    if(colons > 7) {
                         break;
                    }
               }
               else if (str[i] == 'E' || str[i] == 'e') {
                    ees++;
                    if (i == (len - 1) || ees > 1) {
                         is_double = 0;
                    }
               }
               else if (str[i] == '+' || str[i] == '-') {
                    if (i > 0) {
                         if ( (str[i-1] != 'E' && str[i-1] != 'e') ||
                              (i == (len - 1)) ||
                              (!isdigit(str[i+1])) ) {
                              is_double = 0;
                              break;
                         }
                    }
                    else {
                         is_double = 0;
                         break;
                    }
               }
               else {
                    is_double = 0;
                    if(!isxdigit(str[i])) {
                         break;
                    }
               }
          }
     }
     if (is_int) {
          if (is_neg) {
               if (len >= 10) {
                    wsd = wsdata_alloc(dtype_int64);
                    if (wsd) {
                         wsdt_int64_t * ti = (wsdt_int64_t*)wsd->data; 
                         *ti = (wsdt_int64_t)strtoll(str, NULL, 10);
                    }
               }
               else {
                    wsd = wsdata_alloc(dtype_int);
                    if (wsd) {
                         wsdt_int_t * ti = (wsdt_int_t*)wsd->data; 
                         *ti = (wsdt_int_t)strtol(str, NULL, 10);
                    }
               }
          }
          else if (len >= 10) {
               wsd = wsdata_alloc(dtype_uint64);
               if (wsd) {
                    wsdt_uint64_t * u64 = (wsdt_uint64_t*)wsd->data; 
                    //*u64 = (wsdt_uint64_t)atoll(str);
                    *u64 = (wsdt_uint64_t)strtoull(str, NULL, 10);
               }
          }
          else {
               wsd = wsdata_alloc(dtype_uint);
               if (wsd) {
                    wsdt_uint_t * u = (wsdt_uint_t*)wsd->data; 
                    //*u = (wsdt_uint_t)atoi(str);
                    *u = (wsdt_uint_t)strtoul(str, NULL, 10);
               }
          }
     }
     else if (is_double) {
          wsd = wsdata_alloc(dtype_double);
          if (wsd) {
               wsdt_double_t * dbl = (wsdt_double_t*)wsd->data; 
               *dbl = (wsdt_double_t)atof(str);
          }
     }

     return wsd;
}

static inline int dtype_is_exit_flush(wsdata_t * wsd) {
     if (wsd->dtype == dtype_flush) {
          wsdt_flush_t * flush = (wsdt_flush_t*)wsd->data; 
          if (flush->flag == WSDT_FLUSH_EXIT_MSG) {
               return 1;
          }
     }
     return 0;
}

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

void init_wstypes(void *);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSTYPES_H
