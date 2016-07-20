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
#ifndef _WSDT_HUGEBLOCK_H
#define _WSDT_HUGEBLOCK_H
/* datatypes.h
 * here are common input/output datatypes for metadata processing
 */

#include <stdint.h>
#include "waterslide.h"


//the following is not a limit, just a big size
#define WSDT_HUGEBLOCK_LEN (1<<31)

#define WSDT_HUGEBLOCK_STR "HUGEBLOCK_TYPE"

typedef struct _wsdt_hugeblock_t {
     uint64_t len;      // this field is more like a capacity
                        // field; we use it to reserve room 
                        // for memory allocated
     uint64_t actual_len; // used to store actual length of 
                          // actual content (bytes) in our
                          // huge block
     uint32_t num_items; // can ignore when buf is not holding items
     uint32_t seqno;    // sequence number of this hugeblock
     uint16_t linklen;  // if used, this should specify link-layer len for items in hugeblock
     int linktype;      // if used, this should specify link-layer type for items in hugeblock
     char * buf;
} wsdt_hugeblock_t;

#endif
