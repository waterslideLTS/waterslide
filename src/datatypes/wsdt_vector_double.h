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
/*-----------------------------------------------------------------------------
 * file:   wsdt_vector_double.h
 * date:   17 Feb 2011
 *
 * waterslide datatype for an array of doubles.
 *---------------------------------------------------------------------------*/
#ifndef _WSDT_VECTOR_DOUBLE_H
#define _WSDT_VECTOR_DOUBLE_H

#include <stdint.h>
#include "waterslide.h"

#define WSDT_VECTOR_DOUBLE_STR "VECTOR_DOUBLE_TYPE"
#define WSDT_VECTOR_DOUBLE_MAX 128


/*-------------------------------------------------------------------
 * datatype for an array of doubles.
 *-----------------------------------------------------------------*/
typedef struct _wsdt_vector_double_t 
{
   unsigned int len;        /* number of elements in use */
   double value[WSDT_VECTOR_DOUBLE_MAX];
} wsdt_vector_double_t;


/*-----------------------------------------------------------------------------
 * wsdt_vector_double_insert
 *   appends an element to the vector if there is room. 
 * [in] dt - the instance to be added to
 * [in] val - the value to add
 * [out] ret - 0 if error, 1 if value added.
 *---------------------------------------------------------------------------*/
static inline int wsdt_vector_double_insert(wsdt_vector_double_t *dt, double val) 
{
   if (dt->len >= WSDT_VECTOR_DOUBLE_MAX) 
   {
      return 0;
   }

   dt->value[dt->len++] = val;
   return 1;
}


/*-----------------------------------------------------------------------------
 * wsdt_vector_double_insert_wsdata
 * appends an element to the vector if there is room. 
 * [in] dt - the wsdata_t instance to be added 
 *           (which must be of type vector_double)
 * [in] val - the value to add
 * [out] ret - 0 if error, 1 if value added.
 *  
 *---------------------------------------------------------------------------*/
static inline int wsdt_vector_double_insert_wsdata(wsdata_t * dt_wsd, double value) 
{
     wsdt_vector_double_t * dt = (wsdt_vector_double_t*)dt_wsd->data;
     return wsdt_vector_double_insert(dt, value);
}

#endif
