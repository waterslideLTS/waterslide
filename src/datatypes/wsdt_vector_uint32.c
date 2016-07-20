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
 * File:  wsdt_vector_uint32.c
 * Date:  2/17/2011
 *
 * Implementation for vector of uint32_ts datatype.  Used wsdt_double and
 * wsdt_clusterbuffer as examples.
 *
 *
 * Questions to answer:
 * 1 -- why does wsdt_clusterbuffer not need to create a dependency inside
 *      the to_string method?
 * 2 -- Understand the consequences of wsdatatype_register_subelement
 *-----------------------------------------------------------------------------
 * History
 * 2/17/2011  Initial Creation.
 *---------------------------------------------------------------------------*/
//#define DEBUG 1
#include <stddef.h>              //offsetof()
#include "wsdt_vector_uint32.h"
#include "wsdt_uint.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wstypes.h"             //wsdata_create_buffer
#include "sysutil.h"

/* should this be configurable? is there an interface to do so? */
static char *delim_str = ",";

/*-----------------------------------------------------------------------------
 *                     F U N C T I O N   P R O T O S
 *---------------------------------------------------------------------------*/
static inline int append_to_output_string(char *dstbuf,
                                          unsigned int *pos,
                                          unsigned int dstlen,
                                          char *srcbuf,
                                          unsigned int srclen);

/*-----------------------------------------------------------------------------
 * wsdt_print_vector_uint32_wsdata
 * [in] stream    - output stream
 * [in] wsdata    - the wsdata instance to print
 * [in] printtype - the format we should print to (see waterslidedata.h)
 *---------------------------------------------------------------------------*/
static int wsdt_print_vector_uint32_wsdata(FILE * stream, 
					   wsdata_t * wsdata,
					   uint32_t printtype) 
{
   wsdt_vector_uint32_t * dt = (wsdt_vector_uint32_t*)wsdata->data;
   unsigned int i;
   unsigned int rtn = 0;

   switch(printtype)
   {
      case WS_PRINTTYPE_HTML:
	 rtn += fprintf(stream, "&lt;");
	 for (i=0; i < dt->len; i++)
	 {
	    rtn += (i>0) ? fprintf(stream, "%s", delim_str) : 0;
	    rtn += fprintf(stream, "%u", dt->value[i]);
	 }
	 rtn += fprintf(stream, "&gt;");
	 break;

      case WS_PRINTTYPE_TEXT:
	 rtn += fprintf(stream, "<");
	 for (i=0; i < dt->len; i++)
	 {
	    rtn += (i>0) ? fprintf(stream, "%s", delim_str) : 0;
	    rtn += fprintf(stream, "%u", dt->value[i]);
	 }
	 rtn += fprintf(stream, ">");
	 break;

      case WS_PRINTTYPE_BINARY:
	 /* should I support this or leave it blank? */
	 rtn = fwrite(&dt->value, sizeof(uint32_t), 1, stream);
	 break;

      case WS_PRINTTYPE_XML:   /* not supported yet */
      default:
	 return 0;
   }

   return (int)rtn;
}


/*-----------------------------------------------------------------------------
 * wsdt_init_vector_uint32
 *   zero out the length, ignore the string contents.
 *---------------------------------------------------------------------------*/
void wsdt_init_vector_uint32(wsdata_t * wsdata, wsdatatype_t * dtype) 
{
     if (wsdata->data) 
     {
          wsdt_vector_uint32_t * ls = (wsdt_vector_uint32_t*)wsdata->data;
          ls->len = 0;
     }
}


/*-----------------------------------------------------------------------------
 * wsdt_to_string_vector_uint32
 *   converts a vector_uint32 to a string and copies the data.
 *   The general procedure (including the dependency handling) is patterned
 *   after wsdt_double.c
 *
 * [in] wsdata - the instance
 * [out] buf   - will point to the buffer containing the string
 * [out] len   - will contain the length of the buffer
 *---------------------------------------------------------------------------*/
int wsdt_to_string_vector_uint32(wsdata_t *wsdata, char **buf, int*len)
{
   wsdt_vector_uint32_t *dt = (wsdt_vector_uint32_t *)wsdata->data;

   char * lbuf = 0;
   int llen = 0;
   unsigned int bufpos = 0;
   unsigned int i;
   int rv;

   wsdata_t *wsdu;

   /* 54   -> max string size of uint32_t as given in wsdt_double.c,
    * *len -> times the number of uint32_ts to print out, 
    * +len -> each of the delimiters, roughly, 
    * +3   -> the '<' and '>' symbols and a \0   */
   if ((wsdu = wsdata_create_buffer(54*(dt->len)+dt->len+2, 
				    &lbuf, &llen)) == NULL)
   {
      return 0;
   }

   /* Add each of the vector elements. Add '<' first and '>' last */
   rv = append_to_output_string(lbuf, &bufpos, llen-1, "<", 1);
   for ( i=0; (i < dt->len) && (rv != -1); i++ )
   {
      char elembuf[54];
      int elembuf_len;

      if (i != 0)
      {
         append_to_output_string(lbuf, &bufpos, llen-1, delim_str, 1);
      }

      elembuf_len = snprintf(elembuf, 54, "%u", dt->value[i]);

      rv = append_to_output_string(lbuf, &bufpos, llen-1,
                                   elembuf, elembuf_len);
   }

   append_to_output_string(lbuf, &bufpos, llen-1, ">", 1);
   
   if (bufpos > 0)
   {
      wsdata_assign_dependency(wsdu, wsdata);
      *buf = lbuf;
      *len = bufpos;
      return 1;
   }
   else
   {
      wsdata_delete(wsdu);
      return 0;
   }
}


/*-----------------------------------------------------------------------------
 * wsdt_vector_uint32_hash
 *---------------------------------------------------------------------------*/
ws_hashloc_t *wsdt_vector_uint32_hash(wsdata_t * wsdata)
{
     if (!wsdata->has_hashloc) 
     {
          wsdt_vector_uint32_t *dt = (wsdt_vector_uint32_t*)wsdata->data;
          wsdata->hashloc.offset = dt->value; 
          wsdata->hashloc.len = dt->len * sizeof(uint32_t);
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}  


/*-----------------------------------------------------------------------------
 * datatypeloader_init
 *---------------------------------------------------------------------------*/
int datatypeloader_init(void * type_list) 
{
   wsdatatype_t *dt;
   dt = wsdatatype_register(type_list,
			    WSDT_VECTOR_UINT32_STR,
			    sizeof(wsdt_vector_uint32_t),
			    wsdt_vector_uint32_hash,
			    wsdt_init_vector_uint32,
			    wsdatatype_default_delete,
			    wsdt_print_vector_uint32_wsdata,
			    wsdatatype_default_snprint,
			    wsdatatype_default_copy,
			    wsdatatype_default_serialize);
   
   dt->to_string = wsdt_to_string_vector_uint32;

   /* not sure what capability this provides... */
   wsdatatype_register_subelement(dt, type_list,
				  "LENGTH", "UINT_TYPE",
				  offsetof(wsdt_vector_uint32_t, len));

     return 1;
}


/*-----------------------------------------------------------------------------
 * append_to_output_string
 *   appends text to the output string, checking for overruns as it goes.
 *
 * [in] dstbuf - the base destination buffer to which we are writing.
 * [in/out]    - pointer to the variable that holds the offset into the
 *               dstbuf into which we are writing.  This variable is
 *               updated on exit.
 * [in] dstlen - the total length of dstbuf, excluding space for a trailing \0
 *               (i.e., writeable space is dstlen - *pos)
 * [in] srcbuf - the source buffer
 * [in] srclen - number of bytes to be copied from the source buffer
 * [out] ret   - the number of bytes copied, or -1 on an error.
 *---------------------------------------------------------------------------*/
static inline int append_to_output_string(char *dstbuf,
                                          unsigned int *pos,
                                          unsigned int dstlen,
                                          char *srcbuf,
                                          unsigned int srclen)
{
   unsigned int i = 0;

   if (dstlen - *pos < srclen)
      return -1;

   while (i < srclen)
   {
      dstbuf[(*pos)++] = srcbuf[i++];
   }

   dstbuf[*pos] = '\0';
   return srclen;
}
