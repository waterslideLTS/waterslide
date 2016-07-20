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
#ifndef _WSPROTO_LIB_H_
#define _WSPROTO_LIB_H_
#include <string>
#include "wsproto.pb.h"
#include "waterslide.h"
#include "waterslidedata.h"
#include "wstypes.h"
#include "mimo.h"
#include "listhash.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_labelset.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_uint16.h"
#include "datatypes/wsdt_uint8.h"
#include "datatypes/wsdt_label.h"
#include "datatypes/wsdt_int.h"
#include "datatypes/wsdt_vector_double.h"
#include "datatypes/wsdt_vector_uint32.h"
#include "datatypes/wsdt_array_double.h"
#include "datatypes/wsdt_array_uint.h"

#ifdef __cplusplus
CPP_OPEN
#endif

// Format ID
//   pbmeta is format 1 (see ws_protobuf.h)
//   wsproto is format 2
#define WSPROTO_FORMAT_ID 2

// Version history
//   version 1: initial wsproto format
//   version 2: flattened nested protocol buffer structures (2013-07-09)
//   version 3: added support for ipv6 data types (2013-07-18)
#define WSPROTO_FORMAT_VERSION 3
#define WSPROTO_EARLIEST_SUPPORTED_FORMAT_VERSION 2

// loads data from the wsdata_t into the wsproto protocol buffer data 
// structures
static inline void wsproto_fill_data(wsproto::wsdata * sdata,
                                      wsdata_t * member) {
     // write out the member labels
     for (int i = 0; i < member->label_len; i++) {
          sdata->add_label(member->labels[i]->name);
     }
     
     // write out the member data
     if (member->dtype == dtype_tuple) {
          wsdt_tuple_t * tuple = (wsdt_tuple_t*)member->data;

          // set the data type
          sdata->set_dtype(wsproto::wsdata::TUPLE_TYPE);

          // iterate through the tuple elements
          for (int i = 0; i < (int)tuple->len; i++) {
               wsproto::wsdata * element_data = sdata->add_tuple_member();
               // recursively fill tuple elements
               wsproto_fill_data(element_data, tuple->member[i]);
          }
     }
     else if (member->dtype == dtype_string) {
          wsdt_string_t * str = (wsdt_string_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::STRING_TYPE);

          // set the string data
          sdata->set_string_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_binary) {
          wsdt_binary_t * bin = (wsdt_binary_t *)member->data;

          // set the data type
          sdata->set_dtype(wsproto::wsdata::BINARY_TYPE);

          // set the string data
          sdata->set_bytes_data(bin->buf, bin->len);
     }
     else if (member->dtype == dtype_tinystring) {
          wsdt_tinystring_t * str = (wsdt_tinystring_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::TINYSTRING_TYPE);

          // set the string data
          sdata->set_string_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_fixedstring) {
          wsdt_fixedstring_t * str = (wsdt_fixedstring_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::FIXEDSTRING_TYPE);

          // set the string data
          sdata->set_string_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_smallstring) {
          wsdt_smallstring_t * str = (wsdt_smallstring_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::SMALLSTRING_TYPE);

          // set the string data
          sdata->set_string_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_mediumstring) {
          wsdt_mediumstring_t * str = (wsdt_mediumstring_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::MEDIUMSTRING_TYPE);

          // set the string data
          sdata->set_string_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_bigstring) {
          wsdt_bigstring_t * str = (wsdt_bigstring_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::BIGSTRING_TYPE);

          // set the string data
          sdata->set_string_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_double) {
          wsdt_double_t * dbl = (wsdt_double_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::DOUBLE_TYPE);

          // set the double
          sdata->set_double_data(*dbl);
     }
     else if (member->dtype == dtype_uint) {
          wsdt_uint_t * num = (wsdt_uint_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::UINT_TYPE);

          // set the uint32
          sdata->set_uint32_data(*num);
     }
     else if (member->dtype == dtype_uint64) {
          wsdt_uint64_t * num = (wsdt_uint64_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::UINT64_TYPE);

          // set the uint64
          sdata->set_uint64_data(*num);
     }
     else if (member->dtype == dtype_uint16) {
          wsdt_uint16_t * num = (wsdt_uint16_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::UINT16_TYPE);

          // set the uint16
          sdata->set_uint32_data(*num);
     }
     else if (member->dtype == dtype_uint8) {
          wsdt_uint8_t * num = (wsdt_uint8_t *)member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::UINT8_TYPE);

          // set the uint8
          sdata->set_uint32_data(*num);
     }
     else if (member->dtype == dtype_label) {
          wsdt_label_t * label = (wsdt_label_t *) member->data;
          
          // set the data type
          sdata->set_dtype(wsproto::wsdata::LABEL_TYPE);

          // set the string data
          sdata->set_string_data((*label)->name);
     }
     else if (member->dtype == dtype_ts) {
          wsdt_ts_t * ts = (wsdt_ts_t *)member->data;
	
          sdata->set_dtype(wsproto::wsdata::TS_TYPE);
          sdata->set_uint64_data((uint64_t)ts->sec);
          sdata->set_uint64_data2((uint64_t)ts->usec);
     }
     else if (member->dtype == dtype_int) {
          wsdt_int_t * num = (wsdt_int_t *)member->data;

          // set the data type
          sdata->set_dtype(wsproto::wsdata::INT_TYPE);

          // set the ip
          sdata->set_sint32_data(*num);
     }
     else if (member->dtype == dtype_labelset) {
          wsdt_labelset_t * lset = (wsdt_labelset_t*)member->data;

          // set the data type
          sdata->set_dtype(wsproto::wsdata::LABELSET_TYPE);

          // set the len
          sdata->set_sint32_data(lset->len);

          // set the hash
          sdata->set_uint64_data((uint64_t)lset->hash);

          // setup and recursively copy all wslabels existing
          int labelarray_maxlen = 1000, labelarray_curlen = 0, index = 0;
          char * labelarray = (char *)malloc(labelarray_maxlen);
          if(!labelarray) {
               fprintf(stderr, "wsproto - unable to allocate memory\n");
               return;
          }

          int maxlen_changed = 0;
          // iterate though the labels in the set
          for (int i = 0; i < WSDT_LABELSET_MAX; i++) {
               if(lset->labels[i]) {
                    labelarray_curlen += 16 + strlen(lset->labels[i]->name);
                    while(labelarray_curlen > labelarray_maxlen) {
                         labelarray_maxlen = (labelarray_maxlen<<1);
                         if(labelarray_maxlen < 0) {
                              // an overflow has occured :)
                              labelarray_maxlen = (int)((uint32_t)(1<<31)-1);
                         }
                         maxlen_changed = 1;
                    }

                    if(maxlen_changed) {
                         labelarray = (char *)realloc(labelarray, labelarray_maxlen);
                         if(!labelarray) {
                              fprintf(stderr, "wsproto - unable to allocate memory\n");
                              return;
                         }
                         maxlen_changed = 0; // reset
                    }

                    // memory is available...store current wslabel and set all wslabels in a single bundle
                    labelarray[index++] = lset->labels[i]->registered;
                    labelarray[index++] = lset->labels[i]->search;
                    uint16_t *index_id_ptr = (uint16_t*)(labelarray + index);
                    *index_id_ptr = lset->labels[i]->index_id;
                    index += 2;
                    uint64_t *hash_ptr = (uint64_t*)(labelarray + index);
                    *hash_ptr = lset->labels[i]->hash;
                    index += 8;
                    uint32_t *namelen_ptr = (uint32_t*)(labelarray + index);
                    *namelen_ptr = strlen(lset->labels[i]->name);
                    index += 4;
                    memcpy(labelarray + index, lset->labels[i]->name, strlen(lset->labels[i]->name));
                    index += strlen(lset->labels[i]->name);
               }
          }

          // set the array of labels
          sdata->set_bytes_data(labelarray, labelarray_curlen);
     }
     else if (member->dtype == dtype_vector_double) {
          wsdt_vector_double_t * dvec = (wsdt_vector_double_t*)member->data;

          // set the data type
          sdata->set_dtype(wsproto::wsdata::VECTOR_DOUBLE_TYPE);

          // set the len
          sdata->set_uint32_data(dvec->len);

          // set the value
          sdata->set_bytes_data((char*)dvec->value, dvec->len * sizeof(double));
     }
     else if (member->dtype == dtype_vector_uint32) {
          wsdt_vector_uint32_t * uvec = (wsdt_vector_uint32_t*)member->data;

          // set the data type
          sdata->set_dtype(wsproto::wsdata::VECTOR_UINT32_TYPE);

          // set the len
          sdata->set_uint32_data(uvec->len);

          // set the value
          sdata->set_bytes_data((char*)uvec->value, uvec->len * sizeof(uint32_t));
     }
     else if (member->dtype == dtype_array_double) {
          wsdt_array_double_t * darr = (wsdt_array_double_t*)member->data;

          // set the data type
          sdata->set_dtype(wsproto::wsdata::ARRAY_DOUBLE_TYPE);

          // set the len
          sdata->set_sint32_data(darr->len);

          // set the value
          sdata->set_bytes_data((char*)darr->value, darr->len * sizeof(double));
     }
     else if (member->dtype == dtype_array_uint) {
          wsdt_array_uint_t * uarr = (wsdt_array_uint_t*)member->data;

          // set the data type
          sdata->set_dtype(wsproto::wsdata::ARRAY_UINT_TYPE);

          // set the len
          sdata->set_sint32_data(uarr->len);

          // set the value
          sdata->set_bytes_data((char*)uarr->value, uarr->len * sizeof(uint32_t));
     }
     else {
          fprintf(stderr, "wsproto - unsupported datatype\n");
     }
}

static inline wsdata_t * wsproto_fill_buffer_alloc(wsproto::wsdata * wsproto,
                                                    wsdata_t * input_data, int send_labels) {

     wsproto->Clear();
    
     // XXX: currently assumes that input is of type 'dtype_tuple'
     if (input_data->dtype == dtype_tuple) {
          // load the data into the protocol buffer datastructure
          wsproto_fill_data(wsproto, input_data);
     }
     else {
          return NULL;
     }

     uint64_t mlen = (uint64_t)wsproto->ByteSize();
     wsdata_t * wsd_bin = dtype_alloc_binary(mlen);
     if (!wsd_bin) {
          return NULL;
     }
     wsdt_binary_t * bin = (wsdt_binary_t *)wsd_bin->data;
     wsproto->SerializeToArray(bin->buf, bin->len); 

     return wsd_bin;
}

static inline int wsproto_fill_buffer(wsproto::wsdata * wsproto, 
                                      wsdata_t * input_data,
                                      char * buf, uint32_t buflen) {
     wsproto->Clear();

     if (input_data->dtype == dtype_tuple) {
          // load the data into the protocol buffer datastructure
          wsproto_fill_data(wsproto, input_data);
     }
     else {
          return 0;
     }

     uint64_t mlen = (uint64_t) wsproto->ByteSize();
     if (buflen >= mlen) {
          //wsproto->SerializeToArray(buf, buflen); 
          wsproto->SerializeToArray(buf, mlen); 
          return mlen;
     }

     return 0;
}

static inline wsproto::wsdata * wsproto_init() {
     // verifies that you have not accidentally linked against a version of 
     // the library which is incompatible with the version of the headers you 
     // compiled with. If a version mismatch is detected, the program will 
     // abort.
     GOOGLE_PROTOBUF_VERIFY_VERSION;

     wsproto::wsdata * wsproto = new wsproto::wsdata;
     return wsproto;
}

static inline void wsproto_destroy(wsproto::wsdata * wsproto) {
     delete wsproto;
}

static inline int wsproto_header_writefp(uint16_t protocol, uint16_t version, FILE * fp) {
     // get the length of the version number
     uint64_t mlen = sizeof(uint16_t) + sizeof(uint16_t);

     // write out the length
     fwrite(&mlen, sizeof(uint64_t), 1, fp);
     if (!mlen) {
          return 0;
     }

     // write out the protocol and version
     fwrite(&protocol, sizeof(uint16_t), 1, fp);
     fwrite(&version, sizeof(uint16_t), 1, fp);

     return mlen;
}

static inline int wsproto_writefp(wsproto::wsdata * wsproto, FILE * fp) {
     uint64_t mlen = (uint64_t)wsproto->ByteSize();
     fwrite(&mlen, sizeof(uint64_t), 1, fp); 
     if (!mlen) {
          return 0;
     }
#ifdef FULL_PROTOBUF
     wsproto->SerializeToFileDescriptor(fileno(fp));
#else
     char * buf = (char *)malloc(mlen);
     if (!buf) {
          error_print("failed wsproto_writefp malloc of buf");
          return 0;
     }
     wsproto->SerializeToArray(buf, mlen); 

     fwrite(buf, mlen, 1, fp); 

     free(buf);
#endif
     return mlen;
}

static inline int wsproto_tuple_writefp(wsproto::wsdata * wsproto, 
                                         wsdata_t * input_data,
                                         FILE * fp) {
     wsproto->Clear();
    
     // for now, only allow tuples 
     if (input_data->dtype == dtype_tuple) {
          // load the data into the protocol buffer datastructure
          wsproto_fill_data(wsproto, input_data);
     }    
     else {
          return 0;
     }

     return wsproto_writefp(wsproto, fp);
}

static inline wslabel_t * wsproto_register_label(const std::string & label, void * type_table) {
     wslabel_t * wslabel = NULL;

     // lookup by name (or register if it isn't found)
     int len = label.length();

     if (len) {
          wslabel = wsregister_label(type_table, label.c_str());
     }
     else {
          fprintf(stderr,"label has no name\n");
     }
     return wslabel;
}

static inline void wsproto_decode_member(const wsproto::wsdata * rdata, void * type_table, wsdata_t * tdata) {
     wsdata_t * member = NULL;
     wsproto::wsdata_datatype datatype = rdata->dtype();
     std::string errmsg = "";

     if (datatype == wsproto::wsdata::TUPLE_TYPE) {
          fprintf(stderr,"implementation error: tuples shouldn't be processed in wsproto_decode_member\n");
          return;
     }
     else if (datatype == wsproto::wsdata::STRING_TYPE || 
              datatype == wsproto::wsdata::TINYSTRING_TYPE || 
              datatype == wsproto::wsdata::FIXEDSTRING_TYPE || 
              datatype == wsproto::wsdata::SMALLSTRING_TYPE || 
              datatype == wsproto::wsdata::MEDIUMSTRING_TYPE ||
              datatype == wsproto::wsdata::BIGSTRING_TYPE) {
          if(rdata->has_string_data()) {
               const std::string & str = rdata->string_data();
               int len = str.length();
               const char * buf = str.c_str();
               member = tuple_create_string_wsdata(tdata, NULL, len);
               if (member) {
                    wsdt_string_t * str = (wsdt_string_t *)member->data;
                    memcpy(str->buf, buf, len);
               }     
          }
          else {
               errmsg = "DATA IS NOT AN WSPROTO-SUPPORTED STRING TYPE";
          }
     }
     else if (datatype == wsproto::wsdata::BINARY_TYPE) {
          if(rdata->has_bytes_data()) {
               const std::string & str = rdata->bytes_data();
               int len = str.length();
               const char * buf = str.c_str();
               member = tuple_create_binary_wsdata(tdata, NULL, len);
               if (member) {
                    wsdt_binary_t * bin = (wsdt_binary_t *)member->data;
                    memcpy(bin->buf, buf, len);
               }
          }
          else {
               errmsg = "NO BINARY_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::DOUBLE_TYPE) {
          if(rdata->has_double_data()) {
               double dbl = rdata->double_data();
               member = tuple_member_create_wsdata(tdata, dtype_double, NULL);
               if (member) {
                    wsdt_double_t * mdbl = (wsdt_double_t*)member->data;
                    *mdbl = dbl;
               }
          }
          else {
               errmsg = "NO DOUBLE_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::UINT_TYPE) {
          if (rdata->has_uint32_data()) {
               uint32_t num = rdata->uint32_data();
               member = tuple_member_create_wsdata(tdata, dtype_uint, NULL);
               if(member) {
                    wsdt_uint_t * u = (wsdt_uint_t*)member->data;
                    *u = num;
               }
          }
          else {
               errmsg = "NO UINT_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::UINT64_TYPE) {
          if (rdata->has_uint64_data()) {
               uint64_t num = rdata->uint64_data();
               member = tuple_member_create_wsdata(tdata, dtype_uint64, NULL);
               if(member) {
                    wsdt_uint64_t * u = (wsdt_uint64_t*)member->data;
                    *u = num;
               }
          }
          else {
               errmsg = "NO UINT64_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::UINT16_TYPE) {
          if (rdata->has_uint32_data()) {
               uint32_t num = rdata->uint32_data();
               member = tuple_member_create_wsdata(tdata, dtype_uint16, NULL);
               if(member) {
                    wsdt_uint16_t * u = (wsdt_uint16_t*)member->data;
                    *u = (uint16_t)num;
               }
          }
          else {
               errmsg = "NO UINT16_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::UINT8_TYPE) {
          if (rdata->has_uint32_data()) {
               uint32_t num = rdata->uint32_data();
               member = tuple_member_create_wsdata(tdata, dtype_uint8, NULL);
               if(member) {
                    wsdt_uint8_t * u = (wsdt_uint8_t*)member->data;
                    *u = (uint8_t)num;
               }
          }
          else {
               errmsg = "NO UINT8_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::LABEL_TYPE) {
          if(rdata->has_string_data()) {
               member = tuple_member_create_wsdata(tdata, dtype_label, NULL);
               if(member) {
                    wsdt_label_t * lbl = (wsdt_label_t *)member->data;
                    *lbl = wsproto_register_label(rdata->string_data(), type_table);
               }
          }
          else {
               errmsg = "NO LABEL_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::TS_TYPE) {
          if(rdata->has_uint64_data() && rdata->has_uint64_data2()) {
               uint64_t sec = rdata->uint64_data();
               uint64_t usec = rdata->uint64_data2();
               member = tuple_member_create_wsdata(tdata, dtype_ts, NULL);
               if(member) {
                    wsdt_ts_t * ts = (wsdt_ts_t*) member->data;
                    if(sizeof(wsdt_ts_t)>>1  == 4) {
                         ts->sec=(uint32_t) sec;
                         ts->usec=(uint32_t) usec;
                    }
                    else if(sizeof(wsdt_ts_t)>>1 == 8) {
                         ts->sec=sec;
                         ts->usec=usec;
                    }
                    else {
                         fprintf(stderr, "unknown sizeof(wsdt_ts_t)\n");
                    }
               }
          }          
          else {
               errmsg = "NO TS_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::INT_TYPE) {
          if (rdata->has_sint32_data()) {
               int num = rdata->sint32_data();
               member = tuple_member_create_wsdata(tdata, dtype_int, NULL);
               if(member) {
                    wsdt_int_t * u = (wsdt_int_t*)member->data;
                    *u = num;
               }
          }
          else {
               errmsg = "NO INT_TYPE DATA";
          }
     }
     else if (datatype == wsproto::wsdata::LABELSET_TYPE) {
          if(rdata->has_sint32_data() && rdata->has_uint64_data()) {
               int len = (int)rdata->sint32_data();
               uint64_t hash = (uint64_t)rdata->uint64_data();
               member = tuple_member_create_wsdata(tdata, dtype_labelset, NULL);
               if (member) {
                    wsdt_labelset_t * lset = (wsdt_labelset_t *)member->data;
                    lset->len = len;
                    lset->hash = hash;

                    // get wslabels
                    if(rdata->has_bytes_data()) {
                         const std::string & str = rdata->bytes_data();
                         int strlength = str.length();
                         const char * buf = str.c_str();

                         int index = 0;
                         for(int i = 0; i < WSDT_LABELSET_MAX && index < strlength; i++) {
                              lset->labels[i] = new wslabel_t;
                              lset->labels[i]->registered = buf[index++];
                              lset->labels[i]->search = buf[index++];
                              uint16_t *index_id_ptr = (uint16_t *)(buf + index);
                              lset->labels[i]->index_id = index_id_ptr[0];
                              index += 2;
                              uint64_t *hash_ptr = (uint64_t *)(buf + index);
                              lset->labels[i]->hash = hash_ptr[0];
                              index += 8;
                              uint32_t *namelen_ptr = (uint32_t *)(buf + index);
                              index += 4;

                              lset->labels[i]->name = (char *)calloc(1, namelen_ptr[0] + 1);
                              memcpy(lset->labels[i]->name, buf+index, namelen_ptr[0]);
                              index += namelen_ptr[0];
                         }
                    }
                    else {
                         errmsg = "NO LABELS in LABELSET ARRAY";
                    }
               }
          }
          else {
               errmsg = "NO LABELSET_TYPE DATA";
          }
     }
     else if(datatype == wsproto::wsdata::VECTOR_DOUBLE_TYPE) {
          if(rdata->has_uint32_data()) {
               member = tuple_member_create_wsdata(tdata, dtype_vector_double, NULL);
               if (member) {
                    wsdt_vector_double_t * dvec = (wsdt_vector_double_t *)member->data;
                    dvec->len = (unsigned int)rdata->uint32_data();
                    if(rdata->has_bytes_data()) {
                         const std::string & str = rdata->bytes_data();
                         const double * buf = (double *)str.c_str();
                         for(uint32_t i = 0; i < dvec->len; i++) {
                              dvec->value[i] = buf[i];
                         }
                    }
               }
               else {
                    errmsg = "LEN not set in VECTOR_DOUBLE";
               }
          }
          else {
               errmsg = "NO VECTOR_DOUBLE_TYPE DATA";
          }
     }
     else if(datatype == wsproto::wsdata::VECTOR_UINT32_TYPE) {
          if(rdata->has_uint32_data()) {
               member = tuple_member_create_wsdata(tdata, dtype_vector_uint32, NULL);
               if (member) {
                    wsdt_vector_uint32_t * uvec = (wsdt_vector_uint32_t *)member->data;
                    uvec->len = (unsigned int)rdata->uint32_data();
                    if(rdata->has_bytes_data()) {
                         const std::string & str = rdata->bytes_data();
                         const uint32_t * buf = (uint32_t *)str.c_str();
                         for(uint32_t i = 0; i < uvec->len; i++) {
                              uvec->value[i] = buf[i];
                         }
                    }
               }
               else {
                    errmsg = "error creating token member of type VECTOR_UINT32_TYPE";
               }
          }
          else {
               errmsg = "NO VECTOR_UINT32_TYPE DATA";
          }
     }
     else if(datatype == wsproto::wsdata::ARRAY_DOUBLE_TYPE) {
          if(rdata->has_sint32_data()) {
               member = tuple_member_create_wsdata(tdata, dtype_array_double, NULL);
               if (member) {
                    wsdt_array_double_t * darr = (wsdt_array_double_t *)member->data;
                    darr->len = (int)rdata->sint32_data();
                    if(rdata->has_bytes_data()) {
                         const std::string & str = rdata->bytes_data();
                         const double * buf = (double *)str.c_str();
                         for(int i = 0; i < darr->len; i++) {
                              darr->value[i] = buf[i];
                         }
                    }
               }
               else {
                    errmsg = "LEN not set in ARRAY_DOUBLE";
               }
          }
          else {
               errmsg = "NO ARRAY_DOUBLE_TYPE DATA";
          }
     }
     else if(datatype == wsproto::wsdata::ARRAY_UINT_TYPE) {
          if(rdata->has_sint32_data()) {
               member = tuple_member_create_wsdata(tdata, dtype_array_uint, NULL);
               if (member) {
                    wsdt_array_uint_t * uarr = (wsdt_array_uint_t *)member->data;
                    uarr->len = (int)rdata->sint32_data();
                    if(rdata->has_bytes_data()) {
                         const std::string & str = rdata->bytes_data();
                         const uint32_t * buf = (uint32_t *)str.c_str();
                         for(int i = 0; i < uarr->len; i++) {
                              uarr->value[i] = buf[i];
                         }
                    }
               }
               else {
                    errmsg = "error creating token member of type ARRAY_UINT_TYPE";
               }
          }
          else {
               errmsg = "NO ARRAY_UINT_TYPE DATA";
          }
     }
     else {
          fprintf(stderr, "wsproto - unknown wsproto datatype\n");
     }

     if (!member) {
          fprintf(stderr, "wsproto - failed to read data in buffer: %s\n", errmsg.c_str());
          
          return;
     }

     //add labels to tuple member
     int label_len = rdata->label_size();
     int i;
     for (i = 0; i < label_len; i++) {
          wslabel_t * label = wsproto_register_label(rdata->label(i), type_table);
          if (label) {
               tuple_add_member_label(tdata, member, label);
          }
     }
}

static inline int wsproto_decode_tuple(const wsproto::wsdata * wsproto, wsdata_t * tdata, void * type_table) {
     int cnt = 0;
     if(wsproto->dtype() == wsproto::wsdata::TUPLE_TYPE) {
          int i;

          // load the container labels
          int label_len = wsproto->label_size();
          for (i = 0; i < label_len; i++) {
               wslabel_t * wslabel = wsproto_register_label(wsproto->label(i), type_table);
               if (wslabel) {
                    wsdata_add_label(tdata, wslabel);
               }
          }

          // iterate through the elements and load their contents and labels
          int num_members = wsproto->tuple_member_size();
          for (i = 0; i < num_members; i++) {
               const wsproto::wsdata & rdata = wsproto->tuple_member(i);
               
               // this element is a tuple
               if(rdata.dtype() == wsproto::wsdata::TUPLE_TYPE) {
                    wsdata_t * wsd_ntup = wsdata_alloc(dtype_tuple);
                    if(wsd_ntup) {
                         // recurse (to process this nested tuple)
                         cnt += wsproto_decode_tuple(&rdata, wsd_ntup, type_table);
                         add_tuple_member(tdata, wsd_ntup);
                    }
               }
               // this element isn't a tuple
               else {
                    // parse out this element
                    wsproto_decode_member(&rdata, type_table, tdata);
               }
               cnt++;
          }
     }
     else {
          fprintf(stderr,"wsproto::wsdata did not contain a tuple\n");
     }

     return cnt;
}

static inline int wsproto_decode(wsproto::wsdata * wsproto, wsdata_t * tdata,
                                 const char * buf, int buflen, 
                                 void * type_table) {
     if (!wsproto->ParseFromArray(buf, buflen)) {
          return 0;
     }

     return wsproto_decode_tuple(wsproto, tdata, type_table);
}

static inline int wsproto_decode_tuple_ignore(const wsproto::wsdata * wsproto, 
                                              wsdata_t * tdata, 
                                              void * type_table,
                                              wslabel_t * ignore) {
     printf("wsproto_decode_tuple_ignore\n");
     int cnt = 0;
     if(wsproto->dtype() == wsproto::wsdata::TUPLE_TYPE) {
          int i;
          // load the container labels
          int label_len = wsproto->label_size();
          for (i = 0; i < label_len; i++) {
               wslabel_t * wslabel = wsproto_register_label(wsproto->label(i), type_table);
               if (wslabel) {
                    wsdata_add_label(tdata, wslabel);
               }
          }

          // iterate through the elements and load their contents and labels
          int num_members = wsproto->tuple_member_size();
          for (i = 0; i < num_members; i++) {
               const wsproto::wsdata & rdata = wsproto->tuple_member(i);
               
               // this element is a tuple
               if(rdata.dtype() == wsproto::wsdata::TUPLE_TYPE) {
                    wsdata_t * wsd_ntup = wsdata_alloc(dtype_tuple);
                    if(wsd_ntup) {
                         // recurse (to process this nested tuple)
                         cnt += wsproto_decode_tuple(&rdata, wsd_ntup, type_table);
                         add_tuple_member(tdata, wsd_ntup);
                    }
               }
               // this element isn't a tuple
               else {
                    // parse out this element if doesn't have the ignore label 
                    int ignore_member = 0;
                    int label_len = rdata.label_size();
                    int j;
                    for (j = 0; j < label_len; j++) {
                         // XXX: this technically isn't correct since labels
                         // with different ID's could have the same name... 
                         // the wsproto class doesn't keep track of label IDs,
                         // so we have to live with a string compare 
                         if (ignore && (!strcmp(ignore->name, rdata.label(j).c_str()))) {
                              ignore_member = 1;
                              break;
                         }
                    }

                    if (!ignore_member) {
                         wsproto_decode_member(&rdata, type_table, tdata);
                         cnt++;
                    }
               }
          }
     }
     else {
          fprintf(stderr,"wsproto::wsdata did not contain a tuple\n");
     }

     return cnt;
}

static inline int wsproto_decode_ignore(wsproto::wsdata * wsproto, 
                                        wsdata_t * tdata, 
                                        const char * buf, int buflen,
                                        void * type_table, wslabel_t * ignore) {
     if (!wsproto->ParseFromArray(buf, buflen)) {
          return 0;
     }

     return wsproto_decode_tuple_ignore(wsproto, tdata, type_table, ignore);
}


static inline int wsproto_tuple_readbuf(wsproto::wsdata * wsproto, wsdata_t * tdata, void * type_table,
                                         char * buf, int buflen) {
     if (!wsproto->ParseFromArray(buf, buflen)) {
          //fprintf(stderr, "protobuf_decode: bad parse\n");
          return 0;
     }

     if(wsproto->dtype() == wsproto::wsdata::TUPLE_TYPE) {
          wsproto_decode_tuple(wsproto, tdata, type_table);
          return 1;
     }

     // didn't find a tuple
     return 0;
}

#ifdef __cplusplus
CPP_CLOSE
#endif
#endif
