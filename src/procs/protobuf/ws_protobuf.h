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
#ifndef _WS_PROTOBUF_H_
#define _WS_PROTOBUF_H_
#include <string>
#include "wsserial.pb.h"
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
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_uint16.h"
#include "datatypes/wsdt_uint8.h"
#include "datatypes/wsdt_label.h"
#include "datatypes/wsdt_int.h"

#ifdef __cplusplus
CPP_OPEN
#endif

// Format numbering
//   pbmeta is format 1
#define PBMETA_FORMAT_ID 1

// pbmeta format versioning
//   version 1: original pbmeta format
//   version 2: added support
#define PBMETA_FORMAT_VERSION 1

static inline void protobuf_fill_data(wsserial::WSdata * sdata,
                                      wsdata_t * member) {
     if (member->dtype == dtype_string) {
          wsdt_string_t * str = (wsdt_string_t *)member->data;
          sdata->set_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_binary) {
          wsdt_binary_t * str = (wsdt_binary_t *)member->data;
          sdata->set_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_fixedstring) {
          wsdt_fixedstring_t * str = (wsdt_fixedstring_t *)member->data;
          sdata->set_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_mediumstring) {
          wsdt_mediumstring_t * str = (wsdt_mediumstring_t *)member->data;
          sdata->set_data(str->buf, str->len);
     }
     else if (member->dtype == dtype_double) {
          wsdt_double_t * dbl = (wsdt_double_t *)member->data;
          sdata->set_dbl(*dbl);
     }
     else if (member->dtype == dtype_uint) {
          wsdt_uint_t * num = (wsdt_uint_t *)member->data;
          sdata->set_num(*num);
     }
     else if (member->dtype == dtype_uint64) {
          wsdt_uint64_t * num = (wsdt_uint64_t *)member->data;
          sdata->set_num(*num);
     }
     else if (member->dtype == dtype_uint16) {
          wsdt_uint16_t * num = (wsdt_uint16_t *)member->data;
          sdata->set_num(*num);
     }
     else if (member->dtype == dtype_uint8) {
          wsdt_uint8_t * num = (wsdt_uint8_t *)member->data;
          sdata->set_num(*num);
     }
     else if (member->dtype == dtype_label) {
          wsdt_label_t * label = (wsdt_label_t *) member->data;
          sdata->set_data((*label)->name);
     }
     else if (member->dtype == dtype_ts) {
          wsdt_ts_t * str = (wsdt_ts_t *)member->data;
          sdata->set_data(str, sizeof(wsdt_ts_t));
     }
     //converts int to uint if int is positive..
     //  BAD: drops negative ints.. ack!!!
     else if (member->dtype == dtype_int) {
          wsdt_int_t * num = (wsdt_int_t *)member->data;
          if (*num >= 0) {
               sdata->set_num(*num);
          }
     }
}

static inline void protobuf_add_labels(wsserial::WSdata * sdata,
                                       wsdata_t * member,
                                       int send_labels) {
     for (int i = 0; i < member->label_len; i++) {
          sdata->add_labelid(member->labels[i]->hash);
          if (send_labels) {
             sdata->add_labelnames(member->labels[i]->name);
          }
     }
}

static inline void protobuf_fill_tuple(wsserial::WStuple * stup,
                                       wsdata_t * tdata,
                                       int send_labels) {
     int i;
     for (i = 0; i < tdata->label_len; i++) {
          stup->add_labelid(tdata->labels[i]->hash);
     }
     wsdt_tuple_t * tuple = (wsdt_tuple_t*)tdata->data;
     for (i = 0; i < (int)tuple->len; i++) {
          if (tuple->member[i]->dtype == dtype_tuple) {
               wsserial::WStuple * nsub = stup->add_subtuple();
               protobuf_fill_tuple(nsub, tuple->member[i], send_labels);
          }
          else {
               wsserial::WSdata * sdata = stup->add_member();
               sdata->set_dtype(tuple->member[i]->dtype->namehash);
               protobuf_add_labels(sdata, tuple->member[i], send_labels);
               protobuf_fill_data(sdata, tuple->member[i]);
               if (send_labels) {
                    sdata->set_dtypename(tuple->member[i]->dtype->name);
               }
          }
     }
}

static inline void protobuf_fill_label(wsserial::WSlabel * sl,
                                       wslabel_t * label) {
     sl->set_labelid(label->hash);
     sl->set_name(label->name);
}

static inline void protobuf_fill_emptylabel(wsserial::WSlabel * sl) {
     sl->set_labelid(0);
     sl->set_name("");
}

typedef struct _ws_protobuf_t {
     void * type_table;
     wsserial::WSsend * wssend;
} ws_protobuf_t;

static inline ws_protobuf_t * protobuf_init(void * type_table) {
     GOOGLE_PROTOBUF_VERIFY_VERSION;

     ws_protobuf_t * pb = (ws_protobuf_t *)calloc(1, sizeof(ws_protobuf_t));
     if (!pb) {
          error_print("failed protobuf_init calloc of pb");
          return NULL;
     }
     pb->wssend = new wsserial::WSsend;
     pb->type_table = type_table;
     return pb;
}

static inline void protobuf_destroy(ws_protobuf_t * pb) {
     // Delete all global objects allocated by libprotobuf.
     google::protobuf::ShutdownProtobufLibrary();

     delete(pb->wssend);
     free(pb);
}

static inline int protobuf_fill_buffer(ws_protobuf_t * pb, wsdata_t * input_data,
                                       char * buf,
                                       int buflen) {

     wsserial::WSsend * wssend = pb->wssend;
     wssend->Clear();

     if (input_data->dtype == dtype_tuple) {
          wsserial::WStuple * tuple = wssend->mutable_tuple();      
          protobuf_fill_tuple(tuple, input_data, 0);
     }    
     else {
          return 0;
     }

     int mlen = wssend->ByteSize();
     if (buflen >= mlen) {
          wssend->SerializeToArray(buf, buflen); 
          return mlen;
     }
     return 0;

}

static inline int protobuf_writefp(ws_protobuf_t * pb, FILE * fp) {
     wsserial::WSsend * wssend = pb->wssend;
     uint32_t mlen = (uint32_t)wssend->ByteSize();
     fwrite(&mlen, sizeof(uint32_t), 1, fp); 
     if (!mlen) {
          return 0;
     }
#ifdef FULL_PROTOBUF
     wssend->SerializeToFileDescriptor(fileno(fp));
#else
     char * buf = (char *)malloc(mlen);
     if (!buf) {
          error_print("failed protobuf_writefp malloc of buf");
          return 0;
     }
     wssend->SerializeToArray(buf, mlen); 

     fwrite(buf, mlen, 1, fp); 

     free(buf);
#endif
     return mlen;
}

static inline int protobuf_tuple_writefp(ws_protobuf_t * pb, wsdata_t * input_data,
                                         FILE * fp) {

     wsserial::WSsend * wssend = pb->wssend;
     wssend->Clear();

     if (input_data->dtype == dtype_tuple) {
          wsserial::WStuple * tuple = wssend->mutable_tuple();      
          protobuf_fill_tuple(tuple, input_data, 0);
     }    
     else {
          return 0;
     }

     return protobuf_writefp(pb, fp);
}

static void protobuf_callback_addlabel(void * vlbl, void * vpb) {
     wslabel_t * label = (wslabel_t *)vlbl;
     ws_protobuf_t * pb = (ws_protobuf_t*)vpb;

     wsserial::WSsend * wssend = pb->wssend;
     wsserial::WSlabel * olabel = wssend->add_label();      

     olabel->set_labelid(label->hash);
     olabel->set_name(label->name);
}

static inline int protobuf_labels_writefp(ws_protobuf_t * pb,
                                          FILE * fp) {

     wsserial::WSsend * wssend = pb->wssend;
     wssend->Clear();

     //walk list of labels....
     mimo_datalists_t * mdl = (mimo_datalists_t *)pb->type_table;
     listhash_scour(mdl->label_table, protobuf_callback_addlabel, pb);

     return protobuf_writefp(pb, fp);
}

static inline wsdata_t * protobuf_fill_dictionary(ws_protobuf_t * pb) {

     wsserial::WSsend * wssend = pb->wssend;
     wssend->Clear();

     //walk list of labels....
     mimo_datalists_t * mdl = (mimo_datalists_t *)pb->type_table;
     listhash_scour(mdl->label_table, protobuf_callback_addlabel, pb);

     int mlen = wssend->ByteSize();
     wsdata_t * wsd_bin = dtype_alloc_binary(mlen);
     if (!wsd_bin) {
          return NULL;
     }
     wsdt_binary_t * bin = (wsdt_binary_t *)wsd_bin->data;
    
     wssend->SerializeToArray(bin->buf, bin->len); 

     return wsd_bin;
}


static inline wsdata_t * protobuf_fill_buffer_alloc(ws_protobuf_t * pb,
                                                    wsdata_t * input_data, int send_labels) {

     wsserial::WSsend * wssend = pb->wssend;
     wssend->Clear();

     if (input_data->dtype == dtype_tuple) {
          wsserial::WStuple * tuple = wssend->mutable_tuple();      
          protobuf_fill_tuple(tuple, input_data, send_labels);
     }    
     else {
          return NULL;
     }

     int mlen = wssend->ByteSize();
     wsdata_t * wsd_bin = dtype_alloc_binary(mlen);
     if (!wsd_bin) {
          return NULL;
     }
     wsdt_binary_t * bin = (wsdt_binary_t *)wsd_bin->data;
    
     wssend->SerializeToArray(bin->buf, bin->len); 

     return wsd_bin;
}


static inline int protobuf_send_label(ws_protobuf_t * pb,
                                      char * buf,
                                      int buflen,
                                      wslabel_t * label) {

     wsserial::WSsend * wssend = pb->wssend;
     wssend->Clear();

     wsserial::WSlabel * olabel = wssend->add_label();      
     if (label) {
          protobuf_fill_label(olabel, label);
     }
     else {
          protobuf_fill_emptylabel(olabel);
     }

     int mlen = wssend->ByteSize();
     if (buflen >= mlen) {
          wssend->SerializeToArray(buf, buflen); 
          return mlen;
     }
     return 0;
}

static inline wsdata_t * protobuf_decode_string(const wsserial::WSdata * rdata,
                                                int isascii, wsdata_t * tdata) {
     if (rdata->has_data()) {
          const std::string & str = rdata->data();
          int len = str.length();
          const char * buf = str.c_str();
          if (isascii) {
               wsdata_t * wsd_str = tuple_create_string_wsdata(tdata, NULL, len);
               if (!wsd_str) {
                    return NULL;
               }
               wsdt_string_t * str = (wsdt_string_t *)wsd_str->data;
               memcpy(str->buf, buf, len);
               return wsd_str;
          }
          else {
               wsdata_t * wsd_bin = tuple_create_binary_wsdata(tdata, NULL, len);
               if (!wsd_bin) {
                    return NULL;
               }
               wsdt_binary_t * bin = (wsdt_binary_t *)wsd_bin->data;
               memcpy(bin->buf, buf, len);
               return wsd_bin;
          }
     }
     return NULL;
}

static inline void protobuf_decode_member(ws_protobuf_t * pb, const wsserial::WSdata * rdata,
                                          wsdata_t * tdata) {
     wsdata_t * member = NULL;
     uint64_t dtype_hash = rdata->dtype();
     if ((dtype_hash == dtype_string->namehash) ||
         (dtype_hash == dtype_fixedstring->namehash)) {
          member = protobuf_decode_string(rdata, 1, tdata);
     }
     else if (dtype_hash == dtype_binary->namehash) {
          member = protobuf_decode_string(rdata, 0, tdata);
     }
     else if (dtype_hash == dtype_ts->namehash) {
          const std::string & str = rdata->data();
          int len = str.length();
          const char * buf = str.c_str();
          if (len == sizeof(wsdt_ts_t)) {
               member = tuple_member_create_wsdata(tdata, dtype_ts, NULL);
               if (member) {
                    wsdt_ts_t * ts = (wsdt_ts_t*) member->data;
                    memcpy(ts, buf, sizeof(wsdt_ts_t));
               }
          }
          // Special case for 32 bit machine sending timestamp to 64 bit machine - Tested
          else if (len == sizeof(wsdt_ts_t)>>1) {
               member = tuple_member_create_wsdata(tdata, dtype_ts, NULL);
               if (member) {
                    wsdt_ts_t * ts = (wsdt_ts_t*) member->data;
                    ts->sec=*((uint32_t*) buf);
                    ts->usec=*((uint32_t*) (buf+4));
               }
          }
     }
     else if (dtype_hash == dtype_double->namehash) {
          if (rdata->has_dbl()) {
               double dbl = rdata->dbl();
               member = tuple_member_create_wsdata(tdata, dtype_double, NULL);
               if (member) {
                    wsdt_double_t * mdbl = (wsdt_double_t*)member->data;
                    *mdbl = dbl;
               }
          }
     }

     else if (dtype_hash == dtype_uint->namehash) {
          if (rdata->has_num()) {
               uint64_t num = rdata->num();
               member = tuple_member_create_wsdata(tdata, dtype_uint, NULL);
               wsdt_uint_t * u = (wsdt_uint_t*)member->data;
               *u = num;
          }
     }
     else if (dtype_hash == dtype_uint64->namehash) {
          if (rdata->has_num()) {
               uint64_t num = rdata->num();
               member = tuple_member_create_wsdata(tdata, dtype_uint64, NULL);
               wsdt_uint64_t * u = (wsdt_uint64_t*)member->data;
               *u = num;
          }
     }
     else if (dtype_hash == dtype_uint16->namehash) {
          if (rdata->has_num()) {
               uint64_t num = rdata->num();
               member = tuple_member_create_wsdata(tdata, dtype_uint16, NULL);
               wsdt_uint16_t * u = (wsdt_uint16_t*)member->data;
               *u = (uint16_t)num;
          }
     }
     else if (dtype_hash == dtype_uint8->namehash) {
          if (rdata->has_num()) {
               uint64_t num = rdata->num();
               member = tuple_member_create_wsdata(tdata, dtype_uint8, NULL);
               wsdt_uint8_t * u = (wsdt_uint8_t*)member->data;
               *u = (uint8_t)num;
          }
     }
     else if (dtype_hash == dtype_label->namehash) {
          const std::string & str = rdata->data();
          int len = str.length();

          if (len) {
               member = tuple_member_create_wsdata(tdata, dtype_label, NULL);
               if (member) {
                    wsdt_label_t * lbl = (wsdt_label_t *)member->data;
                    *lbl = wsregister_label(pb->type_table, str.c_str());
               }
          }
     }
     //converts int to uint .. 
     else if (dtype_hash == dtype_int->namehash) {
          if (rdata->has_num()) {
               uint64_t num = rdata->num();
               member = tuple_member_create_wsdata(tdata, dtype_uint, NULL);
               wsdt_uint_t * u = (wsdt_uint_t*)member->data;
               *u = num;
          }
     }

     if (!member) {
          return;
     }

     //add labels to tuple member
     int label_len = rdata->labelid_size();
     int i;
     for (i = 0; i < label_len; i++) {
          uint64_t id = rdata->labelid(i);
          wslabel_t * label = wslabel_find_byhash(pb->type_table, id);
          if (label) {
               tuple_add_member_label(tdata, member, label);
          }
     }

}

static inline int protobuf_decode_tuple(ws_protobuf_t * pb,
                                        const wsserial::WStuple * rtup, wsdata_t * tdata) {
     int cnt = 0;
     int i;
     int label_len = rtup->labelid_size();
     for (i = 0; i < label_len; i++) {
          uint64_t id = rtup->labelid(i);
          wslabel_t * label = wslabel_find_byhash(pb->type_table, id);
          if (label) {
               wsdata_add_label(tdata, label);
          }
          else {
               dprint("label not found");
          }
     }

     int num_members = rtup->member_size();
     for (i = 0; i < num_members; i++) {
          const wsserial::WSdata & rdata = rtup->member(i);
          protobuf_decode_member(pb, &rdata, tdata); 
          cnt++;
     }

     int num_subs = rtup->subtuple_size();
     for (i = 0; i < num_subs; i++) {
          const wsserial::WStuple & ntup = rtup->subtuple(i);

          wsdata_t * wsd_ntup = wsdata_alloc(dtype_tuple);
          if (wsd_ntup) {
               cnt++;
               protobuf_decode_tuple(pb, &ntup, wsd_ntup);
               add_tuple_member(tdata, wsd_ntup);
          }
     }

     return cnt;
}

static inline int protobuf_decode_tuple_ignore(ws_protobuf_t * pb,
                                               const wsserial::WStuple * rtup,
                                               wsdata_t * tdata,
                                               wslabel_t * ignore) {
     int i;
     int label_len = rtup->labelid_size();
     for (i = 0; i < label_len; i++) {
          uint64_t id = rtup->labelid(i);
          wslabel_t * label = wslabel_find_byhash(pb->type_table, id);
          if (label) {
               wsdata_add_label(tdata, label);
          }
          else {
               dprint("label not found");
          }
     }

     int num_members = rtup->member_size();
     for (i = 0; i < num_members; i++) {
          int ignore_member = 0;
          const wsserial::WSdata & rdata = rtup->member(i);

          int label_len = rdata.labelid_size();
          int j;
          for (j = 0; j < label_len; j++) {
               uint64_t id = rdata.labelid(j);
               if (ignore && (id == ignore->hash)) {
                    ignore_member = 1;
                    break;
               }
          }

          if (!ignore_member) {
               protobuf_decode_member(pb, &rdata, tdata); 
          }
     }
     int num_subs = rtup->subtuple_size();
     for (i = 0; i < num_subs; i++) {
          const wsserial::WStuple & ntup = rtup->subtuple(i);

          wsdata_t * wsd_ntup = wsdata_alloc(dtype_tuple);
          if (wsd_ntup) {
               protobuf_decode_tuple(pb, &ntup, wsd_ntup);
               add_tuple_member(tdata, wsd_ntup);
          }
     }

     return i;
}


static inline int protobuf_decode(ws_protobuf_t * pb, wsdata_t * tdata,
                                     const char * buf, int buflen) {
     wsserial::WSsend * wsrcv = pb->wssend;

     if (!wsrcv->ParseFromArray(buf, buflen)) {
          dprint("protobuf_decode: bad parse");
          return 0;
     }

     if (!wsrcv->has_tuple()) {
          dprint("protobuf_decode: no tuple");
          //fprintf(stderr, "protobuf_decode: no tuple\n");
          return 0;
     }
     const wsserial::WStuple & rtup = wsrcv->tuple();
     return protobuf_decode_tuple(pb, &rtup, tdata);
}

static inline int protobuf_decode_ignore(ws_protobuf_t * pb, wsdata_t * tdata,
                                         const char * buf, int buflen,
                                         wslabel_t * ignore) {
     wsserial::WSsend * wsrcv = pb->wssend;

     if (!wsrcv->ParseFromArray(buf, buflen)) {
          //fprintf(stderr, "protobuf_decode: bad parse\n");
          return 0;
     }

     if (!wsrcv->has_tuple()) {
          //fprintf(stderr, "protobuf_decode: no tuple\n");
          return 0;
     }
     const wsserial::WStuple & rtup = wsrcv->tuple();
     return protobuf_decode_tuple_ignore(pb, &rtup, tdata, ignore);
}


static inline int protobuf_decode_label(ws_protobuf_t * pb, const char * buf, int buflen) {
     wsserial::WSsend * wsrcv = pb->wssend;

     if (!wsrcv->ParseFromArray(buf, buflen)) {
          //fprintf(stderr, "protobuf_decode: bad parse\n");
          return 0;
     }

     int num_labels = wsrcv->label_size();
     int outlabels = 0;
     int i;
     for (i = 0; i < num_labels; i++) {
          const wsserial::WSlabel & rlabel = wsrcv->label(i);
          uint64_t id = rlabel.labelid();
          if (id) {
               outlabels++;
               const std::string & str = rlabel.name();
               const char * lblstr = str.c_str();
               wsregister_label(pb->type_table, lblstr);
          }
     }
     return outlabels;
}

static inline int protobuf_tuple_readbuf(ws_protobuf_t * pb, wsdata_t * tdata,
                                         char * buf, int buflen) {

     //else we got a buffer to parse
     wsserial::WSsend * wsrcv = pb->wssend;

     if (!wsrcv->ParseFromArray(buf, buflen)) {
          //fprintf(stderr, "protobuf_decode: bad parse\n");
          return 0;
     }

     if (wsrcv->has_tuple()) {
          const wsserial::WStuple & rtup = wsrcv->tuple();
          protobuf_decode_tuple(pb, &rtup, tdata);
          return 1;
     }

     int num_labels = wsrcv->label_size();
     int i;
     for (i = 0; i < num_labels; i++) {
          const wsserial::WSlabel & rlabel = wsrcv->label(i);
          uint64_t id = rlabel.labelid();
          if (id) {
               const std::string & str = rlabel.name();
               const char * lblstr = str.c_str();
               wsregister_label(pb->type_table, lblstr);
          }
     }
     return 2;
}

#ifdef __cplusplus
CPP_CLOSE
#endif
#endif
