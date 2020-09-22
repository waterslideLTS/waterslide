/*

proc_log_keyvalue_parse.c -- parse logs with key=value pairs

Copyright 2019 Morgan Stanley

THIS SOFTWARE IS CONTRIBUTED SUBJECT TO THE TERMS OF YOU MAY OBTAIN A COPY OF
THE LICENSE AT https://www.apache.org/licenses/LICENSE-2.0.

THIS SOFTWARE IS LICENSED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE AND ANY
WARRANTY OF NON-INFRINGEMENT, ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE. THIS SOFTWARE MAY BE REDISTRIBUTED TO OTHERS ONLY BY
EFFECTIVELY USING THIS OR ANOTHER EQUIVALENT DISCLAIMER IN ADDITION TO ANY
OTHER REQUIRED LICENSE TERMS
*/

#define PROC_NAME "log_keyvalue_parse"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include "waterslide.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_fixedstring.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "wstypes.h"
#include "mimo.h"

char proc_version[]     = "1.5";
char *proc_tags[] = {"Decode", NULL};
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "kvparse", "kvlog", "kvp", "syslog", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "decode logs that have key=value substrings";
char *proc_synopsis[] = { "log_keyvalue_parse LABEL_SYSLOG_BUFER", NULL};
char proc_description[] = "decode a string from a log into key, values.  Useful for syslog parsing.";
proc_example_t proc_examples[] = {
     {NULL,""}
};
char proc_requires[] = "";
// proc_input_types and proc_output_types automatically set for procbuffer kids
proc_port_t proc_input_ports[] = {{NULL,NULL}};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};
char proc_nonswitch_opts[] = "";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'R',"","character",
     "delimiter for records",0,0},
     {'k',"","character",
     "delimiter between key and value",0,0},
     {'N',"","LABEL",
     "label of subtuple",0,0},
     {'X',"","",
     "detect and decode hex strings",0,0},
     {'E',"","LABEL",
     "label to apply when no key is found",0,0},
     {'A',"","LABEL",
     "decode all values into a common string",0,0},
     {'I',"","string",
     "ignore elements with a specified key name (can call multiple times)",0,0},
     {'L',"","LABEL prefix",
     "prefix to every new key",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

int is_procbuffer = 1;

int procbuffer_pass_not_found = 1;

#define MAX_LABEL_LEN (64)

typedef struct _ignore_item_t {
     char * buf;
     int len;
} ignore_item_t;

typedef struct _proc_instance_t {
     size_t label_prefix_len;
     char label_buffer[MAX_LABEL_LEN + 1];
     wslabel_t * label_subtuple;
     wslabel_t * label_nokey;
     uint8_t record_delimiter;
     uint8_t kv_delimiter;
     uint8_t quote;
     int detecthex;
     void * type_table;
     ignore_item_t * ignore_list;
     int ignore_cnt;
     
     wslabel_t * label_allvalues;
     int all_len;
     wsdt_string_t * str_allvalues;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"KVPARSE", offsetof(proc_instance_t, label_subtuple)},
     {"NOKEY", offsetof(proc_instance_t, label_nokey)},
     {"",0}
};

char procbuffer_option_str[]    = "E:a:A:i:I:XR:K:k:N:L:";

int procbuffer_option(void * vproc, void * type_table,
                      int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
          case 'X':
               proc->detecthex = 1;
               tool_print("no hex parsing");
               break;
          case 'R':
               if (strlen(str) > 0) {
                    if (strcmp(str,"TAB") == 0) {
                         proc->record_delimiter = '\t';
                         tool_print("record delimter set to tab character");
                    }
                    else {
                         proc->record_delimiter = (uint8_t)str[0];
                         tool_print("record delimiter set to '%c'", str[0]);
                    }
               }
               break;
          case 'a':
          case 'A':
               proc->label_allvalues = wsregister_label(type_table, str);
               break;
          case 'i':
          case 'I':
               {
                    proc->ignore_cnt++;
                    proc->ignore_list = (ignore_item_t*)realloc(proc->ignore_list,
                                                        proc->ignore_cnt *
                                                        sizeof(ignore_item_t));
                    if (!proc->ignore_list) {
                         tool_print("error allocating ignore list");
                         return 0;
                    }
                    proc->ignore_list[proc->ignore_cnt-1].buf = strdup(str);
                    proc->ignore_list[proc->ignore_cnt-1].len = strlen(str);
                    break;
               }
          case 'K':
          case 'k':
               if (strlen(str) > 0) {
                    proc->kv_delimiter = (uint8_t)str[0];
                    tool_print("key-value delimiter set to '%c'", str[0]);
               }
               break;
          case 'N':
               proc->label_subtuple = wsregister_label(type_table, str);
               tool_print("subtuple label set to %s", str);
               break;
          case 'E':
               proc->label_nokey = wsregister_label(type_table, str);
               tool_print("nokey set to %s", str);
               break;
          case 'L':
               proc->label_prefix_len = strlen(str);
               if (proc->label_prefix_len < MAX_LABEL_LEN) {
                    memcpy(proc->label_buffer, str,
                           proc->label_prefix_len);
               }
               else {
                    tool_print("label length too long");
                    return 0;
               }
               tool_print("setting label prefix to %s", str);
               break;
          default:
               return 0;
     }
     return 1;
}

int procbuffer_init(void *vproc, void *type_table) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     proc->type_table = type_table;

     if (!proc->record_delimiter) {
          proc->record_delimiter = ' ';
     }
     if (!proc->kv_delimiter) {
          proc->kv_delimiter = '=';
     }
     return 1;
}

static int32_t get_current_index_size(void * type_table) {
     mimo_datalists_t * mdl = (mimo_datalists_t *) type_table;
     return mdl->index_len;
}

static wslabel_t * get_key_label(proc_instance_t * proc, uint8_t * startkey,
                                 size_t len) {
     if (len <= 0) {
          return NULL;
     }
     //check if key should be ignored
     int i;
     for (i = 0; i < proc->ignore_cnt; i++) {
          if ((len == proc->ignore_list[i].len) &&
              (memcmp(startkey, proc->ignore_list[i].buf, len) == 0)) {
               return NULL;
          }
     }

     if (proc->label_allvalues) {
          return proc->label_nokey;
     }
     if ((len + proc->label_prefix_len) > MAX_LABEL_LEN) {
          len = MAX_LABEL_LEN - proc->label_prefix_len;
     }
     //copy string onto null terminated buffer
     memcpy(proc->label_buffer + proc->label_prefix_len, startkey, len); 
     proc->label_buffer[proc->label_prefix_len + len] = 0;

     return wsregister_label(proc->type_table, proc->label_buffer);
}

static inline uint8_t get_xdigit(uint8_t c) {
     if (isdigit(c)) {
          return (c - 0x30);
     }
     else if (isupper(c)) {
          return (c - 0x37);
     }
     else {
          return (c - 0x57);
     }
}

static char * get_allvalues_buf(proc_instance_t * proc, wsdata_t * tuple, int len) {
     dprint("get_allvalues_buf %d", len);
     if (!proc->label_allvalues) {
          dprint("nolabel");
          return NULL;
     }
     if (!proc->str_allvalues) {
          proc->str_allvalues = 
               tuple_create_string(tuple, proc->label_allvalues, proc->all_len);     
          if (!proc->str_allvalues) {
               dprint("str not allocated");
               return NULL;
          }
          proc->str_allvalues->len = 0;
     }
     if ((proc->str_allvalues->len + 1 + len) > proc->all_len) {
          //string will not fit!!
          dprint("str not fitting %d %d %d", proc->all_len, len,
                 proc->str_allvalues->len);
          return NULL;
     }
     if (proc->str_allvalues->len) {
          proc->str_allvalues->buf[proc->str_allvalues->len] = ' ';
          proc->str_allvalues->len++;
     }

     char * offset = proc->str_allvalues->buf + proc->str_allvalues->len;
     proc->str_allvalues->len += len;
     return offset;
}

static int add_hex_str(proc_instance_t * proc, wsdata_t * tuple,
                                wslabel_t * label_key,
                                uint8_t *buf, int buflen ) {

     dprint("add hex str");
     //tool_print("add_hex_str %d", buflen);
     //check if entire buffer is hexascii
     if (!buflen || ((buflen%2) != 0)) {
          return 0;
     }
     int i;
     for (i = 0; i < buflen; i++) {
          if (!isxdigit(buf[i])) {
               return 0;
          }
     }
     //get buffer for output
     uint8_t * outbuf = NULL;
     if (proc->label_allvalues ) {
          int tlen = (buflen/2);
          outbuf = (uint8_t*)get_allvalues_buf(proc, tuple, tlen+2);
          outbuf[0] = '\'';
          outbuf[tlen+1] = '\'';
          outbuf++;
     }
     else {
          wsdt_string_t * str = tuple_create_string(tuple, label_key,
                                                    (buflen/2));
          if (str) {
               outbuf = (uint8_t*)str->buf;
          }
     }

     if (!outbuf) {
          return 0;
     }
     int x = 0;
     for (i = 0; i < buflen; i = i+2) {
          outbuf[x] = (get_xdigit(buf[i]) << 4) + (get_xdigit(buf[i+1]));
          x++;
     }
     //tool_print("outbuffer %d", str->len);

     return 1;
}

static void add_key_value_dep(proc_instance_t * proc, wsdata_t * tuple,
                              wsdata_t * member, wslabel_t * label_key,
                              char * value, int len) {

     dprint("add key value dep");
     if (proc->label_allvalues) {
          char * dest = get_allvalues_buf(proc, tuple, len);
          if (dest) {
               memcpy(dest, value, len);
          }
     }
     else {
          tuple_member_create_dep_string(tuple, member,
                                         label_key, value, len);
     }
}

///returns length of parsed buffer
static size_t get_next_record(proc_instance_t * proc,
                              wsdata_t * tuple, wsdata_t * member,
                              uint8_t * buf, int len) {

     uint8_t * startbuf = buf;

     //see if there is whitespace in front..
     while (len && (buf[0] == proc->record_delimiter)) {
          buf += 1;
          len -= 1;
     }
     if (!len) {
          return (buf - startbuf);
     } 

     //start looking for kv_delimiter
     uint8_t * startkey = buf;
     uint8_t * keyend = NULL;
     uint8_t * vstart = NULL;
     uint8_t * vend = NULL;

     int uses_quotes = 0;

     while(len && (buf[0] != proc->record_delimiter)) {
          //look for quotes
          if ((buf == vstart) && (buf[0] == '"')) {
               uses_quotes = 1;
               //look for end of quote..
               buf += 1;
               len -= 1;
               vstart = buf;
               vend = NULL;
               while (len) {
                   if (buf[0] == '"') {
                        vend = buf;
                        buf +=1;
                        len -=1;
                        break;
                   }
                   buf += 1;
                   len -= 1;
               }
               if (!vend) {
                    return (buf - startbuf);
               }
               break;
               
          }
          if (!vstart && (buf[0] == proc->kv_delimiter)) {
               if (!keyend) {
                    keyend = buf;
               }
               if (len > 1) {
                    vstart = buf + 1;
               }
          }
          buf += 1;
          len -= 1;
     }
     //key no value... ignore??
     if (vstart) {
          wslabel_t * label_key = get_key_label(proc, startkey,
                                                keyend - startkey);
          if (label_key) { //check if we are not going to ignore this key
               if (!vend) {
                    vend = buf;
               }
               if (vend > vstart) {
                    if (!proc->detecthex || uses_quotes || 
                        !add_hex_str(proc, tuple,
                                     label_key,
                                     vstart, vend - vstart)) {

                         add_key_value_dep(proc, tuple, member, label_key,
                                           (char *)vstart, vend - vstart);
                    }
               }
               else {
                    //value is null..
                    if (!proc->label_allvalues) {
                         char * nullv = "NULL";
                         tuple_dupe_string(tuple, label_key, nullv, 4);
                    }
               }
          }
          else {
               //we are going to ignore this key/value
          }
     }
     else if (buf > startkey) {
          if (!keyend) {
               keyend = buf;
          }
          if (keyend > startkey) {
               //nokey value

               add_key_value_dep(proc, tuple, member, proc->label_nokey,
                                 (char *)startkey, keyend - startkey);
          }
     }

     return (buf - startbuf);
}

static int extract_allvalues(proc_instance_t * proc, wsdata_t * tuple,
                             wsdata_t * member,
                             uint8_t * buf, int len) {

     proc->all_len = len; //set total length
     proc->str_allvalues = NULL;

     while(len > 0) {
          int rlen = (int)get_next_record(proc, tuple, member, buf, len);

          if (rlen <= 0) {
               break;
          }
          buf += rlen;
          len -= rlen;
     }
     return 1;
}

int procbuffer_decode(void * vproc, wsdata_t * tuple,
                      wsdata_t * member,
                      uint8_t * buf, int len) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     if (proc->label_allvalues) {
          return extract_allvalues(proc, tuple, member, buf, len);
     }

     //number of labels before parsing
     uint32_t start_index_size = get_current_index_size(proc->type_table);

     wsdata_t * subtuple = wsdata_alloc(dtype_tuple);
     if (!subtuple) {
          return 1;
     }
     wsdata_add_label(subtuple, proc->label_subtuple);

     while(len > 0) {
          int rlen = (int)get_next_record(proc, subtuple, member, buf,len);

          if (rlen <= 0) {
               break;
          }
          buf += rlen;
          len -= rlen;
     }


     uint32_t end_index_size = get_current_index_size(proc->type_table);
     if (start_index_size != end_index_size) {
          dprint("reapplying tuple labels");
          wsdata_t * newsub = wsdata_alloc(dtype_tuple);
          if (!newsub) {
               wsdata_delete(subtuple);
               return 1;
          }
          if (!tuple_deep_copy(subtuple, newsub)) {
               wsdata_delete(subtuple);
               wsdata_delete(newsub);
               return 1;
          }
          wsdata_delete(subtuple);
          subtuple = newsub;
     }

     add_tuple_member(tuple, subtuple);

     return 1;
}

//return 1 if successful
//return 0 if no..
int procbuffer_destroy(void * vinstance) {
     //proc_instance_t * proc = (proc_instance_t*)vinstance;

     //free dynamic allocations
     //free(proc); // free this in the calling function

     return 1;
}

