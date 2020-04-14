/*

proc_parsexml.c -- parse buffer for xml elements

Copyright 2020 Morgan Stanley

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

#define PROC_NAME "parsexml"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <expat.h>
#include "waterslide.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_fixedstring.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "wstypes.h"
#include "mimo.h"

char proc_version[]     = "1.0";
char *proc_tags[] = {"Decode", NULL};
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "xmlparse", "xml", "xml2tuple", "xmlparser", "xml_in", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "decode xml elements from supplied buffer";
char *proc_synopsis[] = { "parsexml LABEL_TO_PARSE", NULL};
char proc_description[] = "decode xml data from a buffer";
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
     {'N',"","LABEL",
     "label of subtuple for parsed elements",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

int is_procbuffer = 1;

int procbuffer_pass_not_found = 1;

#define MAX_LABEL_LEN (64)

typedef struct _parse_node_t {
     wsdata_t * tuple;
     struct _parse_node_t * parent;
     struct _parse_node_t * next;
} parse_node_t;

typedef struct _parse_tree_t {
     parse_node_t * freeq;
     parse_node_t * head;
     parse_node_t * tail;
     parse_node_t * active;
} parse_tree_t;

#define MAX_VALUE_LEN (65536 * 4)
typedef struct _proc_instance_t {
     uint64_t truncate;
     uint64_t parse_error;
     wslabel_t * label_subtuple;
     wslabel_t * label_value;
     wslabel_t * label_error;
     XML_Parser parser;
     void * type_table;

     wslabel_t * label_active;
     parse_tree_t pt;

     wsdata_t * active_tuple;
     wsdata_t * single_value;
     wslabel_t * buffer_label;

     char buffer[MAX_VALUE_LEN];
     int buffer_len;
} proc_instance_t;

proc_labeloffset_t proc_labeloffset[] =
{
     {"XMLPARSE", offsetof(proc_instance_t, label_subtuple)},
     {"VALUE", offsetof(proc_instance_t, label_value)},
     {"ERROR", offsetof(proc_instance_t, label_error)},
     {"",0}
};


static parse_node_t *parse_node_setnewnode(proc_instance_t * proc,
                                           wsdata_t * tuple, parse_node_t * parent) {
     parse_node_t * cursor;
     if (proc->pt.freeq) {
          cursor=proc->pt.freeq;
          proc->pt.freeq = cursor->next;
     }
     else {
          cursor = malloc(sizeof(parse_node_t));
          if (!cursor) {
               return NULL;
          }
     }
     memset(cursor, 0, sizeof(parse_node_t));
     if (!proc->pt.head) {
          proc->pt.tail = cursor;
     }
     cursor->next = proc->pt.head;
     cursor->tuple = tuple;
     cursor->parent = parent;
     proc->pt.active = cursor;
     proc->pt.head = cursor;
     return cursor;
}

//move list to freeq
static void parse_node_free(proc_instance_t * proc) {
     if (!proc->pt.tail) {
          proc->pt.head = NULL;
          return;
     }
     proc->pt.tail->next = proc->pt.freeq;
     proc->pt.freeq = proc->pt.head;
     proc->pt.head = NULL;
     proc->pt.tail = NULL;
}

//free up memory in freeq
static void parse_node_destroy(proc_instance_t * proc) {
     if (!proc->pt.freeq) {
          return;
     }
     parse_node_t * cursor = proc->pt.freeq;
     parse_node_t * next;
     while (cursor) {
          next = cursor->next;
          free(cursor);
          cursor = next;
     }
}


int procbuffer_instance_size = sizeof(proc_instance_t);

char procbuffer_option_str[]    = "N:";

int procbuffer_option(void * vproc, void * type_table,
                      int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
          case 'N':
               proc->label_subtuple = wsregister_label(type_table, str);
               tool_print("subtuple label set to %s", str);
               break;
                    default:
               return 0;
     }
     return 1;
}

static void add_buffered_values(proc_instance_t * proc) {
     if (proc->single_value) {
          dprint("adding single value");
          add_tuple_member(proc->pt.active->tuple, proc->single_value);
          wsdata_delete(proc->single_value);
          proc->single_value = NULL;
     }
     else if (proc->buffer_len) {
          dprint("adding buffered value");
          tuple_dupe_string(proc->pt.active->tuple, proc->buffer_label,
                            proc->buffer, proc->buffer_len);
          proc->buffer_len = 0;
     }
}


/*callbacks for expat parser*/
static void XMLCALL
startElement(void *userData, const XML_Char *name, const XML_Char **atts) {
     proc_instance_t * proc = userData;

     dprint("startElement %s", name);
     add_buffered_values(proc);

     //check if parent is not a tuple yet
     if (proc->label_active) {
          wsdata_t * subtuple = wsdata_alloc(dtype_tuple);
          if (!subtuple) {
               return;
          }
          wsdata_add_label(subtuple, proc->label_active);
    
          if (proc->pt.active && proc->pt.active->tuple) {
               add_tuple_member(proc->pt.active->tuple, subtuple);
          }
          parse_node_setnewnode(proc, subtuple, proc->pt.active);
     }

     if (atts[0] && !atts[2] && (strcmp(name, "Data") == 0) &&
         (strcmp(atts[0], "Name") == 0)) {
          proc->label_active = wsregister_label(proc->type_table, atts[1]);
          return;
     }

     //create tuple with label name
     proc->label_active = wsregister_label(proc->type_table, name);

     if (!atts[0]) {
          return;
     }

     //has attributes
     //attach attributes to tuple
     wsdata_t * tuple = wsdata_alloc(dtype_tuple);
     if (!tuple) {
          return;
     }
     wsdata_add_label(tuple, proc->label_active);
     proc->label_active = NULL;
     add_tuple_member(proc->pt.active->tuple, tuple);
     parse_node_setnewnode(proc, tuple, proc->pt.active);

     int i;
     for (i = 0; atts[i]; i += 2) {
          wslabel_t * name = wsregister_label(proc->type_table, atts[i]);
          if (name) {
               tuple_dupe_string(tuple, name,
                                 atts[i+1], strlen(atts[i+1]));
          }
     }

}

static void XMLCALL
valueHandler(void * userData, const XML_Char *s, int len) {
     dprint(" valueHandler %d, '%.*s'", len, len, s);

     proc_instance_t * proc = userData;

     //see if we have a prior value
     if (proc->single_value) {
          wsdt_string_t * str = (wsdt_string_t*)proc->single_value->data;
          if (str->len > MAX_VALUE_LEN) {
               str->len = MAX_VALUE_LEN;
               proc->truncate++;
          }
          memcpy(proc->buffer, str->buf, str->len);
          proc->buffer_len = str->len;
          wsdata_delete(proc->single_value);
          proc->single_value = NULL;
     }
     if (proc->buffer_len) {
          if ((proc->buffer_len + len) > MAX_VALUE_LEN) {
               //truncate
               proc->truncate++;
               len = MAX_VALUE_LEN - proc->buffer_len;
               if (len <= 0) {
                    return;
               }
          }
          memcpy(proc->buffer + proc->buffer_len, s, len);
          proc->buffer_len += len;
          return;
     }

     if (len == 1) {
          dprint(" valueHandler charval %02x", s[0]);
          switch(s[0]) {
          case '\n':
          case '\r':
          case '\t':
          case ' ':
               dprint("found bad char");
               return;
          }
     }

     wslabel_t * label = proc->label_active;
     if (!label) {
          label = proc->label_value;
     }
     proc->buffer_label = label;
     wsdata_t * value = wsdata_create_string((char *)s, len);
     wsdata_add_label(value, label);
     wsdata_add_reference(value);
     proc->single_value = value;
}

static void XMLCALL
endElement(void *userData, const XML_Char *name) {
     proc_instance_t * proc = userData;

     add_buffered_values(proc);

     dprint(" endElement %s", name);
     if (proc->label_active) {
          proc->label_active = NULL;
     }
     else if (proc->pt.active && proc->pt.active->parent) {
          proc->pt.active = proc->pt.active->parent;
     }
}

#define ENCODING "UTF-8"

static void parse_set(proc_instance_t * proc) {
     XML_SetUserData(proc->parser, proc);
     XML_SetElementHandler(proc->parser, startElement, endElement);
     XML_SetCharacterDataHandler(proc->parser, valueHandler);
}
static void parse_reset(proc_instance_t * proc) {
     XML_ParserReset(proc->parser, ENCODING);
     parse_set(proc);
}

int procbuffer_init(void *vproc, void *type_table) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     proc->type_table = type_table;

     proc->parser = XML_ParserCreate(ENCODING);
     parse_set(proc);

     return 1;
}

static int32_t get_current_index_size(void * type_table) {
     mimo_datalists_t * mdl = (mimo_datalists_t *) type_table;
     return mdl->index_len;
}


int procbuffer_decode(void * vproc, wsdata_t * tuple,
                      wsdata_t * member,
                      uint8_t * buf, int len) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     uint32_t start_index_size = get_current_index_size(proc->type_table);
     wsdata_t * subtuple = wsdata_alloc(dtype_tuple);
     if (!subtuple) {
          return 1;
     }
     wsdata_add_label(subtuple, proc->label_subtuple);
    
     proc->label_active = NULL;
     parse_node_setnewnode(proc, subtuple, NULL);

     parse_reset(proc);
     int ret = XML_Parse(proc->parser, (char *)buf, len, XML_TRUE);

     dprint("XML_PARSE return value %d", ret);
     int extra_offset = 0;
     if (ret == XML_FALSE) {
          int code = XML_GetErrorCode(proc->parser);
          if (code == XML_ERROR_JUNK_AFTER_DOC_ELEMENT) {
               // treat extra data as separate XML document
               extra_offset = XML_GetCurrentByteIndex(proc->parser);
          }
          else {
               wsdata_add_label(subtuple, proc->label_error);
               proc->parse_error++;
               dprint("ERROR: %d, %s", code, XML_ErrorString(code));
          }
     }

     parse_node_free(proc);

     //free up outstanding data
     if (proc->single_value) {
          wsdata_delete(proc->single_value);
          proc->single_value = 0;
     }
     if (proc->buffer_len) {
          proc->buffer_len = 0;
     }

     //check if resulting tuple has any data
     wsdt_tuple_t * st = subtuple->data;
     if (st->len == 0) {
          wsdata_delete(subtuple);
          return 1;
     }

     //check if new labels were created - if so, reindex subtuple
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

     if (extra_offset && (extra_offset < len)) {
          dprint("extrabuf len %d", len - extra_offset);
          procbuffer_decode(vproc, tuple,
                      member,
                      buf + extra_offset, len - extra_offset);
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int procbuffer_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (proc->truncate) {
          tool_print("truncated values %" PRIu64, proc->truncate);
     }
     if (proc->parse_error) {
          tool_print("parsing errors %" PRIu64, proc->parse_error);
     }

     //free dynamic allocations
     //free(proc); // free this in the calling function

     XML_ParserFree(proc->parser);
     parse_node_destroy(proc);

     return 1;
}

