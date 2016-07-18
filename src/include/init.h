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

#ifndef _INIT_H
#define _INIT_H

#include "waterslide.h"
#include "waterslide_io.h"
#include "wsprocbuffer.h"
#include "wsprockeystate.h"
#include "listhash.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

//note.. ws_sourcev_t is the same as ws_proc_instance_t...
typedef int (*proc_init_t)(wskid_t *, int, char * const *, void **, ws_sourcev_t *,
                           void *);

typedef int (*proc_init_finish_t)(void *);

typedef proc_process_t (*proc_input_set_t)(void *, wsdatatype_t *, wslabel_t *,
                                           ws_outlist_t *, int,
                                           void *);

typedef int (*proc_destroy_t)(void *);

struct _ws_proc_module_t {
     char * name;
     char ** aliases;
     proc_init_t proc_init_f;
     proc_init_finish_t proc_init_finish_f;
     proc_input_set_t proc_input_set_f; 
     proc_destroy_t proc_destroy_f;
     int use_count; 
     int did_init;
     int strdup_set;
     wsprocbuffer_kid_t * pbkid;
     wsprockeystate_kid_t * kskid;
};

// a list element for a set of destinations from a source
//  used prior to assigning subscribers & processing functions to a given source's out data
typedef struct _ws_proc_edge_t {
     ws_proc_instance_t * dst;
     mimo_sink_t * sink;
     wslabel_t * port;
     wslabel_t * src_label;
     int thread_trans;
     struct _ws_proc_edge_t * next;
} ws_proc_edge_t;

struct _ws_proc_instance_t {
     const char * name;
     int version; //based on module use count
     const char * instance_name;  //based on user defined graph 
     char * const * argv;
     int argc;
     wskid_t kid;
     int flush_register;
     ws_proc_module_t * module;
     void * instance;
     ws_outlist_t output_type_list;
     ws_doutput_t doutput;
     ws_input_t input_list; //input and output data
     ws_proc_edge_t * edges;
     int input_index;
     uint32_t thread_id; // should simply be zero in non-pthreads case
     uint32_t tid_assigned; // '0' or '1' indicating whether tid has been assigned or not
     struct _ws_proc_instance_t * next;
     char * srclabel_name; // will be NULL if there's no src_label (on the ws_subscriber_t pointing to me)
     int input_valid;
};

int ws_init_proc_graph(mimo_t *);
void local_free_parse_proc(void *, void *);
void local_free_parse_vars(void *, void *);
int load_parsed_graph(mimo_t *, parse_graph_t * );
void collapse_parse_graph(parse_graph_t *);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _INIT_H
