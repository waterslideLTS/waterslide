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

// waterslide.h
//  main header file needed for most all processing and datatype functions
#ifndef _WATERSLIDE_H
#define _WATERSLIDE_H

#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <libgen.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/time.h>
// items for pthreads
#include "shared/lock_init.h"
#include "sht_registry.h"
#include "wsstack.h"
#include "error_print.h"
#include "status_print.h"
#include "tool_print.h"
#include "dprint.h"
#include "cppwrap.h"

#if defined(__linux__)
#include <sys/stat.h>
#elif defined(__FreeBSD__)
#include <sys/sysctl.h>
#endif

// set current release version
#define WS_MAJOR_VERSION 1
#define WS_MINOR_VERSION 0
#define WS_SUBMINOR_VERSION 0

//prototype for data structures

//---------------------
// defined in mimo.c mimo.h
// interface for inseting metadata from a source program
#define ENV_WS_DATATYPE_PATH "WATERSLIDE_DATATYPE_PATH"
#define ENV_WS_PROC_PATH "WATERSLIDE_PROC_PATH"
#define ENV_WS_ALIAS_PATH "WATERSLIDE_ALIAS_PATH"
#define ENV_WS_CONFIG_PATH "WATERSLIDE_CONFIG_PATH"
#define ENV_WS_BASE_DIR "WATERSLIDE_BASE_DIR"

#define WS_STATESTORE_MAX "WS_STATESTORE_MAX"
#define WS_STATESTORE_DEFAULT 350000

#define WSDEFAULT_ALIAS_PATH ".wsalias"

#include "wsfree_list.h"

#define WS_PROC_MOD_PREFIX "/proc_"
#ifndef WS_PROC_MOD_SUFFIX
  #define WS_PROC_MOD_PARALLEL_SUFFIX ".wsp_so"
  #define WS_PROC_MOD_SERIAL_SUFFIX ".ws_so"
  #ifdef WS_PTHREADS
    #define WS_PROC_MOD_SUFFIX WS_PROC_MOD_PARALLEL_SUFFIX
  #else // !WS_PTHREADS
    #define WS_PROC_MOD_SUFFIX WS_PROC_MOD_SERIAL_SUFFIX
  #endif // WS_PTHREADS
#endif

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus


static inline char* get_executable_path(void)
{
#if defined(__linux__)
     char *path = realpath("/proc/self/exe", NULL);
     if ( !path ) {
          fprintf(stderr, "realpath('/proc/self/exe', NULL) returned NULL, with error %s\n", strerror(errno));
          return NULL;
     }
     if ( !realpath("/proc/self/exe", path) ) {
          fprintf(stderr, "realpath('/proc/self/exe', %p) returned NULL, with error %s\n", path, strerror(errno));
          return NULL;
     }

     return path;
#elif defined(__FreeBSD__)
     int mib[] = {CTL_KERN, KERN_PROC, KERN_PROC_PATHNAME, 0};
     size_t len;
     mib[3] = getpid();
     sysctl(mib, 4, NULL, &len, NULL, 0);
     char *path = (char*)malloc(len+1);
     if (!path) {
          error_print("failed get_executable_path malloc of path");
          return NULL;
     }
     sysctl(mib, 4, path, &len, NULL, 0);

     return path;

#else
     #warning "This platform needs work in get_executable_path(); WS_* environment variables must be manually configured."
     char *tmp = (char *)calloc(1, sizeof(char));
     if (!tmp) {
          error_print("failed get_executable_path calloc of tmp");
          return NULL;
     }

     return tmp;
#endif
}

#define CMDOPTION "command"

static inline void set_default_env(void)
{
     // base paths on executable location
     // any/all values can be overridden by environment variables or
     // command line options
     char wspath[1024];
     char string1[1024];  // TODO:  Should be longer... PATH_MAX (or some semi-portable version)
     char currenv[1024];  // BUGBUG: needed because dirname() appears to affect its parameter...what the crap?

     if (!getenv(ENV_WS_BASE_DIR)) {
	  char *exepath = get_executable_path();
	  if (!exepath) {
	       fprintf(stderr, "*** Can't determine invocation directory; set WS_BASE_DIR environment variable");
	       exit(1);
	  }
	  // chop off /bin/<executable>
	  strncpy(wspath, dirname(dirname(exepath)), sizeof(wspath)-1);
	  wspath[sizeof(wspath)-1] = '\0';
	  free(exepath);
     } else {
	  strncpy(wspath, getenv(ENV_WS_BASE_DIR), 1024);
	  fprintf(stderr, "  base directory from " ENV_WS_BASE_DIR ": %s\n", wspath);
     }

     if (!getenv(ENV_WS_PROC_PATH)) {
          strncpy(string1, wspath, sizeof(string1)-1);
          string1[sizeof(string1)-1] = '\0';
          strncat(string1, "/procs", sizeof(string1) - strlen(string1) - 1);
	  setenv(ENV_WS_PROC_PATH, string1, 0);
     } else {
          strncpy(currenv, getenv(ENV_WS_PROC_PATH), sizeof(currenv)-1);
          currenv[sizeof(currenv)-1] = '\0';
	  if (strcmp(dirname(currenv), wspath)) {
	       fprintf(stderr, "  non-default proc path: %s\n",getenv(ENV_WS_PROC_PATH));
	  }
     }

     if (!getenv(ENV_WS_ALIAS_PATH)) {
          strncpy(string1, wspath, sizeof(string1)-1);
          string1[sizeof(string1)-1] = '\0';
          strncat(string1, "/procs/.wsalias", sizeof(string1) - strlen(string1) - 1);
	  setenv(ENV_WS_ALIAS_PATH, string1, 0);
     } else {
          strncpy(currenv, getenv(ENV_WS_ALIAS_PATH), sizeof(currenv)-1);
          currenv[sizeof(currenv)-1] = '\0';
	  if (strcmp(dirname(currenv), wspath)) {
	       fprintf(stderr, "  non-default alias path: %s\n",getenv(ENV_WS_ALIAS_PATH));
	  }
     }

     if (!getenv(ENV_WS_DATATYPE_PATH)) {
          strncpy(string1, wspath, sizeof(string1)-1);
          string1[sizeof(string1)-1] = '\0';
          strncat(string1, "/lib", sizeof(string1) - strlen(string1) - 1);
	  setenv(ENV_WS_DATATYPE_PATH, string1, 0);
     } else {
          strncpy(currenv, getenv(ENV_WS_DATATYPE_PATH), sizeof(currenv)-1);
          currenv[sizeof(currenv)-1] = '\0';
	  if (strcmp(dirname(currenv), wspath)) {
	       fprintf(stderr, "  non-default datatype path: %s\n",getenv(ENV_WS_DATATYPE_PATH));
	  }
     }

     if (!getenv(ENV_WS_CONFIG_PATH)) {
          strncpy(string1, wspath, sizeof(string1)-1);
          string1[sizeof(string1)-1] = '\0';
          strncat(string1, "/config", sizeof(string1) - strlen(string1) - 1);
	  setenv(ENV_WS_CONFIG_PATH, string1, 0);
     } else {
          strncpy(currenv, getenv(ENV_WS_CONFIG_PATH), sizeof(currenv)-1);
          currenv[sizeof(currenv)-1] = '\0';
	  if (strcmp(dirname(currenv), wspath)) {
	       fprintf(stderr, "  non-default config path: %s\n",getenv(ENV_WS_CONFIG_PATH));
	  }
     }
}


// Globals
#ifdef _WSUTIL
uint32_t work_size;
const char ** global_kid_name;
uint32_t sht_perf;
#else
extern uint32_t work_size;
extern const char ** global_kid_name;
extern uint32_t sht_perf;
#endif

struct _mimo_t;
typedef struct _mimo_t mimo_t;
struct _mimo_source_t;
typedef struct _mimo_source_t mimo_source_t;
struct _mimo_sink_t;
typedef struct _mimo_sink_t mimo_sink_t;

typedef struct { // The argument list for the threads
     mimo_t * mimo;
     int rank;
     FILE * logfp;
} mimo_work_order_t;

typedef void (*mimo_ws_driver_callback)(mimo_work_order_t *);

//create a mimo processor.. must be the first thing that is called
mimo_t * mimo_init(void);

void mimo_set_srand(mimo_t *, unsigned int);

//destroy a mimo processor, frees memory (somewhat..)
int mimo_destroy(mimo_t*);

//Use the following to specify metadata sources for processing
//  use this to get your data into the processing graph
//  please note that the source_name needs to be the same name
///  as one source that is specified in the loaded graph
//  at this time, all sources must be registered FIRST prior to loading the graph..
// RETURNS: a pointer that must be used when emitting/sending data
mimo_source_t * mimo_register_source(mimo_t *, char * /*source name*/,
                                     char * /*data type name*/);

//Use the following to specify metadata sinks for receiving output
// from processing..
//  please note that the source_name needs to be the same name
//  as the sink that is specified in the loaded graph
//  at this time, all sinks must be registered FIRST prior to loading the graph..
// RETURNS: a pointer that must be used when receiving data
mimo_sink_t * mimo_register_sink(mimo_t *, char * /*sink name*/,
                                 char * /*data type name*/,
                                 char * /*data label name*/);

//load a processing graph..

// this loads a processing graph from a command string
int mimo_load_graph(mimo_t *, char * /*dtype str*/);

void mimo_load_datatypes(mimo_t *);
void mimo_set_verbose(mimo_t *);
void mimo_set_valgrind(mimo_t *);
void mimo_set_input_validate(mimo_t *);
void mimo_set_noexitflush(mimo_t *);

// this loads a processing graph from a config file
int mimo_load_graph_file(mimo_t *, char * /*filename*/);
void mimo_cleanup_flex_buffer(void);

int mimo_compile_graph(mimo_t *);
int mimo_compile_graph_internal(mimo_t *);

// the following is used to emit data as a source
// use this function when you want to be allocated a data buffer
// to copy your data into prior to emitting..
void * mimo_emit_data_copy(mimo_source_t *);

//run processing graph to completion based on emitting sources
// source data must be emitted (if available)
// prior to calling this function..
// you don't have to have all your sources emit data.. just some
// so that the graph can do work.
int mimo_run_graph(mimo_t*);
int mimo_run_exiting_graph(mimo_t*);

// the following is used to receive data as a sink
/// call this after you call mimo_run_graph
//  keep calling it until it returns NULL..
//  since you may have several bits of output data collected at a given sink
void * mimo_collect_data(mimo_sink_t *, char *);

//use the following to print out graph upon loading of file or command line
void mimo_output_graphviz(mimo_t *, FILE *);
void mimo_output_p_graphviz(mimo_t *, FILE *);

int mimo_flush_graph(mimo_t * mimo);


void mimo_add_aliases(mimo_t * mimo, char * filename);

typedef struct _wskid_t {
     uint32_t uid;
} wskid_t;

//macro for specifying uint64_t in a print statement.. architecture dependant..
#ifndef PRIu64
#if __WORDSIZE == 64
#define PRIu64 "lu"
#else
#define PRIu64 "llu"
#endif
#endif

// macro to check for null ptr returned by function
#define WS_CHECK_4_NULL(fcn,ptr) {if(!(ptr)) {error_print(#fcn " returned null " #ptr); return 0;} }

//------------------
//defined in init.h
//
// list of a processing modules function pointers and attributes
// loaded from a .so file
struct _ws_proc_module_t;
typedef struct _ws_proc_module_t ws_proc_module_t;

// initialized modules set up to process or generate data
struct _ws_proc_instance_t;
typedef struct _ws_proc_instance_t ws_proc_instance_t;

struct _ws_sourcev_t;
typedef struct _ws_sourcev_t ws_sourcev_t;

// elements to define a graph of processing
// list of proc_instance to proc_instance relationships
struct _ws_proc_vertex_edges_t;
typedef struct _ws_proc_vertex_edges_t ws_proc_vertex_edges_t;

//------------------
//defined in waterslidedata.h
struct _wsdatatype_t;
struct _wsdata_t;
typedef struct _wsdatatype_t wsdatatype_t;
typedef struct _wsdata_t wsdata_t;

// the following structure is used to assign labels to data structures
// used for both labeling input types for a specific processing element with
// ambiguous inputs, but also for assigning a name to a data type such
// as source ip to an ip data type.
typedef struct _wslabel_t {
     uint8_t registered;
     uint8_t search;
     uint16_t index_id;
     uint64_t hash;
     char * name;  //null terminated string
} wslabel_t;  //lookup list stored in mimo->datalists->label_table;

typedef struct _ws_hashloc_t {
     void * offset;  //offset into data
     int len;     //length of data to hash
} ws_hashloc_t;

#define WSDATA_MAX_LABELS 20

//generic structure for storing data references
struct _wsdata_t {
     wsfree_list_node_t fl_node;
     uint32_t has_hashloc : 1; //flag: hashing location defined..
     uint32_t isptr : 1; //flag: is a pointer to a wsdata reference of same type..
     WS_SPINLOCK_DECL(lock)
     int references;  //number of pointer reference to this data
     wsdatatype_t * dtype;
     wslabel_t * labels[WSDATA_MAX_LABELS]; //primary label for this data type..
     int label_len; //primary label for this data type..
     int writer_label_len;
     void * data; //allocated buffer.. of size dtype->len
     wsstack_t * dependency; // whether this is a or child of another
     ws_hashloc_t hashloc;
};

// add a label to a wsdata element.. return 1 if successful, 0 if fail
static inline int wsdata_add_label(wsdata_t * data, wslabel_t * label) {
#ifdef USE_ATOMICS
     int tmp;
     if (!label) return 0;
     tmp = __sync_fetch_and_add(&data->writer_label_len, 1);
     if (__builtin_expect(tmp >= WSDATA_MAX_LABELS, 0)) {
          (void) __sync_fetch_and_sub(&data->writer_label_len, 1);
          return 0;
     }
     data->labels[tmp] = label;
     while(data->label_len != tmp) asm("":::"memory"); // compiler fence
     data->label_len = tmp+1;
     return 1;
#else
     WS_SPINLOCK_LOCK(&data->lock);
     if (!label || (data->label_len >= WSDATA_MAX_LABELS)) {
          WS_SPINLOCK_UNLOCK(&data->lock);
          return 0;
     }
     data->labels[data->label_len] = label;
     data->label_len++;
     WS_SPINLOCK_UNLOCK(&data->lock);
     return 1;
#endif
}

static inline int wsdata_get_reference(wsdata_t * wsd) {
#ifdef USE_ATOMICS
     return __sync_fetch_and_add(&wsd->references, 0);
#else
     int rtn;
     WS_SPINLOCK_LOCK(&wsd->lock);
     rtn = wsd->references;
     WS_SPINLOCK_UNLOCK(&wsd->lock);
     return rtn;
#endif
}

//function to check if a single label is found in the set of labels
// of a wsdata type..
//returns 1 if wsdata has a label.. 0 otherwise
static inline int wsdata_check_label(wsdata_t * data, wslabel_t * label) {
     int i, found = 0;
     for (i = 0; i < data->label_len; i++) {
          if (data->labels[i] == label) {
               found = 1;
               break;
          }
     }
     return found;
}

static inline void wsdata_duplicate_labels(wsdata_t * src, wsdata_t * dst) {
     int i;
     for (i = 0; i < src->label_len; i++) {
          wsdata_add_label(dst, src->labels[i]);
     }
}

wsdatatype_t * wsdatatype_get(void *, const char *);
wslabel_t * wsregister_label(void *, const char * );
wslabel_t * wsregister_label_len(void *, const char *, int);
wslabel_t * wssearch_label(void *, const char * );
wslabel_t * wssearch_label_len(void *, const char *, int);
wslabel_t * wslabel_find_byhash(void *, uint64_t);

static inline wslabel_t * wsregister_label_wprefix(void * tt,
                                                   const char * prefix,
                                                   const char * suffix) {
     if (!prefix) {
          return wsregister_label(tt, suffix);
     }
     else if (!suffix) {
          return wsregister_label(tt, prefix);
     }
     else {
          char buf[1024];
          snprintf(buf, 1023, "%s%s", prefix, suffix);
          return wsregister_label(tt, buf);
     }
}

int wsregister_label_alias(void *, wslabel_t *, char *);

static inline int wsdata_add_reference(wsdata_t * wsd) {
#ifdef USE_ATOMICS
     (void) __sync_fetch_and_add(&wsd->references, 1);
#else
     WS_SPINLOCK_LOCK(&wsd->lock);
     wsd->references++;
     WS_SPINLOCK_UNLOCK(&wsd->lock);
#endif

     return 1;
}

// used to describe data dependancy with parent
static inline int wsdata_assign_dependency(wsdata_t* parent, wsdata_t * child) {
     if (!parent || !child) {
          return 0;
     }

     wsdata_add_reference(parent);

     WS_SPINLOCK_LOCK(&child->lock);
     if (!child->dependency) {
          child->dependency = wsstack_init();
          if (!child->dependency) {
               error_print("failed wsstack_init of child->dependency");
               WS_SPINLOCK_UNLOCK(&child->lock);
               return 0;
          }
     }
     if (!wsstack_add(child->dependency, parent)) {
          error_print("failed wsstack_add alloc");
          WS_SPINLOCK_UNLOCK(&child->lock);
          return 0;
     }
     WS_SPINLOCK_UNLOCK(&child->lock);

     return 1;
}


static inline int wsdata_remove_reference(wsdata_t * wsd) {
     int ret;
#ifdef USE_ATOMICS
     ret = __sync_fetch_and_add(&wsd->references, -1) - 1;
#else
     WS_SPINLOCK_LOCK(&wsd->lock);
     ret = --wsd->references;
     WS_SPINLOCK_UNLOCK(&wsd->lock);
#endif
     return ret;
}


int wsdatatype_match(void *, wsdatatype_t *, const char *);
int wslabel_match(void *, wslabel_t *, const char *);

///---------------
//defined in parse_graph.h
struct _parse_graph_t;
typedef struct _parse_graph_t parse_graph_t;

///---------------
//defined in waterslide_io.h
#define WS_SOURCE_ZEROCOPY 1
#define WS_SOURCE_COPY 0
struct _ws_outtype_t;
typedef struct _ws_outtype_t ws_outtype_t;
struct _ws_outlist_t;
typedef struct _ws_outlist_t ws_outlist_t;
struct _ws_doutput_t;
typedef struct _ws_doutput_t ws_doutput_t;
struct _ws_input_t;
typedef struct _ws_input_t ws_input_t;

//prototype of how a module is to process data and set outputs
typedef int (*proc_process_t)(void *, wsdata_t*, ws_doutput_t*, int);

//functions used by processing modules for setting output datatypes and data
ws_outtype_t * ws_add_outtype(ws_outlist_t *, wsdatatype_t*, wslabel_t *);
ws_outtype_t * ws_add_outtype_byname(void *, ws_outlist_t *, const char *, const char *);
wsdata_t* ws_get_outdata(ws_outtype_t*);

// attach outdata to its subscribers for attaching to job queue
int ws_set_outdata(wsdata_t*, ws_outtype_t*, ws_doutput_t*);
// delete alloc'd data that is not going to the job queue..


int ws_check_subscribers(ws_outtype_t*);

//functions for registering module as a source.. to be called in proc_init
ws_outtype_t* ws_register_source(wsdatatype_t *, proc_process_t,
                                 ws_sourcev_t *);
ws_outtype_t* ws_register_source_byname(void *, const char *, proc_process_t,
                                        ws_sourcev_t *);

ws_outtype_t* ws_register_monitor_source(proc_process_t, ws_sourcev_t *);

//-------------
//  other definitions
//function for processing specific data..

//structure for printing processing module's command line help
typedef struct _proc_option_t {
     char option;
     const char *long_option;
     const char *argument;
     const char *description;
     int  multiple; // 1 or 0
     int  required; // 1 or 0
} proc_option_t;

typedef struct _proc_example_t {
     const char *text;
     const char *description;
} proc_example_t;

typedef struct _proc_labeloffset_t {
     const char * labelstr;
     int offset;
} proc_labeloffset_t;

static inline void wsinit_labeloffset(proc_labeloffset_t * lo, void * vproc, void * type_table) {
     if (!lo) {
          return;
     }
     int i = 0;
     while(lo[i].labelstr && strlen(lo[i].labelstr)) {
          wslabel_t ** lbl = (wslabel_t**)((char *)vproc +
                                           lo[i].offset);
          *lbl = wsregister_label(type_table,
                                  lo[i].labelstr);

          dprint("lo %s %d", lo[i].labelstr, lo[i].offset);
          i++;
     }
}


typedef struct _proc_port_t {
     const char * label;
     const char * description;
} proc_port_t;

//used in tuple member selection and datatype subelement selection
#define WSMAX_LABEL_SET 128
typedef struct _wslabel_set_t {
     int len;
     wslabel_t * labels[WSMAX_LABEL_SET];
     int id[WSMAX_LABEL_SET];
} wslabel_set_t;

#define WSLABEL_NEST_SUBSET 32

typedef struct _wslabel_nested_set_t {
     wslabel_set_t lset[WSLABEL_NEST_SUBSET];
     int subsets;
     int cnt;
} wslabel_nested_set_t;

int wslabel_nested_search_build(void *, wslabel_nested_set_t *, const char *);

typedef struct _wslabel_set_ext_t {
     int len;
     wslabel_t * labels[WSMAX_LABEL_SET];
     int uid[WSMAX_LABEL_SET];
     int nid[WSMAX_LABEL_SET];
} wslabel_set_ext_t;

typedef struct _wslabel_nested_set_ext_t {
     wslabel_set_ext_t lset[WSLABEL_NEST_SUBSET];
     int subsets;
     int cnt;
} wslabel_nested_set_ext_t;

int wslabel_nested_search_build_ext(void *, wslabel_nested_set_ext_t *,
                                    const char *, int);

static inline int wslabel_set_add(void * type_table, wslabel_set_t * lset,
                                  const char * strlabel) {
    if (lset->len >= WSMAX_LABEL_SET) {
         return 0;
    }
    lset->labels[lset->len] = wssearch_label(type_table, strlabel);
    lset->len++;
    return 1;
}

static inline int wslabel_set_add_noindex(void * type_table, wslabel_set_t * lset,
                                          const char * strlabel) {
     if (lset->len >= WSMAX_LABEL_SET) {
          return 0;
     }
    lset->labels[lset->len] = wsregister_label(type_table, strlabel);
    lset->len++;
    return 1;
}


static inline int wslabel_set_add_id(void * type_table, wslabel_set_t * lset,
                                     const char * strlabel, int id) {
    if (lset->len >= WSMAX_LABEL_SET) {
         return 0;
    }
    lset->labels[lset->len] = wssearch_label(type_table, strlabel);
    lset->id[lset->len] = id;
    lset->len++;
    return 1;
}

static inline int wslabel_set_add_id_len(void * type_table, wslabel_set_t * lset,
                                         const char * strlabel, int len, int id) {
    if (lset->len >= WSMAX_LABEL_SET) {
         return 0;
    }
    lset->labels[lset->len] = wssearch_label_len(type_table, strlabel, len);
    lset->id[lset->len] = id;
    lset->len++;
    return 1;
}

static inline uint32_t ws_default_statestore(uint32_t * state_max) {
     uint32_t max = WS_STATESTORE_DEFAULT;

     char * env_state = getenv(WS_STATESTORE_MAX);
     if (env_state) {
          max = atoi(env_state);
          if (max == 0) {
               max = WS_STATESTORE_DEFAULT;
          }
     }
     if (state_max) {
          *state_max = max;
     }
     return max;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WATERSLIDE_H
