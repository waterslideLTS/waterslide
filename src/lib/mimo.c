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
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "waterslide_io.h"
#include "so_loader.h"
#include "sysutil.h"
#include "shared/shared_queue.h"
#include "listhash.h"
#include "mimo.h"
#include "wsprocess.h"
#include "wsperf.h"
#include "parse_graph.h"
#include "init.h"
#include "shared/getrank.h"
#include "shared/barrier_init.h"
#include "shared/setup_startup.h"
#include "shared/mimo_shared.h"
#include "sht_registry.h"
#include "shared/kidshare.h"
#include "graphBuilder.h"

// Global Globals
uint32_t work_size;
const char ** global_kid_name;
uint32_t sht_perf;

// Globals
uint32_t do_exit = 0, num_src_threads = 0, deadlock_firehose_shutoff = 0;
uint32_t ready_to_flush = 0, ready_to_exit = 0;
uint32_t spinning_on_jobs = 0;
uint32_t flushes_aborted = 0;
uint32_t graph_has_cycle = 0;
extern uint32_t work_size;
#ifdef WS_PTHREADS
pthread_mutexattr_t mutex_attr;
WS_MUTEX_DECL(startlock);
WS_MUTEX_DECL(endgame_lock);
WS_MUTEX_DECL(exit_lock);
mimo_work_order_t ** arglist;
pthread_t ** thread;
__thread int thread_rank;
pthread_barrier_t *barrier1;
#endif // WS_PTHREADS


// Prototypes
static int validate_variables (parse_graph_t * pg);
static int verify_procs_have_inputs (mimo_t * mimo);

// defined in mimo.c mimo.h
// interface for inseting metadata from a source program
//create and destroy interface
mimo_t * mimo_init(void) {
     int i;

     // Global work_size is initialized here
     work_size = 1;

     //initialize rand() command with time-based seed

     mimo_t * mimo = (mimo_t*)calloc(1, sizeof(mimo_t));
     if (!mimo) {
          error_print("failed mimo_init calloc of mimo");
          return NULL;
     }

     mimo->srand_seed = (unsigned int) time(NULL);
     srand(mimo->srand_seed);

     mimo->verbose = 0;

     // alloc and init job queues
     mimo->jobq = (nhqueue_t **)calloc(1, sizeof(nhqueue_t *));
     if (!mimo->jobq) {
          error_print("failed mimo_init calloc of mimo->jobq");
          return 0;
     }
     mimo->jobq_freeq = (nhqueue_t **)calloc(1, sizeof(nhqueue_t *));
     if (!mimo->jobq_freeq) {
          error_print("failed mimo_init calloc of mimo->jobq_freeq");
          return 0;
     }
     mimo->sink_freeq = (nhqueue_t **)calloc(1, sizeof(nhqueue_t *));
     if (!mimo->sink_freeq) {
          error_print("failed mimo_init calloc of mimo->sink_freeq");
          return 0;
     }

     i = 0;
     mimo->jobq[i] = queue_init();
     if (!mimo->jobq[i]) {
          error_print("failed mimo_init queue_init of mimo->jobq");
          return NULL;
     }
     mimo->jobq_freeq[i] = queue_init();
     if (!mimo->jobq_freeq[i]) {
          error_print("failed mimo_init queue_init of mimo->jobq_freeq");
          return NULL;
     }

     mimo->sink_freeq[i] = queue_init();
     if (!mimo->sink_freeq[i]) {
          error_print("failed mimo_init queue_init of mimo->sink_freeq");
          return NULL;
     }

     // queue for holding subscribers to be freed at mimo_destroy()
     mimo->subs = queue_init();
     if (!mimo->subs) {
          error_print("failed mimo_init queue_init of mimo->subs");
          return 0;
     }

     mimo->datalists.dtype_table =
          listhash_create(WS_MAX_TYPES, sizeof(wsdatatype_t));
     mimo->datalists.label_table =
          listhash_create(WS_MAX_LABELS, sizeof(wslabel_t));
     mimo->datalists.kidshare_table =
          listhash_create(WS_MAX_KIDSHARE, sizeof(mimo_kidshare_t));

     mimo->deprecated_list = listhash_create(MAX_DEPRECATED_COUNT, MAX_DEPRECATED_NAME);

     set_default_env();

     mimo_load_datatypes(mimo);

     set_sysutil_pConfigPath(getenv(ENV_WS_CONFIG_PATH));

     // '-1' value enables HWLOC (default is to enable HWLOC);
     mimo->thread_global_offset = -1;

     return mimo;
}

int mimo_init_localq(mimo_t * mimo) {

     int i;

     // Realloc the following to their full size (cf. mimo_init())
     mimo->jobq = (nhqueue_t **)realloc(mimo->jobq,
                                        work_size*sizeof(nhqueue_t *));
     if (!mimo->jobq) {
          error_print("failed realloc of mimo->jobq");
          return 0;
     }
     mimo->jobq_freeq = (nhqueue_t **)realloc(mimo->jobq_freeq,
                                              work_size*sizeof(nhqueue_t *));
     if (!mimo->jobq_freeq) {
          error_print("failed realloc of mimo->jobq_freeq");
          return 0;
     }
     mimo->sink_freeq = (nhqueue_t **)realloc(mimo->sink_freeq,
                                              work_size*sizeof(nhqueue_t *));
     if (!mimo->sink_freeq) {
          error_print("failed realloc of mimo->sink_freeq");
          return 0;
     }

     for(i = 1; i < work_size; i++) {
          mimo->jobq[i] = queue_init();
          if (!mimo->jobq[i]) {
               error_print("failed queue_init of mimo->jobq[i]");
               return 0;
          }
     }

     for(i = 1; i < work_size; i++) {
          mimo->jobq_freeq[i] = queue_init();
          if (!mimo->jobq_freeq[i]) {
               error_print("failed queue_init of mimo->jobq_freeq[i]");
               return 0;
          }
     }

     for(i = 1; i < work_size; i++) {
          mimo->sink_freeq[i] = queue_init();
          if (!mimo->sink_freeq[i]) {
               error_print("failed queue_init of mimo->sink_freeq[i]");
               return 0;
          }
     }

     return 1;
}

void mimo_load_datatypes(mimo_t * mimo) {
     //register data types....
     load_datatype_libraries(mimo);

     mimo->flush.outtype_flush.dtype = wsdatatype_get(&mimo->datalists, 
                                                      "FLUSH_TYPE");
     //if we have some datatypes..
     if (mimo->flush.outtype_flush.dtype) {
          init_wstypes(&mimo->datalists);
     }
}


void mimo_cleanup_pg(parse_graph_t * pg) {
     // free parsed_graph stuff
     queue_exit(pg->edges);

     listhash_scour(pg->vars, local_free_parse_vars, NULL);
     listhash_scour(pg->procs, local_free_parse_proc, NULL);
     listhash_destroy(pg->vars);
     listhash_destroy(pg->procs);

     if (pg->port) {
          free(pg->port);
     }
     free(pg);
}

int mimo_destroy(mimo_t* mimo) {
     const int nrank = GETRANK();

     ws_destroy_graph(mimo);

     // Use barrier to force kid summary stats before memory cleanup stats
     BARRIER_WAIT(barrier1);

     if (0 == nrank) {
          mimo_print_deprecated(mimo);
          // clean up the rest of the parsed graph stuff
          mimo_cleanup_pg(mimo->parsed_graph);

          // free misc. sysutil stuff
          free_sysutil_pConfigPath();

          // scour the listhash dtype table to get memory cleanup stats
          if (mimo->verbose) {
               fprintf(stderr,"\nWS Memory Cleanup Statistics:\n");
          }
          if (mimo->verbose) {
               listhash_scour(mimo->datalists.dtype_table, 
                              wsdatatype_profile_verbose, NULL);
          }
          else {
               listhash_scour(mimo->datalists.dtype_table, wsdatatype_profile, 
                              NULL);
          }

          // free dtype_table, label_table, and module names via callback functions.
          // must be done before the corresponding listhash_destroy calls below.
          // kidshare items are freed in the individual kids where they are used.
          listhash_scour(mimo->datalists.dtype_table, wsdatatype_free_dtypes, NULL);
          listhash_scour(mimo->datalists.label_table, wsdatatype_free_label_names, NULL);
          listhash_scour(mimo->proc_module_list, wsdatatype_free_module_names, NULL);

          // free more listhash stuff
          listhash_destroy(mimo->datalists.dtype_table);
          listhash_destroy(mimo->datalists.label_table);
          if (mimo->datalists.kidshare_table) {
               listhash_destroy(mimo->datalists.kidshare_table);
          }
          listhash_destroy(mimo->proc_module_list);
          listhash_destroy(mimo->deprecated_list);

          // free kid dirlist stuff
          free(mimo->kid_dirlist->directories[0]);
          free(mimo->kid_dirlist->directories);
          free(mimo->kid_dirlist);
 
          // free mimo queues
          int i;
          for(i = 0; i < work_size; i++) {
               queue_exit(mimo->jobq[i]);
               queue_exit(mimo->jobq_freeq[i]);
               queue_exit(mimo->sink_freeq[i]);
#ifdef WS_PTHREADS
               if(mimo->tg->num_scc) {
                    fqueue_exit(mimo->failoverq[i]);
               }
               shared_queue_exit(mimo->shared_jobq[i]);
#endif // WS_PTHREADS
          }
          free(mimo->jobq);
          free(mimo->jobq_freeq);
          free(mimo->sink_freeq);
#ifdef WS_PTHREADS
          for(i = 0; i < mimo->tg->num_scc; i++) {
               free(mimo->mgc[i].threadlist);
               free(mimo->mgc[i].fullshq);
          }
          free(mimo->mgc);
          free(mimo->failoverq);
          free(mimo->thread_in_cycle);
          free(mimo->shared_jobq);

          // clean up memory associated with the graph-cycle detection structure
          tarjan_graph_exit(mimo->tg);
#endif // WS_PTHREADS

          // free mimo proc sources (cf. ws_register_source)
          {
               ws_source_list_t * source = mimo->proc_sources, * next;
               while (source) {
                    next = source->next;
                    free(source);
                    source = next;
               }
          }

          // free mimo proc monitors (cf. ws_register_monitor_source)
          {
               ws_source_list_t * source = mimo->proc_monitors, * next;
               while (source) {
                    next = source->next;
                    free(source);
                    source = next;
               }
          }

          // free mimo sources
          {
               mimo_source_t * source = mimo->sources, * next;
               while (source) {
                    next = source->next;
                    ws_outtype_t *ocursor = &source->outtype;
                    while (ocursor->local_subscribers) {
                         ws_subscriber_t *onext = ocursor->local_subscribers->next;
                         free(ocursor->local_subscribers);
                         ocursor->local_subscribers = onext;
                    }
                    while (ocursor->ext_subscribers) {
                         ws_subscriber_t *onext = ocursor->ext_subscribers->next;
                         free(ocursor->ext_subscribers);
                         ocursor->ext_subscribers = onext;
                    }
                    free(source->name);
                    free(source);
                    source = next;
               }
          }

          // free mimo sinks
          {
               mimo_sink_t * sink = mimo->sinks, * next;
               while (sink) {
                    next = sink->next;
                    free(sink->name);
                    queue_exit(sink->dataq);
                    free(sink);
                    sink = next;
               }
          }

          // free mimo subscribers
          queue_exit(mimo->subs);

          // free mimo edges
          queue_exit(mimo->edges);

          // free mimo
          free(mimo);
     }

     // Ensure that all threads leave here at the same time.
     BARRIER_WAIT(barrier1);

     return 1;
}

void mimo_set_verbose(mimo_t * mimo) {
     mimo->verbose++;

     if (mimo->verbose == 2) {
          status_print("mimo srand seed %u", mimo->srand_seed);
     }
}

void mimo_set_valgrind(mimo_t * mimo) {
     mimo->valgrind_dbg = 1;
}

void mimo_set_input_validate(mimo_t * mimo) {
     mimo->input_validate = 1;
}

void mimo_set_srand(mimo_t * mimo, unsigned int seed) {
     mimo->srand_seed = seed;
     srand(seed);
}

//specify a named data type - done at init time.. before graph is loaded
mimo_source_t * mimo_register_source(mimo_t * mimo, char * source_name,
                                     char * dtype_name) {

     wsdatatype_t * dtype = wsdatatype_get(&mimo->datalists,
                                           dtype_name);
     if (!dtype) {
          return NULL;
     }
     mimo_source_t * source = calloc(1, sizeof(mimo_source_t));
     if (!source) {
          error_print("failed mimo_register_source calloc of source");
          return NULL;
     }
     source->name = strdup(source_name);
     source->outtype.dtype = dtype;
     source->mimo = mimo;
     source->next = mimo->sources;
     mimo->sources = source;

     return source;
}

int check_mimo_source(mimo_t * mimo, const char * source_name) {
     mimo_source_t * cursor;

     for (cursor = mimo->sources; cursor; cursor = cursor->next) {
          if (strcmp(source_name, cursor->name) == 0) {
               return 1;
          }
     }
     return 0;
}

mimo_source_t * mimo_lookup_source(mimo_t * mimo, const char * source_name) {
     mimo_source_t * cursor;

     for (cursor = mimo->sources; cursor; cursor = cursor->next) {
          if (strcmp(source_name, cursor->name) == 0) {
               return cursor;
          }
     }
     return NULL;
}

mimo_sink_t * mimo_lookup_sink(mimo_t * mimo, const char * sink_name) {
     mimo_sink_t * cursor;

     for (cursor = mimo->sinks; cursor; cursor = cursor->next) {
          if (strcmp(sink_name, cursor->name) == 0) {
               return cursor;
          }
     }
     return NULL;
}

int check_mimo_sink(mimo_t * mimo, const char * sink_name) {
     mimo_sink_t * cursor;

     for (cursor = mimo->sinks; cursor; cursor = cursor->next) {
          if (strcmp(sink_name, cursor->name) == 0) {
               return 1;
          }
     }
     return 0;
}

mimo_sink_t * mimo_register_sink(mimo_t * mimo, char * sink_name,
                                 char * dtype_name, 
                                 char * label_name) {
     const int nrank = GETRANK();

     wsdatatype_t * dtype = wsdatatype_get(&mimo->datalists,
                                           dtype_name);
     wslabel_t * label = wsregister_label(&mimo->datalists,
                                           label_name);
     if (!dtype) {
          return NULL;
     }
     mimo_sink_t * sink = calloc(1, sizeof(mimo_sink_t));
     if (!sink) {
          error_print("failed mimo_register_sink calloc of sink");
          return NULL;
     }
     sink->dtype = dtype;
     sink->input_label = label;
     sink->dataq = queue_init();
     if (!sink->dataq) 
     {
          error_print("failed queue_init of sink->dataq");
          return NULL;
     }

     sink->sink_freeq = mimo->sink_freeq[nrank];
     sink->name = strdup(sink_name);
     sink->next = mimo->sinks;
     mimo->sinks = sink;

     return sink;
}

int mimo_compile_graph(mimo_t * mimo) {
     int tmp = 0;

     /* Build PG from AST */
     mimo->parsed_graph = pg_buildGraph(mimo);

     if (!mimo->parsed_graph || !mimo->parsed_graph->procs->records) {
          fprintf(stderr," bad graph.. exiting\n");
          return 0;
     }


     // Validate that all stream variables are valid. Can't be done until all 
     // of the process graph have been scanned.
     if (validate_variables(mimo->parsed_graph) == 0) {
          error_print ("bad graph ... exiting");
          return 0;
     }

     collapse_parse_graph(mimo->parsed_graph);

     if (mimo->graphviz_fp) {
          wsprint_graph_dot(mimo->parsed_graph, mimo->graphviz_fp);
          fprintf(stderr,"exiting graph after compile.. plot only\n");
          pg_cleanup();
          exit(0);
     }

     tmp = optind;
     load_parsed_graph(mimo, mimo->parsed_graph);
     optind = tmp;

     work_size = mimo->thread_id+1;
     int max_cpus = (int)sysconf(_SC_NPROCESSORS_ONLN);
     if(work_size > max_cpus) {
          status_print("CAUTION: new work_size %d exceeds max_cpus %d", work_size, max_cpus);
          status_print("         threads will be wrapped around!");
          //return 0;
     }

     if (work_size > 1) {
          fprintf(stderr, "Total number of threads required (to create): %d\n", 
                  work_size);
     }

     // Pre-launch code is here.  All this stuff writes to shared memory, so 
     // all threads will be able to access the pre-launch results.
     if (!SETUP_STARTUP()) {
          return 0;
     }

     // Allocate a bunch of performance counter items, if needed
     if (!INIT_WSPERF()) {
          return 0;
     }

     // Init the stringhash table registry
     if (!init_sht_registry()) {
          return 0;
     }

     if (!mimo_init_localq(mimo)) {
          error_print("unable to init mimo localq");
          return 0;
     }

     if (!MIMO_INIT_SHAREDQ(mimo)) {
          error_print("unable to init mimo sharedq");
          return 0;
     }

     // create threads 1...work_size and invoke mimo_run_threaded_graph()
     if (!MIMO_CREATE_THREADS(mimo)) {
          error_print("unable to create threads");
          return 0;
     }

#ifdef WS_PTHREADS
     // struct for marking threads in a cycle (for deadlock recovery)
     mimo->tg = tarjan_graph_init(work_size);
     if(!mimo->tg) {
          error_print("failed mimo_init - unable to allocate memory for graph cycle detection");
          return 0;
     }
#endif // WS_PTHREADS

     return 1;
}

int mimo_compile_graph_internal(mimo_t * mimo) {

     // The input graph belongs to mimo structures.  Each graph within a 
     // pipeline will be condensed to a collapsed form by the parent thread.
     // This needs only to be done once by one chosen thread since it's shared.
     const int nrank = GETRANK();
     int tmp = optind;
     if (!ws_init_proc_graph(mimo)) {
          error_print ("cannot initialize graph: ws_init_proc_graph");
          if(0 == nrank) {
               optind = tmp;
          }
          return 0;
     }
     optind = tmp;

     // Summarize shared hash table use (and local memory hash tables)
     // Argument is 0 because no expiration counters are printed at this point
     if (sht_perf && 0 == nrank) {
          print_sht_registry(0, mimo->srand_seed);
     }

     // Ensure that all changes to mimo (inits) have been completed before
     // verifying procs have inputs.
     BARRIER_WAIT(barrier1);

     // mimo is not modified by verify_procs_have_inputs().  If mimo must be
     // modified here, then we must ensure the function is thread-safe!
     if(0 == nrank) {
          pg_cleanup();
          mimo_print_deprecated(mimo);
          return verify_procs_have_inputs(mimo);
     }

     return 1;
}


//get a data structure to copy metadata into..
void * mimo_emit_data_copy(mimo_source_t * source) {
     if (!source->outtype.local_subscribers && !source->outtype.ext_subscribers) {
          return NULL;
     }
     wsdata_t * wsdata = wsdata_alloc(source->outtype.dtype);
     if (!wsdata) {
          return NULL;
     }

// XXX: The #ifdef...#else...#endif below has duplicate code, so remember
//      to modify both branches when making any applicable changes. Code is
//      duplicated for performance optimization reasons.
#ifdef WS_PTHREADS
     if (source->outtype.local_subscribers) {
          if (!ws_add_local_job_source(source->mimo, wsdata, 
                                       source->outtype.local_subscribers)) {
               return NULL;
          }
     }

     if (source->outtype.ext_subscribers) {
          if (!ws_add_external_job_source(source->mimo, wsdata, 
                                          source->outtype.ext_subscribers)) {
               return NULL;
          }
     }
#else // !WS_PTHREADS
     if (!ws_add_local_job_source(source->mimo, wsdata, 
                                  source->outtype.local_subscribers)) {
          return NULL;
     }
#endif // WS_PTHREADS

     return wsdata->data;
}

//run processing graph to completion..
int mimo_run_graph(mimo_t* mimo) {
     // First clean up after last loop/run of graph..
     // remove old data from sink_freeq;
     const int nrank = GETRANK();
     if (mimo->sink_freeq[nrank]->size) {
          wsdata_t * wsdata;
          while ((wsdata = queue_remove(mimo->sink_freeq[nrank])) != NULL)
          {
               //remove reference to data and free it if necessary
               wsdata->dtype->delete_func(wsdata);
          }
     }
     
     // ok, now we can run the graph
     return ws_execute_graph(mimo);
}

//run an exiting processing graph to completion..
//this is typically called after the Ctrl-C or end of loop-limit
//in waterslide-parallel.c
int mimo_run_exiting_graph(mimo_t* mimo) {
     // First clean up after last loop/run of graph..
     // remove old data from sink_freeq;
     const int nrank = GETRANK();
     if (mimo->sink_freeq[nrank]->size)
     {
          wsdata_t * wsdata;
          while ((wsdata = queue_remove(mimo->sink_freeq[nrank])) != NULL)
          {
               //remove reference to data and free it if necessary
               wsdata->dtype->delete_func(wsdata);
          }
     }
     
     // ok, now we can run the graph
     return ws_execute_exiting_graph(mimo);
}

void mimo_set_noexitflush(mimo_t * mimo) {
     mimo->no_flush_on_exit = 1;
}

// let the user collect data from the data sink..
void * mimo_collect_data(mimo_sink_t * sink, char * dtype_name) {
     wsdata_t * wsdata;
     if ((wsdata = queue_remove(sink->dataq)) != NULL)
     {
          if (!queue_add(sink->sink_freeq, wsdata))
          {
               error_print("failed queue_add alloc");
               return NULL;
          }
          return wsdata->data;
     }
     return NULL;
}

void mimo_output_graphviz(mimo_t * mimo, FILE * fp) {
     mimo->graphviz_fp = fp;
}

void mimo_output_p_graphviz(mimo_t * mimo, FILE * fp) {
     mimo->graphviz_p_fp = fp;
}

void mimo_add_aliases(mimo_t * mimo, char * filename) {
     if (filename && sysutil_file_exists(filename)) {
          ws_proc_alias_open(mimo, filename);
     }
     else {
          char * alias_env = getenv(ENV_WS_ALIAS_PATH);
          if (alias_env && sysutil_file_exists(alias_env)) {
               ws_proc_alias_open(mimo, alias_env);
          }
          else if (sysutil_file_exists(WSDEFAULT_ALIAS_PATH)) {
               ws_proc_alias_open(mimo, WSDEFAULT_ALIAS_PATH);
          }
     }
}

// Performs validation checks on the stream variables.
// The following checks are performed:
//    - Verify all variables used as input have been assigned
static int validate_variables (parse_graph_t * pg) {
    parse_edge_t *edge, *edge2;
    q_node_t *qnode, *qnode2;
    parse_node_var_t *var, *var2;
    int valid;
    int retval = 1;

    // process all edges in the graph...
    for (qnode = pg->edges->head; qnode; qnode = qnode->next) {
        edge = (parse_edge_t *)qnode->data;

        // if the source (input) is not a variable skip it
        if ((edge->edgetype >> 4) != PARSE_NODE_TYPE_VAR) {
            continue;
        }

        valid = 0;
        var = (parse_node_var_t *)edge->src;

        // verify the input variable has been assigned somewhere
        for (qnode2 = pg->edges->head; qnode2; qnode2 = qnode2->next) {
            edge2 = (parse_edge_t *)qnode2->data;

            // is destination is not a variable (assignment) skip it
            if ((edge2->edgetype & 0xFF) != PARSE_NODE_TYPE_VAR) {
                continue;
            }

            var2 = (parse_node_var_t *)edge2->dst;
            if (strcmp(var->name, var2->name) == 0) {
                valid = 1;
                break;
            }
        }

        if (valid == 0) {
            error_print("variable '%s' used as input but never assigned", 
                        var->name);
            retval = 0;
        }
    }
    return retval;
}

// Verify that all non-source processes have at least one input
// Returns 1 if true, otherwise returns 0.
static int verify_procs_have_inputs (mimo_t * mimo) {
    ws_proc_instance_t *proc;
    ws_source_list_t *src;
    int is_source;

    // walk all the processes ...
    proc = mimo->proc_instance_head;
    while (proc) {

         // source processes do not have inputs; skip them.
         is_source = 0;
         src = mimo->proc_sources;
         while (src) {
              if (proc == src->pinstance){
                   is_source = 1;
                   break;
              }
              src = src->next;
         }
         src = mimo->proc_monitors;
         while (src) {
              if (proc == src->pinstance){
                   is_source = 1;
                   break;
              }
              src = src->next;
         }


         // a non-source process must have at least one input
         if (!is_source && proc->input_list.head == NULL) {
              error_print ("Process %s has no valid inputs ... aborting", 
                           proc->name ? proc->name : "NULL");
              return 0;
         }

         proc = proc->next;
    }

    return 1;
}


void mimo_using_deprecated(mimo_t * mimo, const char * deprecated_feature)
{
     char tmpName[MAX_DEPRECATED_NAME];
     strncpy(tmpName, deprecated_feature, MAX_DEPRECATED_NAME);
     tmpName[MAX_DEPRECATED_NAME-1] = '\0';
     size_t len = strlen(tmpName);
     if ( mimo->deprecated_list->records < MAX_DEPRECATED_COUNT ) {
          char *ptr = (char*)listhash_find_attach(mimo->deprecated_list, deprecated_feature, len);
          if ( ptr ) {
               memcpy(ptr, tmpName, len);
          }
     }
}


static void print_deprecated(void *data, void *ud)
{
     char *str = (char*)data;
     status_print("|\t%*s", MAX_DEPRECATED_NAME, str);
}

void mimo_print_deprecated(mimo_t *mimo)
{
     if ( mimo->deprecated_list->records > 0 ) {
          status_print("WARNING:  The following features are deprecated, and prone to removal.");
          listhash_scour(mimo->deprecated_list, print_deprecated, NULL);
          status_print("END OF DEPRECATION LIST\n\n");
     }
}

