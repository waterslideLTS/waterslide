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
/* This code was initially adapted from the Java-implemented version on http://www.sanfoundry.com/java-program-tarjan-algorithm/
 * References for the fastest-known implementation of finding strongly connected subgraphs can be located the Wikipedia article
 * for Tarjan's algorithm for strongly connected components
 */

#ifndef _TARJAN_GRAPH_H
#define _TARJAN_GRAPH_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "wsqueue.h"
#include "wsstack.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// a graph used to implement Tarjan's algorithm for detecting strongly connected
// components of a directed graph, which gives conditions for the existence of cycles,
// which, in our case, can cause a cyclical wait on shared queues, a necessary 
// condition for the existence of communication deadlocks in WS's directed graph
typedef struct _tarjan_graph_t_ {
     nhqueue_t ** digraph;  // the directed graph
     nhqueue_t * scc_list;  // stores all strongly connected components
     wsstack_t * stack;

     int num_nodes;    // number of vertices
     int num_edges;    // number of edges
     int num_scc;      // cardinality of the set of strongly connected components
     int preCount;          // preorder number counter
     int * lowlink;         // low number of v
     uint8_t * visited;     // for checking if a node, v, is visited
} tarjan_graph_t;


static inline void dfs(tarjan_graph_t *, int); // function prototype
static inline void show_scc(tarjan_graph_t *);

static inline tarjan_graph_t * tarjan_graph_init(int num_nodes) {
     if(num_nodes <= 0) {
          error_print("invalid number of nodes specified");
          return NULL;
     }

     tarjan_graph_t *new_tarjan_graph = (tarjan_graph_t *)calloc(1, sizeof(tarjan_graph_t));
     if(!new_tarjan_graph) {
          error_print("unable to allocate tarjan_graph_t memory.");
          return  NULL;
     }

     int i;
     new_tarjan_graph->num_nodes = num_nodes;
     new_tarjan_graph->digraph = (nhqueue_t **)calloc(new_tarjan_graph->num_nodes, sizeof(nhqueue_t *)); // an array of lists (queues)
     for(i=0; i < new_tarjan_graph->num_nodes; i++) {
          new_tarjan_graph->digraph[i] = queue_init();
     }
     new_tarjan_graph->preCount = 0;
     new_tarjan_graph->lowlink = (int *)calloc(new_tarjan_graph->num_nodes, sizeof(int));
     new_tarjan_graph->visited = (uint8_t *)calloc(new_tarjan_graph->num_nodes, sizeof(uint8_t));
     new_tarjan_graph->stack = wsstack_init();
     new_tarjan_graph->scc_list = queue_init();

     return new_tarjan_graph;
}

static inline void compute_strongly_connected_components(tarjan_graph_t * tg) {
     int v;
     for(v = 0; v < tg->num_nodes; v++) {
          if(!tg->visited[v]) {
               dfs(tg, v);
          }
     }

     show_scc(tg);
}


static inline void show_scc(tarjan_graph_t * tg) {
     int i, j, val;
     nhqueue_t * list;
     int listsz;

     fprintf(stderr, "strongly connected components (WS threads): [");
     for(i=queue_size(tg->scc_list)-1; i >= 0; i--) {
          list = (nhqueue_t *)queue_get_at(tg->scc_list, i);
          listsz = queue_size(list);
          
          if(listsz > 1) {
               tg->num_scc++;
               fprintf(stderr, "[");
               for(j=listsz-1; j >= 0; j--) {
                    val = *(int *)queue_get_at(list, j);
                    fprintf(stderr, (j==listsz-1 ? "%d" : ", %d"), val);
               }
               fprintf(stderr, "]%s", (i==0) ? "\0" : ", ");
          }
     }
     if(0 == tg->num_scc) {
          fprintf(stderr, "]\nThere are NO cycles in your WS graph\n");
     } else if (1 == tg->num_scc) {
          fprintf(stderr, "]\nThere is at least %d cycle in your WS graph\n", tg->num_scc);
     } else {
          fprintf(stderr, "]\nThere are at least %d cycles in your WS graph\n", tg->num_scc);
     }
}

// performs a depth-first-search walk
static inline void dfs(tarjan_graph_t * tg, int v) {
     tg->lowlink[v] = tg->preCount++;
     tg->visited[v] = 1;

     int *vdata = (int*)malloc(sizeof(int));
     *vdata = v;
     wsstack_add(tg->stack, (void*)vdata);
     int min = tg->lowlink[v];

     uint32_t index;
     int w;
     nhqueue_t * graphV = (nhqueue_t *)tg->digraph[v];
     for (index = 0; index < queue_size(graphV); index++) {
          w = *(int *)queue_get_at(graphV, index);
          if(!tg->visited[w]) {
               dfs(tg, w);
          }
          if(tg->lowlink[w] < min) {
               min = tg->lowlink[w];
          }
     }
     if(min < tg->lowlink[v]) {
          tg->lowlink[v] = min;
          return;
     }

     nhqueue_t * component = queue_init();
     int *wdata;
     do {
          wdata = (int*)wsstack_remove(tg->stack);
          w = *wdata;
          queue_add(component, wdata);
          tg->lowlink[w] = tg->num_nodes;
     } while(w != v);
     queue_add(tg->scc_list, component);
}


static inline void tarjan_graph_exit(tarjan_graph_t * tg) {
     int i;
     wsstack_destroy(tg->stack);

     while(queue_size(tg->scc_list)) {
          queue_exit((nhqueue_t *)queue_remove(tg->scc_list));
     }
     queue_exit(tg->scc_list);

     for(i=0; i < tg->num_nodes; i++) {
          queue_exit(tg->digraph[i]);
     }
     free(tg->digraph);
     free(tg->lowlink);
     free(tg->visited);
     free(tg);
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _TARJAN_GRAPH_H
