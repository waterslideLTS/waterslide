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
#include <sys/types.h>
#include <unistd.h>
#include "waterslide.h"
#include "mimo.h"
#include "graphBuilder.h"
#include "setup_exit.h"

// Globals
mimo_t * mimo;
uint32_t nrank = 0, loop_limit = 0;
extern uint32_t do_exit;
char * pidfile = NULL;


void print_cmd_help(FILE * fp) {
     status_print("waterslide - version %d.%d.%d", WS_MAJOR_VERSION, WS_MINOR_VERSION, WS_SUBMINOR_VERSION);
     status_print("  [-F <file>] load processing graph");
     status_print("  [-l <cnt>] loop graph n times");
     status_print("  [-L <file>] file capturing stderr output");
     status_print("  [-X] turn off flushing of kids");
     status_print("  [-s <seed>] set random seed");
     status_print("  [-G <file>] save graphviz graph");
     status_print("  [-Z <file>] save graphviz graph with post-processing data.");
     status_print("  [-C <path>] set config path");
     status_print("  [-D <path>] set datatype path");
     status_print("  [-P <path>] set procs path");
     status_print("  [-A <file>] set path + filename to alias (.wsalias)");
     status_print("  [-h] printing of command line help");
     status_print("  [-p <path>] set PID file path");
     status_print("  [-r] validate input to kids");
     status_print("  [-V] turn on verbose status printing");
     status_print("  [-v] run with Valgrind - keep shared objects at termination");
     status_print("  [-t <level>] print stringhash table summary");
}

static void pidwrite(const char * path) {
     if (path && *path)
     {
          FILE * fptr = fopen(path, "w");
          if (fptr)
          {
               fprintf(fptr, "%d", getpid());
               fclose(fptr);
          }
     }
}

static int read_cmd_options(int argc, char ** argv, mimo_t * mimo, 
                            FILE * logfp) {
     int op;
     char * laststr = NULL;
     FILE * gfp;
     int rtn = 1;

     while ((op = getopt(argc, argv, "Vvrt:C:D:A:P:p:G:Z:l:L:F:s:Xh?")) != EOF) {
          switch (op) {
          case 'X':
               mimo_set_noexitflush(mimo);
               break;
          case 's':
               mimo_set_srand(mimo, atoi(optarg));
               break;
          case 'V':
               mimo_set_verbose(mimo);
               status_print("verbose mode is set");
               break;
          case 'v':
               mimo_set_valgrind(mimo);
               status_print("valgrind_dbg mode is set");
               break;
          case 'r':
               mimo_set_input_validate(mimo);
               status_print("input_validate mode is set");
               break;
          case 't':
               sht_perf = atoi(optarg);
               status_print("reporting data usage statistics on hashtables, level %d",sht_perf);
               break;
          case 'C':
               setenv(ENV_WS_CONFIG_PATH, optarg, 1);
               break;
          case 'D':
               setenv(ENV_WS_DATATYPE_PATH, optarg, 1);
               mimo_load_datatypes(mimo);
               break;
          case 'P':
               setenv(ENV_WS_PROC_PATH, optarg, 1);
               break;
          case 'A':
               setenv(ENV_WS_ALIAS_PATH, optarg, 1);
               break;
          case 'p':
               pidfile = strdup(optarg);
               break;
          case 'G':
               gfp = fopen(optarg, "w");
               if (gfp) {
                    status_print("printing graphviz graph: gfp %p", gfp);
                    mimo_output_graphviz(mimo, gfp);
               }
               else {
                    error_print("unable to open graph file %s", optarg);
               }
               break;
          case 'Z':
               gfp = fopen(optarg, "w");
               if (gfp) {
                    status_print("print graphviz graph");
                    mimo_output_p_graphviz(mimo, gfp);
               }
               else {
                    error_print("unable to open graph file %s", optarg);
               }
               break;
          case 'l':
               loop_limit = atoi(optarg);
               status_print("loop limit %u", loop_limit);
               break;
          case 'L':
               if (!(logfp = fopen(optarg, "w+"))) {
                    error_print("failed to open file '%s'", optarg);
                    return 0;
               }
               stderr = logfp;
               break;
          case 'F':
               pg_add_file(optarg);
               break;
          case 'h':
               print_cmd_help(stderr);
               break;
          case '?':
          default:
               return 0;
          }
     }

     while (optind < argc) {
          laststr = argv[optind];
          optind++;
     }
     if (laststr) {
          if (mimo->verbose) {
               fprintf(stderr,"command \"%s\"\n", laststr);
          }
          pg_set_cmdstring(laststr);
     }

     return rtn;
}

int main(int argc, char ** argv) {

     FILE * logfp = NULL;
     int i;

     //status_print("Number of available CPUs is %d", (int)sysconf(_SC_NPROCESSORS_ONLN));

     mimo = mimo_init();
     if (!mimo) {
          error_print("unable to init mimo");
          return 0;
     }

     mimo_add_aliases(mimo, NULL);

     if (!read_cmd_options(argc, argv, mimo, logfp)) {
          print_cmd_help(stderr);
          return 0;
     }

     if (!pg_parse()) {
          return 0;
     }

     // mimo_compile_graph has been split into the following 2 functions, with 
     // a bunch of pthreads stuff executed between them in the threaded case.
     if (!mimo_compile_graph(mimo)) {
          error_print("unable to compile graph");
          return 0;
     }
     if (!mimo_compile_graph_internal(mimo) ) {
          error_print("unable to complete compile graph");
          return 0;
     }

     if (pidfile) {
          pidwrite(pidfile);
          free(pidfile);
     }

     if (mimo->verbose) {
          fprintf(stderr,"starting to execute graph\n");
     }
     if (loop_limit) {
          for (i = 0; i < loop_limit; i++ ) {
               mimo_run_graph(mimo);
          }
          if (!mimo_flush_graph(mimo)) {
               return 0;
          }
     }
     else {
          //loop until control-C
          setup_exit_signals();
          while (!do_exit) {
               if (!mimo_run_graph(mimo)) {
                    if (mimo->verbose) {
                         status_print("end of source streams");
                    }
                    do_exit = 1;
                    break;
               }
          }
          if (do_exit) {
               if (!mimo_flush_graph(mimo)) {
                    return 0;
               }
          }
     }

     // Free memory, etc.
     if (!ws_cleanup(mimo)) {
          return 0;
     }

     // Close redirected output file
     if (logfp) {
         fclose(logfp);
     }

     return 0;
}
