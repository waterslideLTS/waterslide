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
/* program to print out all the help options for each module*/
/* this program makes use of the aliases file for modules */

#define TOOL_NAME "wsalias"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <dlfcn.h>
#define _WSUTIL
#include "waterslide.h"
#include "waterslidedata.h"
#include "stringhash9a.h"
#include "wstypes.h"

char * sysutil_pConfigPath = NULL;

double (*nameExists)(void *, void *);

stringhash9a_t * exist = NULL; 
//char * sysutil_pConfigPath = "";

__thread int thread_rank = 0;


typedef struct _flow_destroy_attr_t flow_destroy_attr_t;
flow_destroy_attr_t * fdestroy_attr = NULL;
int global_num_flow_task_data = 0;
int global_flow_max_records = 0;

char * get_module_path(const char *name, const char * libpath) {
     int dlen = strlen(libpath);
     int modlen = strlen(name);
     int prefix = strlen(WS_PROC_MOD_PREFIX);
     int suffix = strlen(WS_PROC_MOD_SUFFIX);
     char * fullname = (char *)calloc(1,dlen+prefix+modlen+suffix+1);
     if (!fullname) {
          error_print("failed get_module_path calloc of fullname");
          return NULL;
     }

     memcpy(fullname, libpath, dlen);
     memcpy(fullname + dlen, WS_PROC_MOD_PREFIX, prefix);
     memcpy(fullname + dlen + prefix, name, modlen);
     memcpy(fullname + dlen + prefix + modlen,
            WS_PROC_MOD_SUFFIX, suffix);

     void * sh_file_handle;
     if ((sh_file_handle = dlopen(fullname, RTLD_LAZY|RTLD_LOCAL))) {
          // success indicates that a serial kid (preferred...) exists so proceed normally to use it

          // close the successfully opened shared-module handle
          dlclose(sh_file_handle);
     }
     else {
          free(fullname); // a serial kid with this name does not exist, try parallel kid...

          int suffix = strlen(WS_PROC_MOD_PARALLEL_SUFFIX);
          fullname = (char *)calloc(1,dlen+prefix+modlen+suffix+1);
          if (!fullname) {
               error_print("failed get_module_path calloc of fullname");
               return NULL;
          }

          memcpy(fullname, libpath, dlen);
          memcpy(fullname + dlen, WS_PROC_MOD_PREFIX, prefix);
          memcpy(fullname + dlen + prefix, name, modlen);
          memcpy(fullname + dlen + prefix + modlen,
                 WS_PROC_MOD_PARALLEL_SUFFIX, suffix);
     }
     return fullname;
}

static inline void print_list(FILE * fp, char ** list) {
     int i;
     for (i = 0; list[i]; i++) {
          if (i >0) {
               fprintf(fp, ", ");
          }

          if (stringhash9a_set(exist,list[i], strlen(list[i]))) {
//               fprintf(stderr, "DUPLICATE: alias name already exists '%s'\n", list[i]);
          } 
          fprintf(fp, "%s", list[i]);
     }
}

/**
 * by default this will use the new print out to pager option
 * unless you want to print to xml
 */
void print_module_help(FILE * fp, const char *spath)  {
     void * sh_file_handle;

     //fprintf(stderr, "%s\n", spath);

     //
     if (spath == NULL) {
          return;
     }

     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {
          // get the information from the module that we need to print help
          //char ** proc_menus  = (char**)dlsym(sh_file_handle,"proc_menus");
          char ** proc_alias  = (char**)dlsym(sh_file_handle,"proc_alias");
          char * proc_name    = (char *)dlsym(sh_file_handle,"proc_name");
	  if (!proc_name || !proc_alias) { 
	       fprintf(stderr, "UNSET values in %s\n", spath);
	       return;
	  }
          if (stringhash9a_set(exist,proc_name, strlen(proc_name))) {
//               fprintf(stderr, "DUPLICATE proc_name already exists %s %s\n",
//                       proc_name, spath);
          } 
          if (proc_alias && proc_alias[0]) {
               fprintf(fp, "%s: ", proc_name);
               print_list(fp, proc_alias);
               fprintf(fp, "\n");
          }
          else { // still print the proc_name (a valid alias to itself)
               fprintf(fp, "%s: %s", proc_name, proc_name);
               fprintf(fp, "\n");
          }

          // close this shared-module handle
          dlclose(sh_file_handle);
     }
     else {
          fprintf(stderr,"WSALIAS WARNING: %s\n", dlerror());
          fprintf(stderr,"WSALIAS WARNING: unable to open module %s\n", spath);
          return;
     }
     //fprintf(stderr, "%s -done\n", spath);
}

int datatype_file_filter(const struct dirent * entry) {
     int len;
     int sfx_len = strlen(WS_PROC_MOD_SUFFIX);
     int psfx_len = strlen(WS_PROC_MOD_PARALLEL_SUFFIX); // for kids that only build when WS_PARALLEL is enabled

     //fprintf(stderr,"check %s\n", entry->d_name);
     if (strncmp(entry->d_name, "proc_", 5) == 0) {
          //check end of name...
          len = strlen(entry->d_name);
          if ((strncmp(entry->d_name+(len-sfx_len),WS_PROC_MOD_SUFFIX,sfx_len) == 0) ||
              (strncmp(entry->d_name+(len-psfx_len),WS_PROC_MOD_PARALLEL_SUFFIX,psfx_len) == 0)) {
               //fprintf(stderr,"passed %s\n", entry->d_name);
               return 1;
          }
     }
     return 0;
}


int load_proc_dir(const char * dirname) {

     struct dirent **namelist;
     int rtn = 0;

     int n;
     char spath[5000];

     if (dirname == NULL) {
          fprintf(stderr,"no datatype path.. set environment");
          return 0;
     }
     int dirlen = strlen(dirname);

     n = scandir(dirname, &namelist, datatype_file_filter, alphasort);

     if (n < 0) {
          perror("scandir");
     }
     else {
          memcpy(spath, dirname, dirlen);
          spath[dirlen] = '/';
          int namelen;
          int i;
          for (i = 0; i < n; i++) {
               namelen = strlen(namelist[i]->d_name);
               memcpy(spath + dirlen + 1, namelist[i]->d_name, namelen);
               spath[dirlen+namelen + 1] = '\0';
               
               print_module_help(stdout, spath);
               free(namelist[i]);
          }
          free(namelist);
     }
     //fprintf(stderr, "load_proc_dir %s -done\n", dirname);
     return rtn; //number of libraries loaded
}

#define DIRSEP_STR ":"
void load_multiple_dirs(char * libpath) {
     char * buf = strdup(libpath);
     char * dir = strsep(&buf, DIRSEP_STR);

     while (dir) {
          //fprintf(stderr, "load_multiple_dir %s\n", dir);
          load_proc_dir(dir);
          //fprintf(stderr, "load_multiple_dir %s done loading\n", dir);
          dir = strsep(&buf, DIRSEP_STR);
          //fprintf(stderr, "load_multiple_dir newdir\n");
          if (dir) {
               //fprintf(stderr, "load_multiple_dir newdir %s \n", dir);
          }
     }
     //fprintf(stderr, "load_multiple_dir done %s\n",libpath);
     free(buf);
}

// entry point (possibly receive a quoted command line
int main (int argc, char *argv[]) {

     char * libpath;
     //nothing freed in here --- can not run with memory checks!!!
     //mem_session_init();
     // make sure we have a path to get to our module shared libraries

     set_default_env();

     if ((libpath = getenv(ENV_WS_PROC_PATH)) == NULL) {
          fprintf(stderr, "Please set the %s path variable... ", 
		  ENV_WS_PROC_PATH); 
          fprintf(stderr,"taking a guess\n"); 
          libpath = "./procs";
     }
     exist = stringhash9a_create(0, 1000);

     // are we just doing one (or more) modules
     if (argc > 1) {
          int x;
          for (x = 1; x < argc; x++) {
               print_module_help(stdout, get_module_path(argv[x], libpath));
               fflush(stdout);
          }
     }
     else {
          //print all modules in path
          load_multiple_dirs(libpath);
     }
     exit(0);
}


/* EMACS settings for correct tabbing
 * Local variables:
 *  c-basic-offset: 5
 * End:
 */
