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
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include "waterslide.h"
#include "so_loader.h"
#include "init.h"
#include "listhash.h"
#include "mimo.h"

// NOTE:  mimo->verbose is unset when modules and datatypes are loaded, which is 
//        before read_cmd_options is called. I recommend local control of verbosity, 
//        since we are rarely interested in this output.  So set this item to nonzero 
//        if you want so_loader verbosity.
#define SO_LOADER_VERBOSE 0

// Globals
// These would be a problem, if more than one thread were loading modules or datatypes
void ** stored_handles;
int num_fh = 0;

// find the right .so filename to load as a datatype
int datatype_file_filter(const struct dirent * entry) {
     int len;
     int sfx_len = strlen(WS_PROC_MOD_SUFFIX);

     if (strncmp(entry->d_name, "wsdt_", 5) == 0) {
          //check end of name...
          len = strlen(entry->d_name);
          if (strncmp(entry->d_name+(len-sfx_len),WS_PROC_MOD_SUFFIX,sfx_len) == 0) {
               return 1;
          }
     }
     return 0;
}

static int store_dlopen_file_handle(void * sh_file_handle) {
     if (sh_file_handle) {
          num_fh++;
          stored_handles = (void *)realloc(stored_handles, num_fh*sizeof(void *));
          if (!stored_handles) {
               error_print("failed realloc of stored_handles");
               return 0;
          }
          stored_handles[num_fh-1] = sh_file_handle;
     }
     return 1;
}

void free_dlopen_file_handles(void) {
     void * sh_file_handle;
     int i;
     for (i = 0; i < num_fh; i++) {
          sh_file_handle = stored_handles[i];
          dlclose(sh_file_handle);
     }
     free(stored_handles);
}

#define DATATYPELOADER_FUNC "datatypeloader_init"
typedef int (*datatypeloader_func)(void *);

int load_datatype_library(mimo_t *mimo, const char * dir, const char * file) {
     int dlen = strlen(dir);
     int flen = strlen(file);
     char * fullname = (char *)calloc(1,dlen+flen+2);
     if (!fullname) {
          error_print("failed load_datatype_library calloc of fullname");
          return 0;
     }
     int rtn = 0;

     memcpy(fullname, dir, dlen);
     fullname[dlen]= '/';
     memcpy(fullname + dlen+1, file, flen);

     //ok now we have filename to load..
     if (SO_LOADER_VERBOSE) {
          fprintf(stderr,"loading datatype library %s\n", fullname);
     }
     void * sh_file_handle;
     if ((sh_file_handle = dlopen(fullname, RTLD_NOW))){
          if(!store_dlopen_file_handle(sh_file_handle)) {
               return 0;
          }
          datatypeloader_func d_func =
               (datatypeloader_func) dlsym(sh_file_handle,
                                           DATATYPELOADER_FUNC);

          if (d_func) {
               //launch it..
               d_func(&mimo->datalists);
               if (SO_LOADER_VERBOSE) {
                    fprintf(stderr,"loaded %s successfully\n", fullname);
               }
               rtn = 1;
          }
          else {
               // this branch is encountered, for example, when looking for
               // hardware-architecture dependent datatype on a non-conforming
               // architecture
               if (SO_LOADER_VERBOSE) {
                    error_print("did not load datatype object %s\n", fullname);
                    error_print("proc dlopen %s", dlerror());
               }
          }
     }
     else {
          error_print("failed to load datatype library %s\n", fullname);
          error_print("proc dlopen %s", dlerror());
     }

     free(fullname);
     return rtn;
}

int load_datatype_dir(mimo_t * mimo, const char * dirname) {

     struct dirent **namelist;
     int rtn = 0;

     int n;

     if (dirname == NULL) {
          fprintf(stderr,"no datatype path.. set environment");
          return 0;
     }

     n = scandir(dirname, &namelist, datatype_file_filter, alphasort);

     if (n < 0) {
          perror("scandir");
     }
     else {
          while(n--) {
               rtn += load_datatype_library(mimo, dirname, namelist[n]->d_name);
               free(namelist[n]);
          }
          free(namelist);
     }
     return rtn; //number of libraries loaded
}

static void wsdatatype_init_sub(void * data, void * type_table) {
     wsdatatype_t * dtype = (wsdatatype_t *) data;
     int i;

     for (i = 0; i < dtype->num_subelements; i++) {
          wssubelement_t * sub = &dtype->subelements[i];
          //status_print("setting subelement %s:%s", sub->label->name, sub->dtype_name);
          if (sub->dtype_name) {
               sub->dtype = wsdatatype_get(type_table, sub->dtype_name);
               if (!sub->dtype) {
                    error_print("unknown dtype in subelement %s:%s", sub->label->name, sub->dtype_name);
               }
          }
          else {
               sub->dtype = NULL;
          }
     }
}

static void wsdatatype_init_subelements(mimo_t * mimo) {
     listhash_scour(mimo->datalists.dtype_table, wsdatatype_init_sub, &mimo->datalists);
}

#define PATHDELIM ":"
static int multiple_datapath_lookup(mimo_t * mimo, char * datatype_path) {
     char * dup = strdup(datatype_path);
     char * buf = dup;

     char * path;
     int rtn = 0;
     
     path = strsep(&buf, PATHDELIM);
     while (path) {
          int len = strlen(path);
          if (len) {
               rtn += load_datatype_dir(mimo, path);
          }
          path = strsep(&buf, PATHDELIM);
     }
     if (dup) {
          free(dup);
     }
     return rtn;
     
}

int load_datatype_libraries(mimo_t * mimo) {
     char * datatype_path = getenv(ENV_WS_DATATYPE_PATH);
     int rtn = 0;
     if (!datatype_path) {
          if (SO_LOADER_VERBOSE) {
               fprintf(stderr, "need to set environment %s\n",
                       ENV_WS_DATATYPE_PATH);
          }
          datatype_path="./datatypes";
          if (SO_LOADER_VERBOSE) {
               fprintf(stderr, "trying default %s\n",
                       datatype_path);
          }
          rtn = load_datatype_dir(mimo, datatype_path);
          //return 0;
     }
     else {
          if (SO_LOADER_VERBOSE) {
               fprintf(stderr,"datatype_path %s\n",datatype_path);
          }
          rtn = multiple_datapath_lookup(mimo, datatype_path);
          //rtn = load_datatype_dir(mimo, datatype_path);
     }

     wsdatatype_init_subelements(mimo);
     return rtn;
}

#define WS_MAX_MODULES 1024

#define MAX_ALIAS_BUF 1000
#define ALIAS_TOK ", :"
void ws_proc_alias_open(mimo_t * mimo, const char * filename) {
     FILE * fp = fopen(filename, "r");

     if (!fp) {
          error_print("ws_proc_alias_open input file %s could not be located\n", filename);
          error_print("Alias module not found");
          return;
     }

     if (SO_LOADER_VERBOSE) {
          status_print("loading aliases from %s", filename);
     }

     if (!mimo->proc_module_list) {
          mimo->proc_module_list = listhash_create(WS_MAX_MODULES,
                                                   sizeof(ws_proc_module_t));
     }

     char buf[MAX_ALIAS_BUF];
     int buflen;
     char * tok;
     char * ptok;
     ws_proc_module_t * module;

     while(fgets(buf, MAX_ALIAS_BUF, fp)) {
          module = NULL;
          buflen = strlen(buf);
          //strip of return character
          if (buf[buflen -1]=='\n') {
               buflen--;
               buf[buflen] = 0;
          }
          tok = strtok_r(buf, ALIAS_TOK, &ptok);
          if (tok) {
               //module name
               dprint("alias: setting module %s", tok);
               module = listhash_find_attach(mimo->proc_module_list,
                                             tok, strlen(tok));
               if (!module->name) {
                    module->name = strdup(tok);
               }
               module->strdup_set = 1;
          }
          if (!module) {
               continue;
          }
          //get aliases
          tok = strtok_r(NULL, ALIAS_TOK, &ptok);
          while (tok) {
               dprint("alias: setting alias %s", tok);
               listhash_find_attach_reference(mimo->proc_module_list,
                                              tok,
                                              strlen(tok),
                                              module);
               //get next alias
               tok = strtok_r(NULL, ALIAS_TOK, &ptok);
          }
     }

     fclose(fp);
}

static int so_load_wsprocbuffer(void * sh_file_handle,
                                 ws_proc_module_t * module) {

     module->pbkid = (wsprocbuffer_kid_t*)calloc(1, sizeof(wsprocbuffer_kid_t));
     if (!module->pbkid) {
          error_print("failed so_load_wsprocbuffer calloc of module->pbkid");
          return 0;
     }

     module->pbkid->init_func =
          (wsprocbuffer_sub_init) dlsym(sh_file_handle,"procbuffer_init");
     module->pbkid->option_func =
          (wsprocbuffer_sub_option) dlsym(sh_file_handle,"procbuffer_option");
     module->pbkid->option_str =
          (char *) dlsym(sh_file_handle,"procbuffer_option_str");
     module->pbkid->decode_func =
          (wsprocbuffer_sub_decode) dlsym(sh_file_handle,"procbuffer_decode");
    
     module->pbkid->element_func =
          (wsprocbuffer_sub_element) dlsym(sh_file_handle,"procbuffer_element");

     module->pbkid->destroy_func =
          (wsprocbuffer_sub_destroy) dlsym(sh_file_handle,"procbuffer_destroy");

     module->pbkid->labeloffset =
          (proc_labeloffset_t *) dlsym(sh_file_handle,"proc_labeloffset");

     module->pbkid->name = (char *) dlsym(sh_file_handle,"proc_name");

     if (!module->pbkid->option_str) {
          module->pbkid->option_str = "h";
     }

     int * npass;
     npass = (int*)dlsym(sh_file_handle,"procbuffer_pass_not_found");
     if (npass) {
          module->pbkid->pass_not_found = *npass;
     }

     int * isize;

     isize = (int *)dlsym(sh_file_handle,"procbuffer_instance_size");
     dprint("instance size %d", *isize);

     if (isize) {
          module->pbkid->instance_len = *isize;
     }

     module->proc_init_f = NULL;
     module->proc_input_set_f = wsprocbuffer_input_set;
     module->proc_destroy_f = wsprocbuffer_destroy;
     if (!module->name) {
          module->name = (char *) dlsym(sh_file_handle,"proc_name");
          module->pbkid->name = module->name;
     }

     return 1;
}

static int so_load_wsprockeystate(void * sh_file_handle,
                                 ws_proc_module_t * module) {

     module->kskid = (wsprockeystate_kid_t*)calloc(1, sizeof(wsprockeystate_kid_t));
     if (!module->kskid) {
          error_print("failed so_load_wsprockeystate calloc of module->kskid");
          return 0;
     }

     module->kskid->init_func =
          (wsprockeystate_sub_init) dlsym(sh_file_handle,"prockeystate_init");
     module->kskid->init_mvalue_func =
          (wsprockeystate_sub_init_mvalue) dlsym(sh_file_handle,"prockeystate_init_mvalue");
     module->kskid->option_func =
          (wsprockeystate_sub_option) dlsym(sh_file_handle,"prockeystate_option");
     module->kskid->option_str =
          (char *) dlsym(sh_file_handle,"prockeystate_option_str");
     module->kskid->update_func =
          (wsprockeystate_sub_update) dlsym(sh_file_handle,"prockeystate_update");
     module->kskid->force_expire_func =
          (wsprockeystate_sub_force_expire)
          dlsym(sh_file_handle,"prockeystate_force_expire");
     module->kskid->update_value_func =
          (wsprockeystate_sub_update_value)
          dlsym(sh_file_handle,"prockeystate_update_value");
     module->kskid->update_value_index_func =
          (wsprockeystate_sub_update_value_index)
          dlsym(sh_file_handle,"prockeystate_update_value_index");
     module->kskid->expire_func =
          (wsprockeystate_sub_expire) dlsym(sh_file_handle,"prockeystate_expire");
     module->kskid->expire_multi_func = 
          (wsprockeystate_sub_expire_multi) dlsym(sh_file_handle,"prockeystate_expire_multi");
     module->kskid->flush_func =
          (wsprockeystate_sub_flush) dlsym(sh_file_handle,"prockeystate_flush");
     module->kskid->destroy_func =
          (wsprockeystate_sub_destroy) dlsym(sh_file_handle,"prockeystate_destroy");
     module->kskid->post_update_mvalue_func =
          (wsprockeystate_sub_post_update_mvalue)
          dlsym(sh_file_handle,"prockeystate_post_update_mvalue");

     module->kskid->labeloffset =
          (proc_labeloffset_t *) dlsym(sh_file_handle,"proc_labeloffset");

     module->kskid->name = (char *) dlsym(sh_file_handle,"proc_name");

     int * gradual_expire;
     gradual_expire = (int*)dlsym(sh_file_handle,"prockeystate_gradual_expire");
     if (gradual_expire) {
          module->kskid->gradual_expire = *gradual_expire;
     }

     int * multivalue;
     multivalue = (int*)dlsym(sh_file_handle,"prockeystate_multivalue");
     if (multivalue) {
          module->kskid->multivalue = *multivalue;
     }
     int * value_size;
     value_size = (int*)dlsym(sh_file_handle,"prockeystate_value_size");
     if (value_size) {
          module->kskid->value_size = *value_size;
     }

     if (!module->kskid->option_str) {
          module->kskid->option_str = "";
     }

     int * isize;
     isize = (int *)dlsym(sh_file_handle,"prockeystate_instance_size");
     dprint("instance size %d", *isize);

     if (isize) {
          module->kskid->instance_len = *isize;
     }

     int * state_size;
     state_size = (int *)dlsym(sh_file_handle,"prockeystate_state_size");
     dprint("state size %d", *state_size);

     if (state_size) {
          module->kskid->state_len = *state_size;
     }

     module->proc_init_f = NULL;
     module->proc_input_set_f = wsprockeystate_input_set;
     module->proc_destroy_f = wsprockeystate_destroy;
     if (!module->name) {
          module->name = (char *) dlsym(sh_file_handle,"proc_name");
          module->kskid->name = module->name;
     }

     return 1;
}

#define DIRSEP_STR ":"
#define DIRSEP_CHR ':'
static mimo_directory_list_t * build_kid_dirlist(void) {
     char * envlist = getenv(ENV_WS_PROC_PATH);
     if (!envlist) {
          envlist = "./procs";
     }
     char * dstr = strdup(envlist);
     //find out how big a list
     char * ptr = dstr;

     mimo_directory_list_t * dlist =
          calloc(1, sizeof(mimo_directory_list_t)); 
     if (!dlist) {
          error_print("failed build_kid_dirlist calloc of dlist");
          return NULL;
     }

     dlist->len = 1;

     while((ptr = (char *)strchr(ptr, (int)DIRSEP_CHR)) != NULL) {
          dlist->len++;
          ptr++;
     }

     dlist->directories = calloc(dlist->len, sizeof(char *));
     if (!dlist->directories) {
          error_print("failed build_kid_dirlist calloc of dlist->directories");
          return NULL;
     }

     int j = 0;
     char * buf = dstr;
     char * dir = strsep(&buf, DIRSEP_STR);

     while (dir) {
          dlist->directories[j] = dir;
          int len = strlen(dir);
          dprint("adding kid directory path %s", dir);
          if (len > dlist->longest_path_len) {
               dlist->longest_path_len = len;
          }
          j++;
          if (j == dlist->len) {
               break;
          }
          dir = strsep(&buf, DIRSEP_STR);
     }

     return dlist;
}



ws_proc_module_t * ws_proc_module_dlopen(mimo_t * mimo, const char * fullname, const char * modname) {
     ws_proc_module_t * module;
     void * sh_file_handle;
     int i;

     sh_file_handle = dlopen(fullname, RTLD_NOW);
     
     if (!sh_file_handle) {
          error_print("proc dlopen %s", dlerror());
          return NULL;
     }
     if(!store_dlopen_file_handle(sh_file_handle)) {
          return NULL;
     }

     if (!mimo->proc_module_list) {
          mimo->proc_module_list = listhash_create(WS_MAX_MODULES,
                                                   sizeof(ws_proc_module_t));
     }
     module = listhash_find_attach(mimo->proc_module_list, modname,
                                   strlen(modname));

     int * pb = (int *) dlsym(sh_file_handle,"is_procbuffer");
     int * ks = (int *) dlsym(sh_file_handle,"is_prockeystate");
     if (pb) {
          if(!so_load_wsprocbuffer(sh_file_handle, module)) { 
               return NULL;
          }
     }
     else if (ks) {
          if(!so_load_wsprockeystate(sh_file_handle, module)) { 
               return NULL;
          }
     }
     else {
          module->proc_init_f =
               (proc_init_t) dlsym(sh_file_handle,"proc_init");
          module->proc_init_finish_f =
               (proc_init_finish_t) dlsym(sh_file_handle,"proc_init_finish");
          module->proc_input_set_f =
               (proc_input_set_t) dlsym(sh_file_handle,"proc_input_set");
          module->proc_destroy_f =
               (proc_destroy_t) dlsym(sh_file_handle,"proc_destroy");
          if (!module->name) {
               module->name = (char *) dlsym(sh_file_handle,"proc_name");
          }
     }

     if (!module->name) {
          error_print("module has no name");
          return NULL;
     }

     module->did_init = 1;

     int * dep = (int *)dlsym(sh_file_handle,"is_deprecated");
     if ( dep ) mimo_using_deprecated(mimo, module->name);

     dprint("here in dlopen");

     listhash_find_attach_reference(mimo->proc_module_list,
                                    module->name, strlen(module->name),
                                    module);

     dprint("here in dlopen2");

     module->aliases =
          (char **) dlsym(sh_file_handle,"proc_alias");


     dprint("here in dlopen3");
     
     //walk aliases .. add to module list
     if (module->aliases) {
          i = 0;
          while(module->aliases[i]) {
               dprint("alias %s, %s", module->name, module->aliases[i]);
               listhash_find_attach_reference(mimo->proc_module_list,
                                              module->aliases[i],
                                              strlen(module->aliases[i]),
                                              module);
               i++;
          }
     }

     return module;
}

static char * find_kid_fullpath(const char * modname, mimo_directory_list_t * dlist) {
     //allocate buffer for flength
     int totallen = dlist->longest_path_len + strlen(modname) +
          strlen(WS_PROC_MOD_PREFIX) + strlen(WS_PROC_MOD_SUFFIX) + 5;

     char * fullname = (char *)calloc(1, totallen);
     if (!fullname) {
          error_print("failed find_kid_fullpath calloc of fullname");
          return NULL;
     }

     struct stat statbuffer;

     int i;
     for (i = 0; i < dlist->len; i++) {
          snprintf(fullname, totallen, "%s%s%s%s",
                   dlist->directories[i], WS_PROC_MOD_PREFIX, modname,
                   WS_PROC_MOD_SUFFIX);
          dprint("attemping kid path %s", fullname);

          if (!stat(fullname, &statbuffer)) {
               // we have a file
               return fullname;
          }
     }
     free(fullname);
     return NULL;

}

ws_proc_module_t * ws_proc_module_find(mimo_t * mimo, const char * modname) {
     ws_proc_module_t * module = NULL;

     //see if module is already in list
     if (mimo->proc_module_list) {
          module = listhash_find(mimo->proc_module_list, modname,
                                 strlen(modname));     
          if (module) {
               if (module->did_init) {
                    return module;
               }
               else if (module->name) {
                    if (strcmp(module->name, modname) != 0) {
                         if (SO_LOADER_VERBOSE)  {
                              status_print("using %s for alias %s", module->name,
                                           modname);
                         }
                    }
                    modname = module->name;
               }
          }
     }

     if (!mimo->kid_dirlist) {
          mimo->kid_dirlist = build_kid_dirlist();
          if (!mimo->kid_dirlist) {
               return NULL;
          }
     }

     //get environment list .. try for each possible path

     //find module
     char * fullname = find_kid_fullpath(modname, mimo->kid_dirlist);

     if (!fullname) {
          return NULL;
     }

     //test file here.. with stat

     module = ws_proc_module_dlopen(mimo, fullname, modname);     

     free(fullname);
     return module;
}
