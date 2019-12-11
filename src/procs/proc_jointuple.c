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
// Joins tuples from two input ports, LEFT and RIGHT. There is no
// default input port.
#define PROC_NAME "jointuple"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "procloader.h"

char proc_version[]    = "1.5";
char *proc_menus[]     = { "Filters", NULL };
char *proc_tags[]      = { "Stream manipulation", NULL };
char *proc_alias[]     = { "join", "jointuples", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "joins tuples from two input streams and combines their members";
char *proc_synopsis[] = { "$left:LEFT, $right:RIGHT | jointuple -K <label> -L <label> -R <label> [-M <number>]", NULL };
char proc_description[] = "The jointuple kid implements a streaming join over "
	"two input ports (LEFT and RIGHT). For each incoming tuple on a port, "
	"the values for the KEYLABEL specified with the -K option are checked "
	"against a hash table of tuples stored from the other port. If a match "
	"is found, the members and labels of the stored tuple and the "
	"incoming tuple are combined and output as a single tuple. The "
	"stored tuple is then removed from its hash table so it will not "
	"match any more tuples. If no match is found, the incoming tuple "
	"is stored in its own port's hash table. "
	"The values that are copied to the output tuple can be controlled "
	"using the -L and -R flags.  Specifying -L LABEL causes values "
	"under LABEL to be copied only from the left-port tuple of a pair, "
	"and likewise for -R and the right-port tuples. Perhaps "
	"counterintuitively, -L LABEL -R LABEL means \"neither\" not \"both,\" "
	"so values with that label will not be copied. Only one copy of "
	"the key value that caused the tuples to match will appear in the "
	"output tuple, but if a tuple has multiple values for the key "
	"label, all values with that label from both sides will be copied "
	"(unless -L or -R is used with the KEYLABEL)."
	"The hash table for each port has a default size as with other "
	"waterslide hash tables, which defines the size of the join window. "
	"The size of the table can be changed with the -M flag. Items "
	"expire from the hash tables in the order they arrive, except that "
	"if an item with the same key value as an existing item arrives on "
	"the same port, the old value will expire immediately. Tuples may "
	"have multiple values for a KEYLABEL. If one of the values is "
	"matched by an incoming tuple, then the tuple and each of its key "
	"values is removed from the hash table.  However, a value expires "
	"because a newer tuple with the same key value arrives, or the "
	"table runs out of free slots, then only that value expires.  The "
	"tuple remains in the table until it is matched or all of its key "
	"values have expired. ";
proc_example_t proc_examples[] = {
	{"... | $left:LEFT, $right:RIGHT | jointuple -K KEYLABEL -L COMMON -R RIGHT_A -L RIGHT_B | ...", "Joins tuples from the right and left stream variables into a common tuple using the KEYLABEL to identify events to join."},
	{NULL, ""}
};
char proc_requires[] = "";

enum PortName { LEFT, RIGHT, NUM_PORTS };
#define OTHER_PORT(p) (1-p)

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared right port table with other kids",0,0},
     {'j',"","sharename",
     "shared left port table with other kids",0,0},
     {'K',"","LABEL",
     "key used for join",0,0},
     {'L',"","LABEL",
     "members with LABEL are taken from LEFT port only",0,0},
     {'R',"","LABEL",
     "members with LABEL are taken from RIGHT port only",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {"LEFT", "Tuples to be combined from left stream"},
     {"RIGHT", "Tuples to be combined from right stream"},
     {NULL, NULL}
};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};
char proc_nonswitch_opts[]    = "";

typedef struct _key_data_t {
     uint16_t cnt; // not needed?
     wsdata_t * tuple;
} key_data_t;

//function prototypes for local functions
static int proc_left(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_right(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt[NUM_PORTS];
     uint64_t outcnt[NUM_PORTS];

     stringhash5_t * port_table[NUM_PORTS];
     uint32_t buflen;
     wslabel_set_t lset[NUM_PORTS]; 
     wsdata_t * rmembers[WSDT_TUPLE_MAX];
     int rmember_len;
     wslabel_t * label_key;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;

     char * sharelabel;
     char * sharelabel5;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:j:L:R:K:M:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'j':
               proc->sharelabel5 = strdup(optarg);
               break;
          case 'K':
               proc->label_key = wssearch_label(type_table, optarg);
               tool_print("looking for key at label %s", optarg);
               break;
          case 'L':
               wslabel_set_add(type_table, &proc->lset[LEFT], optarg);
               tool_print("carrying data for label %s from LEFT port only", optarg);
               break;
          case 'R':
               wslabel_set_add(type_table, &proc->lset[RIGHT], optarg);
               tool_print("carrying data for label %s from RIGHT port only", optarg);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          tool_print("ignoring arg %s", argv[optind]);
          optind++;
     }
     
     return 1;
}

// This is a callback used when a slot in the hashtable is reused. If
// a tuple is stored in a slot, then that tuple is deleted. vproc is
// unused here.
static void last_destroy(void * vdata, void * vproc) {
     key_data_t * kdata = (key_data_t *)vdata;

     if (kdata->tuple) {
          wsdata_delete(kdata->tuple);
     }
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
// return 1 if ok
// return 0 if fail
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, ws_sourcev_t * sv,
              void * type_table) {
     
     //allocate proc instance of this processor
     proc_instance_t * proc =
          (proc_instance_t*)calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     ws_default_statestore(&proc->buflen);

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!proc->label_key) {
          tool_print("must specify key");
          return 0;
     }

     //other init - init the stringhash tables

     //init the first hash table
     if (proc->sharelabel5) {
          stringhash5_sh_opts_t * sh5_sh_opts;
          int ret;

          //calloc shared sh5 option struct
          stringhash5_sh_opts_alloc(&sh5_sh_opts);

          //set shared sh5 option fields
          sh5_sh_opts->sh_callback = last_destroy;
          sh5_sh_opts->proc = proc; 

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->port_table[LEFT], 
                                              proc->sharelabel5, proc->buflen, 
                                              sizeof(key_data_t), NULL, sh5_sh_opts); 

          //free shared sh5 option struct
          stringhash5_sh_opts_free(sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          proc->port_table[LEFT] = stringhash5_create(0, proc->buflen, sizeof(key_data_t));
          if (!proc->port_table[LEFT]) {
               return 0;
          }
          stringhash5_set_callback(proc->port_table[LEFT], last_destroy, proc);
     }

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->port_table[LEFT]->max_records;

     //init the second hash table
     if (proc->sharelabel) {
          stringhash5_sh_opts_t * sh5_sh_opts;
          int ret;

          //calloc shared sh5 option struct
          stringhash5_sh_opts_alloc(&sh5_sh_opts);

          //set shared sh5 option fields
          sh5_sh_opts->sh_callback = last_destroy;
          sh5_sh_opts->proc = proc; 

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->port_table[RIGHT], 
                                              proc->sharelabel, proc->buflen, 
                                              sizeof(key_data_t), NULL, sh5_sh_opts);

          //free shared sh5 option struct
          stringhash5_sh_opts_free(sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          proc->port_table[RIGHT] = stringhash5_create(0, proc->buflen, sizeof(key_data_t));
          if (!proc->port_table[RIGHT]) {
               return 0;
          }
          stringhash5_set_callback(proc->port_table[RIGHT], last_destroy, proc);
     }

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed 
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * meta_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, wsdatatype_get(type_table,
                                                                     "TUPLE_TYPE"), NULL);
     }

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")
	 && wslabel_match(type_table, port, "LEFT")) {
          return proc_left;
     }

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")
	 && wslabel_match(type_table, port, "RIGHT")) {
          return proc_right;
     }

     // The default port is unused!
     
     return NULL; // a function pointer
}

static inline key_data_t * get_keydata(proc_instance_t * proc, wsdata_t * key,
                                       wsdata_t * tdata, enum PortName port) {
     key_data_t * kdata = NULL;
     ws_hashloc_t * hashloc = key->dtype->hash_func(key);
     // if the hash of the key is valid, look it up in the table
     if (hashloc && hashloc->len) {
          kdata = (key_data_t *) stringhash5_find(proc->port_table[port],
	      (uint8_t*)hashloc->offset, hashloc->len);
     }

     // Did we find the key? (kdata->tuple should always be set if
     // kdata is valid)
     if( kdata && kdata->tuple ) {
       
       // Delete the found tuple from the hash table so we don't match it again
       stringhash5_delete(proc->port_table[port],
	   (uint8_t*)hashloc->offset, hashloc->len);
       return kdata;
     }

     // Didn't find a match for this key
     if (kdata) {
          stringhash5_unlock(proc->port_table[port]);
     }

     return NULL;
}

static inline void add_rmember(proc_instance_t * proc, wsdata_t * rmember) {
     int i;
     for (i = 0; i < proc->rmember_len; i++) {
          if (rmember == proc->rmembers[i]) {
               return;
          }
     }
     proc->rmembers[proc->rmember_len] = rmember;
     proc->rmember_len++;
}

static inline int not_removed(proc_instance_t * proc, wsdata_t * member) {
     int i;
     for (i = 0; i < proc->rmember_len; i++) {
          if (member == proc->rmembers[i]) {
               return 0;
          }
     }
     return 1;
}

// Copy members from source tuple to dest tuple, except
// those given in the omit set. All tuple labels are copied.
// We're following the example of removefromtuple here.
static inline void copy_members_except( proc_instance_t * proc,
    wsdata_t * src_tuple, wsdata_t * dest_tuple, wslabel_set_t omit )
{
    wsdata_duplicate_labels( src_tuple, dest_tuple );
    int member_cnt = 0;
    wsdata_t ** mset;
    int mset_len;

    int i;
    int j;
    // Make a list of members to exclude from the copy
    for (i = 0; i < omit.len; i++) {
         if (tuple_find_label(src_tuple, omit.labels[i], &mset_len, &mset)) {
	   for (j = 0; j < mset_len; j++ ) {
	     add_rmember(proc, mset[j]);
	   }
	 }
    }

    wsdt_tuple_t * tuple = (wsdt_tuple_t*)src_tuple->data;
    for (i = 0; i < tuple->len; i++) {
      if (not_removed(proc, tuple->member[i])) {
	add_tuple_member(dest_tuple, tuple->member[i]);
	member_cnt++;
      }
    }

}

static inline void store_keydata(proc_instance_t * proc, 
    wsdata_t * input_data, enum PortName port )
{
  int mset_len;
  wsdata_t ** mset;
  if (tuple_find_label(input_data, proc->label_key, &mset_len, &mset)) {
    // Create a new tuple to store the members we want to save from
    // the input tuple (all but those that are supposed to come
    // from the other port.)
    wsdata_t * newtuple = ws_get_outdata(proc->outtype_tuple);

    if( ! newtuple ) {
      return;  // No one is listening
    }

    proc->rmember_len = 0;  // reset the list of items to exclude
    copy_members_except( proc, input_data, newtuple,
	proc->lset[OTHER_PORT(port)] );

    // Now store a pointer to this tuple for each key value that
    // has the key label. We'll use the reference counter to
    // keep track of all the pointers so we only need one copy
    // of the tuple.
    int j;
    for (j = 0; j < mset_len; j++ ) {
      key_data_t * kdata = NULL;
      ws_hashloc_t * hashloc = mset[j]->dtype->hash_func(mset[j]);
      // Get a new hash entry for this item. If this key already exists,
      // replace the existing tuple with the new one. (This seems
      // simpler than merging the two tuples, especially if there
      // are multiple values for our key label!)
      if( hashloc && hashloc->len ) {
	kdata = (key_data_t *) stringhash5_find_attach(
	    proc->port_table[port], (uint8_t*)hashloc->offset, hashloc->len );
      }

      if( ! kdata ) {  // should never happen
	 tool_print( "got null pointer for hash table entry!" );
	 return;
      }

      // There's already a tuple with this key; delete it and reuse
      // the hash table slot.
      if( kdata->tuple ) {
        wsdata_delete(kdata->tuple);
      }

      // Store the tuple pointer and increment the reference counter
      kdata->tuple = newtuple;
      wsdata_add_reference( newtuple );
      stringhash5_unlock(proc->port_table[port]);
    }
  }
}    

// Remove the hash table entry and delete the tuple for a given
// key value.
static inline void remove_keydata( proc_instance_t * proc,
    wsdata_t * key, enum PortName port )
{
  ws_hashloc_t * hashloc = key->dtype->hash_func(key);
  key_data_t * kdata = (key_data_t *) stringhash5_find(
      proc->port_table[port],
      (uint8_t*)hashloc->offset, hashloc->len );
  if( kdata && kdata->tuple ) {	// should always be true
    // This should decrement the reference counter but not
    // necessarily delete the tuple, which we are probably
    // sending to the output.
    wsdata_delete( kdata->tuple );

    // Remove the hash table entry
    stringhash5_delete( proc->port_table[port],
      hashloc, hashloc->len );
    stringhash5_unlock(proc->port_table[port]);
  }
  else if (kdata) {
    stringhash5_unlock(proc->port_table[port]);
  }
}

// Generic processing for either port
static inline int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index, 
			enum PortName port)
{
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt[port]++;
     
     wsdata_t ** mset;
     wsdata_t * matched_member;
     int mset_len;
     int j;
     enum PortName other_port = OTHER_PORT(port);

     //get value
     key_data_t * kdata = NULL;
     if (tuple_find_label(input_data, proc->label_key, &mset_len, &mset)) {
       // Look for matching key in the OPPOSITE table
       for (j = 0; j < mset_len; j++ ) {
	 if ((kdata = get_keydata(proc, mset[j], input_data, other_port))
	     != NULL) {
	   matched_member = mset[j];
	   break; // Found a match!
	 }
       }
     }

     // If no match on opposite port, store the tuple in this port's table
     if (!kdata) {
          store_keydata(proc, input_data, port);
          return 0;
     }

     dprint("found key on port %d matching port %d input", other_port, port);

     // Now we merge members from the tuple that just came in with
     // the tuple that was found in the hash table. The stored
     // tuple contains only members that should be copied from that
     // port, and the tuple we just got might have some members that
     // should not be copied. We'll remove those as we build the
     // final tuple. Always exclude the key value that we joined on so
     // we don't get two copies of it. (This doesn't prevent other members
     // with the key label from being copied, and they might be duplicated.)

     if( not_removed( proc, matched_member ) ) {	// already excluded?
       proc->rmember_len = 0; // reset the list of items to exclude
       add_rmember( proc, matched_member ); 
     } else {
       proc->rmember_len = 0;
     }
     copy_members_except( proc, input_data, kdata->tuple,
	 proc->lset[other_port] );

     // Remove any other hash table references (by different key
     // values) to tuple. In the most complex case, key label L
     // could have values X and Y for the incoming tuple and Y and Z
     // for the stored one. We've already removed the reference to
     // Y (in get_keydata). Now we need to remove the reference to
     // Z, but we should not look for or remove references to X here.
     if (tuple_find_label(kdata->tuple, proc->label_key, &mset_len,
	   &mset)) {
       for (j = 0; j < mset_len; j++ ) {
	 if( mset[j] != matched_member ) {
           //must unlock here as remove_data calls stringhash5_find and
           //sets a new lock
           stringhash5_unlock(proc->port_table[other_port]);
	   remove_keydata( proc, matched_member, other_port );
	 }
       }
     }

     // Make this merged tuple the output tuple, then decrement
     // the reference count (which was used to keep this tuple
     // alive while it waited to be joined).
     ws_set_outdata(kdata->tuple, proc->outtype_tuple, dout);
     proc->outcnt[port]++;
     wsdata_delete(kdata->tuple);

     stringhash5_unlock(proc->port_table[other_port]);
     
     //always return 1 since we don't know if table will flush old data
     return 1;
}

// Process tuples arriving on the LEFT and RIGHT ports
//return 1 if output is available
// return 0 if not output
static int proc_left(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index)
{
  dprint("process left port tuple (%d)", LEFT);
  return proc_tuple( vinstance, input_data, dout, type_index, LEFT );
}

static int proc_right(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index)
{
  dprint("process right port tuple (%d)", RIGHT);
  return proc_tuple( vinstance, input_data, dout, type_index, RIGHT );
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc left cnt %" PRIu64, proc->meta_process_cnt[LEFT]);
     tool_print("meta_proc right cnt %" PRIu64, proc->meta_process_cnt[RIGHT]);
     tool_print("output left cnt %" PRIu64, proc->outcnt[LEFT]);
     tool_print("output right cnt %" PRIu64, proc->outcnt[RIGHT]);

     //destroy table
     stringhash5_scour_and_destroy(proc->port_table[LEFT], last_destroy, proc);
     stringhash5_scour_and_destroy(proc->port_table[RIGHT], last_destroy, proc);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->sharelabel5) {
          free(proc->sharelabel5);
     }
     free(proc);

     return 1;
}

