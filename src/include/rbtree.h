
// Code adapted from the one available at the following website:
// web.mit.edu/~emin/www.old/source_code/red_black_tree/index.html
// Original author:  Emin Martinian
//                   Signals, Information & Algorithms Group, MIT

/*

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that neither the name of Emin
   Martinian nor the names of any contributors are be used to endorse or
   promote products derived from this software without specific prior
   written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
   */

#ifndef _RBTREE_H
#define _RBTREE_H

#include <stdint.h>
#include "wsstack.h"
#include "waterslide.h" // for wsdata
#include "error_print.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

//  CONVENTIONS:  All data structures for red-black trees have the prefix
//                "rb_" to prevent name conflicts.
//                                                                     
//                Function names: Each word in a function name begins with
//                a capital letter.  An example function name is 
//                CreateRedTree(a,b,c). Furthermore, each function name
//                should begin with a capital letter to easily distinguish
//                them from variables.
//                                                                    
//                Variable names: Each word in a variable name begins with
//                a capital letter EXCEPT the first letter of the variable
//                name.  For example, int newLongInt.  Global variables have
//                names beginning with "g".  An example of a global
//                variable name is gNewtonsConstant.

typedef struct _rb_node_t {
     void * key;
     wsdata_t * wsd;
     int type_index;
     int red; // if red = 0 then the node is black
     struct _rb_node_t * left;
     struct _rb_node_t * right;
     struct _rb_node_t * parent;
} rb_node_t;

// cheap free list that is not thread-safe
typedef struct _node_list_t_ {
     // indices into array
     uint32_t head;
     uint32_t tail;
     rb_node_t ** nodelist;
     uint32_t size;
} node_list_t;

static inline node_list_t * node_list_init(uint32_t size) {
     assert(size > 0);
     node_list_t * list = (node_list_t *)calloc(1, sizeof(node_list_t));
     if (!list) {
          error_print("failed node_list_init calloc of list");
          return NULL;
     }

     int i = 0;
     list->size = size;

     list->nodelist = calloc(list->size, sizeof(rb_node_t *));
     if (!list->nodelist) {
          error_print("failed node_list_init calloc of list->nodelist");
          return NULL;
     }

     // allocate actual content
     for(i=0; i < list->size; i++) {
          list->nodelist[i] = calloc(1, sizeof(rb_node_t));
          if (!list->nodelist[i]) {
               error_print("failed node_list_init calloc of list->nodelist[i]");
               return NULL;
          }
     }

     list->tail = list->size; // index of next available is at index (list->tail - 1)

     // return our fresh list of nodes
     return list;
}

static inline rb_node_t * node_list_alloc(node_list_t * list) {
     if(list->tail == list->head) {
          // out of space
          return NULL;
     }

     rb_node_t * newnode = list->nodelist[list->tail - 1];
     list->tail--;
     list->nodelist[list->tail] = NULL;

     return newnode;
}

static inline void node_list_free(node_list_t * list, rb_node_t * thenode) {
     if(list->tail >= list->size) {
          // we have a problem...there is no space to add this node to
          error_print("trying to add more node than this list can hold");
          return;
     }

     memset(thenode, 0, sizeof(rb_node_t));
     list->nodelist[list->tail] = thenode;
     list->tail++;
}


// Compare(a,b) should return 1 if *a > *b, -1 if *a < *b, and 0 otherwise
// Destroy(a) takes a pointer to whatever key might be and frees it accordingly
typedef struct _rb_tree_t {
     int (*Compare)(const void * a, const void * b); 
     void (*DestroyKey)(void * a);
     void (*PrintKey)(const void * a);
  //  A sentinel is used for root and for nil.  These sentinels are
  //  created when RBTreeCreate is called.  root->left should always
  //  point to the node which is the root of the tree.  nil points to a
  //  node which should always be black but has arbitrary children and
  //  parent and no key.  The point of using these sentinels is so
  //  that the root and nil nodes do not require special cases in the code
     rb_node_t * root;             
     rb_node_t * nil;              
     rb_node_t * min_node; // pointer to a node with the least key value
     rb_node_t * max_node; // pointer to a node with the max key value
     rb_node_t * lastins_node; // pointer to the last inserted node
     node_list_t * node_free_list;
     uint32_t num_nodes; // number of actual (internal) nodes in this tree (excluding the leaf nodes)
} rb_tree_t;

static inline void* rb_node_alloc(void *arg)
{
     void *tmp = calloc(1, sizeof(rb_node_t));
     if (!tmp) {
          error_print("failed rb_node_alloc calloc of tmp");
          return NULL;
     }

     return tmp;
}

// default assumes a double value
void DefaultDestroyFunc(void * a) { free((double *)a); }
void DefaultPrintFunc(const void * a) { fprintf(stderr, "%f", *(double *)a); }

//***********************************************************************
//  FUNCTION:  RBTreeCreate
//
//  INPUTS:  All the inputs are names of functions.  CompFunc takes to
//  void pointers to keys and returns 1 if the first arguement is
//  "greater than" the second.   DestFunc takes a pointer to a key and
//  destroys it in the appropriate manner when the node containing that
//  key is deleted.
//  PrintFunc recieves a pointer to the key of a node and prints it.
//  If RBTreePrint is never called the print functions don't have to be
//  defined and NullFunction can be used. 
//
//  OUTPUT:  This function returns a pointer to the newly created
//  red-black tree.
//
//  Modifies Input: none
//***********************************************************************

static inline rb_tree_t * RBTreeCreate( int (*CompFunc) (const void *, const void *),
			      void (*DestFunc) (void *),
			      void (*PrintFunc) (const void *),
                              uint32_t maxsize) {
     rb_tree_t * newTree;
     rb_node_t * temp;

     newTree = (rb_tree_t *) calloc(1, sizeof(rb_tree_t));
     if (!newTree) {
          error_print("failed RBTreeCreate calloc of newTree");
          return NULL;
     }

     if (!CompFunc) {
          error_print("must define compare function pointer");
          return NULL;
     }
     newTree->Compare = CompFunc;

     if(NULL == DestFunc) {
          newTree->DestroyKey = DefaultDestroyFunc;
     }
     else {
          newTree->DestroyKey = DestFunc;
     }

     if(NULL == PrintFunc) {
          newTree->PrintKey = DefaultPrintFunc;
     }
     else {
          newTree->PrintKey = PrintFunc;
     }

     newTree->node_free_list = node_list_init(maxsize + 1);
     if(!newTree->node_free_list) {
          error_print("unable to allocate newTree->node_free_list");
          free(newTree);
          return NULL;
     }

     //  see the comment in the rb_tree_t structure in this file
     //  for information on nil and root
     temp = newTree->nil = (rb_node_t *) malloc(sizeof(rb_node_t));
     if (!newTree->nil) {
          error_print("failed RBTreeCreate malloc of newTree->nil");
          return NULL;
     }
     temp->parent = temp->left = temp->right = temp;
     temp->red = 0;
     temp->key = 0;

     temp = newTree->root = (rb_node_t *) malloc(sizeof(rb_node_t));
     if (!newTree->root) {
          error_print("failed RBTreeCreate malloc of newTree->root");
          return NULL;
     }
     temp->parent = temp->left = temp->right = newTree->nil;
     temp->key = 0;
     temp->red = 0;

     return(newTree);
}

//***********************************************************************
// FUNCTION:  LeftRotate
//
// INPUTS:  This takes a tree so that it can access the appropriate
//          root and nil pointers, and the node to rotate on.
//
// OUTPUT:  None
//
// Modifies Input: tree, x
//
// EFFECTS:  Rotates as described in _Introduction_To_Algorithms by
//           Cormen, Leiserson, Rivest (Chapter 14).  Basically this
//           makes the parent of x be to the left of x, x the parent of
//           its parent before the rotation and fixes other pointers
//           accordingly.
//***********************************************************************

static inline void LeftRotate(rb_tree_t * tree, rb_node_t * x) {
     rb_node_t * y;
     rb_node_t * nil = tree->nil;

     // I originally wrote this function to use the sentinel for
     // nil to avoid checking for nil.  However this introduces a
     // very subtle bug because sometimes this function modifies
     // the parent pointer of nil.  This can be a problem if a
     // function which calls LeftRotate also uses the nil sentinel
     // and expects the nil sentinel's parent pointer to be unchanged
     // after calling this function.  For example, when RBDeleteFixUP
     // calls LeftRotate it expects the parent pointer of nil to be
     // unchanged.

     y = x->right;
     x->right = y->left;

     if (y->left != nil) {
          y->left->parent = x; //used to use sentinel here
     }
     //and do an unconditional assignment instead of testing for nil
  
     y->parent = x->parent;   

     //instead of checking if x->parent is the root as in the book, we
     //count on the root sentinel to implicitly take care of this case
     if (x == x->parent->left) {
          x->parent->left = y;
     } 
     else {
          x->parent->right = y;
     }
     y->left = x;
     x->parent = y;
}

//***********************************************************************
// FUNCTION:  RighttRotate
//
// INPUTS:  This takes a tree so that it can access the appropriate
//          root and nil pointers, and the node to rotate on.
//
// OUTPUT:  None
//
// Modifies Input?: tree, y
//
// EFFECTS:  Rotates as described in _Introduction_To_Algorithms by
//           Cormen, Leiserson, Rivest (Chapter 14).  Basically this
//           makes the parent of x be to the left of x, x the parent of
//           its parent before the rotation and fixes other pointers
//           accordingly.
//***********************************************************************

static inline void RightRotate(rb_tree_t * tree, rb_node_t * y) {
     rb_node_t * x;
     rb_node_t * nil = tree->nil;

     // I originally wrote this function to use the sentinel for
     // nil to avoid checking for nil.  However this introduces a
     // very subtle bug because sometimes this function modifies
     // the parent pointer of nil.  This can be a problem if a
     // function which calls LeftRotate also uses the nil sentinel
     // and expects the nil sentinel's parent pointer to be unchanged
     // after calling this function.  For example, when RBDeleteFixUP
     // calls LeftRotate it expects the parent pointer of nil to be
     // unchanged.

     x = y->left;
     y->left = x->right;

     if (nil != x->right) {
          x->right->parent = y; // used to use sentinel here
     }
     //and do an unconditional assignment instead of testing for nil

     //instead of checking if x->parent is the root as in the book, we
     //count on the root sentinel to implicitly take care of this case
     x->parent = y->parent;
     if (y == y->parent->left) {
          y->parent->left = x;
     } 
     else {
          y->parent->right = x;
     }
     x->right = y;
     y->parent = x;
}

//***********************************************************************
// FUNCTION:  TreeInsertHelp 
//
// INPUTS:  tree is the tree to insert into and z is the node to insert
//
// OUTPUT:  none
//
// Modifies Input:  tree, z
//
// EFFECTS:  Inserts z into the tree as if it were a regular binary tree
//           using the algorithm described in _Introduction_To_Algorithms_
//           by Cormen et al.  This funciton is only intended to be called
//           by the RBTreeInsert function and not by the user
//***********************************************************************

static inline void TreeInsertHelp(rb_tree_t * tree, rb_node_t * z) {
     // This function should only be called by InsertRBTree (see above)
     rb_node_t * x;
     rb_node_t * y;
     rb_node_t * nil = tree->nil;
  
     z->left = z->right = nil;
     y = tree->max_node;
     if (1 != tree->Compare(y->key, z->key)) { //x.key <= z.key
          z->parent = y;
          y->right = z;
          tree->max_node = z;
          tree->lastins_node = z;
          return;
     } 
//  for insert at max node
#if 0
     while ( y->parent != tree->root) {
          if ( 1 == tree->Compare(y->key, z->key)) { //y.key > z.key
              y = y->parent ;
          }
         else {
               break;
          }
     }
#endif

//  for insert at last inserted node
//#if 0
     y = tree->lastins_node;
     int result = tree->Compare(tree->lastins_node->key, z->key);
     if ( 0 != result) {
          if ( 1 == result) {
               while ( (x=y->parent) != tree->root) {
                    if ( -1 == tree->Compare(x->key, z->key) ) break;
                    else y=x;
                }  // leave with y pointing to insert point
           } 
           else {   //implicetly, result = -1
                while ( (x=y->parent) != tree->root) {
                     if ( 1 == tree->Compare(x->key, z->key) ) break;
                     else y=x;
                }                                                             
            }     // leave with y pointing to insert point
     }
//#endif

     x = y;
     while (x != nil) {
          y = x;
          if (1 == tree->Compare(x->key, z->key)) { //x.key > z.key
               x = x->left;
          } 
          else { //x,key <= z.key
               x = x->right;
          }
     }
     z->parent = y;
     if ( (y == tree->root) ||
          (1 == tree->Compare(y->key, z->key))) { //y.key > z.key
          y->left = z;
     } 
     else {
          y->right = z;
     }
     tree->lastins_node = z;
}

// Before calling Insert RBTree the node x should have its key set

//***********************************************************************
// FUNCTION:  RBTreeInsert
//
// INPUTS:  tree is the red-black tree to insert a node which has a key
//          pointed to by key.
//
// OUTPUT:  This function returns a pointer to the newly inserted node
//          which is guarunteed to be valid until this node is deleted.
//          What this means is if another data structure stores this
//          pointer then the tree does not need to be searched when this
//          is to be deleted.
//
// Modifies Input: tree
//
// EFFECTS:  Creates a node node which contains the appropriate key
//           pointer and inserts it into the tree.
//***********************************************************************

static inline rb_node_t * RBTreeInsert_initial(rb_tree_t * tree, void * key, wsdata_t * wsd, int type_index) {
     rb_node_t * y;
     rb_node_t * x;
     rb_node_t * newNode;

     //x = (rb_node_t *) malloc(sizeof(rb_node_t));
     x = (rb_node_t *) node_list_alloc(tree->node_free_list);
     if (!x) {
          error_print("unable to allocate x");
          return NULL;
     }
     x->key = key;
     x->wsd = wsd;
     x->type_index = type_index;

     if (NULL != tree->max_node) TreeInsertHelp(tree, x);
     else {
          tree->max_node = x;
          tree->lastins_node = x;
          tree->root->left = x;
          x->parent = tree->root;
          x->left = x->right = tree->nil;
     }

     newNode = x;

//  As part of our optimization, don't ever insert the min node
//  for the general case where such an insert is allowed, uncomment this section
//  Also check while initially filling the tree
     if (NULL == tree->min_node || NULL == tree->min_node->key ||
         1 == tree->Compare(tree->min_node->key, newNode->key)) {
         tree->min_node = newNode;
     }

     x->red = 1;
     while (x->parent->red) { //use sentinel instead of checking for root
          if (x->parent == x->parent->parent->left) {
               y = x->parent->parent->right;
               if (y->red) {
   	            x->parent->red = 0;
	            y->red = 0;
	            x->parent->parent->red = 1;
	            x = x->parent->parent;
               } 
               else {
	            if (x == x->parent->right) {
	                 x = x->parent;
	                 LeftRotate(tree, x);
	            }
	            x->parent->red = 0;
	            x->parent->parent->red = 1;
	            RightRotate(tree, x->parent->parent);
               } 
          } 
          else { //case for x->parent == x->parent->parent->right
               y = x->parent->parent->left;
               if (y->red) {
   	            x->parent->red = 0;
	            y->red = 0;
	            x->parent->parent->red = 1;
	            x = x->parent->parent;
               } 
               else {
	            if (x == x->parent->left) {
	                 x = x->parent;
	                 RightRotate(tree, x);
	            }
	            x->parent->red = 0;
	            x->parent->parent->red = 1;
	            LeftRotate(tree, x->parent->parent);
               } 
          }
     }
     tree->root->left->red = 0;
     tree->num_nodes++;

     return(newNode);
}

// Before calling Insert RBTree the node x should have its key set

//***********************************************************************
// FUNCTION:  RBTreeInsert
//
// INPUTS:  tree is the red-black tree to insert a node which has a key
//          pointed to by key.
//
// OUTPUT:  This function returns a pointer to the newly inserted node
//          which is guarunteed to be valid until this node is deleted.
//          What this means is if another data structure stores this
//          pointer then the tree does not need to be searched when this
//          is to be deleted.
//
// Modifies Input: tree
//
// EFFECTS:  Creates a node node which contains the appropriate key
//           pointer and inserts it into the tree.
//***********************************************************************

static inline rb_node_t * RBTreeInsert(rb_tree_t * tree, void * key, wsdata_t * wsd, int type_index) {
     rb_node_t * y;
     rb_node_t * x;
     rb_node_t * newNode;

     //x = (rb_node_t *) malloc(sizeof(rb_node_t));
     x = (rb_node_t *) node_list_alloc(tree->node_free_list);
     if (!x) {
          error_print("unable to allocate x");
          return NULL;
     }
     x->key = key;
     x->wsd = wsd;
     x->type_index = type_index;

     TreeInsertHelp(tree, x);
     newNode = x;

//  As part of our optimization, don't ever insert the min node
//  for the general case where such an insert is allowed, uncomment this section
//     if (NULL == tree->min_node || NULL == tree->min_node->key ||
//         1 == tree->Compare(tree->min_node->key, newNode->key)) {
//         tree->min_node = newNode;
//     }


//  Keep track of the max node;
     if ( 1==tree->Compare(newNode->key, tree->max_node->key)) {
          tree->max_node = newNode;
     }

     x->red = 1;
     while (x->parent->red) { //use sentinel instead of checking for root
          if (x->parent == x->parent->parent->left) {
               y = x->parent->parent->right;
               if (y->red) {
   	            x->parent->red = 0;
	            y->red = 0;
	            x->parent->parent->red = 1;
	            x = x->parent->parent;
               } 
               else {
	            if (x == x->parent->right) {
	                 x = x->parent;
	                 LeftRotate(tree, x);
	            }
	            x->parent->red = 0;
	            x->parent->parent->red = 1;
	            RightRotate(tree, x->parent->parent);
               } 
          } 
          else { //case for x->parent == x->parent->parent->right
               y = x->parent->parent->left;
               if (y->red) {
   	            x->parent->red = 0;
	            y->red = 0;
	            x->parent->parent->red = 1;
	            x = x->parent->parent;
               } 
               else {
	            if (x == x->parent->left) {
	                 x = x->parent;
	                 RightRotate(tree, x);
	            }
	            x->parent->red = 0;
	            x->parent->parent->red = 1;
	            LeftRotate(tree, x->parent->parent);
               } 
          }
     }
     tree->root->left->red = 0;
     tree->num_nodes++;

     return(newNode);
}

//***********************************************************************
// FUNCTION:  TreeSuccessor 
//
//   INPUTS:  tree is the tree in question, and x is the node we want the
//            the successor of.
//
//   OUTPUT:  This function returns the successor of x or NULL if no
//            successor exists.
//
//   Modifies Input: none
//
//   Note:  uses the algorithm in _Introduction_To_Algorithms_
//***********************************************************************
  
static inline rb_node_t * TreeSuccessor(rb_tree_t * tree, rb_node_t * x) { 
     rb_node_t * y;
     rb_node_t * nil = tree->nil;
     rb_node_t * root = tree->root;

     if (nil != (y = x->right)) { //assignment to y is intentional
          while (y->left != nil) { //returns the minium of the right subtree of x
               y = y->left;
          }
          return(y);
     } 
     else {
          y = x->parent;
          while (x == y->right) { //sentinel used instead of checking for nil
               x = y;
               y = y->parent;
          }
          if (y == root) return(nil);
          return(y);
     }
}

//***********************************************************************
// FUNCTION:  Treepredecessor 
//
//   INPUTS:  tree is the tree in question, and x is the node we want the
//            the predecessor of.
//
//   OUTPUT:  This function returns the predecessor of x or NULL if no
//            predecessor exists.
//
//   Modifies Input: none
//
//   Note:  uses the algorithm in _Introduction_To_Algorithms_
//***********************************************************************

static inline rb_node_t * TreePredecessor(rb_tree_t * tree, rb_node_t * x) {
     rb_node_t * y;
     rb_node_t * nil = tree->nil;
     rb_node_t * root = tree->root;

     if (nil != (y = x->left)) { //assignment to y is intentional
          while (y->right != nil) { //returns the maximum of the left subtree of x
               y = y->right;
          }
          return(y);
     } 
     else {
          y = x->parent;
          while (x == y->left) { 
               if (y == root) {
                    return(nil); 
               }
               x = y;
               y = y->parent;
          }
          return(y);
     }
}

//***********************************************************************
// FUNCTION:  InorderTreePrint
//
//   INPUTS:  tree is the tree to print and x is the current inorder node
//
//   OUTPUT:  none 
//
//   EFFECTS:  This function recursively prints the nodes of the tree
//             inorder using the PrintKey functions.
//
//   Modifies Input: none
//
//   Note:    This function should only be called from RBTreePrint
//***********************************************************************

static inline void InorderTreePrint(rb_tree_t * tree, rb_node_t * x) {
     rb_node_t * nil = tree->nil;
     rb_node_t * root = tree->root;
     if (x != tree->nil) {
          InorderTreePrint(tree, x->left);
          fprintf(stderr, "  key = "); 
          tree->PrintKey(x->key);
          fprintf(stderr, "  l->key = ");
          if (x->left == nil) {
               fprintf(stderr, "NULL"); 
          }
          else {
               tree->PrintKey(x->left->key);
          }
          fprintf(stderr, "  r->key = ");
          if (x->right == nil) {
               fprintf(stderr, "NULL"); 
          }
          else {
               tree->PrintKey(x->right->key);
          }
          fprintf(stderr, "  p->key = ");
          if (x->parent == root) {
               fprintf(stderr, "NULL"); 
          }
          else {
               tree->PrintKey(x->parent->key);
          }
          fprintf(stderr, "  red = %i\n", x->red);
          InorderTreePrint(tree, x->right);
     }
}

//***********************************************************************
// FUNCTION:  TreeDestHelper
//
//   INPUTS:  tree is the tree to destroy and x is the current node
//
//   OUTPUT:  none 
//
//   EFFECTS:  This function recursively destroys the nodes of the tree
//             postorder using the DestroyKey function.
//
//   Modifies Input: tree, x
//
//   Note:    This function should only be called by RBTreeDestroy
//***********************************************************************

static inline void TreeDestHelper(rb_tree_t * tree, rb_node_t * x) {
     rb_node_t * nil = tree->nil;
     if (x != nil) {
          TreeDestHelper(tree, x->left);
          TreeDestHelper(tree, x->right);
          tree->DestroyKey(x->key);
          //free(x);
          node_list_free(tree->node_free_list, x);
     }
}

//***********************************************************************
// FUNCTION:  RBTreeDestroy
//
//   INPUTS:  tree is the tree to destroy
//
//   OUTPUT:  none
//
//   EFFECT:  Destroys the key and frees memory
//
//   Modifies Input: tree
//
//***********************************************************************

static inline void RBTreeDestroy(rb_tree_t * tree) {
     TreeDestHelper(tree, tree->root->left);
     free(tree->root);
     free(tree->nil);
     free(tree->node_free_list);
     free(tree);
}

//***********************************************************************
// FUNCTION:  RBTreePrint
//
//   INPUTS:  tree is the tree to print
//
//   OUTPUT:  none
//
//   EFFECT:  This function recursively prints the nodes of the tree
//            inorder using the PrintKey functions.
//
//   Modifies Input: none
//
//***********************************************************************

static inline void RBTreePrint(rb_tree_t * tree) {
     InorderTreePrint(tree, tree->root->left);
}

//***********************************************************************
// FUNCTION:  RBExactQuery
//
//   INPUTS:  tree is the tree to print and q is a pointer to the key
//            we are searching for
//
//   OUTPUT:  returns the a node with key equal to q.  If there are
//            multiple nodes with key equal to q this function returns
//            the one highest in the tree
//
//   Modifies Input: none
//
//***********************************************************************
  
static inline rb_node_t * RBExactQuery(rb_tree_t * tree, void * q) {
     rb_node_t * x = tree->root->left;
     rb_node_t * nil = tree->nil;
     int compVal;
     if (x == nil) {
          return(0);
     }
     compVal = tree->Compare(x->key, (int *)q);
     while (0 != compVal) {/*assignemnt*/
          if (1 == compVal) { //x->key > q
               x = x->left;
          } 
          else {
               x = x->right;
          }
          if (x == nil) {
               return(0);
          }
          compVal = tree->Compare(x->key, (int *)q);
     }

     return(x);
}

//***********************************************************************
// FUNCTION:  RBDeleteFixUp
//
//   INPUTS:  tree is the tree to fix and x is the child of the spliced
//            out node in RBTreeDelete.
//
//   OUTPUT:  none
//
//   EFFECT:  Performs rotations and changes colors to restore red-black
//            properties after a node is deleted
//
//   Modifies Input: tree, x
//
//   The algorithm from this function is from _Introduction_To_Algorithms_
//***********************************************************************

static inline void RBDeleteFixUp(rb_tree_t * tree, rb_node_t * x) {
     rb_node_t * root = tree->root->left;
     rb_node_t * w;

     while( (!x->red) && (root != x)) {
          if (x == x->parent->left) {
               w = x->parent->right;
               if (w->red) {
	            w->red = 0;
	            x->parent->red = 1;
	            LeftRotate(tree, x->parent);
	            w = x->parent->right;
               }
               if ( (!w->right->red) && (!w->left->red) ) { 
      	            w->red = 1;
	            x = x->parent;
               } 
               else {
	            if (!w->right->red) {
	                 w->left->red = 0;
	                 w->red = 1;
	                 RightRotate(tree, w);
	                 w = x->parent->right;
	            }
	            w->red = x->parent->red;
	            x->parent->red = 0;
	            w->right->red = 0;
	            LeftRotate(tree, x->parent);
	            x = root; //this is to exit while loop
               }
          } 
          else { //the code below is has left and right switched from above
               w = x->parent->left;
               if (w->red) {
	            w->red = 0;
	            x->parent->red = 1;
	            RightRotate(tree, x->parent);
	            w = x->parent->left;
               }
               if ( (!w->right->red) && (!w->left->red) ) { 
	            w->red = 1;
	            x = x->parent;
               } 
               else {
	            if (!w->left->red) {
	                 w->right->red = 0;
	                 w->red = 1;
	                 LeftRotate(tree, w);
	                 w = x->parent->left;
	            }
	            w->red = x->parent->red;
	            x->parent->red = 0;
	            w->left->red = 0;
	            RightRotate(tree, x->parent);
	            x = root; //this is to exit while loop
               }
          }
     }
     x->red = 0;
}

//***********************************************************************
// FUNCTION:  RBDelete
//
//   INPUTS:  tree is the tree to delete node z from
//
//   OUTPUT:  none
//
//   EFFECT:  Deletes z from tree and frees the key of z using DestroyKey.
//            Then calls RBDeleteFixUp to restore red-black properties
//
//   Modifies Input: tree, z
//
//   The algorithm from this function is from _Introduction_To_Algorithms_
//***********************************************************************

static inline void RBDelete(rb_tree_t * tree, rb_node_t * z){
     rb_node_t * y;
     rb_node_t * x;
     rb_node_t * successor = TreeSuccessor(tree, z);
     rb_node_t * nil = tree->nil;
     rb_node_t * root = tree->root;

     // set the new min node
     if(z == tree->min_node) {
          tree->min_node = successor;
     }

     y = ((z->left == nil) || (z->right == nil)) ? z : successor;
     x = (y->left == nil) ? y->right : y->left;
     if (root == (x->parent = y->parent)) { //assignment of y->p to x->p is intentional
          root->left = x;
     } 
     else {
          if (y == y->parent->left) {
               y->parent->left = x;
          } 
          else {
               y->parent->right = x;
          }
     }

     if (y != z) { //y should not be nil in this case
          //y is the node to splice out and x is its child

          if (!(y->red)) {
               RBDeleteFixUp(tree, x);
          }
  
          tree->DestroyKey(z->key);
          y->left = z->left;
          y->right = z->right;
          y->parent = z->parent;
          y->red = z->red;
          z->left->parent = z->right->parent = y;
          if (z == z->parent->left) {
               z->parent->left = y; 
          } 
          else {
               z->parent->right = y;
          }
          //free(z); 
          node_list_free(tree->node_free_list, z);
     } 
     else {
          tree->DestroyKey(y->key);
          if (!(y->red)) {
               RBDeleteFixUp(tree, x);
          }
          //free(y);
          node_list_free(tree->node_free_list, y);
     }
     tree->num_nodes--;
}

// return the pointer to the min_node of the tree; must delete this
// node after use by calling RBDelete(...)
static inline rb_node_t * RBGetMinimum(rb_tree_t * tree) {
     if(tree->min_node == tree->nil) {
          return NULL;
     }
     return tree->min_node;
}

//***********************************************************************
// FUNCTION:  RBDEnumerate
//
//   INPUTS:  tree is the tree to look for keys >= low
//            and <= high with respect to the Compare function
//
//   OUTPUT:  stack containing pointers to the nodes between [low,high]
//
//   Modifies Input: none
//***********************************************************************

static inline wsstack_t * RBEnumerate(rb_tree_t * tree, void * low, void * high) {
     wsstack_t * enumResultStack;
     rb_node_t * nil = tree->nil;
     rb_node_t * x = tree->root->left;
     rb_node_t * lastBest = nil;

     enumResultStack = wsstack_init();
     while (nil != x) {
          if ( 1 == (tree->Compare(x->key, high)) ) { //x->key > high
               x = x->left;
          } else {
               lastBest = x;
               x = x->right;
          }
     }
     while ( (lastBest != nil) && (1 != tree->Compare(low, lastBest->key))) {
          wsstack_add(enumResultStack, lastBest);
          lastBest = TreePredecessor(tree, lastBest);
     }

     return enumResultStack;
}
      
#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _RBTREE_H
