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
#ifndef _WSMAN_MAP_H
#define _WSMAN_MAP_H

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "wsman_util.h"


typedef struct _map_t {
     char * key;
     char * val;
     struct _map_t *next;
} map_t;


map_t * map_create();
void    map_destroy(map_t *map);
char *  map_find(map_t * map, char *key);
int     map_insert(map_t * map, char * key, char * val);
void    map_print(map_t *map);


map_t * map_create() {
     map_t * map = (map_t *) malloc(sizeof(map_t));
     if (!map) return NULL;

     map->key  = NULL;
     map->val  = NULL;
     map->next = NULL;

     return map;
}

void map_destroy(map_t *map) {
     if (!map) return;

     map_t * tmp1 = map;
     map_t * tmp2;

     while (tmp1 != NULL) {
          tmp2 = tmp1;
          tmp1 = tmp1->next;
          free(tmp2->key);
          free(tmp2->val);
          free(tmp2);

     }
}

char * map_find(map_t * map, char *key) {
     map_t * tmp = map;
     while(tmp != NULL) {
          if (strcmp(tmp->key, key) == 0) {
               return tmp->val;
          }
          tmp = tmp->next;
     }

     return NULL;
}

int map_insert(map_t * map, char * key, char * val) {
     if (!map) return 1;

     // is this map empty?
     if (map->key == NULL) {
          // insert the key/val pair
          map->key = strdup(key);
          map->val = strdup(val);

          if (map->key && map->val) {
               // success
               return 0;
          } else {
               // failure in strdup
               return 1;
          }
     }

     map_t * tmp = map;

     while (tmp->next != NULL) {
          tmp = tmp->next;
     }

     map_t * new_map = (map_t *) malloc(sizeof(map_t));
     if (!new_map) return 1;

     new_map->key = strdup(key);
     new_map->val = strdup(val);

     new_map->next = NULL;
     tmp->next = new_map;

     if (new_map->key && new_map->val) {
          // success
          return 0;
     } else {
          // failure in strdup
          return 1;
     }
}

void map_print(map_t * map) {
     if (!map) return;

     map_t * tmp = map;
     while(tmp != NULL) {
          printf("%p, key: %s, value: %s, next: %p\n",
               tmp, tmp->key, tmp->val, tmp->next);
          tmp = tmp->next;
     }
}

#endif // _WSMAN_MAP_H
