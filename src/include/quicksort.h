
#ifndef _QUICKSORT_H
#define _QUICKSORT_H

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/*
Sort an array of pointers in-place, with order depending on external data
compar(x,y,data) > 0 iff y comes before x in sorted array 
*/

//type declaration for comparison function with external data
typedef int (* quicksort_compar_t)(void *, void *, void *);


void quicksort(void ** A, int len, quicksort_compar_t compar, void * freq){
  int i,j,k;
  void  * temp;

  if (len < 2) 
    return;
  if (len == 2){
    if (compar(A[0],A[1], freq) > 0)
        {temp = A[0]; A[0] = A[1]; A[1] = temp;}
    return;
  }

  //partition around A[0]
  i=-1; j=0; k=len;
  while(j < k-1){
    if(compar(A[j], A[j+1], freq) > 0){
      {temp = A[i+1]; A[i+1] = A[j+1]; A[j+1] = temp;}
      i++;j++;
    }
    else{
    if(compar(A[j+1], A[j], freq) > 0){
      {temp = A[j+1]; A[j+1] = A[k-1]; A[k-1] = temp;}
      k--;
    }
    else
      j++;
    }
  }

  //recursively sort subarrays
  quicksort(A,     i+1,   compar, freq);
  quicksort(&A[k], len-k, compar, freq);
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _QUICKSORT_H
