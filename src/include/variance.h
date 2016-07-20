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

#ifndef _VARIANCE_H
#define _VARIANCE_H 

#include "cppwrap.h"

/**
 * Compute a running variance.  Accumulates
 * the sum and sum of squares along the way so that 
 * variance and mean can be requested at any point
 * with minimum additional computation.
 */

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/** 
 * Stores information eneded to compute variance.
 * The structure methods are designed to work with
 * this structure passed to them zeroed out initially.
 * In that case, the methods will comput a total
 * variance and mean.  If the window_size and previous_items
 * fields are initialized, then the methods will keep a
 * windowed variance and mean calculation of the last
 * window_size items that it sees.  Note that previous_items
 * should be initialized to an array of doubles the length
 * of window_size.
 *
 */
typedef struct _variance {
     int window_size;
     int start;
     double *previous_items;
     
     double sum;
     double sumofsquares;
     double sumcubed;
     double count;
} variance;

/**
 * Add an item seen to the variances.
 * structure.
 */
void var_hit(variance*, double);

/**
 * Get the variance represented by
 * the variance object. Note that 
 * it does a final computation, however
 * it does not have to iterate over all
 * the items.
 */
double var_getVariance(variance*);

/**
 * Get the mean of the items seen
 * Note that it does a final computation,
 * however it does not have to iterate
 * over all the items.
 */
double var_getMean(variance*);


/**
 * Get the skewness represented by the variance
 * object.  Note that this does a final computation,
 * however it does not have to iterate over all the items.
 */
double var_getSkewness( variance* );

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _VARIANCE_H
