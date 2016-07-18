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
#include "stdlib.h"
#include "stdio.h"
#include "variance.h"
#include <math.h>

void var_hit(variance *var, double x) {
     if (var->window_size>0) {
          if (var->count>=var->window_size) {
               double oldestItem = var->previous_items[var->start];
               var->sum-=oldestItem;
               var->sumofsquares-=oldestItem*oldestItem;
          }
          var->previous_items[var->start]=x;
          var->start = (var->start+1) % var->window_size;
     }

     double x2 = x*x;
     var->sum+=x;
     var->sumofsquares+=x2;
     var->sumcubed += x2*x;
     var->count++;

}

double var_getSkewness(variance* var) {
     if( var->count>2 ) {
	  double mean = var_getMean(var);
          double windowCount = (var->window_size>0 && var->count>var->window_size? var->window_size:var->count);
	  double variance = var->sumofsquares/windowCount - mean*mean;
	  //	  double variance = var_getVariance(var);
	  if( variance <= 0.0 ) {
	       return 0.0;
	  }
	  double stddev = sqrt(variance);
	  double numer = var->sumcubed/windowCount - 3.0*mean*variance - mean*mean*mean;
	  double denom = variance*stddev;
	  double skewness = numer / denom;
	  return skewness;
     }
     else {
	  return 0.0;
     }
	  

}


double var_getVariance(variance* var) {
     if (var->count>1) {
          double windowCount = (var->window_size>0 && var->count>var->window_size? var->window_size:var->count);
          return (var->sumofsquares-((var->sum*var->sum)/windowCount))/(windowCount-1);
     } else {
         return 0.0;
     } 
}

double var_getMean(variance *var) {
     double windowCount = (var->window_size>0 && var->count>var->window_size? var->window_size:var->count);
     return var->sum/windowCount;
}
