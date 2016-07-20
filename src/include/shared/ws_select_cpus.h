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

#ifndef _WS_SELECT_CPUS_H
#define _WS_SELECT_CPUS_H

#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <hwloc.h>

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

typedef struct {
    uint32_t OS_cores;
    float *idle_levels;

    hwloc_topology_t topo;
    uint32_t nNodes;
} HWInfo_t;

/**
 * Initialize a HWInfo_t structure.
 * Will query the system to determine physical cpu layout and current load levels.
 *
 * @param info Pointer to uninitialized HWInfo_t
 * @return 1 on success, 0 on error
 */
static int getHardwareInfo(HWInfo_t *info);

/**
 * De-initializes a HWInfo_t structure
 * @param info Structure which needs associated memory freed.
 */
static void freeHardwareInfo(HWInfo_t *info);

/**
 * Returns an array of OS_index core numbers which are "free" to use.
 *
 * @param info Hardware Information parameter, as filled in by GetHardwareInfo()
 * @param minIdle Minimum percentage of CPU time that should be idle to consider a core "idle" (eg. 95.0)
 * @param nCores Number of cores which are being requested.
 * @param cores Array of length nCores which will be filled in by this function
 * @return 1 on success, 0 on error.
 */
static int getFreeCores(HWInfo_t *info, float minIdle, uint32_t nCores, uint32_t *cores);




/*  Internal functions */
static int getCoreIdles(uint32_t ncpu, uint64_t *idle, uint64_t *total, int secondCall);
static int getLoadLevels(float **levels, uint32_t *ncores);
static uint32_t getIdleCores(HWInfo_t *info, hwloc_obj_t baseObj, float minIdle, hwloc_cpuset_t *idleSet);
static void cpuSet_to_Cores(HWInfo_t *info, hwloc_const_cpuset_t set, uint32_t nCores, uint32_t *cores);


/****** IMPLEMENTATION:     PUBLIC FUNCTIONS ******/

/**
 * Initialize a HWInfo_t structure.
 * Will query the system to determine physical cpu layout and current load levels.
 *
 * @param info Pointer to uninitialized HWInfo_t
 * @return 1 on success, 0 on error
 */
static int getHardwareInfo(HWInfo_t *info)
{
    if ( !getLoadLevels(&info->idle_levels, &info->OS_cores) ) {
        return 0;
    }

    if ( hwloc_topology_init(&info->topo) )  // initialization
        return 0;
    if ( hwloc_topology_load(info->topo) )   // actual detection
        return 0;

    info->nNodes = hwloc_get_nbobjs_by_type(info->topo, HWLOC_OBJ_NODE);

    return 1;
}


/**
 * De-initializes a HWInfo_t structure
 * @param info Structure which needs associated memory freed.
 */
static void freeHardwareInfo(HWInfo_t *info)
{
    if ( info ) {
        if ( info->idle_levels ) free(info->idle_levels);

	if ( info->topo ) hwloc_topology_destroy(info->topo);
    }
}



/**
 * Returns an array of OS_index core numbers which are "free" to use.
 *
 * Currently, only will issue on "Physical" cores.  In a SMT or hyper-thread
 * environment, will only count "real" cores.  (Map 1 thread per real core.)
 *
 * @param info Hardware Information parameter, as filled in by GetHardwareInfo()
 * @param minIdle Minimum percentage of CPU time that should be idle to consider a core "idle" (eg. 95.0)
 * @param nCores Number of cores which are being requested.
 * @param cores Array of length nCores which will be filled in by this function
 * @return 1 on success, 0 on error.
 */
static int getFreeCores(HWInfo_t *info, float minIdle, uint32_t nCores, uint32_t *cores)
{
     if ( !cores ) {
          errno = EINVAL;
          return 0;
     }

     if ( nCores > info->OS_cores ) {
          fprintf(stderr, "WARNING: Requesting more cores than on the system!\n");
          errno = EINVAL;
          return 0;
     }

     hwloc_cpuset_t idleSet;

     static const hwloc_obj_type_t checkLevels[] = {
          HWLOC_OBJ_NODE,
          HWLOC_OBJ_SOCKET,
          HWLOC_OBJ_MACHINE
     };
     static const size_t nCheckLevels = sizeof(checkLevels)/sizeof(hwloc_obj_type_t);

     for ( size_t level = 0; level < nCheckLevels ; level++ ) {
          int nObj = hwloc_get_nbobjs_by_type(info->topo, checkLevels[level]);
          if ( nObj > 0 ) {
               hwloc_obj_t obj = hwloc_get_obj_by_type(info->topo, checkLevels[level], 0);

               while ( obj ) {
                    uint32_t nIdle = getIdleCores(info, obj, minIdle, &idleSet);
                    if ( nIdle >= nCores ) {
                         cpuSet_to_Cores(info, idleSet, nCores, cores);
                         hwloc_bitmap_free(idleSet);
                         return 1;
                    }
                    hwloc_bitmap_free(idleSet);

                    obj = obj->next_sibling;
               }

          }
     }

     fprintf(stderr, "WARNING: Not enough idle cores for intelligent thread mapping!\n");
     return 0;
}




/****** IMPLEMENTATION:     INTERNAL FUNCTIONS ******/


#if defined(__linux)
static int getCoreIdles(uint32_t ncpu, uint64_t *idle, uint64_t *total, int secondCall)
{
    char buffer[1024] = {0};
    uint32_t nRead = 0;
    FILE *fp = fopen("/proc/stat", "r");
    if ( !fp ) {
        perror("fopen");
        return 0;
    }

    while ( (nRead < ncpu) && fgets(buffer, 1024, fp) ) {
        uint32_t cpu = 0;
        int read = sscanf(buffer, "cpu%" PRIu32, &cpu);
        if ( (read == 1) && (cpu < ncpu) ) {
            uint32_t count = 0;
            uint64_t coreTot = 0;
            uint64_t coreIdle = 0;

            // Advance past 'cpu'
            char *buf = &buffer[3];

            errno = 0; // Reset
            unsigned long long val = strtoull(buf, &buf, 10);
            if ( errno != 0 ) {
                perror("strtoull");
                fclose(fp);
                return 0;
            }
            if ( val != cpu ) {
                fprintf(stderr, "Mis-read /proc/stat!\n");
                fclose(fp);
                return 0;
            }
            do {
                errno = 0;
                val = strtoull(buf, &buf, 10);
                if ( errno != 0 ) {
                    perror("strtoull");
                    fclose(fp);
                    return 0;
                }
                coreTot += val;
                if ( count == 3 ) coreIdle = val;
                count++;

            } while ( *buf != '\0' && *buf != '\n' );

            if ( count < 3 ) {
                fprintf(stderr, "Mis-read /proc/stat!\n");
                fclose(fp);
                return 0;
            }

            nRead++;

            if ( secondCall ) {
                idle[cpu] = coreIdle - idle[cpu];
                total[cpu] = coreTot - total[cpu];
            } else {
                idle[cpu] = coreIdle;
                total[cpu] = coreTot;
            }
        }

    }


    fclose(fp);
    return 1;
}
#elif defined(__FreeBSD__)
#include <sys/sysctl.h>
#include <sys/resource.h>
static int getCoreIdles(uint32_t ncpu, uint64_t *idle, uint64_t *total, int secondCall)
{
     size_t oldlen = 0;
     int ok = sysctlbyname("kern.cp_times", NULL, &oldlen, NULL, 0);
     if ( ok < 0 ) {
          perror("sysctlbyname");
          return 0;
     }
     long *times = (long*)malloc(oldlen);
     if ( NULL == times ) {
          perror("malloc");
          return 0;
     }
     ok = sysctlbyname("kern.cp_times", times, &oldlen, NULL, 0);
     if ( ok < 0 ) {
          perror("sysctlbyname");
          return 0;
     }

     uint32_t nCPUs = (oldlen / sizeof(long)) / CPUSTATES;

     if ( ncpu > nCPUs ) {
          fprintf(stderr, "Only found information on %" PRIu32 " cpus.  Requested %" PRIu32 "\n", nCPUs, ncpu);
          return 0;
     }

     uint32_t c, s;
     for ( c = 0 ; c < nCPUs ; c++ ) {
          uint64_t coreTot = 0;
          for ( s = 0 ; s < CPUSTATES ; s++ ) {
               coreTot += times[(c*CPUSTATES) + s];
          }
          uint64_t coreIdle = times[(c*CPUSTATES) + CP_IDLE];

          if ( secondCall ) {
              idle[c] = coreIdle - idle[c];
              total[c] = coreTot - total[c];
          } else {
              idle[c] = coreIdle;
              total[c] = coreTot;
          }
     }
     free(times);
     return 1;
}
#else
#error "Don't know how to get Idle cores on this platform."
#endif





/* Caller must free levels */
static int getLoadLevels(float **levels, uint32_t *ncores)
{
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if ( ncpu < 1 ) {
        fprintf(stderr, "Failed getting number of cpus\n");
        perror("sysconf");
        return 0;
    }

    float    *level = (float*)calloc(ncpu, sizeof(float));
    uint64_t *idle  = (uint64_t*)calloc(ncpu, sizeof(uint64_t));
    uint64_t *total = (uint64_t*)calloc(ncpu, sizeof(uint64_t));
    if ( !idle || !total ) {
        perror("calloc");
        return 0;
    }


    /* Get initial readings */
    if ( !getCoreIdles(ncpu, idle, total, 0) )
        return 0;

    sleep(1);

    if ( !getCoreIdles(ncpu, idle, total, 1) )
        return 0;

    long c = 0;
    for ( c = 0 ; c < ncpu ; c++ ) {
        level[c] = (100.0*idle[c])/total[c];
    }

    /* HACK:  We don't like core 0 */
    level[0] = 75.0;

    *levels = level;;
    *ncores = (uint32_t)ncpu;
    free(total);
    free(idle);
    return 1;
}





static uint32_t getIdleCores(HWInfo_t *info, hwloc_obj_t baseObj, float minIdle, hwloc_cpuset_t *idleSet)
{
    hwloc_const_cpuset_t baseSet = baseObj->cpuset;

    *idleSet = hwloc_bitmap_alloc();
    uint32_t nIdle = 0;

    hwloc_obj_type_t type = HWLOC_OBJ_PU;
    uint32_t count = (uint32_t)hwloc_get_nbobjs_inside_cpuset_by_type(info->topo, baseSet, type);


    for (uint32_t n = 0 ; n < count ; n++ ) {
        hwloc_obj_t core = hwloc_get_obj_inside_cpuset_by_type(info->topo, baseSet, type, n);

        float minIdleLogical = info->idle_levels[core->os_index];

        if ( minIdleLogical > minIdle ) {
            hwloc_bitmap_or(*idleSet, *idleSet, core->cpuset);
            nIdle++;
        }
    }

    return nIdle;
}


static void cpuSet_to_Cores(HWInfo_t *info, hwloc_const_cpuset_t set, uint32_t nCores, uint32_t *cores)
{
     int nSet = hwloc_get_nbobjs_inside_cpuset_by_type(info->topo, set, HWLOC_OBJ_PU);
     if ( (int)nCores > nSet ) {
          fprintf(stderr, "WARNING:  CPUSET size is %d.  We asked for %" PRIu32 " cores\n",
                    nSet, nCores);
          return;
     }

     for ( uint32_t n = 0 ; n < nCores ; n++ ) {
          cores[n] = hwloc_get_obj_inside_cpuset_by_type(info->topo, set, HWLOC_OBJ_PU, n)->os_index;
     }
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WS_SELECT_CPUS_H
