/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 * See COPYRIGHT in top-level directory.
 */

/*
 * Creates multiple execution streams and runs ULTs on these execution streams.
 * Users can change the number of execution streams and the number of ULT via
 * arguments. Each ULT prints its ID.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <abt.h>
#include <vector>
#include <algorithm>
#include <cassert>
#include <thread>
#include <chrono>
#include <iostream>
#include <sys/types.h>
#include <signal.h>
#include <unistd.h>
#include <immintrin.h>
#include <sys/mman.h>
#include <cstdint>

#define N_TH 2
#define DEFAULT_NUM_XSTREAMS 1
#define DEFAULT_NUM_THREADS (N_TH)

#define RUN_TIME (5)

static bool quit = false;


#define RUN_TIME (5)
#define VAL_SIZE (64)
#define IS_HUGE (MAP_HUGETLB)
#define MAX_TH (1024)

#define CAPA_TOTAL ((uint64_t)2ULL*1024*1024*1024)
//#define CAPA_TOTAL ((uint64_t)2ULL*1024*1024*1024)
#define N_ITEM ((uint64_t) CAPA_TOTAL/VAL_SIZE)

using ptr_t = void *;
using index_t = uint64_t;
using shuffle_map_t = std::vector<index_t>;

struct val_t {
  ptr_t data[VAL_SIZE/sizeof(ptr_t)];
};

typedef struct {
    int tid;
    val_t *a;
} thread_arg_t;


shuffle_map_t
init_shuffle_map(uint64_t n)
{
  shuffle_map_t shuffle_map(n);
  for (uint64_t i=0; i<n; i++) {
    shuffle_map[i] = i;
  }
  std:random_shuffle(shuffle_map.begin(), shuffle_map.end());
  return shuffle_map;
}

void
set_link(index_t prev_addr, index_t next_addr, val_t *val)
{
  val[prev_addr].data[0] = &(val[next_addr].data[0]);
}

void
gen_ring(shuffle_map_t shuffle_map, val_t *val)
{
  uint64_t n = shuffle_map.size();
  for (uint64_t i=0; i<n; i++) {
    index_t prev_addr = shuffle_map[i];
    index_t next_addr = (i<n-1) ? shuffle_map[i+1] : shuffle_map[0];
    set_link(prev_addr, next_addr, val);
  }
}


typedef struct {
  ptr_t last_p;
  uint64_t iter;
} res_t;
res_t res[MAX_TH];

void hello_world(void *arg)
{
    int iter = 0;
    int tid = ((thread_arg_t *)arg)->tid;
    int i_th = tid;
    printf("Hello world! (thread = %d)\n", tid);
    ptr_t p = ((thread_arg_t *)arg)->a[i_th * N_ITEM / N_TH].data[0];
    while (!quit) {
        asm volatile("prefetcht0 %0" : : "m" (*(ptr_t*)p));
        ABT_thread_yield();
        iter ++;
        p = *(ptr_t *)p;
    }
    res[i_th].last_p = p;
    res[i_th].iter = iter;
}

int main(int argc, char **argv)
{
    int i;
    /* Read arguments. */
    int num_xstreams = DEFAULT_NUM_XSTREAMS;
    int num_threads = DEFAULT_NUM_THREADS;
    while (1) {
        int opt = getopt(argc, argv, "he:n:");
        if (opt == -1)
            break;
        switch (opt) {
            case 'e':
                num_xstreams = atoi(optarg);
                break;
            case 'n':
                num_threads = atoi(optarg);
                break;
            case 'h':
            default:
                printf("Usage: ./hello_world [-e NUM_XSTREAMS] "
                       "[-n NUM_THREADS]\n");
                return -1;
        }
    }
    if (num_xstreams <= 0)
        num_xstreams = 1;
    if (num_threads <= 0)
        num_threads = 1;

    /* Allocate memory. */
    ABT_xstream *xstreams =
        (ABT_xstream *)malloc(sizeof(ABT_xstream) * num_xstreams);
    ABT_pool *pools = (ABT_pool *)malloc(sizeof(ABT_pool) * num_xstreams);
    ABT_thread *threads =
        (ABT_thread *)malloc(sizeof(ABT_thread) * num_threads);
    thread_arg_t *thread_args =
        (thread_arg_t *)malloc(sizeof(thread_arg_t) * num_threads);

    /* Initialize Argobots. */
    ABT_init(argc, argv);

    /* Get a primary execution stream. */
    //ABT_xstream_self(&xstreams[0]);

    /* Create secondary execution streams. */
    for (i = 0; i < num_xstreams; i++) {
        ABT_xstream_create(ABT_SCHED_NULL, &xstreams[i]);
    }

    /* Get default pools. */
    for (i = 0; i < num_xstreams; i++) {
        ABT_xstream_get_main_pools(xstreams[i], 1, &pools[i]);
    }

  val_t *a;
  a = (val_t *)mmap(0, N_ITEM*sizeof(val_t), PROT_READ|PROT_WRITE,
		    MAP_SHARED|IS_HUGE|MAP_ANONYMOUS, -1, 0);
  if (a == MAP_FAILED)
      assert(0);
  
  std::cout << "Shuffling..." << std::endl;
  shuffle_map_t shuffle_map = init_shuffle_map(N_ITEM);
  std::cout << "Done!" << std::endl;
  
  std::cout << "Generating..." << std::endl;
  gen_ring(shuffle_map, a);
  std::cout << "Done!" << std::endl;
  

    
    /* Create ULTs. */
    for (i = 0; i < num_threads; i++) {
        int pool_id = i % num_xstreams;
        thread_args[i].tid = i;
        thread_args[i].a = a;
        ABT_thread_create(pools[pool_id], hello_world, &thread_args[i],
                          ABT_THREAD_ATTR_NULL, &threads[i]);
    }

    for (auto i=0; i<RUN_TIME; i++) {
        sleep(1);
        printf("elapsed %d sec...\n", i+1);
    }
    quit = true;
    
    /* Join and free ULTs. */
    for (i = 0; i < num_threads; i++) {
        ABT_thread_free(&threads[i]);
    }
    uint64_t iter = 0;
    for (i = 0; i < num_threads; i++) {
        iter += res[i].iter;
    }
    double mps = (double)iter / RUN_TIME / 1000 / 1000;
    printf("%d, %f MPS, %f ns\n", iter, mps, 1000 / mps);

    /* Join and free secondary execution streams. */
    for (i = 0; i < num_xstreams; i++) {
        ABT_xstream_join(xstreams[i]);
        ABT_xstream_free(&xstreams[i]);
    }

    /* Finalize Argobots. */
    ABT_finalize();

    /* Free allocated memory. */
    free(xstreams);
    free(pools);
    free(threads);
    free(thread_args);

    return 0;
}
