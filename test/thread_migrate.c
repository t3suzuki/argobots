/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <abt.h>

#define DEFAULT_NUM_XSTREAMS    4
#define DEFAULT_NUM_THREADS     4

#define HANDLE_ERROR(ret,msg)                           \
    if (ret != ABT_SUCCESS) {                           \
        fprintf(stderr, "ERROR[%d]: %s\n", ret, msg);   \
        exit(EXIT_FAILURE);                             \
    }

void thread_func(void *arg)
{
    int i;
    ABT_thread thread;
    size_t my_id = (size_t)arg;
    printf("[TH%lu]: Hello, world!\n", my_id);

    ABT_thread_self(&thread);
    for (i = 0; i < 3; i++) {
        printf("[TH%lu]: request migration ...\n", my_id);
        ABT_thread_migrate(thread);
    }

    ABT_thread_release(thread);
}

int main(int argc, char *argv[])
{
    int i, j;
    int ret;
    int num_xstreams = DEFAULT_NUM_XSTREAMS;
    int num_threads = DEFAULT_NUM_THREADS;
    if (argc > 1) num_xstreams = atoi(argv[1]);
    assert(num_xstreams >= 0);
    if (argc > 2) num_threads = atoi(argv[2]);
    assert(num_threads >= 0);

    ABT_xstream *xstreams;
    ABT_thread **threads;
    xstreams = (ABT_xstream *)malloc(sizeof(ABT_xstream) * num_xstreams);
    threads = (ABT_thread **)malloc(sizeof(ABT_thread *) * num_xstreams);
    for (i = 0; i < num_xstreams; i++) {
        threads[i] = (ABT_thread *)malloc(sizeof(ABT_thread) * num_threads);
    }

    /* Initialize */
    ret = ABT_init(argc, argv);
    HANDLE_ERROR(ret, "ABT_init");

    /* Create Execution Streams */
    ret = ABT_xstream_self(&xstreams[0]);
    HANDLE_ERROR(ret, "ABT_xstream_self");
    for (i = 1; i < num_xstreams; i++) {
        ret = ABT_xstream_create(ABT_SCHED_NULL, &xstreams[i]);
        HANDLE_ERROR(ret, "ABT_xstream_create");
    }

    /* Create threads */
    for (i = 0; i < num_xstreams; i++) {
        for (j = 0; j < num_threads; j++) {
            size_t tid = i * num_threads + j + 1;
            ret = ABT_thread_create(xstreams[i],
                    thread_func, (void *)tid, ABT_THREAD_ATTR_NULL,
                    &threads[i][j]);
            HANDLE_ERROR(ret, "ABT_thread_create");
        }
    }

    /* Switch to other user level threads */
    ABT_thread_yield();

    /* Join Execution Streams */
    for (i = 1; i < num_xstreams; i++) {
        ret = ABT_xstream_join(xstreams[i]);
        HANDLE_ERROR(ret, "ABT_xstream_join");
    }

    /* Free Execution Streams */
    for (i = 1; i < num_xstreams; i++) {
        ret = ABT_xstream_free(&xstreams[i]);
        HANDLE_ERROR(ret, "ABT_xstream_free");
    }

    /* Finalize */
    ret = ABT_finalize();
    HANDLE_ERROR(ret, "ABT_finalize");

    for (i = 0; i < num_xstreams; i++) {
        free(threads[i]);
    }
    free(threads);
    free(xstreams);

    return EXIT_SUCCESS;
}
