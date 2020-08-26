/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 * See COPYRIGHT in top-level directory.
 */

#ifndef ABTI_MEM_H_INCLUDED
#define ABTI_MEM_H_INCLUDED

/* Memory allocation */

/* Round desc_size up to the cacheline size.  The last four bytes will be
 * used to determine whether the descriptor is allocated externally (i.e.,
 * malloc()) or taken from a memory pool. */
#define ABTI_MEM_POOL_DESC_SIZE                                                \
    (((sizeof(ABTI_thread) + 4 + ABT_CONFIG_STATIC_CACHELINE_SIZE - 1) &       \
      (~(ABT_CONFIG_STATIC_CACHELINE_SIZE - 1))) -                             \
     4)

enum {
    ABTI_MEM_LP_MALLOC = 0,
    ABTI_MEM_LP_MMAP_RP,
    ABTI_MEM_LP_MMAP_HP_RP,
    ABTI_MEM_LP_MMAP_HP_THP,
    ABTI_MEM_LP_THP
};

void ABTI_mem_init(ABTI_global *p_global);
void ABTI_mem_init_local(ABTI_xstream *p_local_xstream);
void ABTI_mem_finalize(ABTI_global *p_global);
void ABTI_mem_finalize_local(ABTI_xstream *p_local_xstream);
int ABTI_mem_check_lp_alloc(int lp_alloc);

static inline void *ABTI_mem_alloc_desc(ABTI_local *p_local);
static inline void ABTI_mem_free_desc(ABTI_local *p_local, void *p_desc);

/* Inline functions */
#ifdef ABT_CONFIG_USE_MEM_POOL
static inline void ABTI_mem_alloc_ythread_mempool_desc_stack_impl(
    ABTI_mem_pool_local_pool *p_mem_pool_stack, size_t stacksize,
    ABTI_ythread **pp_ythread, void **pp_stack)
{
    /* stacksize must be a multiple of ABT_CONFIG_STATIC_CACHELINE_SIZE. */
    ABTI_ASSERT((stacksize & (ABT_CONFIG_STATIC_CACHELINE_SIZE - 1)) == 0);
    char *p_ythread = (char *)ABTI_mem_pool_alloc(p_mem_pool_stack);
    *pp_stack = (void *)(((char *)p_ythread) - stacksize);
    *pp_ythread = (ABTI_ythread *)p_ythread;
}
#endif

static inline void ABTI_mem_alloc_ythread_malloc_desc_stack_impl(
    size_t stacksize, ABTI_ythread **pp_ythread, void **pp_stack)
{
    /* stacksize must be a multiple of ABT_CONFIG_STATIC_CACHELINE_SIZE. */
    size_t alloc_stacksize =
        (stacksize + ABT_CONFIG_STATIC_CACHELINE_SIZE - 1) &
        (~(ABT_CONFIG_STATIC_CACHELINE_SIZE - 1));
    char *p_stack = (char *)ABTU_malloc(alloc_stacksize + sizeof(ABTI_ythread));
    *pp_stack = (void *)p_stack;
    *pp_ythread = (ABTI_ythread *)(p_stack + alloc_stacksize);
}

static inline ABTI_ythread *ABTI_mem_alloc_ythread_default(ABTI_local *p_local)
{
    size_t stacksize = ABTI_global_get_thread_stacksize();
    ABTI_ythread *p_ythread;
    void *p_stack;
    /* If an external thread allocates a stack, we use ABTU_malloc. */
    ABTI_xstream *p_local_xstream = ABTI_local_get_xstream_or_null(p_local);
    if (ABTI_IS_EXT_THREAD_ENABLED && p_local_xstream == NULL) {
        ABTI_mem_alloc_ythread_malloc_desc_stack_impl(stacksize, &p_ythread,
                                                      &p_stack);
        p_ythread->thread.type = ABTI_THREAD_TYPE_MEM_MALLOC_DESC_STACK;
    } else {
#ifdef ABT_CONFIG_USE_MEM_POOL
        ABTI_mem_alloc_ythread_mempool_desc_stack_impl(&p_local_xstream
                                                            ->mem_pool_stack,
                                                       stacksize, &p_ythread,
                                                       &p_stack);
        p_ythread->thread.type = ABTI_THREAD_TYPE_MEM_MEMPOOL_DESC_STACK;
#else
        ABTI_mem_alloc_ythread_malloc_desc_stack_impl(stacksize, &p_ythread,
                                                      &p_stack);
        p_ythread->thread.type = ABTI_THREAD_TYPE_MEM_MALLOC_DESC_STACK;
#endif
    }
    /* Initialize members of ABTI_thread_attr. */
    p_ythread->p_stack = p_stack;
    p_ythread->stacksize = stacksize;
    ABTI_VALGRIND_REGISTER_STACK(p_ythread->p_stack, p_ythread->stacksize);
    return p_ythread;
}

#ifdef ABT_CONFIG_USE_MEM_POOL
static inline ABTI_ythread *
ABTI_mem_alloc_ythread_mempool_desc_stack(ABTI_local *p_local,
                                          ABTI_thread_attr *p_attr)
{
    size_t stacksize = ABTI_global_get_thread_stacksize();
    ABTI_ythread *p_ythread;
    void *p_stack;
    /* If an external thread allocates a stack, we use ABTU_malloc. */
    ABTI_xstream *p_local_xstream = ABTI_local_get_xstream_or_null(p_local);
    if (ABTI_IS_EXT_THREAD_ENABLED && p_local_xstream == NULL) {
        ABTI_mem_alloc_ythread_malloc_desc_stack_impl(stacksize, &p_ythread,
                                                      &p_stack);
        p_ythread->thread.type = ABTI_THREAD_TYPE_MEM_MALLOC_DESC_STACK;
    } else {
        ABTI_mem_alloc_ythread_mempool_desc_stack_impl(&p_local_xstream
                                                            ->mem_pool_stack,
                                                       stacksize, &p_ythread,
                                                       &p_stack);
        p_ythread->thread.type = ABTI_THREAD_TYPE_MEM_MEMPOOL_DESC_STACK;
    }
    /* Copy members of p_attr. */
    p_ythread->p_stack = p_stack;
    p_ythread->stacksize = stacksize;
    ABTI_VALGRIND_REGISTER_STACK(p_ythread->p_stack, p_ythread->stacksize);
    return p_ythread;
}
#endif

static inline ABTI_ythread *
ABTI_mem_alloc_ythread_malloc_desc_stack(ABTI_thread_attr *p_attr)
{
    size_t stacksize = p_attr->stacksize;
    ABTI_ythread *p_ythread;
    void *p_stack;
    ABTI_mem_alloc_ythread_malloc_desc_stack_impl(stacksize, &p_ythread,
                                                  &p_stack);
    /* Copy members of p_attr. */
    p_ythread->thread.type = ABTI_THREAD_TYPE_MEM_MALLOC_DESC_STACK;
    p_ythread->stacksize = stacksize;
    p_ythread->p_stack = p_stack;
    ABTI_VALGRIND_REGISTER_STACK(p_ythread->p_stack, p_ythread->stacksize);
    return p_ythread;
}

static inline ABTI_ythread *
ABTI_mem_alloc_ythread_mempool_desc(ABTI_local *p_local,
                                    ABTI_thread_attr *p_attr)
{
    ABTI_ythread *p_ythread;
    if (sizeof(ABTI_ythread) <= ABTI_MEM_POOL_DESC_SIZE) {
        /* Use a descriptor pool for ABT_thread. */
        p_ythread = (ABTI_ythread *)ABTI_mem_alloc_desc(p_local);
        p_ythread->thread.type = ABTI_THREAD_TYPE_MEM_MEMPOOL_DESC;
    } else {
        /* Do not allocate stack, but Valgrind registration is preferred. */
        p_ythread = (ABTI_ythread *)ABTU_malloc(sizeof(ABTI_ythread));
        p_ythread->thread.type = ABTI_THREAD_TYPE_MEM_MALLOC_DESC;
    }
    /* Copy members of p_attr. */
    p_ythread->stacksize = p_attr->stacksize;
    p_ythread->p_stack = p_attr->p_stack;
    /* Note that the valgrind registration is ignored iff p_stack is NULL. */
    ABTI_VALGRIND_REGISTER_STACK(p_ythread->p_stack, p_ythread->stacksize);
    return p_ythread;
}

static inline void ABTI_mem_free_ythread(ABTI_local *p_local,
                                         ABTI_ythread *p_ythread)
{
    ABTI_VALGRIND_UNREGISTER_STACK(p_ythread->p_stack);
    /* Return stack. */
#ifdef ABT_CONFIG_USE_MEM_POOL
    if (p_ythread->thread.type & ABTI_THREAD_TYPE_MEM_MEMPOOL_DESC_STACK) {
        ABTI_xstream *p_local_xstream = ABTI_local_get_xstream_or_null(p_local);
        /* Came from a memory pool. */
#ifndef ABT_CONFIG_DISABLE_EXT_THREAD
        if (p_local_xstream == NULL) {
            /* Return a stack to the global pool. */
            ABTI_spinlock_acquire(&gp_ABTI_global->mem_pool_stack_lock);
            ABTI_mem_pool_free(&gp_ABTI_global->mem_pool_stack_ext, p_ythread);
            ABTI_spinlock_release(&gp_ABTI_global->mem_pool_stack_lock);
            return;
        }
#endif
        ABTI_mem_pool_free(&p_local_xstream->mem_pool_stack, p_ythread);
    } else
#endif
        if (p_ythread->thread.type & ABTI_THREAD_TYPE_MEM_MALLOC_DESC_STACK) {
        ABTU_free(p_ythread->p_stack);
    } else if (p_ythread->thread.type & ABTI_THREAD_TYPE_MEM_MEMPOOL_DESC) {
        ABTI_mem_free_desc(p_local, (void *)p_ythread);
    } else {
        ABTI_ASSERT(p_ythread->thread.type & ABTI_THREAD_TYPE_MEM_MALLOC_DESC);
        ABTU_free(p_ythread);
    }
}

static inline void *ABTI_mem_alloc_desc(ABTI_local *p_local)
{
#ifndef ABT_CONFIG_USE_MEM_POOL
    return ABTU_malloc(ABTI_MEM_POOL_DESC_SIZE);
#else
    void *p_desc;

    ABTI_xstream *p_local_xstream = ABTI_local_get_xstream_or_null(p_local);
    if (ABTI_IS_EXT_THREAD_ENABLED && p_local_xstream == NULL) {
        /* For external threads */
        p_desc = ABTU_malloc(ABTI_MEM_POOL_DESC_SIZE);
        *(uint32_t *)(((char *)p_desc) + ABTI_MEM_POOL_DESC_SIZE) = 1;
        return p_desc;
    }

    /* Find the page that has an empty block */
    p_desc = ABTI_mem_pool_alloc(&p_local_xstream->mem_pool_desc);
    /* To distinguish it from a malloc'ed case, assign non-NULL value. */
    *(uint32_t *)(((char *)p_desc) + ABTI_MEM_POOL_DESC_SIZE) = 0;
    return p_desc;
#endif
}

static inline void ABTI_mem_free_desc(ABTI_local *p_local, void *p_desc)
{
#ifndef ABT_CONFIG_USE_MEM_POOL
    ABTU_free(p_desc);
#else
    ABTI_xstream *p_local_xstream = ABTI_local_get_xstream_or_null(p_local);
#ifndef ABT_CONFIG_DISABLE_EXT_THREAD
    if (*(uint32_t *)(((char *)p_desc) + ABTI_MEM_POOL_DESC_SIZE)) {
        /* This was allocated by an external thread. */
        ABTU_free(p_desc);
        return;
    } else if (!p_local_xstream) {
        /* Return a stack and a descriptor to their global pools. */
        ABTI_spinlock_acquire(&gp_ABTI_global->mem_pool_desc_lock);
        ABTI_mem_pool_free(&gp_ABTI_global->mem_pool_desc_ext, p_desc);
        ABTI_spinlock_release(&gp_ABTI_global->mem_pool_desc_lock);
        return;
    }
#endif
    ABTI_mem_pool_free(&p_local_xstream->mem_pool_desc, p_desc);
#endif
}

static inline ABTI_thread *ABTI_mem_alloc_task(ABTI_local *p_local)
{
    return (ABTI_thread *)ABTI_mem_alloc_desc(p_local);
}

static inline void ABTI_mem_free_task(ABTI_local *p_local, ABTI_thread *p_task)
{
    ABTI_mem_free_desc(p_local, (void *)p_task);
}

#endif /* ABTI_MEM_H_INCLUDED */
