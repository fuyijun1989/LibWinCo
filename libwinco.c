/*
 * Copyright 2019 Yijun Fu <fuyijun1989@gmail.com>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <stdlib.h>
#include <ctype.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <assert.h>
#include <stdio.h>
#include <math.h>


#include "libwinco.h"

/* Declare. */
typedef struct WINCO_Q_T WINCO_Q;
typedef struct WINCO_THRD_T WINCO_THRD;
typedef struct METRIC_T METRIC;

static DWORD WINAPI winco_thrd_loop(LPVOID arg);
VOID __stdcall winco_rt_main(LPVOID arg);

/* Log. */
#ifndef _DEBUG
#define WINCO_LOG(fmt, ...) (void*)(0);
#else
#define WINCO_LOG(fmt, ...) \
    do { fprintf(stderr, fmt"\n", __VA_ARGS__); } while (0)
#endif

#define WINCO_LOG2(fmt, ...) \
    do { fprintf(stderr, fmt"\n", __VA_ARGS__); } while (0)

/* Atomic. */
#define ILEXC64(v, v1) InterlockedExchange64(v, v1)
#define ILGET64(v) InterlockedExchangeAdd64(v, 0)
#define ILSET64(v, v1) do { ILEXC64(v, v1); } while(0)
#define ILINC64(v) do { InterlockedIncrement64(v); } while(0)
#define ILDEC64(v) do { InterlockedDecrement64(v); } while(0)
#define ILADD64(v, v1) do { InterlockedAdd64(v, v1); } while(0)

/* Queue. */
struct WINCO_Q_T {
    WINCO_Q* prev;
    WINCO_Q* next;
    int64_t  len;
};

void winco_q_init(WINCO_Q* q) {
    q->prev = q->next = q;
    q->len = 0;
}

void winco_q_init_node(WINCO_Q* n) {
    n->prev = n->next = NULL;
    n->len = -1;
}

void winco_q_assign(WINCO_Q* from, WINCO_Q* to) {
    *to = *from;
    from->prev->next = to;
    from->next->prev = to;
    winco_q_init(from);
}

void winco_q_pushback(WINCO_Q* q, WINCO_Q* n) {
    n->next = q;
    n->prev = q->prev;
    n->prev->next = n;
    q->prev = n;
    q->len++;
}

void winco_q_pushfront(WINCO_Q* q, WINCO_Q* n) {
    n->prev = q;
    n->next = q->next;
    q->next = n;
    n->next->prev = n;
    q->len++;
}

#define winco_q_owner(n, t, m) ((t*)(((char *)(n)) - offsetof(t, m)))

WINCO_Q* winco_q_front(WINCO_Q* q) {
    return q->len > 0 ? q->next : NULL;
}

WINCO_Q* winco_q_back(WINCO_Q* q) {
    return q->len > 0 ? q->prev : NULL;
}

WINCO_Q* winco_q_detach(WINCO_Q* q, WINCO_Q* n) {
    q->len--;
    n->prev->next = n->next;
    n->next->prev = n->prev;
    n->prev = n->next = NULL;
    return n;
}

WINCO_Q* winco_q_popfront(WINCO_Q* q) {
    return q->len == 0 ? NULL : winco_q_detach(q, winco_q_front(q));
}

WINCO_Q* winco_q_popback(WINCO_Q* q) {
    return q->len == 0 ? NULL : winco_q_detach(q, winco_q_back(q));
}

WINCO_Q* winco_q_iternext(WINCO_Q* q, WINCO_Q* c) {
    if (!c) c = q;
    if (c->next == q) return NULL;
    return c->next;
}

int winco_q_inqueue(WINCO_Q* n) {
    return n->prev != NULL && n->next != NULL;
}


/* Time. */
uint64_t winco_tick_ms() {
    return GetTickCount64();
}

volatile double g_perf_freq = 0;  // 'volatile' enough for weak consistency.
void winco_tick_us_init() {
    LARGE_INTEGER ifreq;
    QueryPerformanceFrequency(&ifreq);
    g_perf_freq = (double)ifreq.QuadPart / 1000000.0;
}

uint64_t winco_tick_us() {
    LARGE_INTEGER now;
    QueryPerformanceCounter(&now);
    return (uint64_t)((double)now.QuadPart / g_perf_freq);
}

/* WINCO. */
struct WINCO_T {
    int thrd_n;
    WINCO_THRD* thrds;

    int next_thrd;

    uint64_t rt_wait_tmout;
    uint64_t thrd_idle_tmout;
    uint64_t wsapoll_intv;

    METRIC* metric;
    uint64_t last_stat;
};


/* Thread & Coroutine. */
struct WINCO_THRD_T {
    WINCO* w;
    int id;  // Thrd id / run flag.
    int next_rt_id;

    HANDLE thrd;
    LPVOID sched_rt;

    WINCO_ROUTINE* active_rt;
    WINCO_Q ready_q;

    WINCO_Q wait_q;

    WINCO_Q awake_q;
    CRITICAL_SECTION awake_q_l;
    int awake_signal;

    WINCO_Q wsapoll_q;
    LPWSAPOLLFD wsapoll_fds;
    ULONG wsapoll_fdn;

    WINCO_Q create_q;
    CRITICAL_SECTION create_q_l;

    WINCO_Q exit_q;
};

__declspec(thread) WINCO_THRD* g_thrd = NULL;

struct WINCO_ROUTINE_T {
    WINCO_THRD* thrd;

    int id;
    LPVOID fiber;

    cort_fn fn;
    void* fn_arg;
    void* fn_ret;
    CRITICAL_SECTION fn_lk;
    CONDITION_VARIABLE fn_condvar;

    WINCO_Q state_node;

    WINCO_Q awake_node;
    uint64_t tmout;
    WINCO_COND_VAR* condvar;
    int condvar_err;

    LPWSAPOLLFD wsapoll_fds;
    ULONG wsapoll_fdn;
    INT wsapoll_tmout;
    int wsapoll_err;
    uint64_t wsapoll_def_tmout;
};

/* Lock. */
#define WINCO_MAX_LOCK_CNT INT32_MAX
#define WINCO_EXL_LOCK_CNT 0
#define WINCO_EXL_LOCK 0
#define WINCO_SHR_LOCK 0
struct WINCO_LOCK_T {
    int32_t n;
    CRITICAL_SECTION lk;
    CONDITION_VARIABLE cond_var;
    int32_t wait_th_n;
    WINCO_Q awake_q;
};

/* Cond var. */
struct WINCO_COND_VAR_T {
    CONDITION_VARIABLE cond_var;
    CRITICAL_SECTION lk;
    int32_t wait_th_n;
    WINCO_Q awake_q;
};

/* Metrics. */
struct METRIC_T {
    int64_t thrd_n;
    int64_t cort_n;
    int64_t ctx_s_n;
    int64_t ctx_s_yl_n;
    int64_t ctx_s_sl_n;
    int64_t ctx_s_lk_n;
    int64_t ctx_s_cw_n;
    int64_t ctx_s_pl_n;
    int64_t exec_t;
    int64_t idle_t;
    int64_t wake_t;
    int64_t wscn_t;
    int64_t poll_t;
    int64_t lock_q;
    int64_t cndw_q;
    int64_t wake_q;
    int64_t wake_q_spn;
    int64_t wait_q;
    int64_t wait_q_spn;
    int64_t wsapoll_q;
    int64_t wsapoll_q_spn;
    int64_t yield_n;
    int64_t sleep_n;
    int64_t lock_n;
    int64_t lock_block_n;
    int64_t unlock_n;
    int64_t cond_w_n;
    int64_t cond_s_n;
    int64_t cond_wk_n;
    int64_t cond_to_n;
    int64_t wsapoll_n;
    int64_t wsapoll_event_n;
    int64_t wsapoll_to_n;
};


WINCO* winco_init(int thrd_n) {
    WINCO* w = (WINCO*)calloc(1, sizeof(*w));
    assert(w);

    winco_cfg(w, WINCO_CFG_THRD_IDLE, 33);
    winco_cfg(w, WINCO_CFG_COROUTINE_IDLE_TMOUT, 33);
    winco_cfg(w, WINCO_CFG_WSAPOLL_INTERV, 33);

    w->metric  = (METRIC*)calloc(1, sizeof(METRIC));
    assert(w->metric);
    w->last_stat = winco_tick_ms();

    if (thrd_n == 0) {
        SYSTEM_INFO sys_inf;
        GetSystemInfo(&sys_inf);
        thrd_n = sys_inf.dwNumberOfProcessors;
    }
    assert(thrd_n > 0); 
    
    winco_tick_us_init();

    w->thrd_n = thrd_n;
    w->thrds = (WINCO_THRD*)calloc(thrd_n, sizeof(WINCO_THRD));

    ILEXC64(&w->metric->thrd_n, thrd_n);

    for (int it = 0; it < w->thrd_n; it++) {
        WINCO_THRD* th = w->thrds + it;
        assert(th);
        th->w = w;
        th->id = 0;
        th->next_rt_id = 0;
        th->thrd = NULL;
        th->sched_rt = NULL;
        th->active_rt = NULL;
        winco_q_init(&th->ready_q);
        winco_q_init(&th->wait_q);
        winco_q_init(&th->awake_q);
        InitializeCriticalSection(&th->awake_q_l);
        th->awake_signal = 0;
        winco_q_init(&th->wsapoll_q);
        th->wsapoll_fdn = 128;
        th->wsapoll_fds = (LPWSAPOLLFD)calloc(
            th->wsapoll_fdn, sizeof(WSAPOLLFD));
        assert(th->wsapoll_fds);
        winco_q_init(&th->create_q);
        InitializeCriticalSection(&th->create_q_l);
        winco_q_init(&th->exit_q);

        th->thrd = CreateThread(NULL, 0, winco_thrd_loop,
            (LPVOID)th, 0, NULL);
    }

    return w;
}

void winco_destroy(WINCO* w) {
    if (w->metric) free(w->metric);
    if (w->thrds) {
        for (int it = 0; it < w->thrd_n; it++) {
            WINCO_THRD* th = w->thrds + it;
            InterlockedExchange(&th->id, 0);
            WaitForSingleObject(th->thrd, INFINITE);
            CloseHandle(th->thrd);
            assert(th->active_rt == NULL);
            assert(th->ready_q.len == 0);
            assert(th->wait_q.len == 0);
            assert(th->awake_q.len == 0);
            DeleteCriticalSection(&th->awake_q_l);
            assert(th->wsapoll_q.len == 0);
            if (th->wsapoll_fds) free(th->wsapoll_fds);
            assert(th->create_q.len == 0);
            DeleteCriticalSection(&th->create_q_l);
            assert(th->exit_q.len == 0);
        }
        free(w->thrds);
    }

    free(w);
}

static DWORD WINAPI winco_thrd_loop(LPVOID arg) {
    assert(arg);
    WINCO_THRD* th = (WINCO_THRD*)arg;
    th->id = GetCurrentThreadId();
    g_thrd = th;

    th->sched_rt = ConvertThreadToFiber(th);
    assert(th->sched_rt);

    uint64_t last_create_exit = 0;
    uint64_t last_tmout_scan = 0;
    uint64_t last_poll = 0;

    while (InterlockedExchangeAdd(&th->id, 0)) {
        /* Schedule. */
        int handle_create_exit_q_type = 0;  // 0: ignore, 1: handle.
        int handle_wait_q_type = 0;  // 0: ignore, 1: scan.
        int handle_awake_q_type = 0;  // 0: ignore, 1: wake.
        int handle_poll_q_type = 0;  // 0: ignore, 1: poll.
        uint64_t now = winco_tick_ms();
        if (now - last_create_exit > 333) {
            handle_create_exit_q_type = 1;
        }
        if (InterlockedExchangeAdd(&th->awake_signal, 0) > 0) {
            handle_awake_q_type = 1;
        }
        if (now - last_tmout_scan > th->w->rt_wait_tmout) {
            handle_wait_q_type = 1;
        }
        if (now - last_poll > th->w->wsapoll_intv) {
            handle_poll_q_type = 1;
        }

        /* Idle. */
        if (th->ready_q.len == 0 && 
            handle_create_exit_q_type == 0 && 
            handle_wait_q_type == 0 && 
            handle_poll_q_type == 0) {
            uint64_t t0 = winco_tick_us();
            Sleep((DWORD)th->w->thrd_idle_tmout);
            uint64_t t1 = winco_tick_us();
            InterlockedAdd64(&th->w->metric->idle_t, t1 - t0);
        }

        /* Handle create / exit rt. */
        if (handle_create_exit_q_type> 0) {
            EnterCriticalSection(&th->create_q_l);
            while (th->create_q.len > 0) {
                WINCO_ROUTINE* rt = winco_q_owner(
                    winco_q_popfront(&th->create_q),
                    WINCO_ROUTINE, state_node);
                winco_q_pushback(&th->ready_q, &rt->state_node);

                ILINC64(&th->w->metric->cort_n);
            }
            LeaveCriticalSection(&th->create_q_l);

            while (th->exit_q.len > 0) {
                WINCO_ROUTINE* rt = winco_q_owner(
                    winco_q_popfront(&th->exit_q),
                    WINCO_ROUTINE, state_node);
                EnterCriticalSection(&rt->fn_lk);
                rt->fn = NULL;
                WakeAllConditionVariable(&rt->fn_condvar);
                LeaveCriticalSection(&rt->fn_lk);
            }

            last_create_exit = winco_tick_ms();
        }

        /* Handle awake queue. */
        if (handle_awake_q_type > 0) {
            uint64_t t0 = winco_tick_us();
            WINCO_Q awake_q;
            winco_q_init(&awake_q);

            // Try lock. Avoid contention.
            if (TryEnterCriticalSection(&th->awake_q_l)) {
                InterlockedExchange(&th->awake_signal, 0);
                winco_q_assign(&th->awake_q, &awake_q);
                LeaveCriticalSection(&th->awake_q_l);
            }

            ILADD64(&th->w->metric->wake_q, awake_q.len);
            ILINC64(&th->w->metric->wake_q_spn);
            while (awake_q.len > 0)
            {
                WINCO_ROUTINE* rt = winco_q_owner(
                    winco_q_popfront(&awake_q), WINCO_ROUTINE, awake_node);
                // If still in wait q.
                if (winco_q_inqueue(&rt->state_node)) {
                    winco_q_detach(&th->wait_q, &rt->state_node);
                }
                winco_q_pushback(&th->ready_q, &rt->state_node);
            }
            uint64_t t1 = winco_tick_us();
            InterlockedAdd64(&th->w->metric->wake_t, t1 - t0);
        }

        /* Handle wait queue. */
        if (handle_wait_q_type > 0) {
            uint64_t t0 = winco_tick_us();
            WINCO_Q waken_q;
            winco_q_init(&waken_q);

            ILADD64(&th->w->metric->wait_q, th->wait_q.len);
            ILINC64(&th->w->metric->wait_q_spn);

            WINCO_Q* prev = NULL, * curr = NULL;
            while (curr = winco_q_iternext(&th->wait_q, prev))
            {
                WINCO_ROUTINE* rt = winco_q_owner(
                    curr, WINCO_ROUTINE, state_node);
                // Wakeup & tmout may both happen, CAS tmout->0 to elect owner.
                uint64_t tmout = ILGET64(&rt->tmout);
                if (tmout < now && tmout != 0 && InterlockedCompareExchange64(
                    &rt->tmout, 0, tmout) == tmout) {
                    winco_q_detach(&th->wait_q, &rt->state_node);
                    winco_q_pushback(&waken_q, &rt->state_node);
                }
                else {
                    prev = curr;
                }
            }

            while (waken_q.len > 0) {
                WINCO_ROUTINE* rt = winco_q_owner(
                    winco_q_popfront(&waken_q), WINCO_ROUTINE, state_node);
                if (rt->condvar) {
                    WINCO_COND_VAR* condvar = rt->condvar;
                    rt->condvar = NULL;
                    rt->condvar_err = 1;
                    EnterCriticalSection(&condvar->lk);
                    // May be removed by waker.
                    if (winco_q_inqueue(&rt->awake_node)) {
                        winco_q_detach(&condvar->awake_q, &rt->awake_node);
                    }
                    LeaveCriticalSection(&condvar->lk);
                }
                winco_q_pushback(&th->ready_q, &rt->state_node);
            }

            last_tmout_scan = winco_tick_ms();
            uint64_t t1 = winco_tick_us();
            InterlockedAdd64(&th->w->metric->wscn_t, t1 - t0);
        }

        /* Handle poll. */
        if (handle_poll_q_type > 0) {
            ILADD64(&th->w->metric->wsapoll_q, th->wsapoll_q.len);
            ILINC64(&th->w->metric->wsapoll_q_spn);

            uint64_t t0 = winco_tick_us();
            if (th->wsapoll_q.len > 0) {
                LPWSAPOLLFD fds = th->wsapoll_fds;
                INT fdn = 0;
                INT tmout = 100;  // TODO...

                WINCO_Q* prev = NULL, * curr = NULL;
                while (curr = winco_q_iternext(&th->wsapoll_q, prev)) {
                    WINCO_ROUTINE* rt = winco_q_owner(
                        curr, WINCO_ROUTINE, state_node);

                    if (fdn + rt->wsapoll_fdn > th->wsapoll_fdn) {
                        fds = (LPWSAPOLLFD)calloc(
                            th->wsapoll_fdn * 2, sizeof(WSAPOLLFD));
                        assert(fds);
                        memcpy(fds, th->wsapoll_fds,
                            th->wsapoll_fdn * sizeof(WSAPOLLFD));
                        free(th->wsapoll_fds);
                        th->wsapoll_fdn *= 2;
                        th->wsapoll_fds = fds;
                    }

                    for (ULONG i = 0; i < rt->wsapoll_fdn; i++) {
                        fds[fdn] = rt->wsapoll_fds[i];
                        fdn++;
                        tmout = min(rt->wsapoll_tmout, tmout);
                    }

                    prev = curr;
                }

                int err = WSAPoll(fds, fdn, tmout);

                int fdpos = 0;
                prev = curr = NULL;
                while (curr = winco_q_iternext(&th->wsapoll_q, prev)) {
                    WINCO_ROUTINE* rt = winco_q_owner(
                        curr, WINCO_ROUTINE, state_node);

                    rt->wsapoll_err = 0;
                    for (ULONG i = 0; i < rt->wsapoll_fdn; i++) {
                        rt->wsapoll_fds[i] = fds[fdpos++];
                        LPWSAPOLLFD fd = &rt->wsapoll_fds[i];
                        if (fd->revents & fd->events) {
                            rt->wsapoll_err++;
                        }
                    }
                    if (rt->wsapoll_err > 0 ||
                        rt->wsapoll_def_tmout < winco_tick_ms()) {
                        winco_q_detach(&th->wsapoll_q, &rt->state_node);
                        winco_q_pushback(&th->ready_q, &rt->state_node);
                    }
                    else {
                        prev = curr;
                    }
                }
            }

            last_poll = winco_tick_ms();
            uint64_t t1 = winco_tick_us();
            InterlockedAdd64(&th->w->metric->poll_t, t1 - t0);
        }

        /* Exec. */
        if (th->ready_q.len > 0)
        {
            uint64_t t0 = winco_tick_us();
            WINCO_ROUTINE* active = winco_q_owner(
                winco_q_popfront(&th->ready_q), WINCO_ROUTINE, state_node);

            th->active_rt = active;
            SwitchToFiber(active->fiber);
            th->active_rt = NULL;
            ILINC64(&th->w->metric->ctx_s_n);

            uint64_t t1 = winco_tick_us();
            InterlockedAdd64(&th->w->metric->exec_t, t1 - t0);
        }
    }

    return 0;
}

VOID __stdcall winco_rt_main(LPVOID arg) {
    WINCO_THRD* th = g_thrd;
    WINCO_ROUTINE* rt = (WINCO_ROUTINE*)arg;

    rt->fn_ret = rt->fn(rt->fn_arg);

    winco_q_pushback(&th->exit_q, &rt->state_node);
    SwitchToFiber(th->sched_rt);
}

WINCO_ROUTINE* winco_create(WINCO* w, cort_fn fn, void* arg) {
    assert(fn);

    int next = InterlockedAdd(&w->next_thrd, 1) % w->thrd_n;
    WINCO_THRD* th = w->thrds + next;

    WINCO_ROUTINE* rt = calloc(1, sizeof(*rt));
    assert(rt);
    rt->thrd = th;
    rt->id = InterlockedExchangeAdd(&th->next_rt_id, 1);
    rt->fiber = CreateFiber(0, winco_rt_main, rt);
    rt->fn = fn;
    rt->fn_arg = arg;
    rt->fn_ret = NULL;
    InitializeCriticalSection(&rt->fn_lk);
    InitializeConditionVariable(&rt->fn_condvar);
    winco_q_init_node(&rt->state_node);
    winco_q_init_node(&rt->awake_node);
    rt->tmout = 0;
    rt->condvar = NULL;
    rt->condvar_err = 0;
    rt->wsapoll_fds = NULL;
    rt->wsapoll_fdn = 0;
    rt->wsapoll_tmout = 0;
    rt->wsapoll_err = NOERROR;
    EnterCriticalSection(&th->create_q_l);
    winco_q_pushback(&th->create_q, &rt->state_node);
    LeaveCriticalSection(&th->create_q_l);

    return rt;
}

void* winco_join(WINCO_ROUTINE* rt) {
    EnterCriticalSection(&rt->fn_lk);
    while (rt->fn) {
        SleepConditionVariableCS(&rt->fn_condvar, &rt->fn_lk, INFINITE);
    }
    LeaveCriticalSection(&rt->fn_lk);

    InterlockedDecrement64(&rt->thrd->w->metric->cort_n);

    return rt->fn_ret;
}

void winco_delete(WINCO_ROUTINE* rt) {
    DeleteFiber(rt->fiber);
    DeleteCriticalSection(&rt->fn_lk);
    assert(!winco_q_inqueue(&rt->state_node));
    assert(!winco_q_inqueue(&rt->awake_node));
    free(rt);
}

/* Yield & Sleep. */
void winco_yield() {
    if (!g_thrd) {
        SwitchToThread();
        return;
    }

    // To ready q, will re-sched.
    WINCO_THRD* th = g_thrd;
    WINCO_ROUTINE* rt = g_thrd->active_rt;
    winco_q_pushback(&th->ready_q, &rt->state_node);

    ILINC64(&th->w->metric->yield_n);
    ILINC64(&th->w->metric->ctx_s_yl_n);

    SwitchToFiber(th->sched_rt);
}

void winco_sleep(uint64_t ms) {
    if (!g_thrd) {
        Sleep((DWORD)ms);
        return;
    }

    // To wait q with tmout, will wake up and re-sched.
    if (ms <= 0) return;
    WINCO_THRD* th = g_thrd;
    WINCO_ROUTINE* rt = g_thrd->active_rt;
    rt->tmout = winco_tick_ms() + ms;
    winco_q_pushback(&th->wait_q, &rt->state_node);

    ILINC64(&th->w->metric->sleep_n);
    ILINC64(&th->w->metric->ctx_s_sl_n);

    SwitchToFiber(th->sched_rt);
}

WINCO_LOCK* winco_lock_init() {
    WINCO_LOCK* lk = (WINCO_LOCK*)calloc(1, sizeof(*lk));
    assert(lk);
    lk->n = WINCO_MAX_LOCK_CNT;
    InitializeCriticalSection(&lk->lk);
    InitializeConditionVariable(&lk->cond_var);
    lk->wait_th_n = 0;
    winco_q_init(&lk->awake_q);
    return lk;
}

void winco_lock_destroy(WINCO_LOCK* lk) {
    assert(lk->n == WINCO_MAX_LOCK_CNT);
    assert(lk->awake_q.len == 0);
    assert(lk->wait_th_n == 0);
    DeleteCriticalSection(&lk->lk);
    free(lk);
}

void winco_lock0(WINCO_LOCK *lk, int mode) {
    int redo = 1;
    int rt_id = g_thrd ? g_thrd->active_rt->id : -1;

    EnterCriticalSection(&lk->lk);
    while (redo) {
        // Exclusive.
        if (mode == WINCO_EXL_LOCK && lk->n == WINCO_MAX_LOCK_CNT) {
            lk->n = WINCO_EXL_LOCK_CNT;
            redo = 0;
        }
        // Shared.
        else if (mode == WINCO_SHR_LOCK && lk->n > WINCO_EXL_LOCK_CNT) {
            lk->n--;
            redo = 0;
        }
        else {
            // Thread. Go idle with cond wait.
            if (rt_id == -1) {
                lk->wait_th_n++;
                SleepConditionVariableCS(&lk->cond_var, 
                    &lk->lk, INFINITE);
                lk->wait_th_n--;
            }
            // Coroutine. Go idle & wait for wakeup.
            else {
                ILINC64(&g_thrd->w->metric->lock_block_n);
                ILINC64(&g_thrd->w->metric->ctx_s_lk_n);
                ILINC64(&g_thrd->w->metric->lock_q);

                WINCO_THRD* th = g_thrd;
                WINCO_ROUTINE* rt = th->active_rt;
                assert(rt);
                winco_q_pushback(&lk->awake_q, &rt->awake_node);

                LeaveCriticalSection(&lk->lk);
                SwitchToFiber(g_thrd->sched_rt);
                EnterCriticalSection(&lk->lk);

                ILDEC64(&g_thrd->w->metric->lock_q);
            }
        }
    }
    LeaveCriticalSection(&lk->lk);

    if (g_thrd) {
        ILINC64(&g_thrd->w->metric->lock_n);
    }
}

void winco_lock(WINCO_LOCK* lk) {
    winco_lock0(lk, WINCO_EXL_LOCK);
}

void winco_shared_lock(WINCO_LOCK* lk) {
    winco_lock0(lk, WINCO_SHR_LOCK);
}

void winco_unlock(WINCO_LOCK* lk) {
    int th_id = GetCurrentThreadId();
    int rt_id = g_thrd ? g_thrd->active_rt->id : -1;

    EnterCriticalSection(&lk->lk);
    assert(lk->n < WINCO_MAX_LOCK_CNT);
    // Exclusive.
    if (lk->n == WINCO_EXL_LOCK_CNT) {
        lk->n = WINCO_MAX_LOCK_CNT;
    }
    // Shared.
    else {
        lk->n++;
    }
    
    // Coroutine. To awake q & signal.
    if (lk->awake_q.len > 0) {
        WINCO_ROUTINE* rt = winco_q_owner(
            winco_q_popfront(&lk->awake_q), 
            WINCO_ROUTINE, awake_node);
        WINCO_THRD* th = rt->thrd;

        EnterCriticalSection(&th->awake_q_l);
        winco_q_pushback(&th->awake_q, &rt->awake_node);
        LeaveCriticalSection(&th->awake_q_l);
        InterlockedAdd(&th->awake_signal, 1);
    }
    // Thread. Signal.
    else if (lk->wait_th_n > 0) {
        WakeConditionVariable(&lk->cond_var);
    }
    LeaveCriticalSection(&lk->lk);

    if (g_thrd) {
        ILINC64(&g_thrd->w->metric->unlock_n);
    }
}

WINCO_COND_VAR* winco_cond_init() {
    WINCO_COND_VAR* cond = (WINCO_COND_VAR*)calloc(1, sizeof(*cond));
    assert(cond);
    InitializeCriticalSection(&cond->lk);
    InitializeConditionVariable(&cond->cond_var);
    cond->wait_th_n = 0;
    winco_q_init(&cond->awake_q);
    return cond;
}

void winco_cond_destroy(WINCO_COND_VAR* cond) {
    assert(cond->wait_th_n == 0);
    assert(cond->awake_q.len == 0);
    EnterCriticalSection(&cond->lk);
    LeaveCriticalSection(&cond->lk);
    DeleteCriticalSection(&cond->lk);
    free(cond);
}

int winco_cond_wait(WINCO_COND_VAR* cond, WINCO_LOCK* lk, uint64_t wait_ms) {
    int r = 0;
    WINCO_THRD* th = g_thrd;

    if (th) {
        ILINC64(&th->w->metric->cond_w_n);
    }

    // Coroutine. Wait in wait q with tmout + wait in awake q.
    if (th) {
        WINCO_ROUTINE* rt = th->active_rt;
        ILEXC64(&rt->tmout, winco_tick_ms() + wait_ms);
        rt->condvar = cond;
        rt->condvar_err = 0;
        winco_q_pushback(&th->wait_q, &rt->state_node);

        EnterCriticalSection(&cond->lk);
        winco_q_pushback(&cond->awake_q, &rt->awake_node);
        LeaveCriticalSection(&cond->lk);

        ILINC64(&th->w->metric->ctx_s_cw_n);
        ILINC64(&g_thrd->w->metric->cndw_q);

        winco_unlock(lk);
        SwitchToFiber(th->sched_rt);
        winco_lock(lk);

        ILDEC64(&g_thrd->w->metric->cndw_q);
        r = rt->condvar_err;
    }
    // Thread. Wait on cond var.
    else {
        EnterCriticalSection(&cond->lk);
        winco_unlock(lk);
        cond->wait_th_n++;
        r = (SleepConditionVariableCS(
            &cond->cond_var, &cond->lk, (DWORD)wait_ms) == 0) ? 1 : 0;
        cond->wait_th_n--;
        LeaveCriticalSection(&cond->lk);
        // Waker locks lk then cond->lk, so unlock cond->lk first.
        winco_lock(lk);
    }

    if (th) {
        if (r == 0) {
            ILINC64(&th->w->metric->cond_wk_n);
        }
        else {
            ILINC64(&th->w->metric->cond_to_n);
        }
    }

    return r;
}

void winco_cond_signal(WINCO_COND_VAR* cond) {
    WINCO_ROUTINE* rt = NULL;
    EnterCriticalSection(&cond->lk);
    // Wake up 1 coroutine.
    if (cond->awake_q.len > 0) {
        rt = winco_q_owner(winco_q_popfront(&cond->awake_q),
            WINCO_ROUTINE, awake_node);
    }
    // Wake up 1 thread.
    else if (cond->wait_th_n > 0) {
        WakeConditionVariable(&cond->cond_var);
    }
    LeaveCriticalSection(&cond->lk);

    if (rt) {
        // To awake q & signal.
        WINCO_THRD* th = rt->thrd;
        // Wakeup & tmout may both happen, CAS tmout->0 to elect owner.
        uint64_t tmout = ILGET64(&rt->tmout);
        if (InterlockedCompareExchange64(&rt->tmout, 0, tmout) == tmout) {
            rt->condvar = NULL;
            EnterCriticalSection(&th->awake_q_l);
            winco_q_pushback(&th->awake_q, &rt->awake_node);
            LeaveCriticalSection(&th->awake_q_l);
            InterlockedAdd(&th->awake_signal, 1);
        }
    }

    if (g_thrd) {
        ILINC64(&g_thrd->w->metric->cond_s_n);
    }
}

void winco_cond_signal_all(WINCO_COND_VAR* cond) {
    WINCO_Q awake_q;
    winco_q_init(&awake_q);
    EnterCriticalSection(&cond->lk);
    // Wake up all coroutines.
    if (cond->awake_q.len > 0) {
        winco_q_assign(&cond->awake_q, &awake_q);
    }
    // Wake up all threads.
    if (cond->wait_th_n > 0)
    {
        WakeAllConditionVariable(&cond->cond_var);
    }
    LeaveCriticalSection(&cond->lk);

    while (awake_q.len > 0) {
        WINCO_ROUTINE* rt = winco_q_owner(
            winco_q_popfront(&awake_q), WINCO_ROUTINE, awake_node);
        // To awake q & signal.
        WINCO_THRD* th = rt->thrd;
        // Wakeup & tmout may both happen, CAS tmout->0 to elect owner.
        uint64_t tmout = ILGET64(&rt->tmout);
        if (InterlockedCompareExchange64(&rt->tmout, 0, tmout) == tmout) {
            rt->condvar = NULL;
            EnterCriticalSection(&th->awake_q_l);
            winco_q_pushback(&th->awake_q, &rt->awake_node);
            LeaveCriticalSection(&th->awake_q_l);
            InterlockedAdd(&th->awake_signal, 1);
        }
    }

    if (g_thrd) {
        ILINC64(&g_thrd->w->metric->cond_s_n);
    }
}



/* Poll. */
int winco_wsapoll(LPWSAPOLLFD fdArray, ULONG fds, INT timeout) {
    WINCO_THRD* th = g_thrd;

    if (!th) {
        return WSAPoll(fdArray, fds, timeout);
    }
    
    ILINC64(&th->w->metric->wsapoll_n);
    ILINC64(&th->w->metric->ctx_s_pl_n);

    WINCO_ROUTINE* rt = th->active_rt;
    assert(rt);
    rt->wsapoll_fds = fdArray;
    rt->wsapoll_fdn = fds;
    rt->wsapoll_tmout = timeout;
    rt->wsapoll_err = NO_ERROR;
    rt->wsapoll_def_tmout = timeout + winco_tick_ms();
    winco_q_pushback(&th->wsapoll_q, &rt->state_node);
    SwitchToFiber(th->sched_rt);

    if (rt->wsapoll_err > 0) {
        ILINC64(&th->w->metric->wsapoll_event_n);
    }
    else {
        ILINC64(&th->w->metric->wsapoll_to_n);
    }

    return rt->wsapoll_err;
}


/* Identity. */
int winco_thrd_id() {
    return GetCurrentThreadId();
}

WINCO_ROUTINE* winco_routine() {
    return g_thrd ? g_thrd->active_rt : NULL;
}

int winco_rt_thread_id(WINCO_ROUTINE* rt) {
    assert(rt);
    return rt->thrd->id;
}

int winco_rt_routine_id(WINCO_ROUTINE* rt) {
    assert(rt);
    return rt->id;
}

/* Stats. */
WINCO_STATS winco_stats(WINCO* w) {
    METRIC mt;
    mt.thrd_n = ILGET64(&w->metric->thrd_n);
    mt.cort_n = ILGET64(&w->metric->cort_n);
    mt.ctx_s_n = ILEXC64(&w->metric->ctx_s_n, 0);
    mt.ctx_s_yl_n = ILEXC64(&w->metric->ctx_s_yl_n, 0);
    mt.ctx_s_sl_n = ILEXC64(&w->metric->ctx_s_sl_n, 0);
    mt.ctx_s_lk_n = ILEXC64(&w->metric->ctx_s_lk_n, 0);
    mt.ctx_s_cw_n = ILEXC64(&w->metric->ctx_s_cw_n, 0);
    mt.ctx_s_pl_n = ILEXC64(&w->metric->ctx_s_pl_n, 0);
    mt.exec_t = ILEXC64(&w->metric->exec_t, 0);
    mt.idle_t = ILEXC64(&w->metric->idle_t, 0);
    mt.poll_t = ILEXC64(&w->metric->poll_t, 0);
    mt.wake_t = ILEXC64(&w->metric->wake_t, 0);
    mt.wscn_t = ILEXC64(&w->metric->wscn_t, 0);
    mt.lock_q = ILGET64(&w->metric->lock_q);
    mt.cndw_q = ILGET64(&w->metric->cndw_q);
    mt.wake_q = ILEXC64(&w->metric->wake_q, 0);
    mt.wake_q_spn = ILEXC64(&w->metric->wake_q_spn, 0);
    mt.wait_q = ILEXC64(&w->metric->wait_q, 0);
    mt.wait_q_spn = ILEXC64(&w->metric->wait_q_spn, 0);
    mt.wsapoll_q = ILEXC64(&w->metric->wsapoll_q, 0);
    mt.wsapoll_q_spn = ILEXC64(&w->metric->wsapoll_q_spn, 0);
    mt.sleep_n = ILEXC64(&w->metric->sleep_n, 0);
    mt.yield_n = ILEXC64(&w->metric->yield_n, 0);
    mt.lock_n = ILEXC64(&w->metric->lock_n, 0);
    mt.lock_block_n = ILEXC64(&w->metric->lock_block_n, 0);
    mt.unlock_n = ILEXC64(&w->metric->unlock_n, 0);
    mt.cond_w_n = ILEXC64(&w->metric->cond_w_n, 0);
    mt.cond_s_n = ILEXC64(&w->metric->cond_s_n, 0);
    mt.cond_wk_n = ILEXC64(&w->metric->cond_wk_n, 0);
    mt.cond_to_n = ILEXC64(&w->metric->cond_to_n, 0);
    mt.wsapoll_n = ILEXC64(&w->metric->wsapoll_n, 0);
    mt.wsapoll_event_n = ILEXC64(&w->metric->wsapoll_event_n, 0);
    mt.wsapoll_to_n = ILEXC64(&w->metric->wsapoll_to_n, 0);

    uint64_t now = winco_tick_ms();
    double t_ms = (double)(now - w->last_stat);
    w->last_stat = now;

    WINCO_STATS st;
    st.thread_n = (double)mt.thrd_n;
    st.coroutine_n = (double)mt.cort_n;
#define __COPY(m, n) st.m = (double)mt.n
#define __COPY_AVG(m, n, k) st.m = ((double)mt.n / (double)mt.k)
#define __COPY_MS(m, n) st.m = ((double)mt.n * 1000) / t_ms
#define __COPY_US2MS(m) st.m= ((double)mt.m) / t_ms
    __COPY_MS(ctx_s, ctx_s_n);
    __COPY_MS(ctx_s_yield, ctx_s_yl_n);
    __COPY_MS(ctx_s_sleep, ctx_s_sl_n);
    __COPY_MS(ctx_s_lock, ctx_s_lk_n);
    __COPY_MS(ctx_s_cndw, ctx_s_cw_n);
    __COPY_MS(ctx_s_poll, ctx_s_pl_n);
    __COPY_US2MS(exec_t);
    __COPY_US2MS(idle_t);
    __COPY_US2MS(poll_t);
    __COPY_US2MS(wake_t);
    __COPY_US2MS(wscn_t);
    __COPY(lock_q_len, lock_q);
    __COPY(cndw_q_len, cndw_q);
    __COPY_AVG(wake_q_len, wake_q, wake_q_spn);
    __COPY_AVG(wait_q_len, wait_q, wait_q_spn);
    __COPY_AVG(poll_q_len, wsapoll_q, wsapoll_q_spn);
    __COPY_MS(yield, yield_n);
    __COPY_MS(sleep, sleep_n);
    __COPY_MS(lock, lock_n);
    __COPY_MS(lblk, lock_block_n);
    __COPY_MS(unlk, unlock_n);
    __COPY_MS(cndw, cond_w_n);
    __COPY_MS(cndw_sig, cond_s_n);
    __COPY_MS(cndw_tmout, cond_to_n);
    __COPY_MS(cndw_wake, cond_wk_n);
    __COPY_MS(poll, wsapoll_n);
    __COPY_MS(poll_event, wsapoll_event_n);
    __COPY_MS(poll_tmout, wsapoll_to_n);

    return st;
}

void winco_stats_str(WINCO_STATS st, char* buf, int buf_len) {
    assert(buf);
    assert(buf_len >= 512);
    snprintf(buf, buf_len, 
        "cfg:\tth=%.0f rt=%.0f\n"
        "ctx_s:\tall=%.1f yl=%.1f sl=%.1f lk=%.1f cw=%.1f pl=%.1f (/s)\n"
        "time:\texec=%.1f idle=%.1f poll=%.1f wake=%.1f wscn=%.1f (ms/s)\n"
        "queue:\tlk=%.1f cw=%.1f wk=%.1f wt=%.1f pl=%.1f\n"
        "ops:\tyl/sl=%.1f/%.1f lk/ul/bl=%.1f/%.1f/%.1f "
        "cw/sg/wk/to=%.1f/%.1f/%.1f/%.1f pl/ev/to=%.1f/%.1f/%.1f (/s)\n",
        st.thread_n,
        st.coroutine_n,
        st.ctx_s,
        st.ctx_s_yield,
        st.ctx_s_sleep,
        st.ctx_s_lock,
        st.ctx_s_cndw,
        st.ctx_s_poll,
        st.exec_t,
        st.idle_t,
        st.poll_t,
        st.wake_t,
        st.wscn_t,
        st.lock_q_len,
        st.cndw_q_len,
        st.wake_q_len,
        st.wait_q_len,
        st.poll_q_len,
        st.yield,
        st.sleep,
        st.lock,
        st.unlk,
        st.lblk,
        st.cndw,
        st.cndw_sig,
        st.cndw_wake,
        st.cndw_tmout,
        st.poll,
        st.poll_event,
        st.poll_tmout
        );
}


/* Config. */
int winco_cfg(WINCO* w, char cfg, int val) {
    switch (cfg)
    {
    case WINCO_CFG_THRD_IDLE:
        w->thrd_idle_tmout = (uint64_t)val;
        break;
    case WINCO_CFG_WSAPOLL_INTERV:
        w->wsapoll_intv = (uint64_t)val;
        break;
    case WINCO_CFG_COROUTINE_IDLE_TMOUT:
        w->rt_wait_tmout = (uint64_t)val;
        break;
    default:
        return -1;
    }
    return 0;
}


