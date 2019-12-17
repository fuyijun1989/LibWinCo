/*
 * Copyright 2019 Yijun Fu
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

/*
 * LibWinCo: a compact Win32 Coroutine Library.
 */

#ifndef LIBWINCO
#define LIBWINCO

#include <stdlib.h>
#include <inttypes.h>
 
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <WinSock2.h>
#include <sysinfoapi.h>


/* Init. Call at least once for a process. */
int winco_init();

/* Destroy.*/
int winco_destroy();

/* Launch new win coroutine. */
typedef struct WINCO_ROUTINE_T WINCO_ROUTINE;
typedef void* (*cort_fn)(void* arg);
WINCO_ROUTINE* winco_create(cort_fn fn, void* arg);
void* winco_join(WINCO_ROUTINE* rt);
void winco_delete(WINCO_ROUTINE* rt);

/* Yield. Ask scheduler to run other coroutines.
 * Important: in cooperative multitasking, without yield, sleep, cond wait,
 * or poll, other tasks may be blocked; call yield() to solve this problem.
 */
void winco_yield();

/* Sleep. Apply to both thread & coroutine. */
void winco_sleep(uint64_t sleep_ms);

/* Lock. Support thread / coroutine mix use. */
typedef struct WINCO_LOCK_T WINCO_LOCK;
WINCO_LOCK* winco_lock_init();
void winco_lock_destroy(WINCO_LOCK* lk);
void winco_lock(WINCO_LOCK* lk);
void winco_shared_lock(WINCO_LOCK* lk);
void winco_unlock(WINCO_LOCK* lk);

/* Cond variable. Support thread / coroutine mix use. */
typedef struct WINCO_COND_VAR_T WINCO_COND_VAR;
WINCO_COND_VAR* winco_cond_init();
void winco_cond_destroy(WINCO_COND_VAR* cond);
int winco_cond_wait(WINCO_COND_VAR* cond, WINCO_LOCK* lk, uint64_t wait_ms);
void winco_cond_signal(WINCO_COND_VAR* cond);
void winco_cond_signal_all(WINCO_COND_VAR* cond);

/* WSAPoll. */
int winco_wsapoll(LPWSAPOLLFD fdArray, ULONG fds, INT timeout);

/* Current thread id (Win32 thread id). */
int winco_thrd_id();

/* Get current coroutine (NULL on thread). */
WINCO_ROUTINE* winco_routine();

/* Get thread id (Win32 thread id) & coroutine id (0 based). */
int winco_rt_thread_id(WINCO_ROUTINE* rt);
int winco_rt_routine_id(WINCO_ROUTINE* rt);

/* Stats. */
typedef struct WINCO_STATS_T {
    int64_t thread_n;
    int64_t coroutine_n;
    double ctx_switch_per_sec;
    double proc_t_ms_per_sec;
    double sleep_per_sec;
    double lock_per_sec;
    double unlock_per_sec;
    double cond_wait_per_sec;
    double cond_signal_per_sec;
    double cond_tmdout_per_sec;
    double cond_signaled_per_sec;
    double wsapoll_per_sec;
    double wsapoll_succ_per_sec;
    double wsapoll_tmdout_per_sec;
} WINCO_STATS;

/* Get stats since last call. */
WINCO_STATS winco_stats();
void winco_stats_str(WINCO_STATS st, char* buf, int buf_len);

/* Config. */
int winco_cfg(char cfg, int val);
#define WINCO_CFG_THRD_IDLE 'A'
#define WINCO_CFG_WSAPOLL_INTERV 'B'
#define WINCO_CFG_COROUTINE_IDLE_TMOUT 'C'
#define WINCO_CFG_THRD_CNT 'D'

#endif // LIBWINCO
