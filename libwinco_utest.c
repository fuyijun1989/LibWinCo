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

#include <stdio.h>

#include "libwinco.h"

int g_test_err = 0;
int g_prt_dbg = 0;

#define PRT(fmt, ...) \
    do { fprintf(stderr, fmt, __VA_ARGS__); } while(0)

#define PRTDBG(fmt, ...) \
    do { if (g_prt_dbg) fprintf(stderr, fmt, __VA_ARGS__); } while(0)

#define EXP(exp, explain) \
    do { \
        if (!(exp)) { \
            g_test_err++; \
            PRT("[    FAIL] " #exp ", "); \
            explain; \
            PRT("\n"); \
        }; \
    } while(0)

#define TEST(fn) \
    do { \
        g_test_err = 0; \
        PRT("[START   ] " #fn "\n"); \
        uint64_t t0 = GetTickCount64(); \
        fn(); \
        uint64_t t = GetTickCount64() - t0; \
        if (g_test_err == 0) \
            PRT("[SUCC    ] " #fn " (%"PRIu64" ms)\n\n", t); \
        else \
            PRT("[    FAIL] " #fn " (%"PRIu64" ms)\n\n", t); \
    } while(0)

typedef struct THRD_EXEC_PARAM_T {
    cort_fn fn;
    void* fn_arg;
    void* fn_ret;
} THRD_EXEC_PARAM ;

static DWORD WINAPI exec_in_thrd(LPVOID arg) {
    THRD_EXEC_PARAM* exex = (THRD_EXEC_PARAM*)arg;
    exex->fn_ret = exex->fn(exex->fn_arg);
    return 0;
}

#define Hundred 100
#define Kilo 1000
#define Million 1000000
#define Billion 1000000000


/* Create & yield & join & delete. */
void* yield_join_j(void* arg) {
    int64_t sum = (int64_t)arg;
    for (int i = 0; i < Million; i++) {
        sum++;
        winco_yield();
    }
    return (void*)sum;
}

void yield_join_t() {
    WINCO* w = winco_init(2);
    WINCO_ROUTINE* rt1 = winco_create(w, yield_join_j, (void*)Million);
    int64_t r = (int64_t)winco_join(rt1);

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    EXP(2 * Million == r, PRT("r = %"PRId64, r));
    winco_delete(rt1);
    winco_destroy(w);
}


/* Sleep. */
void* sleep_j(void* arg) {
    int cnt = 1 * Hundred;
    uint64_t t0 = GetTickCount64();
    while (--cnt > 0) { winco_sleep(1); }
    return (void*)(GetTickCount64() - t0);
}

void sleep_t() {
    // System sleep.
    THRD_EXEC_PARAM exex;
    exex.fn = sleep_j;
    HANDLE thrd = CreateThread(NULL, 0, exec_in_thrd, &exex, 0, NULL);
    WaitForSingleObject(thrd, INFINITE);
    uint64_t r0 = (uint64_t)exex.fn_ret;

    // Winco sleep.
    WINCO* w = winco_init(2);
    WINCO_ROUTINE* rt1 = winco_create(w, sleep_j, NULL);
    int64_t r1 = (int64_t)winco_join(rt1);

    PRTDBG("> 100 System sleep(1) %"PRIu64" ms\n"
           "> 100 WinCo  sleep(1) %"PRIu64" ms\n", r0, r1);

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    winco_delete(rt1);
    winco_destroy(w);
}


/* Lock. */
typedef struct LOCK_TD_T{
    int64_t target;
    int64_t value;
    WINCO_LOCK *lock;
    int to_yield;
} LOCK_TD;

void* lock_add_j(void* arg) {
    LOCK_TD* d = (LOCK_TD*)arg;
    int64_t target = d->target;
    while (--target >= 0) {
        winco_lock(d->lock);
        d->value++;
        if (d->to_yield == 1)
            winco_yield();
        winco_unlock(d->lock);
    }
    return NULL;
}

/* Lock 1, n coroutines in 1 thread. */
void lock_n_cort_1_thrd() {
    // Only a few ctx switches should happen.
    WINCO* w = winco_init(1);
    LOCK_TD d;
    d.value = 0;
    d.lock = winco_lock_init();
    d.target = 1 * Million;
    d.to_yield = 0;
    WINCO_ROUTINE* rt1 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt2 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt3 = winco_create(w, lock_add_j, &d);
    winco_join(rt1);
    winco_join(rt2);
    winco_join(rt3);
    winco_lock_destroy(d.lock);

    EXP(3 * Million == d.value, PRT("val = %"PRId64, d.value));

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    winco_delete(rt1);
    winco_delete(rt2);
    winco_delete(rt3);
    winco_destroy(w);
}

/* Lock 2, n coroutines in 2 thread. */
void lock_n_cort_2_thrd() {
    // Much more ctx switches happen.
    WINCO* w = winco_init(2);
    LOCK_TD d;
    d.value = 0;
    d.lock = winco_lock_init();
    d.target = 1 * Million;
    d.to_yield = 0;
    WINCO_ROUTINE* rt1 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt2 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt3 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt4 = winco_create(w, lock_add_j, &d);
    winco_join(rt1);
    winco_join(rt2);
    winco_join(rt3);
    winco_join(rt4);
    winco_lock_destroy(d.lock);

    EXP(4 * Million == d.value, PRT("val = %"PRId64, d.value));

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    winco_delete(rt1);
    winco_delete(rt2);
    winco_delete(rt3);
    winco_delete(rt4);
    winco_destroy(w);
}

/* Lock 3, n coroutines in 2 thread with yield. */
void lock_n_cort_2_thrd_yield() {
    // Even more ctx switches happen.
    WINCO* w = winco_init(2);
    LOCK_TD d;
    d.value = 0;
    d.lock = winco_lock_init();
    d.target = 1 * Million;
    d.to_yield = 1;
    WINCO_ROUTINE* rt1 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt2 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt3 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt4 = winco_create(w, lock_add_j, &d);
    winco_join(rt1);
    winco_join(rt2);
    winco_join(rt3);
    winco_join(rt4);
    winco_lock_destroy(d.lock);

    EXP(4 * Million == d.value, PRT("val = %"PRId64, d.value));

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    winco_delete(rt1);
    winco_delete(rt2);
    winco_delete(rt3);
    winco_delete(rt4);
    winco_destroy(w);
}

/* Lock 4, threads mix with coroutines. */
void lock_n_thrd_mix_cort() {
    WINCO* w = winco_init(2);
    LOCK_TD d;
    d.value = 0;
    d.lock = winco_lock_init();
    d.target = 1 * Million;
    d.to_yield = 1;  // More contention.
    THRD_EXEC_PARAM exex;
    exex.fn = lock_add_j;
    exex.fn_arg = &d;
    HANDLE thrd1 = CreateThread(NULL, 0, exec_in_thrd, &exex, 0, NULL);
    HANDLE thrd2 = CreateThread(NULL, 0, exec_in_thrd, &exex, 0, NULL);
    WINCO_ROUTINE* rt1 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt2 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt3 = winco_create(w, lock_add_j, &d);
    WINCO_ROUTINE* rt4 = winco_create(w, lock_add_j, &d);
    WaitForSingleObject(thrd1, INFINITE);
    WaitForSingleObject(thrd2, INFINITE);
    winco_join(rt1);
    winco_join(rt2);
    winco_join(rt3);
    winco_join(rt4);
    winco_lock_destroy(d.lock);

    EXP(6 * Million == d.value, PRT("val = %"PRId64, d.value));

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    CloseHandle(thrd1);
    CloseHandle(thrd2);
    winco_delete(rt1);
    winco_delete(rt2);
    winco_delete(rt3);
    winco_delete(rt4);
    winco_destroy(w);
}


/* Cond. */
typedef struct COND_ADD_T {
    int64_t target;
    int64_t val;
    int end;
    WINCO_LOCK* lock;
    WINCO_COND_VAR* condvar;
    int check_wait_ret;
} COND_ADD;

void* cond_produce_j(void* arg) {
    COND_ADD* d = (COND_ADD*)arg;
    int64_t sum = 0;
    while (sum < d->target) {
        winco_lock(d->lock);
        if (d->val == 0) {
            d->val++;
            // Timeout on sum % 10 == 0, else wake up.
            if (sum % 10 != 0) {
                // PRT("%"PRId64" prod signal\n", sum);
                winco_cond_signal(d->condvar);
            }
            else {
                // PRT("%"PRId64" prod mute\n", sum);
                winco_yield();
            }
            winco_unlock(d->lock);
            sum++;
        }
        else {
            // PRT("%"PRId64" prod idle\n", sum);
            winco_unlock(d->lock);
            winco_sleep(10); // Wait consumer to consume val.
        }
        winco_yield();  // Busy loop, yield.
    }
    while (1) {
        winco_lock(d->lock);
        if (d->val == 0) {
            d->end = 1;
            winco_unlock(d->lock);
            break;
        }
        winco_unlock(d->lock);
        winco_sleep(10);
    }
    return NULL;
}

void* cond_consume_j(void* arg) {
    COND_ADD* d = (COND_ADD*)arg;
    int64_t sum = 0;
    while (1) {
        winco_lock(d->lock);
        if (d->end == 1) {
            // PRT("%"PRId64" cons exit\n", sum);
            winco_unlock(d->lock);
            break;
        }

        if (d->val > 0) {
            sum += d->val;
            d->val = 0;
            // PRT("%"PRId64" cons consumed\n", sum);
        }
        // PRT("%"PRId64" cons wait\n", sum);
        // Timeout on sum % 10 == 0, else wake up.
        int r = winco_cond_wait(d->condvar, d->lock, 100);
        // PRT("%"PRId64" cons wake (%"PRId32")\n", sum, r);
        if (sum < d->target && d->check_wait_ret) {
            if (sum % 10 != 0) EXP(r == 0, PRT("r = %"PRId32, r));
            else EXP(r == 1, PRT("r = %"PRId32, r));
        }
        winco_unlock(d->lock);
        winco_yield();  // Busy loop, yield.
    }
    return (void*)sum;
}

/* Cond 1, 2 cort in 1 thread. */
void cond_2_cort_1_thrd() {
    WINCO* w = winco_init(1);
    COND_ADD d;
    d.target = 1 * Hundred;
    d.val = 0;
    d.end = 0;
    d.lock = winco_lock_init();
    d.condvar = winco_cond_init();
    d.check_wait_ret = 1;
    WINCO_ROUTINE* prod = winco_create(w, cond_produce_j, (void*)&d);
    winco_sleep(100);
    WINCO_ROUTINE* cons = winco_create(w, cond_consume_j, (void*)&d);
    winco_join(prod);
    int64_t sum = (int64_t)winco_join(cons);
    EXP(1 * Hundred == sum, PRT("sum = %"PRId64, sum));
    winco_lock_destroy(d.lock);
    winco_cond_destroy(d.condvar);

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    winco_delete(prod);
    winco_delete(cons);
    winco_destroy(w);
}

/* Cond 2, 2 cort in 2 threads. */
void cond_2_cort_2_thrd() {
    WINCO* w = winco_init(2);
    COND_ADD d;
    d.target = 1 * Hundred;
    d.val = 0;
    d.end = 0;
    d.lock = winco_lock_init();
    d.condvar = winco_cond_init();
    d.check_wait_ret = 1;
    WINCO_ROUTINE* prod = winco_create(w, cond_produce_j, (void*)&d);
    winco_sleep(100);
    WINCO_ROUTINE* cons = winco_create(w, cond_consume_j, (void*)&d);
    winco_join(prod);
    int64_t sum = (int64_t)winco_join(cons);
    EXP(1 * Hundred == sum, PRT("sum = %"PRId64, sum));

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    winco_lock_destroy(d.lock);
    winco_cond_destroy(d.condvar);
    winco_delete(prod);
    winco_delete(cons);
    winco_destroy(w);
}

/* Cond 3, cort to thread. */
void cond_cort_to_thrd() {
    WINCO* w = winco_init(2);
    COND_ADD d;
    d.target = 1 * Hundred;
    d.val = 0;
    d.end = 0;
    d.lock = winco_lock_init();
    d.condvar = winco_cond_init();
    d.check_wait_ret = 0;
    THRD_EXEC_PARAM exex;
    exex.fn = cond_consume_j;
    exex.fn_arg = &d;
    WINCO_ROUTINE* prod = winco_create(w, cond_produce_j, (void*)&d);
    winco_sleep(1000);
    HANDLE cons = CreateThread(NULL, 0, exec_in_thrd, &exex, 0, NULL);
    winco_join(prod);
    WaitForSingleObject(cons, INFINITE);
    int64_t sum = (int64_t)exex.fn_ret;
    EXP(1 * Hundred == sum, PRT("sum = %"PRId64, sum));

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    winco_lock_destroy(d.lock);
    winco_cond_destroy(d.condvar);
    winco_delete(prod);
    CloseHandle(cons);
    winco_destroy(w);
}

/* Cond 4, thread to cort. */
void cond_thrd_to_cort() {
    WINCO* w = winco_init(2);
    COND_ADD d;
    d.target = 1 * Hundred;
    d.val = 0;
    d.end = 0;
    d.lock = winco_lock_init();
    d.condvar = winco_cond_init();
    d.check_wait_ret = 1;
    THRD_EXEC_PARAM exex;
    exex.fn = cond_produce_j;
    exex.fn_arg = &d;
    HANDLE prod = CreateThread(NULL, 0, exec_in_thrd, &exex, 0, NULL);
    winco_sleep(100);
    WINCO_ROUTINE* cons = winco_create(w, cond_consume_j, (void*)&d);
    WaitForSingleObject(prod, INFINITE);
    int64_t sum = (int64_t)winco_join(cons);
    EXP(1 * Hundred == sum, PRT("sum = %"PRId64, sum));

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    winco_lock_destroy(d.lock);
    winco_cond_destroy(d.condvar);
    winco_delete(cons);
    CloseHandle(prod);
    winco_destroy(w);
}

/* Poll. */
void poll_sockets_create(int* send_fd, int* recv_fd) {
    SOCKET listen_s = INVALID_SOCKET;
    SOCKET recv_s = INVALID_SOCKET;
    SOCKET send_s = INVALID_SOCKET;
    struct sockaddr_in listen_addr;
    struct sockaddr_in connect_addr;
    int sock_len = 0;
    int bufsz;

    listen_s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
    listen_addr.sin_port = 0;
    bind(listen_s, (struct sockaddr*) & listen_addr, 
        sizeof(listen_addr));

    sock_len = sizeof(connect_addr);
    getsockname(listen_s, (struct sockaddr*) & connect_addr,
        &sock_len);
    listen(listen_s, 1);

    send_s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    connect(send_s, (struct sockaddr*) & connect_addr, 
        sizeof(connect_addr));

    recv_s = accept(listen_s, NULL, NULL);
    closesocket(listen_s);

    int on = 1;
    ioctlsocket((int)recv_s, FIONBIO, &on);
    ioctlsocket((int)send_s, FIONBIO, &on);

    bufsz = 100;
    setsockopt(recv_s, SOL_SOCKET, SO_SNDBUF,
        (const char*)&bufsz, sizeof(bufsz));
    bufsz = 100;
    setsockopt(recv_s, SOL_SOCKET, SO_RCVBUF,
        (const char*)&bufsz, sizeof(bufsz));
    bufsz = 100;
    setsockopt(send_s, SOL_SOCKET, SO_SNDBUF,
        (const char*)&bufsz, sizeof(bufsz));
    bufsz = 100;
    setsockopt(send_s, SOL_SOCKET, SO_RCVBUF,
        (const char*)&bufsz, sizeof(bufsz));

    *recv_fd= (int)recv_s;
    *send_fd= (int)send_s;

    EXP(WSAGetLastError() == NO_ERROR,
        PRT("wsaerr = %d", WSAGetLastError()));
}

void* poll_cons_j(void* arg) {
    WSAPOLLFD pollfd;
    pollfd.fd = (SOCKET)arg;
    pollfd.events = 0;
    pollfd.revents = 0;

    int64_t sum = 0;
    int run = 1;
    while (run) {
        pollfd.events |= POLLIN;
        int r = winco_wsapoll(&pollfd, 1, 300);
        EXP(r >= 0, PRT("r = %"PRId32" err: %d", r, WSAGetLastError()));
        if (r < 0)
            break;
        if (r == 0) {
            // PRT("poll tmout");
            continue;
        }

        int in = pollfd.revents & POLLIN;
        EXP(in > 0, PRT("revent %"PRIx16, pollfd.revents));
        if (in > 0) {
            char buf[8];
            int rn = 0;
            while ((rn = recv(pollfd.fd, buf, 8, 0)) > 0) {
                for (int p = 0; p < rn; p++) {
                    char c = buf[p];
                    if (c == 'Z') {
                        // PRT("recv Z %d %d\n", __winco_th_id(), __winco_rt_id());
                        run = 0;
                    }
                    else if (c == 'A') {
                        sum++;
                        // PRT("recv A %d %d\n", __winco_th_id(), __winco_rt_id());
                    }
                }
            }
        }
    }
    // PRT("exit\n");
    return (void*)sum;
}

void poll_msg_send_tmout() {
    int jobn = 2000;
    SOCKET* send_fds = (SOCKET*)calloc(jobn, sizeof(SOCKET));
    SOCKET* recv_fds = (SOCKET*)calloc(jobn, sizeof(SOCKET));
    WSADATA d;
    (void)WSAStartup(MAKEWORD(2, 2), &d);
    for (int p = 0; p < jobn; p++) {
        poll_sockets_create((int*)&send_fds[p], (int*)&recv_fds[p]);
    }

    int64_t target = 5;
    int64_t sum = 0;

    WINCO* w = winco_init(2);
    WINCO_ROUTINE** rts = (WINCO_ROUTINE**)calloc(jobn, sizeof(uint64_t));
    for (int p = 0; p < jobn; p++) { 
        rts[p] = winco_create(w, poll_cons_j, (void*)(int64_t)recv_fds[p]);
    }

    for (int t = 0; t < target; t++) {
        char buf[1] = { 'A' };
        for (int p = 0; p < jobn; p++) {
            send(send_fds[p], buf, 1, 0);
        }
        Sleep(1000);
    }

    for (int p = 0; p < jobn; p++) {
        char buf[1] = { 'Z' };
        send(send_fds[p], buf, 1, 0);
    }

    for (int p = 0; p < jobn; p++) { 
        sum += (int64_t)winco_join(rts[p]);
        winco_delete(rts[p]);
    }

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    free(rts);
    winco_destroy(w);

    EXP(target * jobn == sum, PRT("sum = %"PRId64, sum));

    for (int p = 0; p < jobn; p++) {
        closesocket(send_fds[p]);
        closesocket(recv_fds[p]);
    }
    free(recv_fds);
    free(send_fds);
    WSACleanup();
}

void poll_msg_send_fast() {
    int jobn = 2000;
    SOCKET* send_fds = (SOCKET*)calloc(jobn, sizeof(SOCKET));
    SOCKET* recv_fds = (SOCKET*)calloc(jobn, sizeof(SOCKET));
    WSADATA d;
    (void)WSAStartup(MAKEWORD(2, 2), &d);
    for (int p = 0; p < jobn; p++) {
        poll_sockets_create((int*)&send_fds[p], (int*)&recv_fds[p]);
    }

    int64_t target = 1 * Kilo;
    int64_t sum = 0;

    WINCO* w = winco_init(2);

    WINCO_ROUTINE** rts = (WINCO_ROUTINE**)calloc(jobn, sizeof(uint64_t));
    for (int p = 0; p < jobn; p++) { 
        rts[p] = winco_create(w, poll_cons_j, (void*)(int64_t)recv_fds[p]);
    }
    Sleep(500);

    for (int t = 0; t < target; t++) {
        char buf[1] = { 'A' };
        for (int p = 0; p < jobn; p++) {
            send(send_fds[p], buf, 1, 0);
        }
    }

    for (int p = 0; p < jobn; p++) {
        char buf[1] = { 'Z' };
        send(send_fds[p], buf, 1, 0);
    }

    for (int p = 0; p < jobn; p++) { 
        sum += (int64_t)winco_join(rts[p]);
        winco_delete(rts[p]);
    }

    char str[512];
    winco_stats_str(winco_stats(w), str, 512);
    PRTDBG("stats:\n%s", str);

    free(rts);
    winco_destroy(w);

    EXP(target * jobn == sum, PRT("sum = %"PRId64, sum));

    for (int p = 0; p < jobn; p++) {
        closesocket(send_fds[p]);
        closesocket(recv_fds[p]);
    }
    free(recv_fds);
    free(send_fds);
    WSACleanup();
}

int main(int argc, char** argv) {
    g_prt_dbg = 1;

goto T;
T:
    TEST(yield_join_t);
    TEST(sleep_t);
    TEST(lock_n_cort_1_thrd);
    TEST(lock_n_cort_2_thrd);
    TEST(lock_n_cort_2_thrd_yield);
    TEST(lock_n_thrd_mix_cort);
    TEST(cond_2_cort_1_thrd);
    TEST(cond_2_cort_2_thrd);
    TEST(cond_cort_to_thrd);
    TEST(cond_thrd_to_cort);
    TEST(poll_msg_send_tmout);
    TEST(poll_msg_send_fast);
goto E;
E:
    return 0;
}