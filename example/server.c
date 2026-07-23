#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"
#include "../include/raft/uv.h"

#define N_SERVERS 3     /* Number of servers in the example cluster */
#define APPLY_RATE 1000 /* Apply a new entry every 125 milliseconds */

#define Log(SERVER_ID, FORMAT) printf("%d: " FORMAT "\n", SERVER_ID)
#define Logf(SERVER_ID, FORMAT, ...) \
    printf("%d: " FORMAT "\n", SERVER_ID, __VA_ARGS__)

/********************************************************************
 *
 * Sample application FSM that just increases a counter.
 *
 ********************************************************************/

struct Fsm
{
    unsigned long long count;
};

static int FsmApply(struct raft_fsm *fsm,
                    const struct raft_buffer *buf,
                    void **result)
{
    struct Fsm *f = fsm->data;
    if (buf->len != 8) {
        return RAFT_MALFORMED;
    }
    f->count += *(uint64_t *)buf->base;
    *result = &f->count;
    return 0;
}

static int FsmSnapshot(struct raft_fsm *fsm,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs)
{
    struct Fsm *f = fsm->data;
    *n_bufs = 1;
    *bufs = raft_malloc(sizeof **bufs);
    if (*bufs == NULL) {
        return RAFT_NOMEM;
    }
    (*bufs)[0].len = sizeof(uint64_t);
    (*bufs)[0].base = raft_malloc((*bufs)[0].len);
    if ((*bufs)[0].base == NULL) {
        return RAFT_NOMEM;
    }
    *(uint64_t *)(*bufs)[0].base = f->count;
    return 0;
}

static int FsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct Fsm *f = fsm->data;
    if (buf->len != sizeof(uint64_t)) {
        return RAFT_MALFORMED;
    }
    f->count = *(uint64_t *)buf->base;
    raft_free(buf->base);
    return 0;
}

static int FsmInit(struct raft_fsm *fsm)
{
    struct Fsm *f = raft_malloc(sizeof *f);
    if (f == NULL) {
        return RAFT_NOMEM;
    }
    f->count = 0;
    fsm->version = 2;
    fsm->data = f;
    fsm->apply = FsmApply;
    fsm->snapshot = FsmSnapshot;
    fsm->snapshot_finalize = NULL;
    fsm->restore = FsmRestore;
    return 0;
}

static void FsmClose(struct raft_fsm *f)
{
    if (f->data != NULL) {
        raft_free(f->data);
    }
}

/********************************************************************
 *
 * Example struct holding a single raft server instance and all its
 * dependencies.
 *
 ********************************************************************/

struct Server;
typedef void (*ServerCloseCb)(struct Server *server);

/* A single client-submitted command, queued for the loop thread to hand off
 * to raft_apply(). Lives on the caller's stack for the lifetime of the
 * blocking ServerSubmit() call. */
struct ClientRequest
{
    struct raft_buffer buf;
    uv_sem_t done;
    int status;
    uint64_t result;
    struct ClientRequest *next;
};

struct Server
{
    void *data;                         /* User data context. */
    struct uv_loop_s *loop;             /* UV loop. */
    const char *dir;                    /* Data dir of UV I/O backend. */
    struct raft_uv_transport transport; /* UV I/O backend transport. */
    struct raft_io io;                  /* UV I/O backend. */
    struct raft_fsm fsm;                /* Sample application FSM. */
    unsigned id;                        /* Raft instance ID. */
    char address[64];                   /* Raft instance address. */
    struct raft raft;                   /* Raft instance. */
    struct raft_transfer transfer;      /* Transfer leadership request. */
    ServerCloseCb close_cb;             /* Optional close callback. */

    /* Cross-thread command submission. */
    uv_async_t submit_async; /* Wakes the loop to drain the queue. */
    uv_mutex_t queue_mutex;  /* Protects the fields below. */
    struct ClientRequest *queue_head;
    struct ClientRequest *queue_tail;
    int stopping; /* Set once shutdown has begun. */

    uv_thread_t submit_thread; /* Demo thread submitting commands. */
    int thread_started;
    uv_work_t join_work; /* Used to join submit_thread off-loop. */
};

/* Runs on the loop thread: completion callback for a raft_apply() issued on
 * behalf of a ClientRequest. Hands the outcome back to the blocked caller. */
static void serverClientApplyCb(struct raft_apply *req,
                                int status,
                                void *result)
{
    struct ClientRequest *creq = req->data;
    raft_free(req);
    creq->status = status;
    if (status == 0) {
        creq->result = *(uint64_t *)result;
    }
    uv_sem_post(&creq->done);
}

/* Runs on the loop thread: woken up by ServerSubmit() from any thread. Drains
 * the queue and issues one raft_apply() per queued request. */
static void serverSubmitAsyncCb(uv_async_t *handle)
{
    struct Server *s = handle->data;
    struct ClientRequest *creq;

    uv_mutex_lock(&s->queue_mutex);
    creq = s->queue_head;
    s->queue_head = NULL;
    s->queue_tail = NULL;
    uv_mutex_unlock(&s->queue_mutex);

    while (creq != NULL) {
        struct ClientRequest *next = creq->next;
        struct raft_apply *req;
        int rv;

        if (s->raft.state != RAFT_LEADER) {
            creq->status = RAFT_NOTLEADER;
            uv_sem_post(&creq->done);
            creq = next;
            continue;
        }

        req = raft_malloc(sizeof *req);
        if (req == NULL) {
            creq->status = RAFT_NOMEM;
            uv_sem_post(&creq->done);
            creq = next;
            continue;
        }
        req->data = creq;

        rv = raft_apply(&s->raft, req, &creq->buf, 1, serverClientApplyCb);
        if (rv != 0) {
            raft_free(req);
            creq->status = rv;
            uv_sem_post(&creq->done);
        }

        creq = next;
    }
}

/* Thread-safe: submit a new command from any thread and block until raft has
 * applied it (or failed to). Multiple threads may call this concurrently. */
static int ServerSubmit(struct Server *s, uint64_t value, uint64_t *result)
{
    struct ClientRequest creq;
    int rv;

    creq.buf.len = sizeof(uint64_t);
    creq.buf.base = raft_malloc(creq.buf.len);
    if (creq.buf.base == NULL) {
        return RAFT_NOMEM;
    }
    *(uint64_t *)creq.buf.base = value;
    creq.next = NULL;
    creq.status = 0;
    creq.result = 0;
    uv_sem_init(&creq.done, 0);

    uv_mutex_lock(&s->queue_mutex);
    if (s->stopping) {
        uv_mutex_unlock(&s->queue_mutex);
        uv_sem_destroy(&creq.done);
        raft_free(creq.buf.base);
        return RAFT_SHUTDOWN;
    }
    if (s->queue_tail != NULL) {
        s->queue_tail->next = &creq;
    } else {
        s->queue_head = &creq;
    }
    s->queue_tail = &creq;
    uv_mutex_unlock(&s->queue_mutex);

    uv_async_send(&s->submit_async);

    uv_sem_wait(&creq.done);
    uv_sem_destroy(&creq.done);

    rv = creq.status;
    if (rv == 0 && result != NULL) {
        *result = creq.result;
    }
    return rv;
}

static void serverRaftCloseCb(struct raft *raft)
{
    struct Server *s = raft->data;
    raft_uv_close(&s->io);
    raft_uv_tcp_close(&s->transport);
    FsmClose(&s->fsm);
    uv_mutex_destroy(&s->queue_mutex);
    if (s->close_cb != NULL) {
        s->close_cb(s);
    }
}

static void serverTransferCb(struct raft_transfer *req)
{
    struct Server *s = req->data;
    raft_id id;
    const char *address;
    raft_leader(&s->raft, &id, &address);
    raft_close(&s->raft, serverRaftCloseCb);
}

/* Final callback in the shutdown sequence, invoked once submit_async has been
 * fully closed (guaranteeing no more queued commands can arrive). */
static void serverSubmitAsyncCloseCb(struct uv_handle_s *handle)
{
    struct Server *s = handle->data;
    if (s->raft.data != NULL) {
        if (s->raft.state == RAFT_LEADER) {
            int rv;
            rv = raft_transfer(&s->raft, &s->transfer, 0, serverTransferCb);
            if (rv == 0) {
                return;
            }
        }
        raft_close(&s->raft, serverRaftCloseCb);
    }
}

/* Runs on a libuv thread-pool worker so that joining the submit thread never
 * blocks the loop thread it may itself be waiting on. */
static void serverJoinWorkCb(uv_work_t *work)
{
    struct Server *s = work->data;
    uv_thread_join(&s->submit_thread);
}

static void serverJoinAfterWorkCb(uv_work_t *work, int status)
{
    struct Server *s = work->data;
    (void)status;
    uv_close((struct uv_handle_s *)&s->submit_async, serverSubmitAsyncCloseCb);
}

/* Initialize the example server struct, without starting it yet. */
static int ServerInit(struct Server *s,
                      struct uv_loop_s *loop,
                      const char *dir,
                      unsigned id)
{
    struct raft_configuration configuration;
    struct timespec now;
    unsigned i;
    int rv;

    memset(s, 0, sizeof *s);

    /* Seed the random generator */
    timespec_get(&now, TIME_UTC);
    srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

    s->loop = loop;

    /* Set up the cross-thread command submission queue. */
    rv = uv_async_init(s->loop, &s->submit_async, serverSubmitAsyncCb);
    if (rv != 0) {
        Logf(s->id, "uv_async_init(): %s", uv_strerror(rv));
        goto err;
    }
    rv = uv_mutex_init(&s->queue_mutex);
    if (rv != 0) {
        Logf(s->id, "uv_mutex_init(): %s", uv_strerror(rv));
        goto err;
    }
    /* Only mark submit_async as ready once the mutex it depends on is also
     * initialized; ServerClose() uses submit_async.data as the guard for
     * whether this whole subsystem needs tearing down. */
    s->submit_async.data = s;
    s->join_work.data = s;

    /* Initialize the TCP-based RPC transport. */
    s->transport.version = 1;
    s->transport.data = NULL;
    rv = raft_uv_tcp_init(&s->transport, s->loop);
    if (rv != 0) {
        goto err;
    }

    /* Initialize the libuv-based I/O backend. */
    rv = raft_uv_init(&s->io, s->loop, dir, &s->transport);
    if (rv != 0) {
        Logf(s->id, "raft_uv_init(): %s", s->io.errmsg);
        goto err_after_uv_tcp_init;
    }

    /* Initialize the finite state machine. */
    rv = FsmInit(&s->fsm);
    if (rv != 0) {
        Logf(s->id, "FsmInit(): %s", raft_strerror(rv));
        goto err_after_uv_init;
    }

    /* Save the server ID. */
    s->id = id;

    /* Render the address. */
    sprintf(s->address, "127.0.0.1:900%d", id);

    /* Initialize and start the engine, using the libuv-based I/O backend. */
    rv = raft_init(&s->raft, &s->io, &s->fsm, id, s->address);
    if (rv != 0) {
        Logf(s->id, "raft_init(): %s", raft_errmsg(&s->raft));
        goto err_after_fsm_init;
    }
    s->raft.data = s;

    /* Bootstrap the initial configuration if needed. */
    raft_configuration_init(&configuration);
    for (i = 0; i < N_SERVERS; i++) {
        char address[64];
        unsigned server_id = i + 1;
        sprintf(address, "127.0.0.1:900%d", server_id);
        rv = raft_configuration_add(&configuration, server_id, address,
                                    RAFT_VOTER);
        if (rv != 0) {
            Logf(s->id, "raft_configuration_add(): %s", raft_strerror(rv));
            goto err_after_configuration_init;
        }
    }
    rv = raft_bootstrap(&s->raft, &configuration);
    if (rv != 0 && rv != RAFT_CANTBOOTSTRAP) {
        goto err_after_configuration_init;
    }
    raft_configuration_close(&configuration);

    raft_set_snapshot_threshold(&s->raft, 64);
    raft_set_snapshot_trailing(&s->raft, 16);
    raft_set_pre_vote(&s->raft, true);

    s->transfer.data = s;

    return 0;

err_after_configuration_init:
    raft_configuration_close(&configuration);
err_after_fsm_init:
    FsmClose(&s->fsm);
err_after_uv_init:
    raft_uv_close(&s->io);
err_after_uv_tcp_init:
    raft_uv_tcp_close(&s->transport);
err:
    return rv;
}

/* Demo external thread: outside the loop, periodically submits a command and
 * blocks on ServerSubmit() for the result, exactly like any other caller
 * would from an independent thread (e.g. an HTTP handler). */
static void serverSubmitterMain(void *arg)
{
    struct Server *s = arg;

    for (;;) {
        uint64_t result;
        int rv;

        uv_sleep(APPLY_RATE);

        rv = ServerSubmit(s, 1, &result);
        if (rv == RAFT_SHUTDOWN) {
            break;
        }
        if (rv != 0) {
            Logf(s->id, "ServerSubmit(): %s", raft_strerror(rv));
            continue;
        }
        Logf(s->id, "count %llu", (unsigned long long)result);
    }
}

/* Start the example server. */
static int ServerStart(struct Server *s)
{
    int rv;

    Log(s->id, "starting");

    rv = raft_start(&s->raft);
    if (rv != 0) {
        Logf(s->id, "raft_start(): %s", raft_errmsg(&s->raft));
        goto err;
    }
    rv = uv_thread_create(&s->submit_thread, serverSubmitterMain, s);
    if (rv != 0) {
        Logf(s->id, "uv_thread_create(): %s", uv_strerror(rv));
        goto err;
    }
    s->thread_started = 1;

    return 0;

err:
    return rv;
}

/* Release all resources used by the example server. */
static void ServerClose(struct Server *s, ServerCloseCb cb)
{
    s->close_cb = cb;

    Log(s->id, "stopping");

    if (s->submit_async.data == NULL) {
        /* ServerInit() never got this far; nothing to tear down. */
        if (s->close_cb != NULL) {
            s->close_cb(s);
        }
        return;
    }

    uv_mutex_lock(&s->queue_mutex);
    s->stopping = 1;
    uv_mutex_unlock(&s->queue_mutex);

    if (!s->thread_started) {
        uv_close((struct uv_handle_s *)&s->submit_async,
                 serverSubmitAsyncCloseCb);
        return;
    }

    /* Join off the loop thread: the submit thread may currently be blocked in
     * ServerSubmit() waiting on an in-flight raft_apply(), which can only
     * complete while the loop keeps running. */
    if (uv_queue_work(s->loop, &s->join_work, serverJoinWorkCb,
                      serverJoinAfterWorkCb) != 0) {
        uv_close((struct uv_handle_s *)&s->submit_async,
                 serverSubmitAsyncCloseCb);
    }
}

/********************************************************************
 *
 * Top-level main loop.
 *
 ********************************************************************/

static void mainServerCloseCb(struct Server *server)
{
    struct uv_signal_s *sigint = server->data;
    uv_close((struct uv_handle_s *)sigint, NULL);
}

/* Handler triggered by SIGINT. It will initiate the shutdown sequence. */
static void mainSigintCb(struct uv_signal_s *handle, int signum)
{
    (void)signum;
    struct Server *server = handle->data;
    assert(signum == SIGINT);
    uv_signal_stop(handle);
    server->data = handle;
    ServerClose(server, mainServerCloseCb);
}

int main(int argc, char *argv[])
{
    struct uv_loop_s loop;
    struct uv_signal_s sigint; /* To catch SIGINT and exit. */
    struct Server server;
    const char *dir;
    unsigned id;
    int rv;

    if (argc != 3) {
        printf("usage: example-server <dir> <id>\n");
        return 1;
    }
    dir = argv[1];
    id = (unsigned)atoi(argv[2]);

    /* Ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254 */
    signal(SIGPIPE, SIG_IGN);

    /* Initialize the libuv loop. */
    rv = uv_loop_init(&loop);
    if (rv != 0) {
        Logf(id, "uv_loop_init(): %s", uv_strerror(rv));
        goto err;
    }

    /* Initialize the example server. */
    rv = ServerInit(&server, &loop, dir, id);
    if (rv != 0) {
        goto err_after_server_init;
    }

    /* Add a signal handler to stop the example server upon SIGINT. */
    rv = uv_signal_init(&loop, &sigint);
    if (rv != 0) {
        Logf(id, "uv_signal_init(): %s", uv_strerror(rv));
        goto err_after_server_init;
    }
    sigint.data = &server;
    rv = uv_signal_start(&sigint, mainSigintCb, SIGINT);
    if (rv != 0) {
        Logf(id, "uv_signal_start(): %s", uv_strerror(rv));
        goto err_after_signal_init;
    }

    /* Start the server. */
    rv = ServerStart(&server);
    if (rv != 0) {
        goto err_after_signal_init;
    }

    /* Run the event loop until we receive SIGINT. */
    rv = uv_run(&loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        Logf(id, "uv_run_start(): %s", uv_strerror(rv));
    }

    uv_loop_close(&loop);

    return rv;

err_after_signal_init:
    uv_close((struct uv_handle_s *)&sigint, NULL);
err_after_server_init:
    ServerClose(&server, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
err:
    return rv;
}
