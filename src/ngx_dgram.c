#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_event.h>
#include "ngx_dgram.h"


#define NGX_INVALID_SOCKET  ((ngx_socket_t) -1)


static ngx_int_t ngx_dgram_init_worker(ngx_cycle_t *cycle);
static void ngx_dgram_exit_worker(ngx_cycle_t *cycle);

static char *ngx_dgram_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_int_t ngx_dgram_create_listening(ngx_conf_t *cf);
static char *ngx_dgram_init_listening(ngx_cycle_t *cycle, void *conf);


ngx_uint_t  ngx_dgram_max_module;


static ngx_atomic_t   dgram_threads = 0;
ngx_atomic_t         *ngx_dgram_threads = &dgram_threads;


static ngx_command_t  ngx_dgram_commands[] = {

    { ngx_string("dgram"),
      NGX_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_dgram_block,
      0,
      0,
      NULL },

      ngx_null_command
};


static ngx_core_module_t  ngx_dgram_module_ctx = {
    ngx_string("dgram"),
    NULL,
    ngx_dgram_init_listening,
};


ngx_module_t  ngx_dgram_module = {
    NGX_MODULE_V1,
    &ngx_dgram_module_ctx,                 /* module context */
    ngx_dgram_commands,                    /* module directives */
    NGX_CORE_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_dgram_init_worker,                 /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_dgram_exit_worker,                 /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static char *
ngx_dgram_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char                         *rv;
    ngx_uint_t                    m, mi, s;
    ngx_conf_t                    pcf;
    ngx_dgram_module_t           *module;
    ngx_dgram_conf_ctx_t         *ctx;
    ngx_dgram_core_srv_conf_t   **cscfp;
    ngx_dgram_core_main_conf_t   *cmcf;

    if (*(ngx_dgram_conf_ctx_t **) conf) {
        return "is duplicate";
    }

    /* the main dgram context */

    ctx = ngx_pcalloc(cf->pool, sizeof(ngx_dgram_conf_ctx_t));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    *(ngx_dgram_conf_ctx_t **) conf = ctx;

    /* count the number of the dgram modules and set up their indices */

    ngx_dgram_max_module = ngx_count_modules(cf->cycle, NGX_DGRAM_MODULE);


    /* the dgram main_conf context, it's the same in the all dgram contexts */

    ctx->main_conf = ngx_pcalloc(cf->pool,
                                 sizeof(void *) * ngx_dgram_max_module);
    if (ctx->main_conf == NULL) {
        return NGX_CONF_ERROR;
    }


    /*
     * the dgram null srv_conf context, it is used to merge
     * the server{}s' srv_conf's
     */

    ctx->srv_conf = ngx_pcalloc(cf->pool,
                                sizeof(void *) * ngx_dgram_max_module);
    if (ctx->srv_conf == NULL) {
        return NGX_CONF_ERROR;
    }


    /*
     * create the main_conf's and the null srv_conf's of the all dgram modules
     */

    for (m = 0; cf->cycle->modules[m]; m++) {
        if (cf->cycle->modules[m]->type != NGX_DGRAM_MODULE) {
            continue;
        }

        module = cf->cycle->modules[m]->ctx;
        mi = cf->cycle->modules[m]->ctx_index;

        if (module->create_main_conf) {
            ctx->main_conf[mi] = module->create_main_conf(cf);
            if (ctx->main_conf[mi] == NULL) {
                return NGX_CONF_ERROR;
            }
        }

        if (module->create_srv_conf) {
            ctx->srv_conf[mi] = module->create_srv_conf(cf);
            if (ctx->srv_conf[mi] == NULL) {
                return NGX_CONF_ERROR;
            }
        }
    }


    pcf = *cf;
    cf->ctx = ctx;

    for (m = 0; cf->cycle->modules[m]; m++) {
        if (cf->cycle->modules[m]->type != NGX_DGRAM_MODULE) {
            continue;
        }

        module = cf->cycle->modules[m]->ctx;

        if (module->preconfiguration) {
            if (module->preconfiguration(cf) != NGX_OK) {
                return NGX_CONF_ERROR;
            }
        }
    }


    /* parse inside the dgram{} block */

    cf->module_type = NGX_DGRAM_MODULE;
    cf->cmd_type = NGX_DGRAM_MAIN_CONF;
    rv = ngx_conf_parse(cf, NULL);

    if (rv != NGX_CONF_OK) {
        *cf = pcf;
        return rv;
    }


    /* init dgram{} main_conf's, merge the server{}s' srv_conf's */

    cmcf = ctx->main_conf[ngx_dgram_core_module.ctx_index];
    cscfp = cmcf->servers.elts;

    for (m = 0; cf->cycle->modules[m]; m++) {
        if (cf->cycle->modules[m]->type != NGX_DGRAM_MODULE) {
            continue;
        }

        module = cf->cycle->modules[m]->ctx;
        mi = cf->cycle->modules[m]->ctx_index;

        /* init dgram{} main_conf's */

        cf->ctx = ctx;

        if (module->init_main_conf) {
            rv = module->init_main_conf(cf, ctx->main_conf[mi]);
            if (rv != NGX_CONF_OK) {
                *cf = pcf;
                return rv;
            }
        }

        for (s = 0; s < cmcf->servers.nelts; s++) {

            /* merge the server{}s' srv_conf's */

            cf->ctx = cscfp[s]->ctx;

            if (module->merge_srv_conf) {
                rv = module->merge_srv_conf(cf,
                                            ctx->srv_conf[mi],
                                            cscfp[s]->ctx->srv_conf[mi]);
                if (rv != NGX_CONF_OK) {
                    *cf = pcf;
                    return rv;
                }
            }
        }
    }

    for (m = 0; cf->cycle->modules[m]; m++) {
        if (cf->cycle->modules[m]->type != NGX_DGRAM_MODULE) {
            continue;
        }

        module = cf->cycle->modules[m]->ctx;

        if (module->postconfiguration) {
            if (module->postconfiguration(cf) != NGX_OK) {
                return NGX_CONF_ERROR;
            }
        }
    }

    if (ngx_dgram_variables_init_vars(cf) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    if (ngx_dgram_create_listening(cf) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    *cf = pcf;

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_dgram_create_listening(ngx_conf_t *cf)
{
    ngx_uint_t                    s, n;
    ngx_cycle_t                  *cycle;
    ngx_listening_t              *ls;
    ngx_dgram_listen_t           *opt, *opts;
    ngx_dgram_core_srv_conf_t   **cscfp, *cscf;
    ngx_dgram_core_main_conf_t   *cmcf;

    cmcf = ngx_dgram_conf_get_module_main_conf(cf, ngx_dgram_core_module);

    cscfp = cmcf->servers.elts;
    for (s = 0; s < cmcf->servers.nelts; s++) {

        cscf = ngx_dgram_conf_get_module_srv_conf(cscfp[s],
            ngx_dgram_core_module);

        opts = cscf->listen->elts;
        for (n = 0; n < cscf->listen->nelts; n++) {

            opt = &opts[n];

            /* trick ngx_create_listening to add the socket to cmcf */

            cycle = cf->cycle;
            cf->cycle = (void *)((u_char *) &cmcf->listening -
                offsetof(ngx_cycle_t, listening));

            ls = ngx_create_listening(cf, opt->sockaddr, opt->socklen);

            cf->cycle = cycle;

            if (ls == NULL) {
                return NGX_ERROR;
            }

            /*
             * set by ngx_memzero():
             *
             *      ls->handler = NULL;
             *      ls->log.handler = NULL;
             */

            ls->addr_ntop = 1;
            ls->pool_size = 256;
            ls->type = opt->type;

            ls->logp = cscf->error_log;
            ls->log.data = &ls->addr_text;

            ls->backlog = opt->backlog;
            ls->rcvbuf = opt->rcvbuf;
            ls->sndbuf = opt->sndbuf;

            ls->wildcard = opt->wildcard;

            ls->keepalive = opt->so_keepalive;
#if (NGX_HAVE_KEEPALIVE_TUNABLE)
            ls->keepidle = opt->tcp_keepidle;
            ls->keepintvl = opt->tcp_keepintvl;
            ls->keepcnt = opt->tcp_keepcnt;
#endif

#if (NGX_HAVE_INET6)
            ls->ipv6only = opt->ipv6only;
#endif

#if (NGX_HAVE_REUSEPORT)
            ls->reuseport = opt->reuseport;
#endif

            ls->servers = opt->ctx;
        }
    }

    return NGX_OK;
}


static char *
ngx_dgram_init_listening(ngx_cycle_t *cycle, void *conf)
{
    ngx_int_t                    rc;
    ngx_uint_t                   i, n;
    ngx_uint_t                   orig_flags;
    ngx_cycle_t                 *old_cycle;
    ngx_cycle_t                  dummy_cycle;
    ngx_array_t                  old_listening;
    ngx_listening_t             *ls, *nls;
    ngx_dgram_core_main_conf_t  *old_cmcf;
    ngx_dgram_core_main_conf_t  *cmcf;

    if (ngx_process == NGX_PROCESS_SIGNALLER) {
        return NGX_CONF_OK;
    }

    old_cycle = cycle->old_cycle;

    if (old_cycle->conf_ctx != NULL) {
        old_cmcf = ngx_dgram_cycle_get_module_main_conf(old_cycle,
            ngx_dgram_core_module);
        old_listening = old_cmcf->listening;

    } else {
        old_listening.nelts = 0;
        old_listening.elts = NULL;
    }

    cmcf = ngx_dgram_cycle_get_module_main_conf(cycle, ngx_dgram_core_module);

    /* Note: code copied from ngx_init_cycle */

    /* handle the listening sockets */

    if (old_listening.nelts) {
        ls = old_listening.elts;
        for (i = 0; i < old_listening.nelts; i++) {
            ls[i].remain = 0;
        }

        nls = cmcf->listening.elts;
        for (n = 0; n < cmcf->listening.nelts; n++) {

            for (i = 0; i < old_listening.nelts; i++) {
                if (ls[i].ignore) {
                    continue;
                }

                if (ls[i].remain) {
                    continue;
                }

                if (ls[i].type != nls[n].type) {
                    continue;
                }

                if (ngx_cmp_sockaddr(nls[n].sockaddr, nls[n].socklen,
                    ls[i].sockaddr, ls[i].socklen, 1)
                    == NGX_OK)
                {
                    nls[n].fd = ls[i].fd;
                    nls[n].previous = &ls[i];
                    ls[i].remain = 1;

                    if (ls[i].backlog != nls[n].backlog) {
                        nls[n].listen = 1;
                    }

#if (NGX_HAVE_DEFERRED_ACCEPT && defined SO_ACCEPTFILTER)

                    /*
                     * FreeBSD, except the most recent versions,
                     * could not remove accept filter
                     */
                    nls[n].deferred_accept = ls[i].deferred_accept;

                    if (ls[i].accept_filter && nls[n].accept_filter) {
                        if (ngx_strcmp(ls[i].accept_filter,
                            nls[n].accept_filter)
                            != 0)
                        {
                            nls[n].delete_deferred = 1;
                            nls[n].add_deferred = 1;
                        }

                    } else if (ls[i].accept_filter) {
                        nls[n].delete_deferred = 1;

                    } else if (nls[n].accept_filter) {
                        nls[n].add_deferred = 1;
                    }
#endif

#if (NGX_HAVE_DEFERRED_ACCEPT && defined TCP_DEFER_ACCEPT)

                    if (ls[i].deferred_accept && !nls[n].deferred_accept) {
                        nls[n].delete_deferred = 1;

                    } else if (ls[i].deferred_accept != nls[n].deferred_accept)
                    {
                        nls[n].add_deferred = 1;
                    }
#endif

#if (NGX_HAVE_REUSEPORT)
                    if (nls[n].reuseport && !ls[i].reuseport) {
                        nls[n].add_reuseport = 1;
                    }
#endif

                    break;
                }
            }

            if (nls[n].fd == NGX_INVALID_SOCKET) {
                nls[n].open = 1;
#if (NGX_HAVE_DEFERRED_ACCEPT && defined SO_ACCEPTFILTER)
                if (nls[n].accept_filter) {
                    nls[n].add_deferred = 1;
                }
#endif
#if (NGX_HAVE_DEFERRED_ACCEPT && defined TCP_DEFER_ACCEPT)
                if (nls[n].deferred_accept) {
                    nls[n].add_deferred = 1;
                }
#endif
            }
        }

    } else {
        ls = cmcf->listening.elts;
        for (i = 0; i < cmcf->listening.nelts; i++) {
            ls[i].open = 1;
#if (NGX_HAVE_DEFERRED_ACCEPT && defined SO_ACCEPTFILTER)
            if (ls[i].accept_filter) {
                ls[i].add_deferred = 1;
            }
#endif
#if (NGX_HAVE_DEFERRED_ACCEPT && defined TCP_DEFER_ACCEPT)
            if (ls[i].deferred_accept) {
                ls[i].add_deferred = 1;
            }
#endif
        }
    }

    /* trick ngx_open_listening_sockets to process cmcf sockets */

    dummy_cycle.log = cycle->log;
    dummy_cycle.listening = cmcf->listening;

    /* enable iocp flag to avoid setting the socket as nonblocking */

    orig_flags = ngx_event_flags;
    ngx_event_flags |= NGX_USE_IOCP_EVENT;

    rc = ngx_open_listening_sockets(&dummy_cycle);

    ngx_event_flags = orig_flags;

    if (rc != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    if (!ngx_test_config) {
        ngx_configure_listening_sockets(&dummy_cycle);
    }

    /* close the unnecessary listening sockets */

    ls = old_listening.elts;
    for (i = 0; i < old_listening.nelts; i++) {

        if (ls[i].remain || ls[i].fd == NGX_INVALID_SOCKET) {
            continue;
        }

        if (ngx_close_socket(ls[i].fd) == -1) {
            ngx_log_error(NGX_LOG_EMERG, cycle->log, ngx_socket_errno,
                ngx_close_socket_n " listening socket on %V failed",
                &ls[i].addr_text);
        }
    }

    return NGX_CONF_OK;
}


static u_char *
ngx_dgram_log_error(ngx_log_t *log, u_char *buf, size_t len)
{
    u_char               *p;
    ngx_dgram_session_t  *s;

    if (log->action) {
        p = ngx_snprintf(buf, len, " while %s", log->action);
        len -= p - buf;
        buf = p;
    }

    s = log->data;

    p = ngx_snprintf(buf, len, ", %sserver: %V",
        s->connection->type == SOCK_DGRAM ? "udp " : "",
        &s->connection->listening->addr_text);
    len -= p - buf;
    buf = p;

    if (s->log_handler) {
        p = s->log_handler(log, buf, len);
    }

    return p;
}


static ngx_dgram_session_t *
ngx_dgram_init_session(ngx_listening_t *ls)
{
    ngx_log_t                   *log;
    ngx_time_t                  *tp;
    ngx_pool_t                  *pool;
    ngx_event_t                 *rev, *wev;
    ngx_connection_t            *c;
    ngx_dgram_session_t         *s;
    ngx_dgram_conf_ctx_t        *ctx;
    ngx_dgram_core_srv_conf_t   *cscf;
    ngx_dgram_core_main_conf_t  *cmcf;

    ctx = ls->servers;

    cscf = ngx_dgram_get_module_srv_conf(ctx, ngx_dgram_core_module);

    pool = ngx_create_pool(1024, cscf->error_log);
    if (pool == NULL) {
        return NULL;
    }

    s = ngx_pcalloc(pool, sizeof(ngx_dgram_session_t));
    if (s == NULL) {
        goto failed;
    }

    c = ngx_pcalloc(pool, sizeof(ngx_connection_t));
    if (c == NULL) {
        goto failed;
    }

    rev = ngx_pcalloc(pool, sizeof(ngx_event_t));
    if (rev == NULL) {
        goto failed;
    }

    wev = ngx_pcalloc(pool, sizeof(ngx_event_t));
    if (wev == NULL) {
        goto failed;
    }

    log = ngx_palloc(pool, sizeof(ngx_log_t));
    if (log == NULL) {
        goto failed;
    }

    rev->index = NGX_INVALID_INDEX;
    wev->index = NGX_INVALID_INDEX;

    rev->data = c;
    wev->data = c;

    wev->write = 1;

    c->read = rev;
    c->write = wev;

    c->type = ls->type;
    c->pool = pool;

    *log = ls->log;

    c->log = log;
    c->pool->log = log;

    c->listening = ls;
    c->local_sockaddr = ls->sockaddr;
    c->local_socklen = ls->socklen;

    /* event module may have not yet set ngx_io */

    c->recv = c->type == SOCK_STREAM ? ngx_os_io.recv : ngx_os_io.udp_recv;
    c->send = ngx_os_io.send;

    c->number = ngx_atomic_fetch_add(ngx_connection_counter, 1);

    s->signature = NGX_DGRAM_MODULE;
    s->main_conf = ctx->main_conf;
    s->srv_conf = ctx->srv_conf;

    s->connection = c;
    c->data = s;

    cscf = ngx_dgram_get_module_srv_conf(s, ngx_dgram_core_module);

    ngx_set_connection_log(c, cscf->error_log);

    c->log->connection = c->number;
    c->log->handler = ngx_dgram_log_error;
    c->log->data = s;
    c->log->action = "initializing session";
    c->log_error = NGX_ERROR_INFO;

    s->ctx = ngx_pcalloc(pool, sizeof(void *) * ngx_dgram_max_module);
    if (s->ctx == NULL) {
        goto failed;
    }

    cmcf = ngx_dgram_get_module_main_conf(s, ngx_dgram_core_module);

    s->variables = ngx_pcalloc(pool,
                               cmcf->variables.nelts
                               * sizeof(ngx_dgram_variable_value_t));

    if (s->variables == NULL) {
        goto failed;
    }

    tp = ngx_timeofday();
    s->start_sec = tp->sec;
    s->start_msec = tp->msec;

    return s;

failed:

    ngx_destroy_pool(pool);

    return NULL;
}


static void *
ngx_dgram_thread_cycle(void *data)
{
    socklen_t                   socklen;
    ngx_err_t                   err;
    ngx_msec_t                  timeout;
    ngx_socket_t                fd;
    struct timeval              tv;
    ngx_sockaddr_t              sa;
    ngx_listening_t            *ls;
    ngx_connection_t           *c;
    ngx_dgram_session_t        *s;
    ngx_dgram_core_srv_conf_t  *cscf;

    ls = data;

    (void) ngx_atomic_fetch_add(ngx_dgram_threads, 1);

    ngx_log_debug1(NGX_LOG_DEBUG_CORE, &ls->log, 0,
        "ngx_dgram_thread_cycle: thread \"%V\" started", &ls->addr_text);

    fd = NGX_INVALID_SOCKET;

    s = ngx_dgram_init_session(ls);
    if (s == NULL) {
        goto done;
    }

    cscf = ngx_dgram_get_module_srv_conf(s, ngx_dgram_core_module);

    timeout = ls->type == SOCK_STREAM ? cscf->accept_timeout :
        cscf->recv_timeout;

    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (timeout % 1000) * 1000;

    if (setsockopt(ls->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        ngx_log_error(NGX_LOG_ERR, s->connection->log, ngx_socket_errno,
            "setsockopt(SO_RCVTIMEO) failed");
    }

    if (ls->type == SOCK_DGRAM) {
        s->connection->fd = ls->fd;
        cscf->handler(s);
        goto done;
    }

    tv.tv_sec = cscf->recv_timeout / 1000;
    tv.tv_usec = (cscf->recv_timeout % 1000) * 1000;

    c = s->connection;

    while (!ngx_terminate && !ngx_exiting) {

        fd = accept(ls->fd, &sa.sockaddr, &socklen);
        if (fd == NGX_INVALID_SOCKET) {
            err = ngx_socket_errno;

            if (err == NGX_EAGAIN || err == NGX_EINTR) {
                continue;
            }

            ngx_log_error(NGX_LOG_ALERT, c->log, err,
                "accept() failed");
            break;
        }

        if (socklen > c->socklen) {
            c->sockaddr = ngx_palloc(c->pool, socklen);
            if (c->sockaddr == NULL) {
                break;
            }
        }

        ngx_memcpy(c->sockaddr, &sa, socklen);
        c->socklen = socklen;

#if (NGX_DEBUG)
        {
            ngx_str_t  addr;
            u_char     text[NGX_SOCKADDR_STRLEN];

            if (c->log->log_level & NGX_LOG_DEBUG_EVENT) {
                addr.data = text;
                addr.len = ngx_sock_ntop(c->sockaddr, c->socklen, text,
                    NGX_SOCKADDR_STRLEN, 1);

                ngx_log_debug2(NGX_LOG_DEBUG_EVENT, c->log, 0,
                    "ngx_dgram_thread_cycle: accept: %V fd:%d",
                    &addr, c->fd);
            }

        }
#endif

        s->connection->fd = fd;

        if (cscf->tcp_nodelay
            && ngx_tcp_nodelay(s->connection) != NGX_OK)
        {
            break;
        }

        if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
            ngx_log_error(NGX_LOG_ERR, s->connection->log, ngx_socket_errno,
                "setsockopt(SO_RCVTIMEO) failed");
        }

        cscf->handler(s);

        if (ngx_close_socket(fd) == -1) {
            ngx_log_error(NGX_LOG_ALERT, s->connection->log, ngx_socket_errno,
                ngx_close_socket_n " failed");
        }

        fd = NGX_INVALID_SOCKET;
    }

done:

    if (fd != NGX_INVALID_SOCKET) {
        if (ngx_close_socket(fd) == -1) {
            ngx_log_error(NGX_LOG_ALERT, s->connection->log, ngx_socket_errno,
                ngx_close_socket_n " failed");
        }
    }

    if (s != NULL) {
        ngx_destroy_pool(s->connection->pool);
    }

    ngx_log_debug1(NGX_LOG_DEBUG_CORE, &ls->log, 0,
        "ngx_dgram_thread_cycle: thread \"%V\" done", &ls->addr_text);

    (void) ngx_atomic_fetch_add(ngx_dgram_threads, -1);

    return NULL;
}


static ngx_int_t
ngx_dgram_thread_init(ngx_log_t *log, ngx_listening_t *ls)
{
    int             err;
    pthread_t       tid;
    pthread_attr_t  attr;

    err = pthread_attr_init(&attr);
    if (err) {
        ngx_log_error(NGX_LOG_ALERT, log, err,
            "pthread_attr_init() failed");
        return NGX_ERROR;
    }

    err = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (err) {
        ngx_log_error(NGX_LOG_ALERT, log, err,
            "pthread_attr_setdetachstate() failed");
        (void) pthread_attr_destroy(&attr);
        return NGX_ERROR;
    }

    err = pthread_create(&tid, &attr, ngx_dgram_thread_cycle, ls);
    if (err) {
        ngx_log_error(NGX_LOG_ALERT, log, err,
            "pthread_create() failed");
        (void) pthread_attr_destroy(&attr);
        return NGX_ERROR;
    }

    (void) pthread_attr_destroy(&attr);

    return NGX_OK;
}


static ngx_int_t
ngx_dgram_init_worker(ngx_cycle_t *cycle)
{
    ngx_uint_t                   i;
    ngx_listening_t             *ls;
    ngx_dgram_core_main_conf_t  *cmcf;

    cmcf = ngx_dgram_cycle_get_module_main_conf(cycle, ngx_dgram_core_module);

    ls = cmcf->listening.elts;
    for (i = 0; i < cmcf->listening.nelts; i++) {
        ngx_dgram_thread_init(cycle->log, &ls[i]);
    }

    return NGX_OK;
}

static void
ngx_dgram_exit_worker(ngx_cycle_t *cycle)
{
    ngx_uint_t  i;

    for (i = 0; i < 50; i++) {

        if (*ngx_dgram_threads <= 0) {
            ngx_log_debug0(NGX_LOG_DEBUG_CORE, cycle->log, 0,
                "ngx_dgram_exit_worker: all threads finished");
            return;
        }

        ngx_msleep(100);
    }

    ngx_log_error(NGX_LOG_ALERT, cycle->log, 0,
        "ngx_dgram_exit_worker: timed out waiting for threads to quit");
}
