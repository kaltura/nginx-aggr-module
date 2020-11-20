#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_dgram.h"


static ngx_int_t ngx_dgram_core_preconfiguration(ngx_conf_t *cf);
static void *ngx_dgram_core_create_main_conf(ngx_conf_t *cf);
static char *ngx_dgram_core_init_main_conf(ngx_conf_t *cf, void *conf);
static void *ngx_dgram_core_create_srv_conf(ngx_conf_t *cf);
static char *ngx_dgram_core_merge_srv_conf(ngx_conf_t *cf, void *parent,
    void *child);
static char *ngx_dgram_core_error_log(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_dgram_core_server(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_dgram_core_listen(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_command_t  ngx_dgram_core_commands[] = {

    { ngx_string("variables_hash_max_size"),
      NGX_DGRAM_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_DGRAM_MAIN_CONF_OFFSET,
      offsetof(ngx_dgram_core_main_conf_t, variables_hash_max_size),
      NULL },

    { ngx_string("variables_hash_bucket_size"),
      NGX_DGRAM_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_DGRAM_MAIN_CONF_OFFSET,
      offsetof(ngx_dgram_core_main_conf_t, variables_hash_bucket_size),
      NULL },

    { ngx_string("server"),
      NGX_DGRAM_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_dgram_core_server,
      0,
      0,
      NULL },

    { ngx_string("listen"),
      NGX_DGRAM_SRV_CONF|NGX_CONF_1MORE,
      ngx_dgram_core_listen,
      NGX_DGRAM_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("error_log"),
      NGX_DGRAM_MAIN_CONF|NGX_DGRAM_SRV_CONF|NGX_CONF_1MORE,
      ngx_dgram_core_error_log,
      NGX_DGRAM_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("tcp_nodelay"),
      NGX_DGRAM_MAIN_CONF|NGX_DGRAM_SRV_CONF|NGX_CONF_FLAG,
      ngx_conf_set_flag_slot,
      NGX_DGRAM_SRV_CONF_OFFSET,
      offsetof(ngx_dgram_core_srv_conf_t, tcp_nodelay),
      NULL },

      ngx_null_command
};


static ngx_dgram_module_t  ngx_dgram_core_module_ctx = {
    ngx_dgram_core_preconfiguration,       /* preconfiguration */
    NULL,                                  /* postconfiguration */

    ngx_dgram_core_create_main_conf,       /* create main configuration */
    ngx_dgram_core_init_main_conf,         /* init main configuration */

    ngx_dgram_core_create_srv_conf,        /* create server configuration */
    ngx_dgram_core_merge_srv_conf          /* merge server configuration */
};


ngx_module_t  ngx_dgram_core_module = {
    NGX_MODULE_V1,
    &ngx_dgram_core_module_ctx,            /* module context */
    ngx_dgram_core_commands,               /* module directives */
    NGX_DGRAM_MODULE,                      /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_dgram_core_preconfiguration(ngx_conf_t *cf)
{
    return ngx_dgram_variables_add_core_vars(cf);
}


static void *
ngx_dgram_core_create_main_conf(ngx_conf_t *cf)
{
    ngx_dgram_core_main_conf_t  *cmcf;

    cmcf = ngx_pcalloc(cf->pool, sizeof(ngx_dgram_core_main_conf_t));
    if (cmcf == NULL) {
        return NULL;
    }

    if (ngx_array_init(&cmcf->servers, cf->pool, 4,
                       sizeof(ngx_dgram_core_srv_conf_t *))
        != NGX_OK)
    {
        return NULL;
    }

    if (ngx_array_init(&cmcf->listening, cf->pool, 10, sizeof(ngx_listening_t))
        != NGX_OK)
    {
        return NULL;
    }

    cmcf->variables_hash_max_size = NGX_CONF_UNSET_UINT;
    cmcf->variables_hash_bucket_size = NGX_CONF_UNSET_UINT;

    return cmcf;
}


static char *
ngx_dgram_core_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_dgram_core_main_conf_t  *cmcf = conf;

    ngx_conf_init_uint_value(cmcf->variables_hash_max_size, 1024);
    ngx_conf_init_uint_value(cmcf->variables_hash_bucket_size, 64);

    cmcf->variables_hash_bucket_size =
               ngx_align(cmcf->variables_hash_bucket_size, ngx_cacheline_size);

    if (cmcf->ncaptures) {
        cmcf->ncaptures = (cmcf->ncaptures + 1) * 3;
    }

    return NGX_CONF_OK;
}


static void *
ngx_dgram_core_create_srv_conf(ngx_conf_t *cf)
{
    ngx_dgram_core_srv_conf_t  *cscf;

    cscf = ngx_pcalloc(cf->pool, sizeof(ngx_dgram_core_srv_conf_t));
    if (cscf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc():
     *
     *     cscf->handler = NULL;
     *     cscf->error_log = NULL;
     *     cscf->listen = NULL;
     */

    cscf->file_name = cf->conf_file->file.name.data;
    cscf->line = cf->conf_file->line;
    cscf->tcp_nodelay = NGX_CONF_UNSET;
    cscf->accept_timeout = NGX_CONF_UNSET_MSEC;
    cscf->recv_timeout = NGX_CONF_UNSET_MSEC;

    return cscf;
}


static char *
ngx_dgram_core_merge_srv_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_dgram_core_srv_conf_t  *prev = parent;
    ngx_dgram_core_srv_conf_t  *conf = child;

    if (conf->handler == NULL) {
        ngx_log_error(NGX_LOG_EMERG, cf->log, 0,
                      "no handler for server in %s:%ui",
                      conf->file_name, conf->line);
        return NGX_CONF_ERROR;
    }

    if (conf->error_log == NULL) {
        if (prev->error_log) {
            conf->error_log = prev->error_log;
        } else {
            conf->error_log = &cf->cycle->new_log;
        }
    }

    ngx_conf_merge_value(conf->tcp_nodelay, prev->tcp_nodelay, 1);

    ngx_conf_merge_msec_value(conf->accept_timeout,
                              prev->accept_timeout, 1000);

    ngx_conf_merge_msec_value(conf->recv_timeout,
                              prev->recv_timeout, 1000);

    return NGX_CONF_OK;
}


static char *
ngx_dgram_core_error_log(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_dgram_core_srv_conf_t  *cscf = conf;

    return ngx_log_set_log(cf, &cscf->error_log);
}


static char *
ngx_dgram_core_server(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char                        *rv;
    void                        *mconf;
    ngx_uint_t                   m;
    ngx_conf_t                   pcf;
    ngx_dgram_module_t          *module;
    ngx_dgram_conf_ctx_t        *ctx, *dgram_ctx;
    ngx_dgram_core_srv_conf_t   *cscf, **cscfp;
    ngx_dgram_core_main_conf_t  *cmcf;

    ctx = ngx_pcalloc(cf->pool, sizeof(ngx_dgram_conf_ctx_t));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    dgram_ctx = cf->ctx;
    ctx->main_conf = dgram_ctx->main_conf;

    /* the server{}'s srv_conf */

    ctx->srv_conf = ngx_pcalloc(cf->pool,
                                sizeof(void *) * ngx_dgram_max_module);
    if (ctx->srv_conf == NULL) {
        return NGX_CONF_ERROR;
    }

    for (m = 0; cf->cycle->modules[m]; m++) {
        if (cf->cycle->modules[m]->type != NGX_DGRAM_MODULE) {
            continue;
        }

        module = cf->cycle->modules[m]->ctx;

        if (module->create_srv_conf) {
            mconf = module->create_srv_conf(cf);
            if (mconf == NULL) {
                return NGX_CONF_ERROR;
            }

            ctx->srv_conf[cf->cycle->modules[m]->ctx_index] = mconf;
        }
    }

    /* the server configuration context */

    cscf = ctx->srv_conf[ngx_dgram_core_module.ctx_index];
    cscf->ctx = ctx;

    cmcf = ctx->main_conf[ngx_dgram_core_module.ctx_index];

    cscfp = ngx_array_push(&cmcf->servers);
    if (cscfp == NULL) {
        return NGX_CONF_ERROR;
    }

    *cscfp = cscf;


    /* parse inside server{} */

    pcf = *cf;
    cf->ctx = ctx;
    cf->cmd_type = NGX_DGRAM_SRV_CONF;

    rv = ngx_conf_parse(cf, NULL);

    *cf = pcf;

    if (rv == NGX_CONF_OK && cscf->listen == NULL) {
        ngx_log_error(NGX_LOG_EMERG, cf->log, 0,
                      "no \"listen\" is defined for server in %s:%ui",
                      cscf->file_name, cscf->line);
        return NGX_CONF_ERROR;
    }

    return rv;
}


static char *
ngx_dgram_core_listen(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_dgram_core_srv_conf_t    *cscf = conf;

    ngx_str_t                    *value, size;
    ngx_url_t                     u;
    ngx_uint_t                    i, n, backlog;
    ngx_dgram_listen_t           *ls, *als;

    value = cf->args->elts;

    ngx_memzero(&u, sizeof(ngx_url_t));

    u.url = value[1];
    u.listen = 1;

    if (ngx_parse_url(cf->pool, &u) != NGX_OK) {
        if (u.err) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "%s in \"%V\" of the \"listen\" directive",
                               u.err, &u.url);
        }

        return NGX_CONF_ERROR;
    }

    if (cscf->listen == NULL) {
        cscf->listen = ngx_array_create(cf->pool, 4,
                                        sizeof(ngx_dgram_listen_t));
        if (cscf->listen == NULL) {
            return NGX_CONF_ERROR;
        }
    }

    ls = ngx_array_push_n(cscf->listen, u.naddrs);
    if (ls == NULL) {
        return NGX_CONF_ERROR;
    }

    ngx_memzero(ls, sizeof(ngx_dgram_listen_t));

    ls->backlog = NGX_LISTEN_BACKLOG;
    ls->rcvbuf = -1;
    ls->sndbuf = -1;
    ls->type = SOCK_DGRAM;
    ls->ctx = cf->ctx;

#if (NGX_HAVE_INET6)
    ls->ipv6only = 1;
#endif

    backlog = 0;

    for (i = 2; i < cf->args->nelts; i++) {

        if (ngx_strcmp(value[i].data, "tcp") == 0) {
            ls->type = SOCK_STREAM;
            continue;
        }

        if (ngx_strcmp(value[i].data, "bind") == 0) {
            ls->bind = 1;
            continue;
        }

        if (ngx_strncmp(value[i].data, "backlog=", 8) == 0) {
            ls->backlog = ngx_atoi(value[i].data + 8, value[i].len - 8);
            ls->bind = 1;

            if (ls->backlog == NGX_ERROR || ls->backlog == 0) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid backlog \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            backlog = 1;

            continue;
        }

        if (ngx_strncmp(value[i].data, "rcvbuf=", 7) == 0) {
            size.len = value[i].len - 7;
            size.data = value[i].data + 7;

            ls->rcvbuf = ngx_parse_size(&size);
            ls->bind = 1;

            if (ls->rcvbuf == NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid rcvbuf \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "sndbuf=", 7) == 0) {
            size.len = value[i].len - 7;
            size.data = value[i].data + 7;

            ls->sndbuf = ngx_parse_size(&size);
            ls->bind = 1;

            if (ls->sndbuf == NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid sndbuf \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "ipv6only=o", 10) == 0) {
#if (NGX_HAVE_INET6 && defined IPV6_V6ONLY)
            if (ngx_strcmp(&value[i].data[10], "n") == 0) {
                ls->ipv6only = 1;

            } else if (ngx_strcmp(&value[i].data[10], "ff") == 0) {
                ls->ipv6only = 0;

            } else {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid ipv6only flags \"%s\"",
                                   &value[i].data[9]);
                return NGX_CONF_ERROR;
            }

            ls->bind = 1;
            continue;
#else
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "bind ipv6only is not supported "
                               "on this platform");
            return NGX_CONF_ERROR;
#endif
        }

        if (ngx_strcmp(value[i].data, "reuseport") == 0) {
#if (NGX_HAVE_REUSEPORT)
            ls->reuseport = 1;
            ls->bind = 1;
#else
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "reuseport is not supported "
                               "on this platform, ignored");
#endif
            continue;
        }

        if (ngx_strncmp(value[i].data, "so_keepalive=", 13) == 0) {

            if (ngx_strcmp(&value[i].data[13], "on") == 0) {
                ls->so_keepalive = 1;

            } else if (ngx_strcmp(&value[i].data[13], "off") == 0) {
                ls->so_keepalive = 2;

            } else {

#if (NGX_HAVE_KEEPALIVE_TUNABLE)
                u_char     *p, *end;
                ngx_str_t   s;

                end = value[i].data + value[i].len;
                s.data = value[i].data + 13;

                p = ngx_strlchr(s.data, end, ':');
                if (p == NULL) {
                    p = end;
                }

                if (p > s.data) {
                    s.len = p - s.data;

                    ls->tcp_keepidle = ngx_parse_time(&s, 1);
                    if (ls->tcp_keepidle == (time_t) NGX_ERROR) {
                        goto invalid_so_keepalive;
                    }
                }

                s.data = (p < end) ? (p + 1) : end;

                p = ngx_strlchr(s.data, end, ':');
                if (p == NULL) {
                    p = end;
                }

                if (p > s.data) {
                    s.len = p - s.data;

                    ls->tcp_keepintvl = ngx_parse_time(&s, 1);
                    if (ls->tcp_keepintvl == (time_t) NGX_ERROR) {
                        goto invalid_so_keepalive;
                    }
                }

                s.data = (p < end) ? (p + 1) : end;

                if (s.data < end) {
                    s.len = end - s.data;

                    ls->tcp_keepcnt = ngx_atoi(s.data, s.len);
                    if (ls->tcp_keepcnt == NGX_ERROR) {
                        goto invalid_so_keepalive;
                    }
                }

                if (ls->tcp_keepidle == 0 && ls->tcp_keepintvl == 0
                    && ls->tcp_keepcnt == 0)
                {
                    goto invalid_so_keepalive;
                }

                ls->so_keepalive = 1;

#else

                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "the \"so_keepalive\" parameter accepts "
                                   "only \"on\" or \"off\" on this platform");
                return NGX_CONF_ERROR;

#endif
            }

            ls->bind = 1;

            continue;

#if (NGX_HAVE_KEEPALIVE_TUNABLE)
        invalid_so_keepalive:

            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "invalid so_keepalive value: \"%s\"",
                               &value[i].data[13]);
            return NGX_CONF_ERROR;
#endif
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    if (ls->type == SOCK_DGRAM) {
        if (backlog) {
            return "\"backlog\" parameter is incompatible with \"udp\"";
        }

        if (ls->so_keepalive) {
            return "\"so_keepalive\" parameter is incompatible with \"udp\"";
        }
    }

    als = cscf->listen->elts;

    for (n = 0; n < u.naddrs; n++) {
        ls[n] = ls[0];

        ls[n].sockaddr = u.addrs[n].sockaddr;
        ls[n].socklen = u.addrs[n].socklen;
        ls[n].addr_text = u.addrs[n].name;
        ls[n].wildcard = ngx_inet_wildcard(ls[n].sockaddr);

        for (i = 0; i < cscf->listen->nelts - u.naddrs + n; i++) {
            if (ls[n].type != als[i].type) {
                continue;
            }

            if (ngx_cmp_sockaddr(als[i].sockaddr, als[i].socklen,
                                 ls[n].sockaddr, ls[n].socklen, 1)
                != NGX_OK)
            {
                continue;
            }

            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "duplicate \"%V\" address and port pair",
                               &ls[n].addr_text);
            return NGX_CONF_ERROR;
        }
    }

    return NGX_CONF_OK;
}
