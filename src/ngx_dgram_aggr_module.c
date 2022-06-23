#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_dgram.h"
#include "ngx_aggr.h"
#include "ngx_aggr_output.h"


static void *ngx_dgram_aggr_create_srv_conf(ngx_conf_t *cf);
static char *ngx_dgram_aggr_merge_srv_conf(ngx_conf_t *cf, void *parent,
    void *child);

static char *ngx_dgram_aggr_input(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static ngx_int_t ngx_dgram_aggr_init_worker(ngx_cycle_t *cycle);


typedef struct {
    ngx_aggr_window_conf_t    window;
    ngx_aggr_outputs_conf_t   outputs;
} ngx_dgram_aggr_srv_conf_t;


static ngx_command_t  ngx_dgram_aggr_commands[] = {

    { ngx_string("aggr_input"),
      NGX_DGRAM_SRV_CONF|NGX_CONF_ANY,
      ngx_dgram_aggr_input,
      NGX_DGRAM_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("aggr_input_window"),
      NGX_DGRAM_MAIN_CONF|NGX_DGRAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_sec_slot,
      NGX_DGRAM_SRV_CONF_OFFSET,
      offsetof(ngx_dgram_aggr_srv_conf_t, window.interval),
      NULL },

    { ngx_string("aggr_input_buf_size"),
      NGX_DGRAM_MAIN_CONF|NGX_DGRAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_DGRAM_SRV_CONF_OFFSET,
      offsetof(ngx_dgram_aggr_srv_conf_t, window.buf_size),
      NULL },

    { ngx_string("aggr_input_max_buffers"),
      NGX_DGRAM_MAIN_CONF|NGX_DGRAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_DGRAM_SRV_CONF_OFFSET,
      offsetof(ngx_dgram_aggr_srv_conf_t, window.max_buffers),
      NULL },

    { ngx_string("aggr_input_recv_size"),
      NGX_DGRAM_MAIN_CONF|NGX_DGRAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_DGRAM_SRV_CONF_OFFSET,
      offsetof(ngx_dgram_aggr_srv_conf_t, window.recv_size),
      NULL },

    { ngx_string("aggr_output_file"),
      NGX_DGRAM_SRV_CONF|NGX_CONF_BLOCK|NGX_CONF_1MORE,
      ngx_aggr_output_file,
      NGX_DGRAM_SRV_CONF_OFFSET,
      offsetof(ngx_dgram_aggr_srv_conf_t, outputs),
      NULL },

#if (NGX_HAVE_LIBRDKAFKA)
    { ngx_string("aggr_output_kafka"),
      NGX_DGRAM_SRV_CONF|NGX_CONF_BLOCK|NGX_CONF_2MORE,
      ngx_aggr_output_kafka,
      NGX_DGRAM_SRV_CONF_OFFSET,
      offsetof(ngx_dgram_aggr_srv_conf_t, outputs),
      NULL },
#endif

      ngx_null_command
};


static ngx_dgram_module_t  ngx_dgram_aggr_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    ngx_dgram_aggr_create_srv_conf,        /* create server configuration */
    ngx_dgram_aggr_merge_srv_conf          /* merge server configuration */
};


ngx_module_t  ngx_dgram_aggr_module = {
    NGX_MODULE_V1,
    &ngx_dgram_aggr_module_ctx,            /* module context */
    ngx_dgram_aggr_commands,               /* module directives */
    NGX_DGRAM_MODULE,                      /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_dgram_aggr_init_worker,            /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static void
ngx_dgram_aggr_input_handler(ngx_dgram_session_t *s)
{
    u_char                     *recv_buf;
    ssize_t                     n;
    ngx_int_t                   rc;
    ngx_aggr_buf_t             *buf;
    ngx_connection_t           *c;
    ngx_aggr_window_t          *window;
    ngx_dgram_aggr_srv_conf_t  *ascf;

    window = ngx_dgram_get_module_ctx(s, ngx_dgram_aggr_module);
    if (window == NULL) {
        ascf = ngx_dgram_get_module_srv_conf(s, ngx_dgram_aggr_module);

        window = ngx_aggr_window_create(s->connection->pool, &ascf->window,
            (ngx_aggr_bucket_handler_pt) ngx_aggr_outputs_push,
            &ascf->outputs);
        if (window == NULL) {
            return;
        }

        ngx_dgram_set_ctx(s, window, ngx_dgram_aggr_module);
    }

    c = s->connection;

    while (!ngx_terminate && !ngx_exiting) {

        ngx_time_update();

        rc = ngx_aggr_window_get_recv_buf(window, &buf);
        if (rc != NGX_OK) {
            if (rc == NGX_AGAIN) {
                ngx_msleep(500);
                continue;
            }

            return;
        }


        recv_buf = buf->last;

        n = ngx_dgram_recv(c, recv_buf, buf->end - buf->last - 1);
        if (n <= 0) {
            switch (n) {

            case 0:
                return;

            case NGX_AGAIN:
                continue;

            default:        /* NGX_ERROR */
                return;
            }
        }

        s->received += n;

        if (recv_buf[n - 1] == '\0') {
            buf->last += n;
            n--;

        } else {
            buf->last += n + 1;
            recv_buf[n] = '\0';
        }
    }
}


static char *
ngx_dgram_aggr_input(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_int_t                   rv;
    ngx_str_t                  *value;
    ngx_uint_t                  i;
    ngx_dgram_core_srv_conf_t  *cscf;
    ngx_dgram_aggr_srv_conf_t  *ascf;

    cscf = ngx_dgram_conf_get_module_srv_conf(cf, ngx_dgram_core_module);

    if (cscf->handler != NULL) {
        return "is duplicate";
    }

    cscf->handler = ngx_dgram_aggr_input_handler;

    ascf = ngx_dgram_conf_get_module_srv_conf(cf, ngx_dgram_aggr_module);

    value = cf->args->elts;

    for (i = 1; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "name=", 5) == 0) {
            ascf->window.name.data = value[i].data + 5;
            ascf->window.name.len = value[i].len - 5;

            rv = ngx_aggr_add_window(cf, &ascf->window.name, &ascf->window);

            if (rv == NGX_OK) {
                continue;
            }

            if (rv == NGX_BUSY) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "conflicting parameter \"%V\"", &ascf->window.name);
            }

            return NGX_CONF_ERROR;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static void *
ngx_dgram_aggr_create_srv_conf(ngx_conf_t *cf)
{
    ngx_dgram_aggr_srv_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf == NULL) {
        return NULL;
    }

    if (ngx_aggr_outputs_init(cf, &conf->outputs) != NGX_OK) {
        return NULL;
    }

    ngx_aggr_window_conf_init(&conf->window);

    return conf;
}

static char *
ngx_dgram_aggr_merge_srv_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_dgram_aggr_srv_conf_t  *prev = parent;
    ngx_dgram_aggr_srv_conf_t  *conf = child;

    ngx_aggr_window_conf_merge(&conf->window, &prev->window);

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_dgram_aggr_init_worker(ngx_cycle_t *cycle)
{
    ngx_uint_t                    i;
    ngx_dgram_core_srv_conf_t   **cscfp, *cscf;
    ngx_dgram_aggr_srv_conf_t    *ascf;
    ngx_dgram_core_main_conf_t   *cmcf;

    cmcf = ngx_dgram_cycle_get_module_main_conf(cycle, ngx_dgram_core_module);
    if (cmcf == NULL) {
        return NGX_OK;
    }

    cscfp = cmcf->servers.elts;
    for (i = 0; i < cmcf->servers.nelts; i++) {

        cscf = cscfp[i];
        ascf = ngx_dgram_conf_get_module_srv_conf(cscf, ngx_dgram_aggr_module);

        if (ngx_aggr_outputs_start(cscf->error_log, &ascf->outputs)
            != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}
