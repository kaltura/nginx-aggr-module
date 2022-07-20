#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_aggr.h"
#include "ngx_aggr_output.h"


#define NGX_HTTP_AGGR_INPUT_OVERFLOW_PERIOD  (5)


static char *ngx_http_aggr_input(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static void *ngx_http_aggr_input_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_aggr_input_merge_loc_conf(ngx_conf_t *cf, void *parent,
    void *child);


typedef struct {
    size_t                   event_buf_size;
    ngx_aggr_window_conf_t   window_conf;
    ngx_aggr_outputs_arr_t   outputs;
    time_t                   log_overflow_time;
} ngx_http_aggr_input_loc_conf_t;


typedef struct {
    ngx_aggr_window_t       *window;
    ngx_aggr_window_conf_t  *window_conf;
    ngx_buf_t               *buf;
} ngx_http_aggr_ctx_t;


static ngx_command_t  ngx_http_aggr_input_commands[] = {

    { ngx_string("aggr_input"),
      NGX_HTTP_LOC_CONF|NGX_CONF_ANY,
      ngx_http_aggr_input,
      0,
      0,
      NULL },

    { ngx_string("aggr_input_event_buf_size"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_aggr_input_loc_conf_t, event_buf_size),
      NULL },

    { ngx_string("aggr_input_window"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_sec_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_aggr_input_loc_conf_t, window_conf.interval),
      NULL },

    { ngx_string("aggr_input_buf_size"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_aggr_input_loc_conf_t, window_conf.buf_size),
      NULL },

    { ngx_string("aggr_input_max_buffers"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_aggr_input_loc_conf_t, window_conf.max_buffers),
      NULL },

    { ngx_string("aggr_input_delim"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_char_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_aggr_input_loc_conf_t, window_conf.delim),
      NULL },

    { ngx_string("aggr_output_file"),
      NGX_HTTP_LOC_CONF|NGX_CONF_BLOCK|NGX_CONF_1MORE,
      ngx_aggr_output_file,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_aggr_input_loc_conf_t, outputs),
      NULL },

#if (NGX_HAVE_LIBRDKAFKA)
    { ngx_string("aggr_output_kafka"),
      NGX_HTTP_LOC_CONF|NGX_CONF_BLOCK|NGX_CONF_2MORE,
      ngx_aggr_output_kafka,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_aggr_input_loc_conf_t, outputs),
      NULL },
#endif

      ngx_null_command
};


static ngx_http_module_t  ngx_http_aggr_input_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_aggr_input_create_loc_conf,   /* create location configuration */
    ngx_http_aggr_input_merge_loc_conf     /* merge location configuration */
};


ngx_module_t  ngx_http_aggr_input_module = {
    NGX_MODULE_V1,
    &ngx_http_aggr_input_module_ctx,       /* module context */
    ngx_http_aggr_input_commands,          /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static u_char *
ngx_http_aggr_input_chain_rchr(ngx_chain_t **cl, u_char ch)
{
    u_char       *p;
    ngx_buf_t    *b;
    ngx_chain_t  *last, *prev;

    last = NULL;

    for ( ;; ) {

        for (prev = *cl; prev->next != last; prev = prev->next);

        b = prev->buf;
        p = memrchr(b->pos, ch, b->last - b->pos);
        if (p != NULL) {
            *cl = prev;
            return p;
        }

        if (prev == *cl) {
            return NULL;
        }

        last = prev;
    }
}


static ngx_flag_t
ngx_http_aggr_input_append_chain(ngx_buf_t *dst, ngx_chain_t *cl, u_char *p)
{
    size_t      src_left, dst_left;
    ngx_buf_t  *b;

    /* Note: returns zero in case of dest buffer overflow */

    b = cl->buf;

    for ( ;; ) {

        src_left = b->last - p;
        dst_left = dst->end - dst->last;
        if (dst_left <= src_left) {
            if (dst_left <= 0) {
                return 1;
            }

            dst->last = ngx_copy(dst->last, p, dst_left);
            return 0;
        }

        dst->last = ngx_copy(dst->last, p, src_left);

        cl = cl->next;
        if (cl == NULL) {
            return 1;
        }

        b = cl->buf;
        p = b->pos;
    }
}


static ngx_int_t
ngx_http_aggr_input_init_ctx(ngx_http_request_t *r)
{
    ngx_aggr_window_t               *window;
    ngx_http_aggr_ctx_t             *ctx;
    ngx_aggr_bucket_handler_pt       handler;
    ngx_http_aggr_input_loc_conf_t  *ailcf;

    ailcf = ngx_http_get_module_loc_conf(r, ngx_http_aggr_input_module);

    if (ailcf->window_conf.window == NULL) {
        handler = (ngx_aggr_bucket_handler_pt) ngx_aggr_outputs_push;

        window = ngx_aggr_window_create(ngx_cycle->pool,
            &ailcf->window_conf, handler, &ailcf->outputs);
        if (window == NULL) {
            return NGX_ERROR;
        }
    }

    ctx = ngx_pcalloc(r->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->buf = ngx_create_temp_buf(r->pool, ailcf->event_buf_size);
    if (ctx->buf == NULL) {
        return NGX_ERROR;
    }

    ctx->buf->end--;        /* save room for delimiter on flush */

    ctx->window = ailcf->window_conf.window;
    ctx->window_conf = &ailcf->window_conf;

    ngx_http_set_ctx(r, ctx, ngx_http_aggr_input_module);

    return NGX_OK;
}


static ngx_int_t
ngx_http_aggr_input_flush(ngx_http_request_t *r)
{
    ngx_int_t             rc;
    ngx_buf_t            *b;
    ngx_http_aggr_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
        "ngx_http_aggr_input_flush: called");

    ctx = ngx_http_get_module_ctx(r, ngx_http_aggr_input_module);

    b = ctx->buf;
    if (b->pos >= b->last) {
        return NGX_OK;
    }

    *(b->last)++ = ctx->window_conf->delim;

    rc = ngx_aggr_window_write(ctx->window, b->pos, b->last);
    if (rc != NGX_OK) {
        return rc;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_aggr_input_process(ngx_http_request_t *r, ngx_chain_t *out)
{
    u_char                          *p;
    u_char                           delim;
    ngx_int_t                        rc;
    ngx_buf_t                       *b;
    ngx_chain_t                     *cl, *ln;
    ngx_aggr_window_t               *window;
    ngx_http_aggr_ctx_t             *ctx;
    ngx_http_aggr_input_loc_conf_t  *ailcf;

    if (out == NULL) {
        return NGX_OK;
    }

#if (NGX_DEBUG)
    {
    size_t      size;
    ngx_uint_t  count;

    count = 0;
    size = 0;
    for (cl = out; cl; cl = cl->next) {
        count++;
        size += cl->buf->last - cl->buf->pos;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
        "ngx_http_aggr_input_process: count: %ui, size: %uz", count, size);
    }
#endif

    /* Note: appending only complete events to the window, since another
        request may run in parallel to this one. Incomplete events are
        buffered in ctx->buf until the next chunk arrives / request ends */

    ctx = ngx_http_get_module_ctx(r, ngx_http_aggr_input_module);

    window = ctx->window;
    delim = ctx->window_conf->delim;

    cl = out;
    p = ngx_http_aggr_input_chain_rchr(&cl, delim);
    if (p == NULL) {
        p = cl->buf->pos;
        goto append;
    }

    p++;    /* include the delimiter */

    b = ctx->buf;

    rc = ngx_aggr_window_write(window, b->pos, b->last);
    if (rc != NGX_OK) {
        return rc;
    }

    b->last = b->start;

    for (ln = out; ln != cl; ln = ln->next) {
        b = ln->buf;

        rc = ngx_aggr_window_write(window, b->pos, b->last);
        if (rc != NGX_OK) {
            return rc;
        }
    }

    b = cl->buf;

    rc = ngx_aggr_window_write(window, b->pos, p);
    if (rc != NGX_OK) {
        return rc;
    }

append:

    if (!ngx_http_aggr_input_append_chain(ctx->buf, cl, p)) {
        ailcf = ngx_http_get_module_loc_conf(r, ngx_http_aggr_input_module);

        if (ngx_time() >= ailcf->log_overflow_time) {
            ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                "ngx_http_aggr_input_process: event buffer overflow, "
                "consider increasing \"aggr_input_event_buf_size\"");
            ailcf->log_overflow_time = ngx_time()
                + NGX_HTTP_AGGR_INPUT_OVERFLOW_PERIOD;
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_aggr_input_do_process(ngx_http_request_t *r)
{
    ngx_int_t     rc;
    ngx_buf_t    *b;
    ngx_chain_t  *out, *ln;

    out = r->request_body->bufs;
    r->request_body->bufs = NULL;

    for ( ;; ) {

        rc = ngx_http_aggr_input_process(r, out);
        if (rc != NGX_OK && rc != NGX_AGAIN) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        if (!r->reading_body) {
            rc = ngx_http_aggr_input_flush(r);
            if (rc != NGX_OK && rc != NGX_AGAIN) {
                return NGX_HTTP_INTERNAL_SERVER_ERROR;
            }

            return NGX_HTTP_NO_CONTENT;
        }

        while (out) {
            ln = out;
            out = out->next;

            b = ln->buf;
            b->pos = b->last;

            ngx_free_chain(r->pool, ln);
        }

        rc = ngx_http_read_unbuffered_request_body(r);

        if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
            return rc;
        }

        out = r->request_body->bufs;
        r->request_body->bufs = NULL;

        if (out == NULL) {
            return NGX_AGAIN;
        }
    }
}


static void
ngx_http_aggr_input_read_handler(ngx_http_request_t *r)
{
    ngx_int_t  rc;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
        "ngx_http_aggr_input_read_handler: called");

    rc = ngx_http_aggr_input_do_process(r);
    if (rc == NGX_AGAIN) {
        return;
    }

    ngx_http_finalize_request(r, rc);
}


static void
ngx_http_aggr_input_client_body_handler(ngx_http_request_t *r)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
        "ngx_http_aggr_input_client_body_handler: called");
}


static ngx_int_t
ngx_http_aggr_input_handler(ngx_http_request_t *r)
{
    ngx_int_t  rc;

    if (!(r->method & (NGX_HTTP_POST | NGX_HTTP_PUT))) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    if (ngx_http_aggr_input_init_ctx(r) != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    r->request_body_no_buffering = 1;

    rc = ngx_http_read_client_request_body(r,
        ngx_http_aggr_input_client_body_handler);

    if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
        return rc;
    }

    if (r->request_body == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "ngx_http_aggr_input_handler: "
            "%V request body is unavailable", &r->method_name);
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;

    } else {
        rc = ngx_http_aggr_input_do_process(r);
    }

    if (rc != NGX_AGAIN) {
        r->main->count--;   /* ngx_http_read_client_request_body
                                increments the count */
        return rc;
    }

    r->read_event_handler = ngx_http_aggr_input_read_handler;

    return NGX_DONE;
}


static char *
ngx_http_aggr_input(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_int_t                        rv;
    ngx_str_t                        name;
    ngx_str_t                       *value;
    ngx_uint_t                       i;
    ngx_http_core_loc_conf_t        *clcf;
    ngx_http_aggr_input_loc_conf_t  *ailcf;

    ailcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_aggr_input_module);

    value = cf->args->elts;

    for (i = 1; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "name=", 5) == 0) {

            if (ailcf->window_conf.name.data != NULL) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "duplicate name parameter \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            name.data = value[i].data + 5;
            name.len = value[i].len - 5;

            rv = ngx_aggr_add_window(cf, &name, &ailcf->window_conf);

            if (rv == NGX_OK) {
                ailcf->window_conf.name = name;
                continue;
            }

            if (rv == NGX_BUSY) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "conflicting parameter \"%V\"", &name);
            }

            return NGX_CONF_ERROR;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_aggr_input_handler;

    return NGX_CONF_OK;
}


static void *
ngx_http_aggr_input_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_aggr_input_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf == NULL) {
        return NULL;
    }

    if (ngx_aggr_outputs_init(cf, &conf->outputs) != NGX_OK) {
        return NULL;
    }

    ngx_aggr_window_conf_init(&conf->window_conf);
    conf->window_conf.recv_size = 0;

    conf->event_buf_size = NGX_CONF_UNSET_SIZE;

    return conf;
}


static char *
ngx_http_aggr_input_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_core_loc_conf_t        *clcf;
    ngx_http_aggr_input_loc_conf_t  *prev = parent;
    ngx_http_aggr_input_loc_conf_t  *conf = child;

    ngx_aggr_window_conf_merge(&conf->window_conf, &prev->window_conf);

    ngx_conf_merge_size_value(conf->event_buf_size,
                              prev->event_buf_size, 2048);

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);

    conf->outputs.log = clcf->error_log;

    return NGX_CONF_OK;
}
