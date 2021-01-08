#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_json_parser.h"
#include "ngx_aggr.h"
#include "ngx_dgram_aggr_module.h"


typedef struct {
    ngx_http_complex_value_t    name;
    ngx_aggr_query_t           *query;
} ngx_http_aggr_query_t;


typedef struct {
    ngx_http_request_t         *r;
    ngx_http_aggr_query_t      *queries;
    ngx_str_t                  *windows;
    ngx_uint_t                  nelts;
    ngx_flag_t                  last_buf;

    ngx_int_t                   rc;
    ngx_chain_t                *out;
    ngx_chain_t               **last;
    off_t                       size;
    ngx_str_t                  *content_type;
} ngx_http_aggr_thread_ctx_t;


typedef struct {
    ngx_http_complex_value_t   *name;
    ngx_aggr_query_t           *query;
    ngx_array_t                 queries;    /* ngx_http_aggr_query_t */
#if (NGX_THREADS)
    ngx_thread_pool_t          *tp;
#endif
} ngx_http_aggr_loc_conf_t;


static void *ngx_http_aggr_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_aggr_merge_loc_conf(ngx_conf_t *cf, void *prev,
    void *conf);

static char *ngx_http_aggr_dynamic(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_aggr_dynamic_block(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_aggr_static(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_aggr_status(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

#if (NGX_THREADS)
static char *ngx_thread_pool_slot(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
#endif


static ngx_command_t  ngx_http_aggr_commands[] = {

    { ngx_string("aggr_dynamic"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_aggr_dynamic,
      0,
      0,
      NULL },

    { ngx_string("aggr_dynamic_block"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_BLOCK|NGX_CONF_TAKE1,
      ngx_http_aggr_dynamic_block,
      0,
      0,
      NULL },

    { ngx_string("aggr_static"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_BLOCK|NGX_CONF_TAKE1,
      ngx_http_aggr_static,
      0,
      0,
      NULL },

    { ngx_string("aggr_status"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_aggr_status,
      0,
      0,
      NULL },

#if (NGX_THREADS)
    { ngx_string("aggr_thread_pool"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_thread_pool_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_aggr_loc_conf_t, tp),
      NULL },
#endif

      ngx_null_command
};


static ngx_http_module_t  ngx_http_aggr_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_aggr_create_loc_conf,         /* create location configuration */
    ngx_http_aggr_merge_loc_conf           /* merge location configuration */
};


ngx_module_t  ngx_http_aggr_module = {
    NGX_MODULE_V1,
    &ngx_http_aggr_module_ctx,             /* module context */
    ngx_http_aggr_commands,                /* module directives */
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


static ngx_str_t  ngx_http_aggr_type_json = ngx_string("application/json");

static ngx_str_t  ngx_http_aggr_type_text = ngx_string("text/plain");


#if (NGX_THREADS)
static char *
ngx_thread_pool_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char  *p = conf;

    ngx_str_t           *value;
    ngx_conf_post_t     *post;
    ngx_thread_pool_t  **tp;


    tp = (ngx_thread_pool_t **) (p + cmd->offset);

    if (*tp != NGX_CONF_UNSET_PTR) {
        return "is duplicate";
    }

    value = cf->args->elts;

    if (cf->args->nelts > 1) {
        *tp = ngx_thread_pool_add(cf, &value[1]);

    } else {
        *tp = ngx_thread_pool_add(cf, NULL);
    }

    if (*tp == NULL) {
        return NGX_CONF_ERROR;
    }

    if (cmd->post) {
        post = cmd->post;
        return post->post_handler(cf, post, tp);
    }

    return NGX_CONF_OK;
}
#endif


static ngx_int_t
ngx_http_aggr_send_response(ngx_http_request_t *r, ngx_str_t *content_type,
    ngx_chain_t *out, off_t size)
{
    ngx_int_t  rc;

    r->headers_out.status = size > 0 ? NGX_HTTP_OK : NGX_HTTP_NO_CONTENT;
    r->headers_out.content_length_n = size;

    r->headers_out.content_type = *content_type;
    r->headers_out.content_type_len = content_type->len;
    r->headers_out.content_type_lowcase = NULL;

    rc = ngx_http_send_header(r);

    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }

    return ngx_http_output_filter(r, out);
}


static ngx_chain_t **
ngx_http_aggr_write_delim(ngx_pool_t *pool, ngx_chain_t **last, off_t *size)
{
    ngx_buf_t    *b;
    ngx_chain_t  *cl;

    b = ngx_create_temp_buf(pool, 1);
    if (b == NULL) {
        return NULL;
    }

    cl = ngx_alloc_chain_link(pool);
    if (cl == NULL) {
        return NULL;
    }

    *b->last++ = '\n';
    (*size)++;

    cl->buf = b;
    *last = cl;
    last = &cl->next;

    return last;
}


static ngx_chain_t **
ngx_http_aggr_write_status(ngx_pool_t *pool, ngx_chain_t **last, off_t *size)
{
    ngx_buf_t    *b;
    ngx_chain_t  *cl;

    b = ngx_aggr_result_get_stats(pool);
    if (b == NULL) {
        return NULL;
    }

    cl = ngx_alloc_chain_link(pool);
    if (cl == NULL) {
        return NULL;
    }

    (*size) += b->last - b->pos;

    cl->buf = b;
    *last = cl;
    last = &cl->next;

    return last;
}


static ngx_int_t
ngx_http_aggr_write_last_buf(ngx_pool_t *pool, ngx_chain_t **last,
    ngx_flag_t last_buf)
{
    ngx_buf_t    *b;
    ngx_chain_t  *cl;

    b = ngx_calloc_buf(pool);
    if (b == NULL) {
        return NGX_ERROR;
    }

    b->last_buf = last_buf;
    b->last_in_chain = 1;

    cl = ngx_alloc_chain_link(pool);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    cl->buf = b;
    cl->next = NULL;

    *last = cl;

    return NGX_OK;
}


static void
ngx_http_aggr_thread(void *data, ngx_log_t *log)
{
    off_t                         size;
    ngx_pool_t                   *pool;
    ngx_uint_t                    i;
    ngx_chain_t                 **last;
    ngx_http_aggr_query_t        *query;
    ngx_http_aggr_thread_ctx_t   *ctx = data;

    size = 0;
    last = &ctx->out;
    pool = ctx->r->pool;

    ctx->content_type = &ngx_http_aggr_type_text;

    for (i = 0; i < ctx->nelts; i++) {

        if (size > 0) {
            last = ngx_http_aggr_write_delim(pool, last, &size);
            if (last == NULL) {
                goto failed;
            }
        }

        query = &ctx->queries[i];
        if (query->query == NULL) {
            last = ngx_http_aggr_write_status(pool, last, &size);
            if (last == NULL) {
                goto failed;
            }

            continue;
        }

        last = ngx_dgram_aggr_query(pool, (ngx_cycle_t *) ngx_cycle,
            &ctx->windows[i], query->query, last, &size);
        if (last == NULL) {
            goto failed;
        }

        if (query->query->fmt == ngx_aggr_query_fmt_json) {
            ctx->content_type = &ngx_http_aggr_type_json;
        }
    }

    if (ngx_http_aggr_write_last_buf(pool, last, ctx->last_buf)  != NGX_OK) {
        goto failed;
    }

    ctx->size = size;
    ctx->rc = NGX_OK;

    return;

failed:

    ctx->rc = NGX_ERROR;
}


#if (NGX_THREADS)
static void
ngx_http_aggr_thread_done(ngx_event_t *ev)
{
    ngx_int_t                    rc;
    ngx_http_aggr_thread_ctx_t  *ctx;

    ctx = ev->data;

    if (ctx->rc != NGX_OK) {
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto done;
    }

    rc = ngx_http_aggr_send_response(ctx->r, ctx->content_type, ctx->out,
        ctx->size);

done:

    ngx_http_finalize_request(ctx->r, rc);
}
#endif


static ngx_int_t
ngx_http_aggr_post_queries(ngx_http_request_t *r,
    ngx_http_aggr_query_t *queries, ngx_uint_t nelts)
{
    ngx_uint_t                   i;
#if (NGX_THREADS)
    ngx_thread_task_t           *task;
    ngx_http_aggr_loc_conf_t    *alcf;
#endif
    ngx_http_aggr_thread_ctx_t  *ctx;

    ctx = ngx_palloc(r->pool, sizeof(*ctx) + sizeof(ctx->windows[0]) * nelts);
    if (ctx == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ctx->r = r;
    ctx->queries = queries;
    ctx->windows = (void *) (ctx + 1);
    ctx->nelts = nelts;
    ctx->last_buf = (r == r->main) ? 1 : 0;

    for (i = 0; i < nelts; i++) {
        if (ngx_http_complex_value(r, &ctx->queries[i].name, &ctx->windows[i])
            != NGX_OK)
        {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
    }

#if (NGX_THREADS)
    alcf = ngx_http_get_module_loc_conf(r, ngx_http_aggr_module);

    if (alcf->tp != NULL) {
        task = ngx_thread_task_alloc(r->pool, 0);
        if (task == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        task->ctx = ctx;

        task->handler = ngx_http_aggr_thread;
        task->event.handler = ngx_http_aggr_thread_done;
        task->event.data = ctx;

        if (ngx_thread_task_post(alcf->tp, task) != NGX_OK) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        r->main->count++;

        return NGX_DONE;
    }
#endif

    ngx_http_aggr_thread(ctx, r->connection->log);
    if (ctx->rc != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    return ngx_http_aggr_send_response(ctx->r, ctx->content_type, ctx->out,
        ctx->size);
}


static ngx_int_t
ngx_http_aggr_static_handler(ngx_http_request_t *r)
{
    ngx_int_t                  rc;
    ngx_http_aggr_loc_conf_t  *alcf;

    if (!(r->method & (NGX_HTTP_GET|NGX_HTTP_HEAD))) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    rc = ngx_http_discard_request_body(r);

    if (rc != NGX_OK) {
        return rc;
    }

    alcf = ngx_http_get_module_loc_conf(r, ngx_http_aggr_module);

    return ngx_http_aggr_post_queries(r, alcf->queries.elts,
        alcf->queries.nelts);
}


static void
ngx_http_aggr_body_handler(ngx_http_request_t *r)
{
    size_t                     size;
    ngx_buf_t                 *b, *nb;
    ngx_int_t                  rc;
    ngx_pool_t                *temp_pool;
    ngx_json_value_t           json;
    ngx_http_aggr_query_t     *query;
    ngx_http_aggr_loc_conf_t  *alcf;

    u_char  error[128];

    temp_pool = NULL;

    if (r->request_body == NULL || r->request_body->bufs == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "ngx_http_aggr_body_handler: no request body");
        rc = NGX_HTTP_UNSUPPORTED_MEDIA_TYPE;
        goto done;
    }

    if (r->request_body->temp_file) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "ngx_http_aggr_body_handler: request body was saved to temp file");
        rc = NGX_HTTP_REQUEST_ENTITY_TOO_LARGE;
        goto done;
    }

    b = r->request_body->bufs->buf;
    if (b->last >= b->end) {
        ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
            "ngx_http_aggr_body_handler: no room for null terminator");

        size = b->last - b->pos;
        nb = ngx_create_temp_buf(r->connection->pool, size + 1);
        if (nb == NULL) {
            rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
            goto done;
        }

        nb->last = ngx_copy(nb->last, b->pos, size);

        b = nb;
    }

    *b->last = '\0';


    query = ngx_palloc(r->pool, sizeof(*query));
    if (query == NULL) {
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto done;
    }

    temp_pool = ngx_create_pool(2048, r->connection->log);
    if (temp_pool == NULL) {
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto done;
    }

    rc = ngx_json_parse(temp_pool, b->pos, &json, error, sizeof(error));
    switch (rc) {

    case NGX_JSON_BAD_DATA:
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "ngx_http_aggr_body_handler: failed to parse json %i, %s",
            rc, error);
        rc = NGX_HTTP_UNSUPPORTED_MEDIA_TYPE;
        goto done;

    case NGX_JSON_ALLOC_FAILED:
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto done;
    }

    alcf = ngx_http_get_module_loc_conf(r, ngx_http_aggr_module);

    rc = ngx_aggr_query_json(r->pool, temp_pool, &json, alcf->query,
        &query->query);

    switch (rc) {

    case NGX_BAD_QUERY:
        rc = NGX_HTTP_BAD_REQUEST;
        goto done;

    case NGX_ERROR:
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto done;
    }

    ngx_destroy_pool(temp_pool);
    temp_pool = NULL;


    query->name = *alcf->name;

    rc = ngx_http_aggr_post_queries(r, query, 1);

done:

    if (temp_pool != NULL) {
        ngx_destroy_pool(temp_pool);
    }

    ngx_http_finalize_request(r, rc);
}


static ngx_int_t
ngx_http_aggr_dynamic_handler(ngx_http_request_t *r)
{
    ngx_int_t         rc;
    ngx_table_elt_t  *content_type;

    if (!(r->method & NGX_HTTP_POST)) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    content_type = r->headers_in.content_type;
    if (content_type == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "ngx_http_aggr_dynamic_handler: missing content type");
        return NGX_HTTP_UNSUPPORTED_MEDIA_TYPE;
    }

    if (content_type->value.len < ngx_http_aggr_type_json.len
        || ngx_strncasecmp(content_type->value.data,
            ngx_http_aggr_type_json.data,
            ngx_http_aggr_type_json.len)
        != 0)
    {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "ngx_http_aggr_dynamic_handler: invalid content type %V",
            &content_type->value);
        return NGX_HTTP_UNSUPPORTED_MEDIA_TYPE;
    }

    r->request_body_in_single_buf = 1;

    rc = ngx_http_read_client_request_body(r, ngx_http_aggr_body_handler);

    if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
        ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0,
            "ngx_http_aggr_dynamic_handler: read request body failed %i", rc);
        return rc;
    }

    return NGX_DONE;
}


static void *
ngx_http_aggr_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_aggr_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_aggr_loc_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    if (ngx_array_init(&conf->queries, cf->pool, 1,
                       sizeof(ngx_http_aggr_query_t))
        != NGX_OK)
    {
        return NULL;
    }

#if (NGX_THREADS)
    conf->tp = NGX_CONF_UNSET_PTR;
#endif

    return conf;
}


static char *
ngx_http_aggr_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
#if (NGX_THREADS)
    ngx_http_aggr_loc_conf_t  *prev = parent;
    ngx_http_aggr_loc_conf_t  *conf = child;

    ngx_conf_merge_ptr_value(conf->tp,
                             prev->tp, NULL);
#endif

    return NGX_CONF_OK;
}


static char *
ngx_http_aggr_dynamic(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                         *value;
    ngx_http_aggr_loc_conf_t          *alcf;
    ngx_http_core_loc_conf_t          *clcf;
    ngx_http_compile_complex_value_t   ccv;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);

    if (clcf->handler != NULL) {
        return "is duplicate";
    }

    clcf->handler = ngx_http_aggr_dynamic_handler;

    alcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_aggr_module);

    alcf->name = ngx_palloc(cf->pool, sizeof(ngx_http_complex_value_t));
    if (alcf->name == NULL) {
        return NGX_CONF_ERROR;
    }

    value = cf->args->elts;

    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

    ccv.cf = cf;
    ccv.value = &value[1];
    ccv.complex_value = alcf->name;

    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static char *
ngx_http_aggr_dynamic_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char                      *rv;
    ngx_http_aggr_loc_conf_t  *alcf;

    rv = ngx_http_aggr_dynamic(cf, cmd, conf);
    if (rv != NGX_CONF_OK) {
        return rv;
    }

    alcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_aggr_module);

    alcf->query = ngx_aggr_query_block(cf, 0);
    if (alcf->query == NULL) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static char *
ngx_http_aggr_static(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                         *value;
    ngx_http_aggr_query_t             *query;
    ngx_http_aggr_loc_conf_t          *alcf;
    ngx_http_core_loc_conf_t          *clcf;
    ngx_http_compile_complex_value_t   ccv;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);

    if (clcf->handler != NULL &&
        clcf->handler != ngx_http_aggr_static_handler)
    {
        return "is duplicate";
    }

    clcf->handler = ngx_http_aggr_static_handler;

    alcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_aggr_module);

    query = ngx_array_push(&alcf->queries);
    if (query == NULL) {
        return NGX_CONF_ERROR;
    }

    value = cf->args->elts;

    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

    ccv.cf = cf;
    ccv.value = &value[1];
    ccv.complex_value = &query->name;

    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    query->query = ngx_aggr_query_block(cf, 1);
    if (query->query == NULL) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static char *
ngx_http_aggr_status(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_aggr_query_t     *query;
    ngx_http_aggr_loc_conf_t  *alcf;
    ngx_http_core_loc_conf_t  *clcf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);

    if (clcf->handler != NULL &&
        clcf->handler != ngx_http_aggr_static_handler)
    {
        return "is duplicate";
    }

    clcf->handler = ngx_http_aggr_static_handler;

    alcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_aggr_module);

    query = ngx_array_push(&alcf->queries);
    if (query == NULL) {
        return NGX_CONF_ERROR;
    }

    ngx_memzero(query, sizeof(*query));

    return NGX_CONF_OK;
}
