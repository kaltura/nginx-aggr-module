#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"
#include "ngx_aggr_output.h"


static char *ngx_aggr_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static void *ngx_aggr_create_conf(ngx_cycle_t *cycle);
static char *ngx_aggr_init_conf(ngx_cycle_t *cycle, void *conf);


typedef struct {
    ngx_hash_t               windows_hash;
    ngx_hash_keys_arrays_t  *windows_keys;

    ngx_uint_t               windows_hash_max_size;
    ngx_uint_t               windows_hash_bucket_size;
} ngx_aggr_conf_t;


typedef struct {
    ngx_str_t               *window_name;
    ngx_log_t               *log;
} ngx_aggr_log_ctx_t;


static ngx_command_t  ngx_aggr_commands[] = {

    { ngx_string("aggr"),
      NGX_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_block,
      0,
      0,
      NULL },

    { ngx_string("windows_hash_max_size"),
      NGX_AGGR_GLOBAL_CONF|NGX_DIRECT_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      0,
      offsetof(ngx_aggr_conf_t, windows_hash_max_size),
      NULL },

    { ngx_string("windows_hash_bucket_size"),
      NGX_AGGR_GLOBAL_CONF|NGX_DIRECT_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      0,
      offsetof(ngx_aggr_conf_t, windows_hash_bucket_size),
      NULL },

      ngx_null_command
};


static ngx_core_module_t  ngx_aggr_module_ctx = {
    ngx_string("aggr"),
    ngx_aggr_create_conf,
    ngx_aggr_init_conf
};


ngx_module_t  ngx_aggr_module = {
    NGX_MODULE_V1,
    &ngx_aggr_module_ctx,                  /* module context */
    ngx_aggr_commands,                     /* module directives */
    NGX_CORE_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static char *
ngx_aggr_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char        *rv;
    ngx_uint_t   save;

    save = cf->cmd_type;
    cf->cmd_type = NGX_AGGR_GLOBAL_CONF;

    rv = ngx_conf_parse(cf, NULL);

    cf->cmd_type = save;

    return rv;
}


static void *
ngx_aggr_create_conf(ngx_cycle_t *cycle)
{
    ngx_aggr_conf_t  *acf;

    acf = ngx_pcalloc(cycle->pool, sizeof(ngx_aggr_conf_t));
    if (acf == NULL) {
        return NULL;
    }

    acf->windows_keys = ngx_pcalloc(cycle->pool,
        sizeof(ngx_hash_keys_arrays_t));
    if (acf->windows_keys == NULL) {
        return NULL;
    }

    acf->windows_keys->pool = cycle->pool;
    acf->windows_keys->temp_pool = cycle->pool;

    if (ngx_hash_keys_array_init(acf->windows_keys, NGX_HASH_SMALL)
        != NGX_OK)
    {
        return NULL;
    }

    acf->windows_hash_max_size = NGX_CONF_UNSET_UINT;
    acf->windows_hash_bucket_size = NGX_CONF_UNSET_UINT;

    return acf;
}


static char *
ngx_aggr_init_conf(ngx_cycle_t *cycle, void *conf)
{
    ngx_aggr_conf_t         *acf = conf;
    ngx_hash_init_t          hash;
    ngx_hash_keys_arrays_t  *keys;

    ngx_conf_init_uint_value(acf->windows_hash_max_size, 512);
    ngx_conf_init_uint_value(acf->windows_hash_bucket_size, 64);

    hash.hash = &acf->windows_hash;
    hash.key = ngx_hash_key;
    hash.max_size = acf->windows_hash_max_size;
    hash.bucket_size = acf->windows_hash_bucket_size;
    hash.name = "windows_hash";
    hash.pool = cycle->pool;
    hash.temp_pool = NULL;

    keys = acf->windows_keys;
    if (ngx_hash_init(&hash, keys->keys.elts, keys->keys.nelts) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    acf->windows_keys = NULL;

    return NGX_CONF_OK;
}


ngx_int_t
ngx_aggr_add_window(ngx_conf_t *cf, ngx_str_t *name,
    ngx_aggr_window_conf_t *window)
{
    ngx_aggr_conf_t  *acf;

    acf = (ngx_aggr_conf_t *) ngx_get_conf(cf->cycle->conf_ctx,
        ngx_aggr_module);

    return ngx_hash_add_key(acf->windows_keys, name, window,
        NGX_HASH_READONLY_KEY);
}


static u_char *
ngx_aggr_log_error(ngx_log_t *log, u_char *buf, size_t len)
{
    u_char              *p;
    ngx_aggr_log_ctx_t  *ctx;

    ctx = log->data;

    p = buf;

    p = ngx_snprintf(buf, len, ", window_name: %V", ctx->window_name);
    len -= p - buf;
    buf = p;

    log = ctx->log;
    if (log->handler) {
        p = log->handler(log, buf, len);
    }

    return p;
}


ngx_chain_t **
ngx_aggr_query(ngx_pool_t *pool, ngx_cycle_t *cycle, ngx_str_t *name,
    ngx_aggr_query_t *query, ngx_chain_t **last, off_t *size)
{
    ngx_log_t                log;
    ngx_uint_t               key;
    ngx_aggr_conf_t         *acf;
    ngx_aggr_result_t       *ar;
    ngx_aggr_log_ctx_t       log_ctx;
    ngx_aggr_window_conf_t  *conf;

    acf = (ngx_aggr_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_aggr_module);

    log = *pool->log;

    log_ctx.window_name = name;
    log_ctx.log = pool->log;

    log.handler = ngx_aggr_log_error;
    log.data = &log_ctx;

    key = ngx_hash_key(name->data, name->len);

    conf = ngx_hash_find(&acf->windows_hash, key, name->data, name->len);
    if (conf == NULL) {
        ngx_log_error(NGX_LOG_ERR, &log, 0,
            "ngx_aggr_query: unknown window \"%V\"", name);
        return NULL;
    }

    ar = ngx_aggr_result_create(query, &log, ngx_time(), NULL);
    if (ar == NULL) {
        return NULL;
    }

    if (conf->window != NULL) {
        if (ngx_aggr_window_process(conf->window, pool, ar) != NGX_OK) {
            goto failed;
        }
    }

    last = ngx_aggr_result_write(ar, pool, last, size);
    if (last == NULL) {
        goto failed;
    }

    ngx_aggr_result_destroy(ar);

    return last;

failed:

    ngx_aggr_result_destroy(ar);

    return NULL;
}
