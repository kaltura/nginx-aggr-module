#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


typedef struct {
    ngx_uint_t                  hash_max_size;
    ngx_uint_t                  hash_bucket_size;
} ngx_aggr_map_conf_t;


typedef struct {
    ngx_hash_keys_arrays_t      keys;

    ngx_array_t                *values_hash;
#if (NGX_PCRE)
    ngx_array_t                 regexes;
#endif

    ngx_aggr_variable_value_t  *default_value;
    ngx_conf_t                 *cf;
} ngx_aggr_map_conf_ctx_t;


typedef struct {
    ngx_aggr_map_t              map;
    ngx_aggr_complex_value_t    value;
    ngx_aggr_variable_value_t  *default_value;
} ngx_aggr_map_ctx_t;


static void *ngx_aggr_map_create_conf(ngx_conf_t *cf);
static char *ngx_aggr_map_block(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_command_t  ngx_aggr_map_commands[] = {

    { ngx_string("map"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_TAKE2,
      ngx_aggr_map_block,
      NGX_AGGR_MAIN_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("map_hash_max_size"),
      NGX_AGGR_MAIN_CONF |NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_map_conf_t, hash_max_size),
      NULL },

    { ngx_string("map_hash_bucket_size"),
      NGX_AGGR_MAIN_CONF |NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_map_conf_t, hash_bucket_size),
      NULL },

      ngx_null_command
};


static ngx_aggr_module_t  ngx_aggr_map_module_ctx = {
    ngx_aggr_map_create_conf,              /* create main configuration */
    NULL                                   /* init main configuration */
};


ngx_module_t  ngx_aggr_map_module = {
    NGX_MODULE_V1,
    &ngx_aggr_map_module_ctx,              /* module context */
    ngx_aggr_map_commands,                 /* module directives */
    NGX_AGGR_MODULE,                       /* module type */
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
ngx_aggr_map_variable(ngx_aggr_result_t *ar, ngx_aggr_variable_value_t *v,
    uintptr_t data)
{
    ngx_aggr_map_ctx_t  *map = (ngx_aggr_map_ctx_t *) data;

    ngx_str_t                   val, str;
    ngx_aggr_complex_value_t   *cv;
    ngx_aggr_variable_value_t  *value;

    if (ngx_aggr_complex_value(ar, &map->value, &val) != NGX_OK) {
        return NGX_ERROR;
    }

    value = ngx_aggr_map_find(ar, &map->map, &val);

    if (value == NULL) {
        value = map->default_value;
    }

    if (!value->valid) {
        cv = (ngx_aggr_complex_value_t *) value->data;

        if (ngx_aggr_complex_value(ar, cv, &str) != NGX_OK) {
            return NGX_ERROR;
        }

        v->valid = 1;
        v->no_cacheable = 0;
        v->not_found = 0;
        v->len = str.len;
        v->data = str.data;

    } else {
        *v = *value;
    }

    return NGX_OK;
}


static char *
ngx_aggr_map(ngx_conf_t *cf, ngx_command_t *dummy, void *conf)
{
    u_char                            *data;
    size_t                             len;
    ngx_int_t                          rv;
    ngx_str_t                         *value, v;
    ngx_uint_t                         i, key;
    ngx_aggr_map_conf_ctx_t           *ctx;
    ngx_aggr_complex_value_t           cv, *cvp;
    ngx_aggr_variable_value_t         *var, **vp;
    ngx_aggr_compile_complex_value_t   ccv;

    ctx = cf->ctx;

    value = cf->args->elts;

    if (cf->args->nelts != 2) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid number of the map parameters");
        return NGX_CONF_ERROR;
    }

    key = 0;

    for (i = 0; i < value[1].len; i++) {
        key = ngx_hash(key, value[1].data[i]);
    }

    key %= ctx->keys.hsize;

    vp = ctx->values_hash[key].elts;

    if (vp) {
        for (i = 0; i < ctx->values_hash[key].nelts; i++) {

            if (vp[i]->valid) {
                data = vp[i]->data;
                len = vp[i]->len;

            } else {
                cvp = (ngx_aggr_complex_value_t *) vp[i]->data;
                data = cvp->value.data;
                len = cvp->value.len;
            }

            if (value[1].len != len) {
                continue;
            }

            if (ngx_strncmp(value[1].data, data, len) == 0) {
                var = vp[i];
                goto found;
            }
        }

    } else {
        if (ngx_array_init(&ctx->values_hash[key], cf->pool, 4,
                           sizeof(ngx_aggr_variable_value_t *))
            != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }
    }

    var = ngx_palloc(ctx->keys.pool, sizeof(ngx_aggr_variable_value_t));
    if (var == NULL) {
        return NGX_CONF_ERROR;
    }

    v.len = value[1].len;
    v.data = ngx_pstrdup(ctx->keys.pool, &value[1]);
    if (v.data == NULL) {
        return NGX_CONF_ERROR;
    }

    ngx_memzero(&ccv, sizeof(ngx_aggr_compile_complex_value_t));

    ccv.init = ctx->cf->handler_conf;
    ccv.value = &v;
    ccv.complex_value = &cv;

    if (ngx_aggr_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    if (cv.lengths != NULL) {
        cvp = ngx_palloc(ctx->keys.pool, sizeof(ngx_aggr_complex_value_t));
        if (cvp == NULL) {
            return NGX_CONF_ERROR;
        }

        *cvp = cv;

        var->len = 0;
        var->data = (u_char *) cvp;
        var->valid = 0;

    } else {
        var->len = v.len;
        var->data = v.data;
        var->valid = 1;
    }

    var->no_cacheable = 0;
    var->not_found = 0;

    vp = ngx_array_push(&ctx->values_hash[key]);
    if (vp == NULL) {
        return NGX_CONF_ERROR;
    }

    *vp = var;

found:

    if (ngx_strcmp(value[0].data, "default") == 0) {

        if (ctx->default_value) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "duplicate default map parameter");
            return NGX_CONF_ERROR;
        }

        ctx->default_value = var;

        return NGX_CONF_OK;
    }

#if (NGX_PCRE)

    if (value[0].len && value[0].data[0] == '~') {
        ngx_regex_compile_t    rc;
        ngx_aggr_map_regex_t  *regex;
        u_char                 errstr[NGX_MAX_CONF_ERRSTR];

        regex = ngx_array_push(&ctx->regexes);
        if (regex == NULL) {
            return NGX_CONF_ERROR;
        }

        value[0].len--;
        value[0].data++;

        ngx_memzero(&rc, sizeof(ngx_regex_compile_t));

        if (value[0].data[0] == '*') {
            value[0].len--;
            value[0].data++;
            rc.options = NGX_REGEX_CASELESS;
        }

        rc.pattern = value[0];
        rc.err.len = NGX_MAX_CONF_ERRSTR;
        rc.err.data = errstr;

        ngx_aggr_query_init_t  *init = ctx->cf->handler_conf;

        regex->regex = ngx_aggr_regex_compile(init, &rc);
        if (regex->regex == NULL) {
            return NGX_CONF_ERROR;
        }

        regex->value = var;

        return NGX_CONF_OK;
    }

#endif

    if (value[0].len && value[0].data[0] == '\\') {
        value[0].len--;
        value[0].data++;
    }

    rv = ngx_hash_add_key(&ctx->keys, &value[0], var, 0);

    if (rv == NGX_OK) {
        return NGX_CONF_OK;
    }

    if (rv == NGX_BUSY) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "conflicting parameter \"%V\"", &value[0]);
    }

    return NGX_CONF_ERROR;
}


static void *
ngx_aggr_map_create_conf(ngx_conf_t *cf)
{
    ngx_aggr_map_conf_t  *mcf;

    mcf = ngx_palloc(cf->pool, sizeof(ngx_aggr_map_conf_t));
    if (mcf == NULL) {
        return NULL;
    }

    mcf->hash_max_size = NGX_CONF_UNSET_UINT;
    mcf->hash_bucket_size = NGX_CONF_UNSET_UINT;

    return mcf;
}


static char *
ngx_aggr_map_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_aggr_map_conf_t  *mcf = conf;

    char                              *rv;
    ngx_str_t                         *value, name;
    ngx_conf_t                         save;
    ngx_pool_t                        *pool;
    ngx_hash_init_t                    hash;
    ngx_aggr_map_ctx_t                *map;
    ngx_aggr_variable_t               *var;
    ngx_aggr_query_init_t             *init;
    ngx_aggr_map_conf_ctx_t            ctx;
    ngx_aggr_compile_complex_value_t   ccv;

    init = cf->handler_conf;

    if (mcf->hash_max_size == NGX_CONF_UNSET_UINT) {
        mcf->hash_max_size = 2048;
    }

    if (mcf->hash_bucket_size == NGX_CONF_UNSET_UINT) {
        mcf->hash_bucket_size = ngx_cacheline_size;

    } else {
        mcf->hash_bucket_size = ngx_align(mcf->hash_bucket_size,
                                          ngx_cacheline_size);
    }

    map = ngx_pcalloc(cf->pool, sizeof(ngx_aggr_map_ctx_t));
    if (map == NULL) {
        return NGX_CONF_ERROR;
    }

    value = cf->args->elts;

    ngx_memzero(&ccv, sizeof(ngx_aggr_compile_complex_value_t));

    ccv.init = init;
    ccv.value = &value[1];
    ccv.complex_value = &map->value;

    if (ngx_aggr_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    name = value[2];

    if (name.data[0] != '$') {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid variable name \"%V\"", &name);
        return NGX_CONF_ERROR;
    }

    name.len--;
    name.data++;

    var = ngx_aggr_add_variable(init, &name, NGX_AGGR_VAR_CHANGEABLE);
    if (var == NULL) {
        return NGX_CONF_ERROR;
    }

    var->get_handler = ngx_aggr_map_variable;
    var->data = (uintptr_t) map;

    pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, cf->log);
    if (pool == NULL) {
        return NGX_CONF_ERROR;
    }

    ctx.keys.pool = cf->pool;
    ctx.keys.temp_pool = pool;

    if (ngx_hash_keys_array_init(&ctx.keys, NGX_HASH_LARGE) != NGX_OK) {
        ngx_destroy_pool(pool);
        return NGX_CONF_ERROR;
    }

    ctx.values_hash = ngx_pcalloc(pool, sizeof(ngx_array_t) * ctx.keys.hsize);
    if (ctx.values_hash == NULL) {
        ngx_destroy_pool(pool);
        return NGX_CONF_ERROR;
    }

#if (NGX_PCRE)
    if (ngx_array_init(&ctx.regexes, cf->pool, 2,
                       sizeof(ngx_aggr_map_regex_t))
        != NGX_OK)
    {
        ngx_destroy_pool(pool);
        return NGX_CONF_ERROR;
    }
#endif

    ctx.default_value = NULL;
    ctx.cf = &save;

    save = *cf;
    cf->pool = pool;
    cf->ctx = &ctx;
    cf->handler = ngx_aggr_map;

    rv = ngx_conf_parse(cf, NULL);

    *cf = save;

    if (rv != NGX_CONF_OK) {
        ngx_destroy_pool(pool);
        return rv;
    }

    map->default_value = ctx.default_value ? ctx.default_value:
                                             &ngx_aggr_variable_null_value;

    hash.key = ngx_hash_key_lc;
    hash.max_size = mcf->hash_max_size;
    hash.bucket_size = mcf->hash_bucket_size;
    hash.name = "map_hash";
    hash.pool = cf->pool;

    if (ctx.keys.keys.nelts) {
        hash.hash = &map->map.hash;
        hash.temp_pool = NULL;

        if (ngx_hash_init(&hash, ctx.keys.keys.elts, ctx.keys.keys.nelts)
            != NGX_OK)
        {
            ngx_destroy_pool(pool);
            return NGX_CONF_ERROR;
        }
    }

#if (NGX_PCRE)

    if (ctx.regexes.nelts) {
        map->map.regex = ctx.regexes.elts;
        map->map.nregex = ctx.regexes.nelts;
    }

#endif

    ngx_destroy_pool(pool);

    return rv;
}
