#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"
#include "ngx_aggr_map.h"


typedef struct {
    ngx_uint_t                  hash_max_size;
    ngx_uint_t                  hash_bucket_size;
} ngx_aggr_map_conf_t;


typedef struct {
    ngx_aggr_map_t              map;
    ngx_aggr_complex_value_t    value;
    ngx_aggr_variable_value_t  *default_value;
} ngx_aggr_map_ctx_t;


typedef struct {
    ngx_hash_keys_arrays_t      keys;

    ngx_array_t                *values_hash;
#if (NGX_PCRE)
    ngx_array_t                 regexes;
#endif

    ngx_aggr_variable_value_t  *default_value;
    ngx_aggr_query_init_t      *init;
    ngx_aggr_map_ctx_t         *map;
    ngx_pool_t                 *pool;
} ngx_aggr_map_conf_ctx_t;


static void *ngx_aggr_map_create_conf(ngx_aggr_query_init_t *init);
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


static ngx_int_t
ngx_aggr_map_add_value(ngx_aggr_map_conf_ctx_t *ctx, ngx_str_t *input,
    ngx_str_t *output)
{
    u_char                            *data;
    size_t                             len;
    ngx_int_t                          rv;
    ngx_str_t                          v;
    ngx_uint_t                         i, key;
    ngx_aggr_query_init_t             *init;
    ngx_aggr_complex_value_t           cv, *cvp;
    ngx_aggr_variable_value_t         *var, **vp;
    ngx_aggr_compile_complex_value_t   ccv;

    init = ctx->init;

    key = 0;

    for (i = 0; i < output->len; i++) {
        key = ngx_hash(key, output->data[i]);
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

            if (output->len != len) {
                continue;
            }

            if (ngx_strncmp(output->data, data, len) == 0) {
                var = vp[i];
                goto found;
            }
        }

    } else {
        if (ngx_array_init(&ctx->values_hash[key], init->pool, 4,
                           sizeof(ngx_aggr_variable_value_t *))
            != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    var = ngx_palloc(ctx->keys.pool, sizeof(ngx_aggr_variable_value_t));
    if (var == NULL) {
        return NGX_ERROR;
    }

    v.len = output->len;
    v.data = ngx_pstrdup(ctx->keys.pool, output);
    if (v.data == NULL) {
        return NGX_ERROR;
    }

    ngx_memzero(&ccv, sizeof(ngx_aggr_compile_complex_value_t));

    ccv.init = init;
    ccv.value = &v;
    ccv.complex_value = &cv;

    if (ngx_aggr_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_ERROR;
    }

    if (cv.lengths != NULL) {
        cvp = ngx_palloc(ctx->keys.pool, sizeof(ngx_aggr_complex_value_t));
        if (cvp == NULL) {
            return NGX_ERROR;
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
        return NGX_ERROR;
    }

    *vp = var;

found:

    if (ngx_str_equals_c(*input, "default")) {

        if (ctx->default_value) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                          "duplicate default map parameter");
            return NGX_BAD_QUERY;
        }

        ctx->default_value = var;

        return NGX_OK;
    }

#if (NGX_PCRE)

    if (input->len && input->data[0] == '~') {
        ngx_regex_compile_t    rc;
        ngx_aggr_map_regex_t  *regex;
        u_char                 errstr[NGX_MAX_CONF_ERRSTR];

        regex = ngx_array_push(&ctx->regexes);
        if (regex == NULL) {
            return NGX_ERROR;
        }

        input->len--;
        input->data++;

        ngx_memzero(&rc, sizeof(ngx_regex_compile_t));

        if (input->data[0] == '*') {
            input->len--;
            input->data++;
            rc.options = NGX_REGEX_CASELESS;
        }

        /* the pattern must be null terminated */
        rc.pattern.len = input->len;
        rc.pattern.data = ngx_pnalloc(init->temp_pool, rc.pattern.len + 1);
        if (rc.pattern.data == NULL) {
            return NGX_ERROR;
        }

        ngx_memcpy(rc.pattern.data, input->data, rc.pattern.len);
        rc.pattern.data[rc.pattern.len] = '\0';

        rc.err.len = NGX_MAX_CONF_ERRSTR;
        rc.err.data = errstr;

        regex->regex = ngx_aggr_regex_compile(init, &rc);
        if (regex->regex == NULL) {
            return NGX_ERROR;
        }

        regex->value = var;

        return NGX_OK;
    }

#endif

    if (input->len && input->data[0] == '\\') {
        input->len--;
        input->data++;
    }

    rv = ngx_hash_add_key(&ctx->keys, input, var, 0);

    if (rv == NGX_OK) {
        return NGX_OK;
    }

    if (rv == NGX_BUSY) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                      "conflicting parameter \"%V\"", input);
        return NGX_BAD_QUERY;
    }

    return NGX_ERROR;
}


static ngx_int_t
ngx_aggr_map_preconfiguration(ngx_aggr_query_init_t *init,
    ngx_aggr_map_conf_ctx_t *ctx, ngx_str_t *input, ngx_str_t *output)
{
    ngx_str_t                          name;
    ngx_pool_t                        *pool;
    ngx_aggr_map_ctx_t                *map;
    ngx_aggr_variable_t               *var;
    ngx_aggr_compile_complex_value_t   ccv;

    map = ngx_pcalloc(init->pool, sizeof(ngx_aggr_map_ctx_t));
    if (map == NULL) {
        return NGX_ERROR;
    }

    ngx_memzero(&ccv, sizeof(ngx_aggr_compile_complex_value_t));

    ccv.init = init;
    ccv.value = input;
    ccv.complex_value = &map->value;

    if (ngx_aggr_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_ERROR;
    }

    name = *output;

    if (!name.len || name.data[0] != '$') {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                      "invalid variable name \"%V\"", &name);
        return NGX_BAD_QUERY;
    }

    name.len--;
    name.data++;

    var = ngx_aggr_add_variable(init, &name, NGX_AGGR_VAR_CHANGEABLE);
    if (var == NULL) {
        return NGX_ERROR;
    }

    var->get_handler = ngx_aggr_map_variable;
    var->data = (uintptr_t) map;

    pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, init->pool->log);
    if (pool == NULL) {
        return NGX_ERROR;
    }

    ctx->keys.pool = init->pool;
    ctx->keys.temp_pool = pool;

    if (ngx_hash_keys_array_init(&ctx->keys, NGX_HASH_LARGE) != NGX_OK) {
        ngx_destroy_pool(pool);
        return NGX_ERROR;
    }

    ctx->values_hash = ngx_pcalloc(pool,
                                   sizeof(ngx_array_t) * ctx->keys.hsize);
    if (ctx->values_hash == NULL) {
        ngx_destroy_pool(pool);
        return NGX_ERROR;
    }

#if (NGX_PCRE)
    if (ngx_array_init(&ctx->regexes, init->pool, 2,
                       sizeof(ngx_aggr_map_regex_t))
        != NGX_OK)
    {
        ngx_destroy_pool(pool);
        return NGX_ERROR;
    }
#endif

    ctx->default_value = NULL;
    ctx->init = init;
    ctx->map = map;
    ctx->pool = pool;

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_map_postconfiguration(ngx_aggr_map_conf_ctx_t *ctx)
{
    ngx_hash_init_t       hash;
    ngx_aggr_map_ctx_t   *map;
    ngx_aggr_map_conf_t  *mcf;

    map = ctx->map;

    map->default_value = ctx->default_value ? ctx->default_value:
                                              &ngx_aggr_variable_null_value;

    mcf = ngx_aggr_get_module_main_conf(ctx->init, ngx_aggr_map_module);

    if (mcf->hash_max_size == NGX_CONF_UNSET_UINT) {
        mcf->hash_max_size = 2048;
    }

    if (mcf->hash_bucket_size == NGX_CONF_UNSET_UINT) {
        mcf->hash_bucket_size = ngx_cacheline_size;

    } else {
        mcf->hash_bucket_size = ngx_align(mcf->hash_bucket_size,
                                          ngx_cacheline_size);
    }

    hash.key = ngx_hash_key_lc;
    hash.max_size = mcf->hash_max_size;
    hash.bucket_size = mcf->hash_bucket_size;
    hash.name = "map_hash";
    hash.pool = ctx->init->pool;

    if (ctx->keys.keys.nelts) {
        hash.hash = &map->map.hash;
        hash.temp_pool = NULL;

        if (ngx_hash_init(&hash, ctx->keys.keys.elts, ctx->keys.keys.nelts)
            != NGX_OK)
        {
            ngx_destroy_pool(ctx->pool);
            return NGX_ERROR;
        }
    }

#if (NGX_PCRE)

    if (ctx->regexes.nelts) {
        map->map.regex = ctx->regexes.elts;
        map->map.nregex = ctx->regexes.nelts;
    }

#endif

    ngx_destroy_pool(ctx->pool);

    return NGX_OK;
}


static void *
ngx_aggr_map_create_conf(ngx_aggr_query_init_t *init)
{
    ngx_aggr_map_conf_t  *mcf;

    mcf = ngx_palloc(init->temp_pool, sizeof(ngx_aggr_map_conf_t));
    if (mcf == NULL) {
        return NULL;
    }

    mcf->hash_max_size = NGX_CONF_UNSET_UINT;
    mcf->hash_bucket_size = NGX_CONF_UNSET_UINT;

    return mcf;
}


static char *
ngx_aggr_map(ngx_conf_t *cf, ngx_command_t *dummy, void *conf)
{
    ngx_str_t                *value;
    ngx_aggr_map_conf_ctx_t  *ctx;

    ctx = cf->ctx;

    value = cf->args->elts;

    if (cf->args->nelts != 2) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid number of map parameters");
        return NGX_CONF_ERROR;
    }

    if (ngx_aggr_map_add_value(ctx, &value[0], &value[1]) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static char *
ngx_aggr_map_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char                     *rv;
    ngx_str_t                *value;
    ngx_conf_t                save;
    ngx_aggr_query_init_t    *init;
    ngx_aggr_map_conf_ctx_t   ctx;

    init = cf->ctx;
    value = cf->args->elts;

    if (ngx_aggr_map_preconfiguration(init, &ctx, &value[1], &value[2])
        != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }

    save = *cf;
    cf->pool = ctx.pool;
    cf->ctx = &ctx;
    cf->handler = ngx_aggr_map;

    rv = ngx_conf_parse(cf, NULL);

    *cf = save;

    if (rv != NGX_CONF_OK) {
        ngx_destroy_pool(ctx.pool);
        return rv;
    }

    if (ngx_aggr_map_postconfiguration(&ctx) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return rv;
}


static ngx_int_t
ngx_aggr_map_json(ngx_aggr_query_init_t *init, ngx_json_object_t *obj)
{
    ngx_int_t                 rc;
    ngx_uint_t                i, n;
    ngx_str_t                *input;
    ngx_str_t                *output;
    ngx_json_object_t        *values;
    ngx_json_key_value_t     *elts;
    ngx_aggr_map_conf_ctx_t   ctx;

    input = NULL;
    output = NULL;
    values = NULL;

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "input")) {
                input = &elts[i].value.v.str;
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "output")) {
                output = &elts[i].value.v.str;
                continue;
            }
            break;

        case NGX_JSON_OBJECT:
            if (ngx_str_equals_c(elts[i].key, "values")) {
                values = &elts[i].value.v.obj;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (input == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_json: missing \"input\" key");
        return NGX_BAD_QUERY;
    }

    if (output == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_json: missing \"output\" key");
        return NGX_BAD_QUERY;
    }

    if (values == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_json: missing \"values\" key");
        return NGX_BAD_QUERY;
    }

    rc = ngx_aggr_map_preconfiguration(init, &ctx, input, output);
    if (rc != NGX_OK) {
        return rc;
    }

    elts = values->elts;
    for (i = 0; i < values->nelts; i++) {
        if (elts[i].value.type != NGX_JSON_STRING) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_map_json: invalid value type");
            ngx_destroy_pool(ctx.pool);
            return NGX_BAD_QUERY;
        }

        rc = ngx_aggr_map_add_value(&ctx, &elts[i].key, &elts[i].value.v.str);
        if (rc != NGX_OK) {
            ngx_destroy_pool(ctx.pool);
            return rc;
        }
    }

    rc = ngx_aggr_map_postconfiguration(&ctx);
    if (rc != NGX_OK) {
        return rc;
    }

    return NGX_OK;
}


ngx_int_t
ngx_aggr_maps_json(ngx_aggr_query_init_t *init, ngx_json_array_t *arr)
{
    ngx_int_t           rc;
    ngx_array_part_t   *part;
    ngx_json_object_t  *obj;

    part = &arr->part;

    for (obj = part->first; ; obj++) {

        if ((void *) obj >= part->last) {
            if (part->next == NULL) {
                break;
            }

            part = part->next;
            obj = part->first;
        }

        rc = ngx_aggr_map_json(init, obj);
        if (rc != NGX_OK) {
            return rc;
        }
    }

    return NGX_OK;
}
