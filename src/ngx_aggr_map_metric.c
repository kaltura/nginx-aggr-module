#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"

#include <math.h>   /* INFINITY */


typedef struct {
    double                      min;
    double                      max;
    ngx_aggr_variable_value_t  *value;
} ngx_aggr_map_metric_elt_t;


typedef struct {
    ngx_aggr_map_metric_elt_t  *elts;
    ngx_uint_t                  nelts;
    ngx_uint_t                  offset;
    ngx_aggr_variable_value_t  *default_value;
} ngx_aggr_map_metric_ctx_t;


typedef struct {
    ngx_array_t                *values_hash;
    ngx_array_t                *values;
    ngx_uint_t                  hsize;
    ngx_aggr_variable_value_t  *default_value;
    ngx_aggr_query_init_t      *init;
    ngx_aggr_map_metric_ctx_t  *map;
    ngx_pool_t                 *pool;
} ngx_aggr_map_metric_conf_ctx_t;


static char *ngx_aggr_map_metric_block(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_command_t  ngx_aggr_map_metric_commands[] = {

    { ngx_string("map_metric"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_TAKE2,
      ngx_aggr_map_metric_block,
      NGX_AGGR_MAIN_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};


static ngx_aggr_module_t  ngx_aggr_map_metric_module_ctx = {
    NULL,                                  /* create main configuration */
    NULL                                   /* init main configuration */
};


ngx_module_t  ngx_aggr_map_metric_module = {
    NGX_MODULE_V1,
    &ngx_aggr_map_metric_module_ctx,       /* module context */
    ngx_aggr_map_metric_commands,          /* module directives */
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
ngx_aggr_map_metric_variable(ngx_aggr_result_t *ar,
    ngx_aggr_variable_value_t *v, uintptr_t data)
{
    ngx_aggr_map_metric_ctx_t  *map = (ngx_aggr_map_metric_ctx_t *) data;

    double                      metric;
    ngx_int_t                   left, right, index;
    ngx_str_t                   str;
    ngx_aggr_event_t           *event;
    ngx_aggr_complex_value_t   *cv;
    ngx_aggr_variable_value_t  *value;
    ngx_aggr_map_metric_elt_t  *elt;

    event = ar->cur;
    metric = *(double *) (event->data + map->offset);

    /* binary search for 'metric' */
    left = 0;
    right = map->nelts - 1;

    for ( ;; ) {
        if (left > right) {
            value = map->default_value;
            break;
        }

        index = (left + right) / 2;
        elt = &map->elts[index];

        if (metric < elt->min) {
            right = index - 1;

        } else if (metric >= elt->max) {
            left = index + 1;

        } else {
            value = elt->value;
            break;
        }
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
ngx_aggr_map_metric_strtod(ngx_pool_t *pool, u_char *p, u_char *last,
    double *out)
{
    char        *copy, *end;
    ngx_uint_t   len;

    len = last - p;
    copy = ngx_pnalloc(pool, len + 1);
    if (copy == NULL) {
        return NGX_ERROR;
    }

    ngx_memcpy(copy, p, len);
    copy[len] = '\0';

    *out = strtod(copy, &end);

    if (end != copy + len) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_aggr_map_metric_strtod: invalid value \"%s\"", copy);
        return NGX_BAD_QUERY;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_map_range(ngx_pool_t *pool, ngx_str_t *input,
    ngx_aggr_map_metric_elt_t *elt)
{
    u_char  *last;
    u_char  *p;
    ngx_int_t  rc;

    last = input->data + input->len;
    p = ngx_strlchr(input->data, last, ':');
    if (p == NULL) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_aggr_map_metric_add_value: "
            "invalid range \"%V\", missing ':'", input);
        return NGX_BAD_QUERY;
    }

    if (p > input->data) {
        rc = ngx_aggr_map_metric_strtod(pool, input->data, p, &elt->min);
        if (rc != NGX_OK) {
            return rc;
        }

    } else {
        elt->min = -INFINITY;
    }

    if (p + 1 < last) {
        rc = ngx_aggr_map_metric_strtod(pool, p + 1, last, &elt->max);
        if (rc != NGX_OK) {
            return rc;
        }

    } else {
        elt->max = INFINITY;
    }

    if (elt->min >= elt->max) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_aggr_map_metric_add_value: "
            "invalid range \"%V\", lower bound must be less than upper",
            input);
        return NGX_BAD_QUERY;
    }

    return NGX_OK;
}


static ngx_flag_t
ngx_aggr_map_range_overlap(ngx_aggr_map_metric_elt_t *elts, ngx_uint_t nelts,
    ngx_aggr_map_metric_elt_t *elt)
{
    ngx_uint_t  i;

    for (i = 0; i < nelts; i++) {
        if (elts[i].min < elt->max && elt->min < elts[i].max) {
            return 1;
        }
    }

    return 0;
}


static ngx_int_t
ngx_aggr_map_metric_add_value(ngx_aggr_map_metric_conf_ctx_t *ctx,
    ngx_str_t *input, ngx_str_t *output)
{
    u_char                            *data;
    size_t                             len;
    ngx_str_t                          v;
    ngx_int_t                          rc;
    ngx_uint_t                         i, key;
    ngx_aggr_query_init_t             *init;
    ngx_aggr_complex_value_t           cv, *cvp;
    ngx_aggr_map_metric_elt_t          elt, *pelt;
    ngx_aggr_variable_value_t         *var, **vp;
    ngx_aggr_compile_complex_value_t   ccv;

    init = ctx->init;

    key = 0;

    for (i = 0; i < output->len; i++) {
        key = ngx_hash(key, output->data[i]);
    }

    key %= ctx->hsize;

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

    var = ngx_palloc(init->pool, sizeof(ngx_aggr_variable_value_t));
    if (var == NULL) {
        return NGX_ERROR;
    }

    v.len = output->len;
    v.data = ngx_pstrdup(init->pool, output);
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
        cvp = ngx_palloc(init->pool, sizeof(ngx_aggr_complex_value_t));
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

    rc = ngx_aggr_map_range(ctx->pool, input, &elt);
    if (rc != NGX_OK) {
        return rc;
    }

    if (ngx_aggr_map_range_overlap(ctx->values->elts, ctx->values->nelts,
        &elt))
    {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "range \"%V\" overlaps a previous range", input);
        return NGX_BAD_QUERY;
    }

    elt.value = var;

    pelt = ngx_array_push(ctx->values);
    if (pelt == NULL) {
        return NGX_ERROR;
    }

    *pelt = elt;

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_map_metric_get_offset(ngx_aggr_query_init_t *init,
    ngx_aggr_map_metric_ctx_t *map, ngx_str_t *name)
{
    ngx_uint_t                  **offp;
    ngx_aggr_query_metric_t       metric;
    ngx_aggr_query_metric_in_t   *input;

    ngx_memzero(&metric, sizeof(metric));

    metric.input = *name;
    metric.type = ngx_aggr_query_metric_sum;

    input = ngx_aggr_query_metric_input_get(init, &metric);
    if (input == NULL) {
        return NGX_ERROR;
    }

    map->offset = input->offset;

    offp = ngx_array_push(&init->metric_offs);
    if (offp == NULL) {
        return NGX_ERROR;
    }

    *offp = &map->offset;

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_map_metric_preconfiguration(ngx_aggr_query_init_t *init,
    ngx_aggr_map_metric_conf_ctx_t *ctx, ngx_str_t *input, ngx_str_t *output)
{
    ngx_str_t                   name;
    ngx_pool_t                 *pool;
    ngx_aggr_variable_t        *var;
    ngx_aggr_map_metric_ctx_t  *map;

    map = ngx_pcalloc(init->pool, sizeof(ngx_aggr_map_metric_ctx_t));
    if (map == NULL) {
        return NGX_ERROR;
    }

    if (ngx_aggr_map_metric_get_offset(init, map, input) != NGX_OK) {
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

    var->get_handler = ngx_aggr_map_metric_variable;
    var->data = (uintptr_t) map;

    pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, init->pool->log);
    if (pool == NULL) {
        return NGX_ERROR;
    }

    ctx->hsize = NGX_HASH_LARGE_HSIZE;

    ctx->values_hash = ngx_pcalloc(pool,
                                   sizeof(ngx_array_t) * ctx->hsize);
    if (ctx->values_hash == NULL) {
        ngx_destroy_pool(pool);
        return NGX_ERROR;
    }

    ctx->values = ngx_array_create(pool, 1, sizeof(ngx_aggr_map_metric_elt_t));
    if (ctx->values == NULL) {
        ngx_destroy_pool(pool);
        return NGX_ERROR;
    }

    ctx->default_value = NULL;
    ctx->init = init;
    ctx->map = map;
    ctx->pool = pool;

    return NGX_OK;
}


static int ngx_libc_cdecl
ngx_aggr_map_metric_compare(const void *one, const void *two)
{
    ngx_aggr_map_metric_elt_t  *first, *second;

    first = (ngx_aggr_map_metric_elt_t *) one;
    second = (ngx_aggr_map_metric_elt_t *) two;

    if (first->min < second->min) {
        return -1;
    }

    if (first->min > second->min) {
        return 1;
    }

    return 0;
}


static ngx_int_t
ngx_aggr_map_metric_postconfiguration(ngx_aggr_map_metric_conf_ctx_t *ctx)
{
    size_t                      size;
    ngx_aggr_map_metric_ctx_t  *map;

    map = ctx->map;

    map->default_value = ctx->default_value ? ctx->default_value:
                                              &ngx_aggr_variable_null_value;

    map->nelts = ctx->values->nelts;

    size = sizeof(map->elts[0]) * map->nelts;
    map->elts = ngx_palloc(ctx->init->pool, size);
    if (map->elts == NULL) {
        return NGX_ERROR;
    }

    ngx_memcpy(map->elts, ctx->values->elts, size);

    ngx_qsort(map->elts, map->nelts, sizeof(map->elts[0]),
        ngx_aggr_map_metric_compare);

    ngx_destroy_pool(ctx->pool);

    return NGX_OK;
}


static char *
ngx_aggr_map_metric(ngx_conf_t *cf, ngx_command_t *dummy, void *conf)
{
    ngx_str_t                       *value;
    ngx_aggr_map_metric_conf_ctx_t  *ctx;

    ctx = cf->ctx;

    value = cf->args->elts;

    if (cf->args->nelts != 2) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid number of map_metric parameters");
        return NGX_CONF_ERROR;
    }

    if (ngx_aggr_map_metric_add_value(ctx, &value[0], &value[1]) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static char *
ngx_aggr_map_metric_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char                            *rv;
    ngx_str_t                       *value;
    ngx_conf_t                       save;
    ngx_aggr_query_init_t           *init;
    ngx_aggr_map_metric_conf_ctx_t   ctx;

    init = cf->ctx;
    value = cf->args->elts;

    if (ngx_aggr_map_metric_preconfiguration(init, &ctx, &value[1], &value[2])
        != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }

    save = *cf;
    cf->pool = ctx.pool;
    cf->ctx = &ctx;
    cf->handler = ngx_aggr_map_metric;

    rv = ngx_conf_parse(cf, NULL);

    *cf = save;

    if (rv != NGX_CONF_OK) {
        ngx_destroy_pool(ctx.pool);
        return rv;
    }

    if (ngx_aggr_map_metric_postconfiguration(&ctx) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return rv;
}
