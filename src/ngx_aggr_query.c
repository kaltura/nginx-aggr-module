#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr_query.h"


#define ngx_str_equals(ns, s)                                                \
    (ns.len == sizeof(s) - 1 && ngx_strncmp(ns.data, s, sizeof(s) - 1) == 0)


/* must match ngx_aggr_query_fmt_xxx enum in order */
static ngx_str_t  ngx_aggr_query_fmt_type_names[] = {
    ngx_string("json"),
    ngx_string("prom"),
    ngx_null_string
};

/* must match ngx_aggr_query_dim_xxx enum in order */
static ngx_str_t  ngx_aggr_query_dim_type_names[] = {
    ngx_string("group"),
    ngx_string("select"),
    ngx_null_string
};


/* must match ngx_aggr_query_metric_xxx enum in order */
static ngx_str_t  ngx_aggr_query_metric_type_names[] = {
    ngx_string("sum"),
    ngx_string("max"),
    ngx_null_string
};


static ngx_int_t
ngx_aggr_query_enum(ngx_str_t *values, ngx_str_t *value)
{
    ngx_str_t  *cur;

    for (cur = values; cur->len != 0; cur++) {
        if (cur->len == value->len &&
            ngx_strncmp(cur->data, value->data, cur->len) == 0)
        {
            return cur - values;
        }
    }

    return -1;
}


static ngx_hash_key_t *
ngx_aggr_query_hash_key_get(ngx_array_t *hash_keys, ngx_str_t *key)
{
    ngx_uint_t       i;
    ngx_hash_key_t  *hk;

    hk = hash_keys->elts;

    for (i = 0; i < hash_keys->nelts; i++) {
        if (key->len == hk[i].key.len &&
            ngx_strncmp(key->data, hk[i].key.data, key->len) == 0)
        {
            return &hk[i];
        }
    }

    return NULL;
}

static ngx_int_t
ngx_aggr_query_hash_init(ngx_pool_t *pool, ngx_pool_t *temp_pool,
    ngx_hash_t *hash, ngx_array_t *arr, ngx_hash_init_t  *hi)
{
    void            **pelt;
    ngx_str_t        *elt;
    ngx_uint_t        i;
    ngx_array_t      *value;
    ngx_array_t       hash_keys;
    ngx_hash_key_t   *hk;

    if (ngx_array_init(&hash_keys, temp_pool, 4, sizeof(ngx_hash_key_t))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (arr != NGX_CONF_UNSET_PTR) {

        for (i = 0; i < arr->nelts; i++) {

            elt = (void *) ((u_char *) arr->elts + arr->size * i);

            hk = ngx_aggr_query_hash_key_get(&hash_keys, elt);
            if (hk == NULL) {

                hk = ngx_array_push(&hash_keys);
                if (hk == NULL) {
                    return NGX_ERROR;
                }

                value = ngx_array_create(pool, 1, sizeof(void *));
                if (value == NULL) {
                    return NGX_ERROR;
                }

                hk->key = *elt;
                hk->key_hash = ngx_hash_key(elt->data, elt->len);
                hk->value = value;

            } else {
                value = hk->value;
            }

            pelt = ngx_array_push(value);
            if (pelt == NULL) {
                return NGX_ERROR;
            }

            *pelt = elt;
        }
    }

    hi->hash = hash;
    hi->key = ngx_hash_key;
    hi->pool = pool;
    hi->temp_pool = NULL;

    if (ngx_hash_init(hi, hash_keys.elts, hash_keys.nelts) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


/* dim */

static ngx_aggr_query_dim_t *
ngx_aggr_query_dim_push(ngx_aggr_query_t *query, ngx_str_t *name)
{
    ngx_aggr_query_dim_t  *dim;

    dim = ngx_array_push(&query->dims);
    if (dim == NULL) {
        return NULL;
    }

    dim->input = *name;
    dim->output = *name;
    dim->type = ngx_aggr_query_dim_group;
    dim->default_value.len = 0;
    dim->default_value.data = NULL;
    dim->lower = 0;

    return dim;
}

static ngx_int_t
ngx_aggr_query_dim_init(ngx_aggr_query_dim_t *dim, ngx_pool_t *pool)
{
    u_char  *copy;

    if (dim->lower && dim->default_value.len > 0) {
        copy = ngx_pnalloc(pool, dim->default_value.len);
        if (copy == NULL) {
            return NGX_ERROR;
        }

        ngx_strlow(copy, dim->default_value.data, dim->default_value.len);
        dim->default_value.data = copy;
    }

    return NGX_OK;
}

static char *
ngx_aggr_query_dim_conf(ngx_conf_t *cf, ngx_aggr_query_t *query)
{
    ngx_str_t              cur;
    ngx_str_t             *value;
    ngx_uint_t             i;
    ngx_aggr_query_dim_t  *dim;

    if (cf->args->nelts < 2) {
        return "invalid number of arguments in \"dim\" directive";
    }

    value = cf->args->elts;

    if (cf->args->nelts == 3 && ngx_strcmp(value[2].data, "type=time") == 0) {
        if (query->time_dim.len != 0) {
            return "is duplicate";
        }

        query->time_dim = value[1];
        return NGX_CONF_OK;
    }

    dim = ngx_aggr_query_dim_push(query, &value[1]);
    if (dim == NULL) {
        return NGX_CONF_ERROR;
    }

    for (i = 2; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "input=", 6) == 0) {

            dim->input.data = value[i].data + 6;
            dim->input.len = value[i].len - 6;

            continue;
        }

        if (ngx_strncmp(value[i].data, "default=", 8) == 0) {

            dim->default_value.data = value[i].data + 8;
            dim->default_value.len = value[i].len - 8;

            continue;
        }

        if (ngx_strncmp(value[i].data, "type=", 5) == 0) {

            cur.data = value[i].data + 5;
            cur.len = value[i].len - 5;

            dim->type = ngx_aggr_query_enum(ngx_aggr_query_dim_type_names,
                &cur);
            if (dim->type >= 0) {
                continue;
            }
        }

        if (ngx_strcmp(value[i].data, "lower") == 0) {
            dim->lower = 1;
            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "invalid parameter \"%V\"", &value[i]);

        return NGX_CONF_ERROR;
    }

    if (ngx_aggr_query_dim_init(dim, cf->pool) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_aggr_query_dim_json(ngx_aggr_query_t *query, ngx_str_t *name,
    ngx_json_object_t *attrs)
{
    ngx_uint_t             i, n;
    ngx_json_key_value_t  *elts;
    ngx_aggr_query_dim_t  *dim;

    elts = attrs->elts;
    n = attrs->nelts;

    if (n == 1 && ngx_str_equals(elts[0].key, "type") &&
        elts[0].value.type == NGX_JSON_STRING &&
        ngx_str_equals(elts[0].value.v.str, "time"))
    {
        if (query->time_dim.len != 0) {
            ngx_log_error(NGX_LOG_ERR, query->pool->log, 0,
                "ngx_aggr_query_dim_json: duplicate time dim");
            return NGX_BAD_QUERY;
        }

        query->time_dim = elts[0].value.v.str;
        return NGX_OK;
    }

    dim = ngx_aggr_query_dim_push(query, name);
    if (dim == NULL) {
        return NGX_ERROR;
    }

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals(elts[i].key, "input")) {
                dim->input = elts[i].value.v.str;
                continue;
            }

            if (ngx_str_equals(elts[i].key, "default")) {
                dim->default_value = elts[i].value.v.str;
                continue;
            }

            if (ngx_str_equals(elts[i].key, "type")) {
                dim->type = ngx_aggr_query_enum(ngx_aggr_query_dim_type_names,
                    &elts[i].value.v.str);
                if (dim->type >= 0) {
                    continue;
                }
            }
            break;

        case NGX_JSON_BOOL:
            if (ngx_str_equals(elts[i].key, "lower")) {
                dim->lower = elts[i].value.v.boolean;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, query->pool->log, 0,
            "ngx_aggr_query_dim_json: invalid parameter \"%V\"", &elts[i].key);

        return NGX_BAD_QUERY;
    }

    if (ngx_aggr_query_dim_init(dim, query->pool) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


/* dims */

static ngx_int_t
ngx_aggr_query_dims_json(ngx_aggr_query_t *query, ngx_json_object_t *dims)
{
    ngx_int_t              rc;
    ngx_uint_t             i, n;
    ngx_json_key_value_t  *elts;

    elts = dims->elts;
    n = dims->nelts;

    for (i = 0; i < n; i++) {
        if (elts[i].value.type != NGX_JSON_OBJECT) {
            ngx_log_error(NGX_LOG_ERR, query->pool->log, 0,
                "ngx_aggr_query_dims_json: invalid type \"%V\"", &elts[i].key);
            return NGX_BAD_QUERY;
        }

        rc = ngx_aggr_query_dim_json(query, &elts[i].key,
            &elts[i].value.v.obj);
        if (rc != NGX_OK) {
            return rc;
        }
    }

    return NGX_OK;
}


static void
ngx_aggr_query_dims_sort(ngx_aggr_query_t *query)
{
    ngx_uint_t             n;
    ngx_uint_t             i, j;
    ngx_aggr_query_dim_t   tmp;
    ngx_aggr_query_dim_t  *dims;

    n = query->dims.nelts;
    if (n < 2) {
        return;
    }

    /* using bubble sort since it's a stable sort */

    dims = query->dims.elts;
    for (i = 0; i < n - 1; i++) {
        for (j = 0; j < n - i - 1; j++) {
            if (dims[j].type <= dims[j + 1].type) {
                continue;
            }

            tmp = dims[j];
            dims[j] = dims[j + 1];
            dims[j + 1] = tmp;
        }
    }
}


static ngx_int_t
ngx_aggr_query_dims_init(ngx_aggr_query_t *query, ngx_pool_t *temp_pool,
    ngx_hash_init_t *hash)
{
    ngx_uint_t             i, n;
    ngx_aggr_query_dim_t  *dims;

    ngx_aggr_query_dims_sort(query);

    /* count the number of dims per type and set indexes */
    ngx_memzero(query->dim_count, sizeof(query->dim_count));

    dims = query->dims.elts;
    n = query->dims.nelts;

    for (i = 0; i < n; i++) {
        dims[i].index = i;
        query->dim_count[dims[i].type]++;
    }

    hash->name = "dims_hash";

    return ngx_aggr_query_hash_init(query->pool, temp_pool, &query->dims_hash,
        &query->dims, hash);
}


/* metric */

static ngx_aggr_query_metric_t *
ngx_aggr_query_metric_push(ngx_aggr_query_t *query, ngx_str_t *name)
{
    ngx_aggr_query_metric_t  *metric;

    metric = ngx_array_push(&query->metrics);
    if (metric == NULL) {
        return NULL;
    }

    metric->input = *name;
    metric->output = *name;
    metric->type = ngx_aggr_query_metric_sum;
    metric->default_value = 0;
    metric->index = query->metrics.nelts - 1;

    return metric;
}


static char *
ngx_aggr_query_metric_conf(ngx_conf_t *cf, ngx_aggr_query_t  *query)
{
    u_char                    *end;
    ngx_str_t                  cur;
    ngx_str_t                 *value;
    ngx_uint_t                 i;
    ngx_aggr_query_metric_t   *metric;

    if (cf->args->nelts < 2) {
        return "invalid number of arguments in \"metric\" directive";
    }

    value = cf->args->elts;

    metric = ngx_aggr_query_metric_push(query, &value[1]);
    if (metric == NULL) {
        return NGX_CONF_ERROR;
    }

    for (i = 2; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "input=", 6) == 0) {

            metric->input.data = value[i].data + 6;
            metric->input.len = value[i].len - 6;

            continue;
        }

        if (ngx_strncmp(value[i].data, "default=", 8) == 0) {

            metric->default_value = strtod(
                (char *) value[i].data + 8, (char **) &end);

            if (end == value[i].data + value[i].len) {
                continue;
            }
        }

        if (ngx_strncmp(value[i].data, "type=", 5) == 0) {

            cur.data = value[i].data + 5;
            cur.len = value[i].len - 5;

            metric->type = ngx_aggr_query_enum(
                ngx_aggr_query_metric_type_names, &cur);
            if (metric->type >= 0) {
                continue;
            }
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "invalid parameter \"%V\"", &value[i]);

        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_aggr_query_metric_json(ngx_aggr_query_t *query, ngx_str_t *name,
    ngx_json_object_t *attrs)
{
    ngx_uint_t                i, n;
    ngx_json_key_value_t     *elts;
    ngx_aggr_query_metric_t  *metric;

    metric = ngx_aggr_query_metric_push(query, name);
    if (metric == NULL) {
        return NGX_ERROR;
    }

    elts = attrs->elts;
    n = attrs->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals(elts[i].key, "input")) {
                metric->input = elts[i].value.v.str;
                continue;
            }

            if (ngx_str_equals(elts[i].key, "type")) {
                metric->type = ngx_aggr_query_enum(
                    ngx_aggr_query_metric_type_names, &elts[i].value.v.str);
                if (metric->type >= 0) {
                    continue;
                }
            }
            break;

        case NGX_JSON_NUMBER:
            if (ngx_str_equals(elts[i].key, "default")) {
                metric->default_value = elts[i].value.v.num;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, query->pool->log, 0,
            "ngx_aggr_query_metric_json: invalid parameter \"%V\"",
            &elts[i].key);

        return NGX_BAD_QUERY;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_query_metrics_json(ngx_aggr_query_t *query,
    ngx_json_object_t *metrics)
{
    ngx_int_t              rc;
    ngx_uint_t             i, n;
    ngx_json_key_value_t  *elts;

    elts = metrics->elts;
    n = metrics->nelts;

    for (i = 0; i < n; i++) {
        if (elts[i].value.type != NGX_JSON_OBJECT) {
            ngx_log_error(NGX_LOG_ERR, query->pool->log, 0,
                "ngx_aggr_query_metrics_json: invalid type \"%V\"",
                &elts[i].key);
            return NGX_BAD_QUERY;
        }

        rc = ngx_aggr_query_metric_json(query, &elts[i].key,
            &elts[i].value.v.obj);
        if (rc != NGX_OK) {
            return rc;
        }
    }

    return NGX_OK;
}


/* shared */

static size_t
ngx_aggr_query_json_base_size(ngx_aggr_query_t *query)
{
    size_t                    size;
    ngx_str_t                *key;
    ngx_uint_t                i;
    ngx_aggr_query_dim_t     *dims;
    ngx_aggr_query_metric_t  *metrics;

    size = sizeof("{}\n");

    if (query->time_dim.len != 0) {
        size += sizeof("\"\":\"\",") - 1 + query->time_dim.len +
            NGX_ISO8601_TIMESTAMP_LEN;
    }

    dims = query->dims.elts;
    for (i = 0; i < query->dims.nelts; i++) {
        key = &dims[i].output;
        size += sizeof("\"\":\"\",") - 1 + key->len;
    }

    metrics = query->metrics.elts;
    for (i = 0; i < query->metrics.nelts; i++) {
        key = &metrics[i].output;
        size += sizeof("\"\":,") - 1 + key->len +
            NGX_AGGR_QUERY_DOUBLE_LEN;
    }

    return size;
}


static size_t
ngx_aggr_query_prom_base_size(ngx_aggr_query_t *query)
{
    size_t                    size;
    size_t                    label_size;
    ngx_str_t                *key;
    ngx_uint_t                i;
    ngx_aggr_query_dim_t     *dims;
    ngx_aggr_query_metric_t  *metrics;

    label_size = sizeof("{}") - 1;
    dims = query->dims.elts;
    for (i = 0; i < query->dims.nelts; i++) {
        key = &dims[i].output;
        label_size += key->len + sizeof("=\"\",") - 1;
    }

    size = 0;
    metrics = query->metrics.elts;
    for (i = 0; i < query->metrics.nelts; i++) {
        key = &metrics[i].output;
        size += key->len + label_size + sizeof(" \n") - 1 +
            NGX_AGGR_QUERY_DOUBLE_LEN;
    }

    return size;
}


static ngx_int_t
ngx_aggr_query_init(ngx_aggr_query_t *query, ngx_pool_t *temp_pool)
{
    ngx_hash_init_t  hash;

    ngx_conf_init_value(query->fmt, ngx_aggr_query_fmt_json);
    ngx_conf_init_value(query->granularity, 30);

    ngx_conf_init_uint_value(query->hash_max_size, 512);
    ngx_conf_init_uint_value(query->hash_bucket_size, 64);
    ngx_conf_init_size_value(query->max_event_size, 2048);
    ngx_conf_init_size_value(query->output_buf_size, 65536);

    hash.max_size = query->hash_max_size;
    hash.bucket_size = query->hash_bucket_size;

    hash.name = "metrics_hash";

    if (query->metrics_hash.buckets == NULL &&
        ngx_aggr_query_hash_init(query->pool, temp_pool, &query->metrics_hash,
            &query->metrics, &hash) != NGX_OK)
    {
        return NGX_ERROR;
    }

    ngx_aggr_query_dims_init(query, temp_pool, &hash);

    query->write_size[ngx_aggr_query_fmt_json] =
        ngx_aggr_query_json_base_size(query);

    query->write_size[ngx_aggr_query_fmt_prom] =
        ngx_aggr_query_prom_base_size(query);

    return NGX_OK;
}


static ngx_aggr_query_t *
ngx_aggr_query_create(ngx_pool_t *pool)
{
    ngx_aggr_query_t  *query;

    query = ngx_pcalloc(pool, sizeof(*query));
    if (query == NULL) {
        return NULL;
    }

    if (ngx_array_init(&query->dims, pool, 4,
                       sizeof(ngx_aggr_query_dim_t))
        != NGX_OK)
    {
        return NULL;
    }

    if (ngx_array_init(&query->metrics, pool, 4,
                       sizeof(ngx_aggr_query_metric_t))
        != NGX_OK)
    {
        return NULL;
    }

    query->pool = pool;

    query->fmt = NGX_CONF_UNSET;
    query->granularity = NGX_CONF_UNSET;

    query->hash_max_size = NGX_CONF_UNSET_UINT;
    query->hash_bucket_size = NGX_CONF_UNSET_UINT;
    query->max_event_size = NGX_CONF_UNSET_SIZE;
    query->output_buf_size = NGX_CONF_UNSET_SIZE;

    return query;
}


static char *
ngx_aggr_query_conf_handler(ngx_conf_t *cf, ngx_command_t *dummy, void *conf)
{
    ngx_str_t         *value;
    ngx_aggr_query_t  *query;

    value = cf->args->elts;

    query = cf->ctx;

    if (ngx_strcmp(value[0].data, "dim") == 0) {
        return ngx_aggr_query_dim_conf(cf, query);

    } else if (ngx_strcmp(value[0].data, "metric") == 0) {
        return ngx_aggr_query_metric_conf(cf, query);

    } else if (ngx_strcmp(value[0].data, "format") == 0) {
        if (cf->args->nelts != 2) {
            goto invalid;
        }

        query->fmt = ngx_aggr_query_enum(ngx_aggr_query_fmt_type_names,
            &value[1]);
        if (query->fmt < 0) {
            return "invalid value";
        }

        return NGX_CONF_OK;

    } else if (ngx_strcmp(value[0].data, "granularity") == 0) {
        if (cf->args->nelts != 2) {
            goto invalid;
        }

        query->granularity = ngx_parse_time(&value[1], 1);
        if (query->granularity == (time_t) NGX_ERROR) {
            return "invalid value";
        }

        return NGX_CONF_OK;

    } else if (ngx_strcmp(value[0].data, "hash_max_size") == 0) {
        if (cf->args->nelts != 2) {
            goto invalid;
        }

        query->hash_max_size = ngx_atoi(value[1].data, value[1].len);
        if (query->hash_max_size == (size_t) NGX_ERROR) {
            return "invalid value";
        }

        return NGX_CONF_OK;

    } else if (ngx_strcmp(value[0].data, "hash_bucket_size") == 0) {
        if (cf->args->nelts != 2) {
            goto invalid;
        }

        query->hash_bucket_size = ngx_atoi(value[1].data, value[1].len);
        if (query->hash_bucket_size == (size_t) NGX_ERROR) {
            return "invalid value";
        }

        return NGX_CONF_OK;

    } else if (ngx_strcmp(value[0].data, "max_event_size") == 0) {
        if (cf->args->nelts != 2) {
            goto invalid;
        }

        query->max_event_size = ngx_parse_size(&value[1]);
        if (query->max_event_size == (size_t) NGX_ERROR) {
            return "invalid value";
        }

        return NGX_CONF_OK;

    } else if (ngx_strcmp(value[0].data, "output_buf_size") == 0) {
        if (cf->args->nelts != 2) {
            goto invalid;
        }

        query->output_buf_size = ngx_parse_size(&value[1]);
        if (query->output_buf_size == (size_t) NGX_ERROR) {
            return "invalid value";
        }

        return NGX_CONF_OK;

    } else {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            " unknown directive \"%V\"", &value[0]);

        return NGX_CONF_ERROR;
    }

invalid:

    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
        "invalid number of arguments in \"%V\" directive", &value[0]);

    return NGX_CONF_ERROR;
}

ngx_aggr_query_t *
ngx_aggr_query_block(ngx_conf_t *cf, ngx_flag_t init)
{
    char              *rv;
    ngx_conf_t         save;
    ngx_aggr_query_t  *query;

    query = ngx_aggr_query_create(cf->pool);
    if (query == NULL) {
        return NULL;
    }

    save = *cf;
    cf->ctx = query;
    cf->handler = ngx_aggr_query_conf_handler;

    rv = ngx_conf_parse(cf, NULL);

    *cf = save;

    if (rv == NGX_CONF_ERROR) {
        return NULL;
    }

    if (rv != NGX_CONF_OK) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "%s", rv);
        return NULL;
    }

    if (!init) {
        return query;
    }

    if (query->dims.nelts <= 0 && query->metrics.nelts <= 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "no dim/metric specified");
        return NULL;
    }

    if (ngx_aggr_query_init(query, cf->temp_pool) != NGX_OK) {
        return NULL;
    }

    return query;
}


ngx_int_t
ngx_aggr_query_json(ngx_pool_t *pool, ngx_pool_t *temp_pool,
    ngx_json_value_t *json, ngx_aggr_query_t *base, ngx_aggr_query_t **result)
{
    ngx_int_t              rc;
    ngx_uint_t             i, n;
    ngx_aggr_query_t      *query;
    ngx_json_key_value_t  *elts;

    if (json->type != NGX_JSON_OBJECT) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_aggr_query_json: invalid type");
        return NGX_BAD_QUERY;
    }

    query = ngx_aggr_query_create(pool);
    if (query == NULL) {
        return NGX_ERROR;
    }

    if (base != NULL) {
        query->fmt = base->fmt;
        query->hash_max_size = base->hash_max_size;
        query->hash_bucket_size = base->hash_bucket_size;
        query->max_event_size = base->max_event_size;
        query->output_buf_size = base->output_buf_size;
    }

    elts = json->v.obj.elts;
    n = json->v.obj.nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_OBJECT:
            if (ngx_str_equals(elts[i].key, "dims")) {
                rc = ngx_aggr_query_dims_json(query, &elts[i].value.v.obj);
                if (rc != NGX_OK) {
                    return rc;
                }
                continue;

            } else if (ngx_str_equals(elts[i].key, "metrics")) {
                rc = ngx_aggr_query_metrics_json(query, &elts[i].value.v.obj);
                if (rc != NGX_OK) {
                    return rc;
                }
                continue;
            }

            break;
        }

        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_aggr_query_json: invalid parameter \"%V\"", &elts[i].key);

        return NGX_BAD_QUERY;
    }

    if (query->dims.nelts <= 0 && query->metrics.nelts <= 0) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "no dim/metric specified");
        return NGX_BAD_QUERY;
    }

    if (ngx_aggr_query_init(query, temp_pool) != NGX_OK) {
        return NGX_ERROR;
    }

    *result = query;

    return NGX_OK;
}
