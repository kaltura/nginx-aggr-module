#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr_query.h"


#define ngx_str_equals(s1, s2)                                              \
    ((s1).len == (s2).len && ngx_memcmp((s1).data, (s2).data, (s1).len) == 0)

#define ngx_str_equals_c(ns, s)                                             \
    ((ns).len == sizeof(s) - 1 &&                                           \
     ngx_strncmp((ns).data, (s), sizeof(s) - 1) == 0)


typedef struct {
    ngx_pool_t        *pool;
    ngx_pool_t        *temp_pool;
    ngx_aggr_query_t  *query;
} ngx_aggr_query_init_t;


typedef struct {
    ngx_str_t          input;          /* must be first */
    ngx_str_t          output;
    ngx_int_t          type;
    ngx_str_t          default_value;
    ngx_flag_t         lower;
    ngx_uint_t         index;
} ngx_aggr_query_dim_t;


typedef struct {
    ngx_str_t          input;          /* must be first */
    ngx_str_t          output;
    ngx_int_t          type;
    double             default_value;
    ngx_uint_t         index;
} ngx_aggr_query_metric_t;


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


/* a copy of ngx_hash_init with ngx_strlow replaced by ngx_memcpy */
#define NGX_HASH_ELT_SIZE(name)                                             \
    (sizeof(void *) + ngx_align((name)->key.len + 2, sizeof(void *)))

static ngx_int_t
ngx_hash_init_case_sensitive(ngx_hash_init_t *hinit, ngx_hash_key_t *names,
    ngx_uint_t nelts)
{
    u_char          *elts;
    size_t           len;
    u_short         *test;
    ngx_uint_t       i, n, key, size, start, bucket_size;
    ngx_hash_elt_t  *elt, **buckets;

    if (hinit->max_size == 0) {
        ngx_log_error(NGX_LOG_EMERG, hinit->pool->log, 0,
                      "could not build %s, you should "
                      "increase %s_max_size: %i",
                      hinit->name, hinit->name, hinit->max_size);
        return NGX_ERROR;
    }

    if (hinit->bucket_size > 65536 - ngx_cacheline_size) {
        ngx_log_error(NGX_LOG_EMERG, hinit->pool->log, 0,
                      "could not build %s, too large "
                      "%s_bucket_size: %i",
                      hinit->name, hinit->name, hinit->bucket_size);
        return NGX_ERROR;
    }

    for (n = 0; n < nelts; n++) {
        if (hinit->bucket_size < NGX_HASH_ELT_SIZE(&names[n]) + sizeof(void *))
        {
            ngx_log_error(NGX_LOG_EMERG, hinit->pool->log, 0,
                          "could not build %s, you should "
                          "increase %s_bucket_size: %i",
                          hinit->name, hinit->name, hinit->bucket_size);
            return NGX_ERROR;
        }
    }

    test = ngx_alloc(hinit->max_size * sizeof(u_short), hinit->pool->log);
    if (test == NULL) {
        return NGX_ERROR;
    }

    bucket_size = hinit->bucket_size - sizeof(void *);

    start = nelts / (bucket_size / (2 * sizeof(void *)));
    start = start ? start : 1;

    if (hinit->max_size > 10000 && nelts && hinit->max_size / nelts < 100) {
        start = hinit->max_size - 1000;
    }

    for (size = start; size <= hinit->max_size; size++) {

        ngx_memzero(test, size * sizeof(u_short));

        for (n = 0; n < nelts; n++) {
            if (names[n].key.data == NULL) {
                continue;
            }

            key = names[n].key_hash % size;
            len = test[key] + NGX_HASH_ELT_SIZE(&names[n]);

            if (len > bucket_size) {
                goto next;
            }

            test[key] = (u_short) len;
        }

        goto found;

    next:

        continue;
    }

    size = hinit->max_size;

    ngx_log_error(NGX_LOG_WARN, hinit->pool->log, 0,
                  "could not build optimal %s, you should increase "
                  "either %s_max_size: %i or %s_bucket_size: %i; "
                  "ignoring %s_bucket_size",
                  hinit->name, hinit->name, hinit->max_size,
                  hinit->name, hinit->bucket_size, hinit->name);

found:

    for (i = 0; i < size; i++) {
        test[i] = sizeof(void *);
    }

    for (n = 0; n < nelts; n++) {
        if (names[n].key.data == NULL) {
            continue;
        }

        key = names[n].key_hash % size;
        len = test[key] + NGX_HASH_ELT_SIZE(&names[n]);

        if (len > 65536 - ngx_cacheline_size) {
            ngx_log_error(NGX_LOG_EMERG, hinit->pool->log, 0,
                          "could not build %s, you should "
                          "increase %s_max_size: %i",
                          hinit->name, hinit->name, hinit->max_size);
            ngx_free(test);
            return NGX_ERROR;
        }

        test[key] = (u_short) len;
    }

    len = 0;

    for (i = 0; i < size; i++) {
        if (test[i] == sizeof(void *)) {
            continue;
        }

        test[i] = (u_short) (ngx_align(test[i], ngx_cacheline_size));

        len += test[i];
    }

    if (hinit->hash == NULL) {
        hinit->hash = ngx_pcalloc(hinit->pool, sizeof(ngx_hash_wildcard_t)
                                             + size * sizeof(ngx_hash_elt_t *));
        if (hinit->hash == NULL) {
            ngx_free(test);
            return NGX_ERROR;
        }

        buckets = (ngx_hash_elt_t **)
                      ((u_char *) hinit->hash + sizeof(ngx_hash_wildcard_t));

    } else {
        buckets = ngx_pcalloc(hinit->pool, size * sizeof(ngx_hash_elt_t *));
        if (buckets == NULL) {
            ngx_free(test);
            return NGX_ERROR;
        }
    }

    elts = ngx_palloc(hinit->pool, len + ngx_cacheline_size);
    if (elts == NULL) {
        ngx_free(test);
        return NGX_ERROR;
    }

    elts = ngx_align_ptr(elts, ngx_cacheline_size);

    for (i = 0; i < size; i++) {
        if (test[i] == sizeof(void *)) {
            continue;
        }

        buckets[i] = (ngx_hash_elt_t *) elts;
        elts += test[i];
    }

    for (i = 0; i < size; i++) {
        test[i] = 0;
    }

    for (n = 0; n < nelts; n++) {
        if (names[n].key.data == NULL) {
            continue;
        }

        key = names[n].key_hash % size;
        elt = (ngx_hash_elt_t *) ((u_char *) buckets[key] + test[key]);

        elt->value = names[n].value;
        elt->len = (u_short) names[n].key.len;

        ngx_memcpy(elt->name, names[n].key.data, names[n].key.len);

        test[key] = (u_short) (test[key] + NGX_HASH_ELT_SIZE(&names[n]));
    }

    for (i = 0; i < size; i++) {
        if (buckets[i] == NULL) {
            continue;
        }

        elt = (ngx_hash_elt_t *) ((u_char *) buckets[i] + test[i]);

        elt->value = NULL;
    }

    ngx_free(test);

    hinit->hash->buckets = buckets;
    hinit->hash->size = size;

    return NGX_OK;
}


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
ngx_aggr_query_hash_init(ngx_aggr_query_init_t *init, ngx_hash_t *hash,
    ngx_array_t *arr, ngx_hash_init_t  *hi)
{
    void            **pelt;
    ngx_str_t        *elt;
    ngx_uint_t        i;
    ngx_array_t      *value;
    ngx_array_t       hash_keys;
    ngx_hash_key_t   *hk;

    if (ngx_array_init(&hash_keys, init->temp_pool, 4, sizeof(ngx_hash_key_t))
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

                value = ngx_array_create(init->pool, 1, sizeof(void *));
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
    hi->pool = init->pool;
    hi->temp_pool = NULL;

    if (ngx_hash_init_case_sensitive(hi, hash_keys.elts, hash_keys.nelts) 
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    return NGX_OK;
}


/* dim */

static void
ngx_aggr_query_dim_init(ngx_aggr_query_dim_t *dim, ngx_str_t *name)
{
    ngx_memzero(dim, sizeof(*dim));

    dim->input = *name;
    dim->output = *name;

    /*
     * set by ngx_memzero()
     *
     *     dim->type = ngx_aggr_query_dim_group;
     *     dim->default_value.len = 0;
     *     dim->default_value.data = NULL;
     *     dim->lower = 0;
     */
}


static ngx_aggr_query_dim_in_t *
ngx_aggr_query_dim_get_input(ngx_aggr_query_t *query,
    ngx_aggr_query_dim_t *dim)
{
    ngx_uint_t                i, n;
    ngx_aggr_query_dim_in_t  *input;

    input = query->dims_in.elts;
    n = query->dims_in.nelts;

    for (i = 0; i < n; i++) {
        if (ngx_str_equals(input[i].name, dim->input) &&
            ngx_str_equals(input[i].default_value, dim->default_value) &&
            input[i].lower == dim->lower)
        {
            if (dim->type < input[i].type) {
                input[i].type = dim->type;
            }

            return &input[i];
        }
    }

    input = ngx_array_push(&query->dims_in);
    if (input == NULL) {
        return NULL;
    }

    input->name = dim->input;
    input->lower = dim->lower;
    input->offset = query->dims_in.nelts - 1;
    input->type = dim->type;
    input->default_value = dim->default_value;

    return input;
}


static ngx_int_t
ngx_aggr_query_dim_push(ngx_aggr_query_t *query, ngx_aggr_query_dim_t *dim)
{
    ngx_aggr_query_dim_in_t   *input;
    ngx_aggr_query_dim_out_t  *output;

    input = ngx_aggr_query_dim_get_input(query, dim);
    if (input == NULL) {
        return NGX_ERROR;
    }

    output = ngx_array_push(&query->dims_out);
    if (output == NULL) {
        return NGX_ERROR;
    }

    output->name = dim->output;
    output->offset = input->offset;

    return NGX_OK;
}


static char *
ngx_aggr_query_dim_conf(ngx_conf_t *cf, ngx_aggr_query_t *query)
{
    ngx_str_t              cur;
    ngx_str_t             *value;
    ngx_uint_t             i;
    ngx_aggr_query_dim_t   dim;

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

    ngx_aggr_query_dim_init(&dim, &value[1]);

    for (i = 2; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "input=", 6) == 0) {

            dim.input.data = value[i].data + 6;
            dim.input.len = value[i].len - 6;

            continue;
        }

        if (ngx_strncmp(value[i].data, "default=", 8) == 0) {

            dim.default_value.data = value[i].data + 8;
            dim.default_value.len = value[i].len - 8;

            continue;
        }

        if (ngx_strncmp(value[i].data, "type=", 5) == 0) {

            cur.data = value[i].data + 5;
            cur.len = value[i].len - 5;

            dim.type = ngx_aggr_query_enum(ngx_aggr_query_dim_type_names,
                &cur);
            if (dim.type >= 0) {
                continue;
            }
        }

        if (ngx_strcmp(value[i].data, "lower") == 0) {
            dim.lower = 1;
            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "invalid parameter \"%V\"", &value[i]);

        return NGX_CONF_ERROR;
    }

    if (ngx_aggr_query_dim_push(query, &dim) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_aggr_query_dim_json(ngx_aggr_query_init_t *init, ngx_str_t *name,
    ngx_json_object_t *attrs)
{
    ngx_uint_t             i, n;
    ngx_aggr_query_t      *query;
    ngx_json_key_value_t  *elts;
    ngx_aggr_query_dim_t   dim;

    query = init->query;

    elts = attrs->elts;
    n = attrs->nelts;

    if (n == 1 && ngx_str_equals_c(elts[0].key, "type") &&
        elts[0].value.type == NGX_JSON_STRING &&
        ngx_str_equals_c(elts[0].value.v.str, "time"))
    {
        if (query->time_dim.len != 0) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_dim_json: duplicate time dim");
            return NGX_BAD_QUERY;
        }

        query->time_dim = elts[0].value.v.str;
        return NGX_OK;
    }

    ngx_aggr_query_dim_init(&dim, name);

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "input")) {
                dim.input = elts[i].value.v.str;
                continue;
            }

            if (ngx_str_equals_c(elts[i].key, "default")) {
                dim.default_value = elts[i].value.v.str;
                continue;
            }

            if (ngx_str_equals_c(elts[i].key, "type")) {
                dim.type = ngx_aggr_query_enum(ngx_aggr_query_dim_type_names,
                    &elts[i].value.v.str);
                if (dim.type >= 0) {
                    continue;
                }
            }
            break;

        case NGX_JSON_BOOL:
            if (ngx_str_equals_c(elts[i].key, "lower")) {
                dim.lower = elts[i].value.v.boolean;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_dim_json: invalid parameter \"%V\"", &elts[i].key);

        return NGX_BAD_QUERY;
    }

    if (ngx_aggr_query_dim_push(query, &dim) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


/* dims */

static ngx_int_t
ngx_aggr_query_dims_json(ngx_aggr_query_init_t *init, ngx_json_object_t *dims)
{
    ngx_int_t              rc;
    ngx_uint_t             i, n;
    ngx_json_key_value_t  *elts;

    elts = dims->elts;
    n = dims->nelts;

    for (i = 0; i < n; i++) {
        if (elts[i].value.type != NGX_JSON_OBJECT) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_dims_json: invalid type \"%V\"", &elts[i].key);
            return NGX_BAD_QUERY;
        }

        rc = ngx_aggr_query_dim_json(init, &elts[i].key,
            &elts[i].value.v.obj);
        if (rc != NGX_OK) {
            return rc;
        }
    }

    return NGX_OK;
}


static void
ngx_aggr_query_dims_set_in_offsets(ngx_aggr_query_t *query)
{
    ngx_int_t                 type;
    ngx_uint_t                i, n;
    ngx_uint_t                offset;
    ngx_aggr_query_dim_in_t  *input;

    input = query->dims_in.elts;
    n = query->dims_in.nelts;

    offset = 0;
    for (type = 0; type < ngx_aggr_query_dim_types; type++) {
        for (i = 0; i < n; i++) {
            if (input[i].type != type) {
                continue;
            }

            input[i].offset = offset;
            offset += sizeof(ngx_str_t *);

            query->size[type] += sizeof(ngx_str_t *);
        }
    }

    query->event_size = query->size[ngx_aggr_query_dim_group] +
        query->size[ngx_aggr_query_dim_select];
}


static void
ngx_aggr_query_dims_set_default(ngx_aggr_query_t *query)
{
    ngx_str_t                **default_value;
    ngx_uint_t                 i, n;
    ngx_aggr_query_dim_in_t   *input;

    input = query->dims_in.elts;
    n = query->dims_in.nelts;

    for (i = 0; i < n; i++) {
        if (input[i].default_value.len == 0) {
            continue;
        }

        default_value = (ngx_str_t **) (query->default_event +
            input[i].offset);
        *default_value = &input[i].default_value;
    }
}


static void
ngx_aggr_query_dims_set_out_offsets(ngx_aggr_query_t *query)
{
    ngx_uint_t                 index;
    ngx_uint_t                 i, n;
    ngx_aggr_query_dim_in_t   *input;
    ngx_aggr_query_dim_out_t  *output;

    input = query->dims_in.elts;

    output = query->dims_out.elts;
    n = query->dims_out.nelts;

    for (i = 0; i < n; i++) {
        index = output[i].offset;
        output[i].offset = input[index].offset;
    }
}


/* metric */

static void
ngx_aggr_query_metric_init(ngx_aggr_query_metric_t *metric, ngx_str_t *name)
{
    ngx_memzero(metric, sizeof(*metric));

    metric->input = *name;
    metric->output = *name;

    /*
     * set by ngx_memzero()
     *
     *     metric->type = ngx_aggr_query_metric_sum;
     *     metric->default_value = 0;
     */
}


static ngx_aggr_query_metric_in_t *
ngx_aggr_query_metric_get_input(ngx_aggr_query_t *query,
    ngx_aggr_query_metric_t *metric)
{
    ngx_uint_t                   i, n;
    ngx_aggr_query_metric_in_t  *input;

    input = query->metrics_in.elts;
    n = query->metrics_in.nelts;

    for (i = 0; i < n; i++) {
        if (ngx_str_equals(input[i].name, metric->input) &&
            input[i].type == metric->type)
        {
            return &input[i];
        }
    }

    input = ngx_array_push(&query->metrics_in);
    if (input == NULL) {
        return NULL;
    }

    input->name = metric->input;
    input->type = metric->type;
    input->offset = query->metrics_in.nelts - 1;
    input->default_value = metric->default_value;

    return input;
}


static ngx_int_t
ngx_aggr_query_metric_push(ngx_aggr_query_t *query,
    ngx_aggr_query_metric_t *metric)
{
    ngx_aggr_query_metric_in_t   *input;
    ngx_aggr_query_metric_out_t  *output;

    input = ngx_aggr_query_metric_get_input(query, metric);
    if (input == NULL) {
        return NGX_ERROR;
    }

    output = ngx_array_push(&query->metrics_out);
    if (output == NULL) {
        return NGX_ERROR;
    }

    output->name = metric->output;
    output->offset = input->offset;

    return NGX_OK;
}


static char *
ngx_aggr_query_metric_conf(ngx_conf_t *cf, ngx_aggr_query_t *query)
{
    u_char                   *end;
    ngx_str_t                 cur;
    ngx_str_t                *value;
    ngx_uint_t                i;
    ngx_aggr_query_metric_t   metric;

    if (cf->args->nelts < 2) {
        return "invalid number of arguments in \"metric\" directive";
    }

    value = cf->args->elts;

    ngx_aggr_query_metric_init(&metric, &value[1]);

    for (i = 2; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "input=", 6) == 0) {

            metric.input.data = value[i].data + 6;
            metric.input.len = value[i].len - 6;

            continue;
        }

        if (ngx_strncmp(value[i].data, "default=", 8) == 0) {

            metric.default_value = strtod(
                (char *) value[i].data + 8, (char **) &end);

            if (end == value[i].data + value[i].len) {
                continue;
            }
        }

        if (ngx_strncmp(value[i].data, "type=", 5) == 0) {

            cur.data = value[i].data + 5;
            cur.len = value[i].len - 5;

            metric.type = ngx_aggr_query_enum(
                ngx_aggr_query_metric_type_names, &cur);
            if (metric.type >= 0) {
                continue;
            }
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "invalid parameter \"%V\"", &value[i]);

        return NGX_CONF_ERROR;
    }

    if (ngx_aggr_query_metric_push(query, &metric) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_aggr_query_metric_json(ngx_aggr_query_init_t *init, ngx_str_t *name,
    ngx_json_object_t *attrs)
{
    ngx_uint_t                i, n;
    ngx_json_key_value_t     *elts;
    ngx_aggr_query_metric_t   metric;

    ngx_aggr_query_metric_init(&metric, name);

    elts = attrs->elts;
    n = attrs->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "input")) {
                metric.input = elts[i].value.v.str;
                continue;
            }

            if (ngx_str_equals_c(elts[i].key, "type")) {
                metric.type = ngx_aggr_query_enum(
                    ngx_aggr_query_metric_type_names, &elts[i].value.v.str);
                if (metric.type >= 0) {
                    continue;
                }
            }
            break;

        case NGX_JSON_NUMBER:
            if (ngx_str_equals_c(elts[i].key, "default")) {
                metric.default_value = elts[i].value.v.num;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_metric_json: invalid parameter \"%V\"",
            &elts[i].key);

        return NGX_BAD_QUERY;
    }

    if (ngx_aggr_query_metric_push(init->query, &metric) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


/* metrics */

static ngx_int_t
ngx_aggr_query_metrics_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *metrics)
{
    ngx_int_t              rc;
    ngx_uint_t             i, n;
    ngx_json_key_value_t  *elts;

    elts = metrics->elts;
    n = metrics->nelts;

    for (i = 0; i < n; i++) {
        if (elts[i].value.type != NGX_JSON_OBJECT) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_metrics_json: invalid type \"%V\"",
                &elts[i].key);
            return NGX_BAD_QUERY;
        }

        rc = ngx_aggr_query_metric_json(init, &elts[i].key,
            &elts[i].value.v.obj);
        if (rc != NGX_OK) {
            return rc;
        }
    }

    return NGX_OK;
}


static void
ngx_aggr_query_metrics_set_in_offsets(ngx_aggr_query_t *query)
{
    ngx_uint_t                   i, n;
    ngx_uint_t                   offset;
    ngx_aggr_query_metric_in_t  *input;

    input = query->metrics_in.elts;
    n = query->metrics_in.nelts;

    offset = query->event_size;
    for (i = 0; i < n; i++) {
        input[i].offset = offset;
        offset += sizeof(double);
    }

    query->event_size = offset;
}


static void
ngx_aggr_query_metrics_set_default(ngx_aggr_query_t *query)
{
    double                      *default_value;
    ngx_uint_t                   i, n;
    ngx_aggr_query_metric_in_t  *input;

    input = query->metrics_in.elts;
    n = query->metrics_in.nelts;

    for (i = 0; i < n; i++) {
        default_value = (double *) (query->default_event + input[i].offset);
        *default_value = input[i].default_value;
    }
}


static void
ngx_aggr_query_metrics_set_out_offsets(ngx_aggr_query_t *query)
{
    ngx_uint_t                    index;
    ngx_uint_t                    i, n;
    ngx_aggr_query_metric_in_t   *input;
    ngx_aggr_query_metric_out_t  *output;

    input = query->metrics_in.elts;

    output = query->metrics_out.elts;
    n = query->metrics_out.nelts;

    for (i = 0; i < n; i++) {
        index = output[i].offset;
        output[i].offset = input[index].offset;
    }
}


/* shared */

static size_t
ngx_aggr_query_json_base_size(ngx_aggr_query_t *query)
{
    size_t                        size;
    ngx_str_t                    *key;
    ngx_uint_t                    i, n;
    ngx_aggr_query_dim_out_t     *dims;
    ngx_aggr_query_metric_out_t  *metrics;

    size = sizeof("{}\n");

    if (query->time_dim.len != 0) {
        size += sizeof("\"\":\"\",") - 1 + query->time_dim.len +
            NGX_ISO8601_TIMESTAMP_LEN;
    }


    dims = query->dims_out.elts;
    n = query->dims_out.nelts;

    for (i = 0; i < n; i++) {
        key = &dims[i].name;
        size += sizeof("\"\":\"\",") - 1 + key->len;
    }


    metrics = query->metrics_out.elts;
    n = query->metrics_out.nelts;

    for (i = 0; i < n; i++) {
        key = &metrics[i].name;
        size += sizeof("\"\":,") - 1 + key->len +
            NGX_AGGR_QUERY_DOUBLE_LEN;
    }

    return size;
}


static size_t
ngx_aggr_query_prom_base_size(ngx_aggr_query_t *query)
{
    size_t                        size;
    size_t                        label_size;
    ngx_str_t                    *key;
    ngx_uint_t                    i, n;
    ngx_aggr_query_dim_out_t     *dims;
    ngx_aggr_query_metric_out_t  *metrics;

    label_size = sizeof("{}") - 1;

    dims = query->dims_out.elts;
    n = query->dims_out.nelts;

    for (i = 0; i < n; i++) {
        key = &dims[i].name;
        label_size += key->len + sizeof("=\"\",") - 1;
    }


    size = 0;

    metrics = query->metrics_out.elts;
    n = query->metrics_out.nelts;

    for (i = 0; i < n; i++) {
        key = &metrics[i].name;
        size += key->len + label_size + sizeof(" \n") - 1 +
            NGX_AGGR_QUERY_DOUBLE_LEN;
    }

    return size;
}


static ngx_int_t
ngx_aggr_query_init(ngx_aggr_query_init_t *init, ngx_pool_t *temp_pool)
{
    ngx_hash_init_t    hash;
    ngx_aggr_query_t  *query;

    query = init->query;

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
        ngx_aggr_query_hash_init(init, &query->metrics_hash,
            &query->metrics_in, &hash) != NGX_OK)
    {
        return NGX_ERROR;
    }

    hash.name = "dims_hash";

    if (ngx_aggr_query_hash_init(init, &query->dims_hash,
        &query->dims_in, &hash) != NGX_OK)
    {
        return NGX_ERROR;
    }


    ngx_aggr_query_dims_set_in_offsets(query);
    ngx_aggr_query_metrics_set_in_offsets(query);

    query->default_event = ngx_pcalloc(init->pool, query->event_size);
    if (query->default_event == NULL) {
        return NGX_ERROR;
    }

    ngx_aggr_query_dims_set_default(query);
    ngx_aggr_query_metrics_set_default(query);

    ngx_aggr_query_dims_set_out_offsets(query);
    ngx_aggr_query_metrics_set_out_offsets(query);


    query->write_size[ngx_aggr_query_fmt_json] =
        ngx_aggr_query_json_base_size(query);

    query->write_size[ngx_aggr_query_fmt_prom] =
        ngx_aggr_query_prom_base_size(query);

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_query_create(ngx_aggr_query_init_t *init)
{
    ngx_aggr_query_t  *query;

    query = ngx_pcalloc(init->pool, sizeof(*query));
    if (query == NULL) {
        return NGX_ERROR;
    }

    if (ngx_array_init(&query->dims_out, init->pool, 4,
                       sizeof(ngx_aggr_query_dim_out_t))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_array_init(&query->metrics_out, init->pool, 4,
                       sizeof(ngx_aggr_query_metric_out_t))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_array_init(&query->dims_in, init->pool, 4,
                       sizeof(ngx_aggr_query_dim_in_t)) != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_array_init(&query->metrics_in, init->pool, 4,
                       sizeof(ngx_aggr_query_metric_in_t)) != NGX_OK)
    {
        return NGX_ERROR;
    }

    query->fmt = NGX_CONF_UNSET;
    query->granularity = NGX_CONF_UNSET;

    query->hash_max_size = NGX_CONF_UNSET_UINT;
    query->hash_bucket_size = NGX_CONF_UNSET_UINT;
    query->max_event_size = NGX_CONF_UNSET_SIZE;
    query->output_buf_size = NGX_CONF_UNSET_SIZE;

    init->query = query;

    return NGX_OK;
}


static char *
ngx_aggr_query_conf_handler(ngx_conf_t *cf, ngx_command_t *dummy, void *conf)
{
    ngx_str_t              *value;
    ngx_aggr_query_t       *query;
    ngx_aggr_query_init_t  *init;

    value = cf->args->elts;

    init = cf->ctx;
    query = init->query;

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
ngx_aggr_query_block(ngx_conf_t *cf, ngx_flag_t do_init)
{
    char                   *rv;
    ngx_conf_t              save;
    ngx_aggr_query_t       *query;
    ngx_aggr_query_init_t   init;

    init.pool = cf->pool;
    init.temp_pool = cf->temp_pool;

    if (ngx_aggr_query_create(&init) != NGX_OK) {
        return NULL;
    }

    query = init.query;

    save = *cf;
    cf->ctx = &init;
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

    if (!do_init) {
        return query;
    }

    if (query->dims_out.nelts <= 0 && query->metrics_out.nelts <= 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "no dim/metric specified");
        return NULL;
    }

    if (ngx_aggr_query_init(&init, cf->temp_pool) != NGX_OK) {
        return NULL;
    }

    return query;
}


ngx_int_t
ngx_aggr_query_json(ngx_pool_t *pool, ngx_pool_t *temp_pool,
    ngx_json_value_t *json, ngx_aggr_query_t *base, ngx_aggr_query_t **result)
{
    ngx_int_t               rc;
    ngx_uint_t              i, n;
    ngx_aggr_query_t       *query;
    ngx_json_key_value_t   *elts;
    ngx_aggr_query_init_t   init;

    if (json->type != NGX_JSON_OBJECT) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_aggr_query_json: invalid type");
        return NGX_BAD_QUERY;
    }

    init.pool = pool;
    init.temp_pool = temp_pool;

    if (ngx_aggr_query_create(&init) != NGX_OK) {
        return NGX_ERROR;
    }

    query = init.query;

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
            if (ngx_str_equals_c(elts[i].key, "dims")) {
                rc = ngx_aggr_query_dims_json(&init, &elts[i].value.v.obj);
                if (rc != NGX_OK) {
                    return rc;
                }
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "metrics")) {
                rc = ngx_aggr_query_metrics_json(&init, &elts[i].value.v.obj);
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

    if (query->dims_out.nelts <= 0 && query->metrics_out.nelts <= 0) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "no dim/metric specified");
        return NGX_BAD_QUERY;
    }

    if (ngx_aggr_query_init(&init, temp_pool) != NGX_OK) {
        return NGX_ERROR;
    }

    *result = query;

    return NGX_OK;
}
