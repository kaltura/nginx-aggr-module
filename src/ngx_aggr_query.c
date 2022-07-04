#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"
#include "ngx_aggr_map.h"
#include "ngx_aggr_map_ip2l.h"


static void *ngx_aggr_query_create(ngx_aggr_query_init_t *init);

static ngx_int_t ngx_aggr_query_init(ngx_aggr_query_init_t *init, void *conf);

static char *ngx_aggr_query_dim_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static char *ngx_aggr_query_metric_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_conf_enum_t  ngx_query_formats[] = {
    { ngx_string("json"), ngx_aggr_query_fmt_json },
    { ngx_string("prom"), ngx_aggr_query_fmt_prom },
    { ngx_null_string, 0 }
};


static ngx_command_t  ngx_aggr_query_commands[] = {

    { ngx_string("dim"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_1MORE,
      ngx_aggr_query_dim_conf,
      NGX_AGGR_MAIN_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("metric"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_1MORE,
      ngx_aggr_query_metric_conf,
      NGX_AGGR_MAIN_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("filter"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_filter_block,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, filter),
      NULL },

    { ngx_string("having"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_filter_block,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, having),
      NULL },

    { ngx_string("format"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_enum_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, fmt),
      &ngx_query_formats },

    { ngx_string("granularity"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_sec_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, granularity),
      NULL },

    { ngx_string("dims_hash_max_size"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, dims_hash_max_size),
      NULL },

    { ngx_string("dims_hash_bucket_size"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, dims_hash_bucket_size),
      NULL },

    { ngx_string("metrics_hash_max_size"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, metrics_hash_max_size),
      NULL },

    { ngx_string("metrics_hash_bucket_size"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, metrics_hash_bucket_size),
      NULL },

    { ngx_string("variables_hash_max_size"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, variables_hash_max_size),
      NULL },

    { ngx_string("variables_hash_bucket_size"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, variables_hash_bucket_size),
      NULL },

    { ngx_string("max_event_size"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, max_event_size),
      NULL },

    { ngx_string("output_buf_size"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, output_buf_size),
      NULL },

      ngx_null_command
};


static ngx_aggr_module_t  ngx_aggr_query_module_ctx = {
    ngx_aggr_query_create,                 /* create main configuration */
    ngx_aggr_query_init                    /* init main configuration */
};


ngx_module_t  ngx_aggr_query_module = {
    NGX_MODULE_V1,
    &ngx_aggr_query_module_ctx,            /* module context */
    ngx_aggr_query_commands,               /* module directives */
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


static ngx_str_t  ngx_aggr_map_type = ngx_string("type");
static ngx_str_t  ngx_aggr_map_default_type = ngx_string("dim");


static ngx_uint_t  ngx_aggr_max_module = 0;


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
ngx_aggr_query_hash_key_add(ngx_aggr_query_init_t *init,
    ngx_array_t *hash_keys, ngx_str_t *key, ngx_uint_t offset)
{
    ngx_uint_t       *pelt;
    ngx_array_t      *value;
    ngx_hash_key_t   *hk;

    hk = ngx_aggr_query_hash_key_get(hash_keys, key);
    if (hk == NULL) {

        hk = ngx_array_push(hash_keys);
        if (hk == NULL) {
            return NGX_ERROR;
        }

        value = ngx_array_create(init->pool, 1, sizeof(void *));
        if (value == NULL) {
            return NGX_ERROR;
        }

        hk->key = *key;
        hk->key_hash = ngx_hash_key(key->data, key->len);
        hk->value = value;

    } else {
        value = hk->value;
    }

    pelt = ngx_array_push(value);
    if (pelt == NULL) {
        return NGX_ERROR;
    }

    *pelt = offset;

    return NGX_OK;
}


static void
ngx_aggr_query_hash_set_offsets(ngx_array_t *hash_keys, ngx_array_t *arr)
{
    ngx_uint_t       i, j;
    ngx_uint_t      *elts;
    ngx_array_t     *value;
    ngx_hash_key_t  *hk;

    hk = hash_keys->elts;
    for (i = 0; i < hash_keys->nelts; i++) {
        value = hk[i].value;

        elts = value->elts;
        for (j = 0; j < value->nelts; j++) {
            elts[j] = (ngx_uint_t) arr->elts + elts[j] * arr->size;
        }
    }
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


ngx_aggr_query_dim_in_t *
ngx_aggr_query_dim_input_get(ngx_aggr_query_t *query,
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

    return NULL;
}

ngx_aggr_query_dim_in_t *
ngx_aggr_query_dim_input_add(ngx_aggr_query_t *query,
    ngx_aggr_query_dim_t *dim)
{
    ngx_aggr_query_dim_in_t  *input;

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


ngx_aggr_query_dim_in_t *
ngx_aggr_query_dim_input_get_simple(ngx_aggr_query_init_t *init,
    ngx_aggr_query_dim_t *dim)
{
    ngx_aggr_query_t         *query;
    ngx_aggr_query_dim_in_t  *input;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    input = ngx_aggr_query_dim_input_get(query, dim);
    if (input != NULL) {
        return input;
    }

    input = ngx_aggr_query_dim_input_add(query, dim);
    if (input == NULL) {
        return NULL;
    }

    if (ngx_aggr_query_hash_key_add(init, &init->dim_hash_keys, &input->name,
                                    input->offset)
        != NGX_OK)
    {
        return NULL;
    }

    return input;
}


ngx_aggr_query_dim_in_t *
ngx_aggr_query_dim_input_get_complex(ngx_aggr_query_init_t *init,
    ngx_aggr_query_dim_t *dim)
{
    ngx_aggr_query_t                  *query;
    ngx_aggr_query_dim_in_t           *input;
    ngx_aggr_query_dim_complex_t      *complex;
    ngx_aggr_compile_complex_value_t   ccv;

    if (!ngx_aggr_script_variables_count(&dim->input)) {
        return ngx_aggr_query_dim_input_get_simple(init, dim);
    }

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    input = ngx_aggr_query_dim_input_get(query, dim);
    if (input != NULL) {
        return input;
    }

    input = ngx_aggr_query_dim_input_add(query, dim);
    if (input == NULL) {
        return NULL;
    }

    complex = ngx_array_push(&query->dims_complex);
    if (complex == NULL) {
        return NULL;
    }

    complex->temp_offset = input->offset;

    ngx_memzero(&ccv, sizeof(ngx_aggr_compile_complex_value_t));

    ccv.init = init;
    ccv.value = &dim->input;
    ccv.complex_value = &complex->value;

    if (ngx_aggr_compile_complex_value(&ccv) != NGX_OK) {
        return NULL;
    }

    return input;
}


static ngx_int_t
ngx_aggr_query_dim_push(ngx_aggr_query_init_t *init, ngx_aggr_query_dim_t *dim)
{
    ngx_aggr_query_t          *query;
    ngx_aggr_query_dim_in_t   *input;
    ngx_aggr_query_dim_out_t  *output;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    input = ngx_aggr_query_dim_input_get_complex(init, dim);
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
ngx_aggr_query_dim_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t               cur;
    ngx_str_t              *value;
    ngx_uint_t              i;
    ngx_aggr_query_t       *query;
    ngx_aggr_query_dim_t    dim;
    ngx_aggr_query_init_t  *init;

    query = conf;
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



    init = cf->ctx;
    if (ngx_aggr_query_dim_push(init, &dim) != NGX_OK) {
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

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

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

    if (ngx_aggr_query_dim_push(init, &dim) != NGX_OK) {
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
    ngx_uint_t                temp_offset;
    ngx_str_hash_t           *sh;
    ngx_aggr_query_dim_in_t  *input;

    input = query->dims_in.elts;
    n = query->dims_in.nelts;

    offset = 0;
    temp_offset = 0;

    for (type = 0; type < ngx_aggr_query_dim_types; type++) {
        for (i = 0; i < n; i++) {
            if (input[i].type != type) {
                continue;
            }

            if (input[i].default_value.len != 0) {
                sh = (ngx_str_hash_t *) (query->temp_default + temp_offset);
                sh->s = input[i].default_value;
                sh->hash = ngx_hash_key(sh->s.data, sh->s.len);
            }

            input[i].offset = offset;
            offset += sizeof(ngx_str_t *);

            input[i].temp_offset = temp_offset;
            temp_offset += sizeof(ngx_str_hash_t);

            query->size[type] += sizeof(ngx_str_t *);
        }
    }

    query->event_size = query->size[ngx_aggr_query_dim_group] +
        query->size[ngx_aggr_query_dim_select];
}


static void
ngx_aggr_query_dims_set_complex_offsets(ngx_aggr_query_t *query)
{
    ngx_uint_t                     index;
    ngx_uint_t                     i, n;
    ngx_aggr_query_dim_in_t       *input;
    ngx_aggr_query_dim_complex_t  *complex;

    input = query->dims_in.elts;

    complex = query->dims_complex.elts;
    n = query->dims_complex.nelts;

    for (i = 0; i < n; i++) {
        index = complex[i].temp_offset;
        complex[i].temp_offset = input[index].temp_offset;
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


ngx_aggr_query_metric_in_t *
ngx_aggr_query_metric_input_get(ngx_aggr_query_init_t *init,
    ngx_aggr_query_metric_t *metric)
{
    ngx_uint_t                   i, n;
    ngx_aggr_query_t            *query;
    ngx_aggr_query_metric_in_t  *input;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

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

    if (ngx_aggr_query_hash_key_add(init, &init->metric_hash_keys,
                                    &input->name, input->offset)
        != NGX_OK)
    {
        return NULL;
    }

    return input;
}


ngx_aggr_query_metric_out_t *
ngx_aggr_query_metric_output_get(ngx_aggr_query_t *query, ngx_str_t *name)
{
    ngx_uint_t                    i, n;
    ngx_aggr_query_metric_out_t  *output;

    output = query->metrics_out.elts;
    n = query->metrics_out.nelts;

    for (i = 0; i < n; i++) {
        if (ngx_str_equals(output[i].name, *name)) {
            return &output[i];
        }
    }

    return NULL;
}


static ngx_int_t
ngx_aggr_query_metric_push(ngx_aggr_query_init_t *init,
    ngx_aggr_query_metric_t *metric)
{
    ngx_aggr_query_t             *query;
    ngx_aggr_query_metric_in_t   *input;
    ngx_aggr_query_metric_out_t  *output;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    input = ngx_aggr_query_metric_input_get(init, metric);
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


static ngx_int_t
ngx_aggr_query_metric_top(ngx_aggr_query_init_t *init, ngx_int_t top)
{
    ngx_aggr_query_t  *query;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    if (query->top_count > 0) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_metric_top: "
            "multiple metrics marked with \"top\"");
        return NGX_BAD_QUERY;
    }

    init->top_index = query->metrics_out.nelts - 1;

    if (top > 0) {
        query->top_count = top;

    } else {
        query->top_count = -top;
        query->top_inverted = 1;
    }

    return NGX_OK;
}


static char *
ngx_aggr_query_metric_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    u_char                   *end;
    ngx_int_t                 top;
    ngx_str_t                 cur;
    ngx_str_t                *value;
    ngx_uint_t                i;
    ngx_flag_t                minus;
    ngx_aggr_query_init_t    *init;
    ngx_aggr_query_metric_t   metric;

    value = cf->args->elts;

    ngx_aggr_query_metric_init(&metric, &value[1]);
    top = 0;

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

        if (ngx_strncmp(value[i].data, "top=", 4) == 0) {

            cur.data = value[i].data + 4;
            cur.len = value[i].len - 4;

            if (cur.data[0] == '-') {
                minus = 1;
                cur.data++;
                cur.len--;

            } else {
                minus = 0;
            }

            top = ngx_atoi(cur.data, cur.len);
            if (top > 0) {
                if (minus) {
                    top = -top;
                }
                continue;
            }
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "invalid parameter \"%V\"", &value[i]);

        return NGX_CONF_ERROR;
    }


    init = cf->ctx;
    if (ngx_aggr_query_metric_push(init, &metric) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    if (top != 0) {
        if (ngx_aggr_query_metric_top(init, top) != NGX_OK) {
            return NGX_CONF_ERROR;
        }
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_aggr_query_metric_json(ngx_aggr_query_init_t *init, ngx_str_t *name,
    ngx_json_object_t *attrs)
{
    ngx_int_t                 top;
    ngx_uint_t                i, n;
    ngx_json_key_value_t     *elts;
    ngx_aggr_query_metric_t   metric;

    ngx_aggr_query_metric_init(&metric, name);
    top = 0;

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

            if (ngx_str_equals_c(elts[i].key, "top")) {
                top = (ngx_int_t) elts[i].value.v.num;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_metric_json: invalid parameter \"%V\"",
            &elts[i].key);

        return NGX_BAD_QUERY;
    }

    if (ngx_aggr_query_metric_push(init, &metric) != NGX_OK) {
        return NGX_ERROR;
    }

    if (top != 0) {
        if (ngx_aggr_query_metric_top(init, top) != NGX_OK) {
            return NGX_BAD_QUERY;
        }
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
    double                      *default_value;
    ngx_uint_t                   i, n;
    ngx_uint_t                   offset;
    ngx_aggr_query_metric_in_t  *input;

    input = query->metrics_in.elts;
    n = query->metrics_in.nelts;

    offset = 0;
    for (i = 0; i < n; i++) {
        default_value = (double *) (query->metrics_default + offset);
        *default_value = input[i].default_value;

        input[i].offset = query->event_size + offset;
        offset += sizeof(double);
    }

    query->event_size += offset;
}


static void
ngx_aggr_query_metrics_set_out_offsets(ngx_aggr_query_init_t *init)
{
    ngx_uint_t                    index;
    ngx_uint_t                    i, n;
    ngx_aggr_query_t             *query;
    ngx_aggr_query_metric_in_t   *input;
    ngx_aggr_query_metric_out_t  *output;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);
    input = query->metrics_in.elts;

    output = query->metrics_out.elts;
    n = query->metrics_out.nelts;

    for (i = 0; i < n; i++) {
        index = output[i].offset;
        output[i].offset = input[index].offset;
    }

    if (query->top_count > 0) {
        query->top_offset = output[init->top_index].offset;
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
ngx_aggr_query_postconfiguration(ngx_aggr_query_init_t *init,
    ngx_pool_t *temp_pool)
{
    ngx_uint_t          m, mi;
    ngx_cycle_t        *cycle;
    ngx_hash_init_t     hash;
    ngx_aggr_query_t   *query;
    ngx_aggr_module_t  *module;

    cycle = init->cycle;

    for (m = 0; cycle->modules[m]; m++) {
        if (cycle->modules[m]->type != NGX_AGGR_MODULE) {
            continue;
        }

        module = cycle->modules[m]->ctx;
        mi = cycle->modules[m]->ctx_index;

        if (module->init_main_conf) {
            if (module->init_main_conf(init, init->main_conf[mi]) != NGX_OK) {
                return NGX_ERROR;
            }
        }
    }

    if (ngx_aggr_variables_init_vars(init) != NGX_OK) {
        return NGX_ERROR;
    }

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    hash.key = ngx_hash_key;
    hash.pool = init->pool;
    hash.temp_pool = NULL;

    hash.name = "metrics_hash";
    hash.hash = &query->metrics_hash;
    hash.max_size = query->metrics_hash_max_size;
    hash.bucket_size = query->metrics_hash_bucket_size;

    ngx_aggr_query_hash_set_offsets(&init->metric_hash_keys,
        &query->metrics_in);

    if (ngx_hash_init_case_sensitive(&hash, init->metric_hash_keys.elts,
                                     init->metric_hash_keys.nelts)
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    hash.name = "dims_hash";
    hash.hash = &query->dims_hash;
    hash.max_size = query->dims_hash_max_size;
    hash.bucket_size = query->dims_hash_bucket_size;

    ngx_aggr_query_hash_set_offsets(&init->dim_hash_keys,
        &query->dims_in);

    if (ngx_hash_init_case_sensitive(&hash, init->dim_hash_keys.elts,
                                     init->dim_hash_keys.nelts)
        != NGX_OK)
    {
        return NGX_ERROR;
    }


    query->temp_size = query->dims_in.nelts * sizeof(ngx_str_hash_t);
    query->temp_default = ngx_pcalloc(init->pool, query->temp_size);
    if (query->temp_default == NULL) {
        return NGX_ERROR;
    }

    query->metrics_size = query->metrics_in.nelts * sizeof(double);
    query->metrics_default = ngx_pcalloc(init->pool, query->metrics_size);
    if (query->metrics_default == NULL) {
        return NGX_ERROR;
    }


    ngx_aggr_query_dims_set_in_offsets(query);
    ngx_aggr_query_metrics_set_in_offsets(query);

    ngx_aggr_query_dims_set_out_offsets(query);
    ngx_aggr_query_metrics_set_out_offsets(init);

    ngx_aggr_query_dims_set_complex_offsets(query);

    ngx_aggr_filter_dims_set_offsets(init);
    ngx_aggr_filter_metrics_set_offsets(init);


    query->write_size[ngx_aggr_query_fmt_json] =
        ngx_aggr_query_json_base_size(query);

    query->write_size[ngx_aggr_query_fmt_prom] =
        ngx_aggr_query_prom_base_size(query);

    return NGX_OK;
}


static void *
ngx_aggr_query_create(ngx_aggr_query_init_t *init)
{
    ngx_aggr_query_t  *query;

    query = ngx_pcalloc(init->pool, sizeof(*query));
    if (query == NULL) {
        return NULL;
    }

    if (ngx_array_init(&query->dims_out, init->pool, 4,
                       sizeof(ngx_aggr_query_dim_out_t))
        != NGX_OK)
    {
        return NULL;
    }

    if (ngx_array_init(&query->metrics_out, init->pool, 4,
                       sizeof(ngx_aggr_query_metric_out_t))
        != NGX_OK)
    {
        return NULL;
    }

    if (ngx_array_init(&query->dims_in, init->pool, 4,
                       sizeof(ngx_aggr_query_dim_in_t)) != NGX_OK)
    {
        return NULL;
    }

    if (ngx_array_init(&query->metrics_in, init->pool, 4,
                       sizeof(ngx_aggr_query_metric_in_t)) != NGX_OK)
    {
        return NULL;
    }

    if (ngx_array_init(&query->dims_complex, init->pool, 1,
                       sizeof(ngx_aggr_query_dim_complex_t)) != NGX_OK)
    {
        return NULL;
    }

    query->fmt = NGX_CONF_UNSET;
    query->granularity = NGX_CONF_UNSET;

    query->dims_hash_max_size = NGX_CONF_UNSET_UINT;
    query->dims_hash_bucket_size = NGX_CONF_UNSET_UINT;
    query->metrics_hash_max_size = NGX_CONF_UNSET_UINT;
    query->metrics_hash_bucket_size = NGX_CONF_UNSET_UINT;
    query->variables_hash_max_size = NGX_CONF_UNSET_UINT;
    query->variables_hash_bucket_size = NGX_CONF_UNSET_UINT;
    query->max_event_size = NGX_CONF_UNSET_SIZE;
    query->output_buf_size = NGX_CONF_UNSET_SIZE;

    return query;
}


static ngx_int_t
ngx_aggr_query_init(ngx_aggr_query_init_t *init, void *conf)
{
    ngx_aggr_query_t  *query;

    query = conf;

    ngx_conf_init_value(query->fmt, ngx_aggr_query_fmt_json);
    ngx_conf_init_value(query->granularity, 30);

    ngx_conf_init_uint_value(query->dims_hash_max_size, 512);
    ngx_conf_init_uint_value(query->dims_hash_bucket_size, 64);
    ngx_conf_init_uint_value(query->metrics_hash_max_size, 512);
    ngx_conf_init_uint_value(query->metrics_hash_bucket_size, 64);
    ngx_conf_init_size_value(query->max_event_size, 2048);
    ngx_conf_init_size_value(query->output_buf_size, 65536);

    ngx_conf_init_uint_value(query->variables_hash_max_size, 1024);
    ngx_conf_init_uint_value(query->variables_hash_bucket_size, 64);

    query->variables_hash_bucket_size =
              ngx_align(query->variables_hash_bucket_size, ngx_cacheline_size);

    if (query->ncaptures) {
        query->ncaptures = (query->ncaptures + 1) * 3;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_aggr_query_preconfiguration(ngx_aggr_query_init_t *init)
{
    ngx_uint_t          m, mi;
    ngx_cycle_t        *cycle;
    ngx_aggr_module_t  *module;

    if (ngx_array_init(&init->dim_hash_keys, init->temp_pool, 4,
                       sizeof(ngx_hash_key_t))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_array_init(&init->metric_hash_keys, init->temp_pool, 4,
                       sizeof(ngx_hash_key_t))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_array_init(&init->dim_temp_offs, init->temp_pool, 4,
                       sizeof(ngx_uint_t *))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_array_init(&init->metric_offs, init->temp_pool, 4,
                       sizeof(ngx_uint_t *))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    cycle = init->cycle;

    if (!ngx_aggr_max_module) {
        ngx_aggr_max_module = ngx_count_modules(cycle, NGX_AGGR_MODULE);
    }

    init->main_conf = ngx_pcalloc(init->temp_pool,
                                  sizeof(void *) * ngx_aggr_max_module);
    if (init->main_conf == NULL) {
        return NGX_ERROR;
    }

    init->filter_conf = ngx_pcalloc(init->temp_pool,
                                    sizeof(void *) * ngx_aggr_max_module);
    if (init->filter_conf == NULL) {
        return NGX_ERROR;
    }

    for (m = 0; cycle->modules[m]; m++) {
        if (cycle->modules[m]->type != NGX_AGGR_MODULE) {
            continue;
        }

        module = cycle->modules[m]->ctx;
        mi = cycle->modules[m]->ctx_index;

        if (module->create_main_conf) {
            init->main_conf[mi] = module->create_main_conf(init);
            if (init->main_conf[mi] == NULL) {
                return NGX_ERROR;
            }
        }
    }

    if (ngx_aggr_variables_add_core_vars(init) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


ngx_aggr_query_t *
ngx_aggr_query_block(ngx_conf_t *cf, ngx_flag_t do_init)
{
    char                   *rv;
    ngx_conf_t              save;
    ngx_aggr_query_t       *query;
    ngx_aggr_query_init_t   init;

    init.cycle = cf->cycle;
    init.pool = cf->pool;
    init.temp_pool = cf->temp_pool;

    if (ngx_aggr_query_preconfiguration(&init) != NGX_OK) {
        return NULL;
    }

    query = ngx_aggr_get_module_main_conf(&init, ngx_aggr_query_module);

    save = *cf;

    cf->ctx = &init;

    cf->module_type = NGX_AGGR_MODULE;
    cf->cmd_type = NGX_AGGR_MAIN_CONF;

    rv = ngx_conf_parse(cf, NULL);

    if (rv != NGX_CONF_OK) {
        if (rv != NGX_CONF_ERROR) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "%s", rv);
        }

        *cf = save;
        return NULL;
    }

    *cf = save;

    if (!do_init) {
        return query;
    }

    if (query->dims_out.nelts <= 0 && query->metrics_out.nelts <= 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "no dim/metric specified");
        return NULL;
    }

    if (ngx_aggr_query_postconfiguration(&init, cf->temp_pool) != NGX_OK) {
        return NULL;
    }

    return query;
}


static ngx_int_t
ngx_aggr_query_maps_json(ngx_aggr_query_init_t *init, ngx_json_object_t *obj)
{
    ngx_int_t              rc;
    ngx_str_t             *type;
    ngx_uint_t             i, n;
    ngx_json_value_t      *value;
    ngx_json_object_t     *map_obj;
    ngx_json_key_value_t  *elts;

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {
        if (elts[i].value.type != NGX_JSON_OBJECT) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_maps_json: invalid map object type");
            return NGX_BAD_QUERY;
        }

        map_obj = &elts[i].value.v.obj;

        value = ngx_json_object_get(map_obj, &ngx_aggr_map_type);
        if (value == NULL) {
            type = &ngx_aggr_map_default_type;

        } else if (value->type == NGX_JSON_STRING) {
            type = &value->v.str;

        } else {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_maps_json: invalid \"type\" property");
            return NGX_BAD_QUERY;
        }

        if (ngx_str_equals_c(*type, "dim")) {
            rc = ngx_aggr_map_json(init, &elts[i].key, map_obj);
            if (rc != NGX_OK) {
                return rc;
            }

        } else if (ngx_str_equals_c(*type, "ip2l")) {
            rc = ngx_aggr_map_ip2l_json(init, &elts[i].key, map_obj);
            if (rc != NGX_OK) {
                return rc;
            }

        } else {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_maps_json: invalid map type \"%V\"", type);
            return NGX_BAD_QUERY;
        }
    }

    return NGX_OK;
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

    init.cycle = (ngx_cycle_t *) ngx_cycle;
    init.pool = pool;
    init.temp_pool = temp_pool;

    if (ngx_aggr_query_preconfiguration(&init) != NGX_OK) {
        return NGX_ERROR;
    }

    query = ngx_aggr_get_module_main_conf(&init, ngx_aggr_query_module);

    if (base != NULL) {
        query->fmt = base->fmt;
        query->dims_hash_max_size = base->dims_hash_max_size;
        query->dims_hash_bucket_size = base->dims_hash_bucket_size;
        query->metrics_hash_max_size = base->metrics_hash_max_size;
        query->metrics_hash_bucket_size = base->metrics_hash_bucket_size;
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

            } else if (ngx_str_equals_c(elts[i].key, "filter")) {
                init.ctx = ngx_aggr_query_ctx_filter;
                rc = ngx_aggr_filter_json(&init, &elts[i].value.v.obj,
                    &query->filter);
                if (rc != NGX_OK) {
                    return rc;
                }
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "having")) {
                init.ctx = ngx_aggr_query_ctx_having;
                rc = ngx_aggr_filter_json(&init, &elts[i].value.v.obj,
                    &query->having);
                if (rc != NGX_OK) {
                    return rc;
                }
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "maps")) {
                rc = ngx_aggr_query_maps_json(&init, &elts[i].value.v.obj);
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
            "ngx_aggr_query_json: no dim/metric specified");
        return NGX_BAD_QUERY;
    }

    if (ngx_aggr_query_postconfiguration(&init, temp_pool) != NGX_OK) {
        return NGX_ERROR;
    }

    *result = query;

    return NGX_OK;
}
