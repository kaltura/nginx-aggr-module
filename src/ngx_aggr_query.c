#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


#define ngx_str_equals(s1, s2)                                              \
    ((s1).len == (s2).len && ngx_memcmp((s1).data, (s2).data, (s1).len) == 0)

#define ngx_str_equals_c(ns, s)                                             \
    ((ns).len == sizeof(s) - 1 &&                                           \
     ngx_strncmp((ns).data, (s), sizeof(s) - 1) == 0)


#define NGX_AGGR_MODULE           0x52474741   /* "AGGR" */

#define NGX_AGGR_MAIN_CONF        0x02000000
#define NGX_AGGR_FILTER_CONF      0x04000000
#define NGX_AGGR_HAVING_CONF      0x08000000

#define NGX_AGGR_MAIN_CONF_OFFSET    offsetof(ngx_aggr_conf_ctx_t, main_conf)
#define NGX_AGGR_FILTER_CONF_OFFSET  offsetof(ngx_aggr_conf_ctx_t, filter_conf)


typedef struct {
    ngx_str_t              input;
    ngx_str_t              output;
    ngx_int_t              type;
    double                 default_value;
    ngx_uint_t             index;
} ngx_aggr_query_metric_t;


typedef ngx_int_t (*ngx_aggr_query_filter_json_pt)(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data);


typedef struct {
    ngx_str_t                      name;
    ngx_aggr_query_filter_json_pt  parse;
    ngx_aggr_query_filter_pt       handler;
} ngx_aggr_query_filter_json_t;


static ngx_int_t ngx_aggr_query_filter_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, ngx_aggr_query_filter_t *filter);

static char *ngx_aggr_query_dim_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static char *ngx_aggr_query_metric_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static char *ngx_aggr_query_filter_block(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static char *ngx_aggr_query_filter_match_conf(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);

static char *ngx_aggr_query_filter_regex_conf(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);

static char *ngx_aggr_query_filter_compare_conf(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);


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
      ngx_aggr_query_filter_block,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, filter),
      ngx_aggr_filter_and },

    { ngx_string("having"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_query_filter_block,
      NGX_AGGR_MAIN_CONF_OFFSET,
      offsetof(ngx_aggr_query_t, having),
      ngx_aggr_filter_and },

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

    { ngx_string("in"),
      NGX_AGGR_FILTER_CONF|NGX_CONF_2MORE,
      ngx_aggr_query_filter_match_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_in },

    { ngx_string("contains"),
      NGX_AGGR_FILTER_CONF|NGX_CONF_2MORE,
      ngx_aggr_query_filter_match_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_contains },

#if (NGX_PCRE)
    { ngx_string("regex"),
      NGX_AGGR_FILTER_CONF|NGX_CONF_2MORE,
      ngx_aggr_query_filter_regex_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_regex },
#endif

    { ngx_string("gt"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_TAKE2,
      ngx_aggr_query_filter_compare_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_gt },

    { ngx_string("lt"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_TAKE2,
      ngx_aggr_query_filter_compare_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_lt },

    { ngx_string("gte"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_TAKE2,
      ngx_aggr_query_filter_compare_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_gte },

    { ngx_string("lte"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_TAKE2,
      ngx_aggr_query_filter_compare_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_lte },

    { ngx_string("and"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_query_filter_block,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_and },

    { ngx_string("or"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_query_filter_block,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_or },

    { ngx_string("not"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_query_filter_block,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_not },

      ngx_null_command
};


static ngx_aggr_module_t  ngx_aggr_query_module_ctx = {
    NULL,                                  /* create main configuration */
    NULL                                   /* init main configuration */
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

static ngx_str_t  ngx_aggr_query_filter_type = ngx_string("type");


static ngx_uint_t  ngx_aggr_max_module;


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
    ngx_array_t *hash_keys, ngx_str_t *key, void *elt)
{
    void            **pelt;
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

    *pelt = elt;

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

    query = init->query;

    input = ngx_aggr_query_dim_input_get(query, dim);
    if (input != NULL) {
        return input;
    }

    input = ngx_aggr_query_dim_input_add(query, dim);
    if (input == NULL) {
        return NULL;
    }

    if (ngx_aggr_query_hash_key_add(init, &init->dim_hash_keys, &input->name,
                                    input)
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

    query = init->query;

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

    query = init->query;

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



    init = cf->handler_conf;
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


static ngx_aggr_query_metric_in_t *
ngx_aggr_query_metric_input_get(ngx_aggr_query_init_t *init,
    ngx_aggr_query_metric_t *metric)
{
    ngx_uint_t                   i, n;
    ngx_aggr_query_t            *query;
    ngx_aggr_query_metric_in_t  *input;

    query = init->query;

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
                                    &input->name, input)
        != NGX_OK)
    {
        return NULL;
    }

    return input;
}


static ngx_aggr_query_metric_out_t *
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
    ngx_aggr_query_metric_in_t   *input;
    ngx_aggr_query_metric_out_t  *output;

    input = ngx_aggr_query_metric_input_get(init, metric);
    if (input == NULL) {
        return NGX_ERROR;
    }

    output = ngx_array_push(&init->query->metrics_out);
    if (output == NULL) {
        return NGX_ERROR;
    }

    output->name = metric->output;
    output->offset = input->offset;

    return NGX_OK;
}


static char *
ngx_aggr_query_metric_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    u_char                   *end;
    ngx_str_t                 cur;
    ngx_str_t                *value;
    ngx_uint_t                i;
    ngx_aggr_query_init_t    *init;
    ngx_aggr_query_metric_t   metric;

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


    init = cf->handler_conf;
    if (ngx_aggr_query_metric_push(init, &metric) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_aggr_query_metric_json(ngx_aggr_query_init_t *init, ngx_str_t *name,
    ngx_json_object_t *attrs)
{
    ngx_int_t                 top;
    ngx_uint_t                i, n;
    ngx_aggr_query_t         *query;
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
        query = init->query;

        if (query->top_count > 0) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_metric_json: "
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

    query = init->query;
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


/* filter */

static ngx_int_t
ngx_aggr_query_str_hash(ngx_pool_t *pool, ngx_str_hash_t *dst, ngx_str_t *src,
    ngx_flag_t lower)
{
    if (lower) {
        dst->s.data = ngx_pnalloc(pool, src->len);
        if (dst->s.data == NULL) {
            return NGX_ERROR;
        }

        ngx_strlow(dst->s.data, src->data, src->len);
        dst->s.len = src->len;

    } else {
        dst->s = *src;
    }

    dst->hash = ngx_hash_key(dst->s.data, dst->s.len);

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_query_filter_get_metric_offset(ngx_aggr_query_init_t *init,
    ngx_aggr_query_metric_t *metric, ngx_uint_t *offset)
{
    ngx_aggr_query_metric_in_t   *input;
    ngx_aggr_query_metric_out_t  *output;

    switch (init->ctx) {

    case ngx_aggr_query_ctx_filter:
        input = ngx_aggr_query_metric_input_get(init, metric);
        if (input == NULL) {
            return NGX_ERROR;
        }

        *offset = input->offset;
        break;

    case ngx_aggr_query_ctx_having:
        output = ngx_aggr_query_metric_output_get(init->query, &metric->input);
        if (output == NULL) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_filter_get_metric_offset: "
                "unknown metric \"%V\"", &metric->input);
            return NGX_BAD_QUERY;
        }

        *offset = output->offset;
        break;

    default:
        *offset = 0;
    }

    return NGX_OK;
}


static void
ngx_aggr_query_filter_dims_set_offsets(ngx_aggr_query_init_t *init)
{
    ngx_uint_t                 i, n;
    ngx_uint_t                 index;
    ngx_uint_t               **offp;
    ngx_aggr_query_dim_in_t   *input;

    input = init->query->dims_in.elts;

    offp = init->dim_temp_offs.elts;
    n = init->dim_temp_offs.nelts;

    for (i = 0; i < n; i++) {
        index = *offp[i];
        *offp[i] = input[index].temp_offset;
    }
}


static void
ngx_aggr_query_filter_metrics_set_offsets(ngx_aggr_query_init_t *init)
{
    ngx_uint_t                    i, n;
    ngx_uint_t                    index;
    ngx_uint_t                  **offp;
    ngx_aggr_query_metric_in_t   *input;

    input = init->query->metrics_in.elts;

    offp = init->metric_offs.elts;
    n = init->metric_offs.nelts;

    for (i = 0; i < n; i++) {
        index = *offp[i];
        *offp[i] = input[index].offset;
    }
}


/* filter json */

static ngx_str_hash_t *
ngx_aggr_query_copy_json_str_list(ngx_pool_t *pool, ngx_json_array_t *arr,
    ngx_flag_t lower)
{
    ngx_str_t         *src;
    ngx_str_hash_t    *dst;
    ngx_str_hash_t    *list;
    ngx_array_part_t  *part;

    list = ngx_palloc(pool, sizeof(list[0]) * arr->count);
    if (list == NULL) {
        return NULL;
    }

    dst = list;
    part = &arr->part;

    for (src = part->first; ; src++) {

        if ((void *) src >= part->last) {
            if (part->next == NULL) {
                break;
            }

            part = part->next;
            src = part->first;
        }

        if (ngx_aggr_query_str_hash(pool, dst, src, lower) != NGX_OK) {
            return NULL;
        }

        dst++;
    }

    return list;
}


static ngx_int_t
ngx_aggr_query_filter_match_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    ngx_uint_t                       i, n;
    ngx_uint_t                     **offp;
    ngx_json_array_t                *values;
    ngx_json_key_value_t            *elts;
    ngx_aggr_query_dim_t             dim;
    ngx_aggr_query_dim_in_t         *input;
    ngx_aggr_query_filter_match_t   *ctx;

    values = NULL;
    ngx_memzero(&dim, sizeof(dim));

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "type")) {
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "dim")) {
                dim.input = elts[i].value.v.str;
                continue;
            }
            break;

        case NGX_JSON_BOOL:
            if (ngx_str_equals_c(elts[i].key, "case_sensitive")) {
                dim.lower = !elts[i].value.v.boolean;
                continue;
            }
            break;

        case NGX_JSON_ARRAY:
            if (ngx_str_equals_c(elts[i].key, "values") &&
                elts[i].value.v.arr.type == NGX_JSON_STRING)
            {
                values = &elts[i].value.v.arr;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_match_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (dim.input.data == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_match_json: missing \"dim\" key");
        return NGX_BAD_QUERY;
    }

    if (values == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_match_json: missing \"values\" key");
        return NGX_BAD_QUERY;
    }


    dim.type = ngx_aggr_query_dim_temp;

    input = ngx_aggr_query_dim_input_get_complex(init, &dim);
    if (input == NULL) {
        return NGX_ERROR;
    }


    ctx = ngx_palloc(init->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->values = ngx_aggr_query_copy_json_str_list(init->pool, values,
        dim.lower);
    if (ctx->values == NULL) {
        return NGX_ERROR;
    }

    ctx->values_len = values->count;
    ctx->temp_offset = input->offset;

    offp = ngx_array_push(&init->dim_temp_offs);
    if (offp == NULL) {
        return NGX_ERROR;
    }

    *offp = &ctx->temp_offset;

    *data = ctx;

    return NGX_OK;
}


#if (NGX_PCRE)
static ngx_int_t
ngx_aggr_query_filter_regex_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    ngx_str_t                       *pattern;
    ngx_uint_t                       i, n;
    ngx_uint_t                     **offp;
    ngx_flag_t                       case_sensitive;
    ngx_regex_compile_t              rc;
    ngx_json_key_value_t            *elts;
    ngx_aggr_query_dim_t             dim;
    ngx_aggr_query_dim_in_t         *input;
    ngx_aggr_query_filter_regex_t   *ctx;
    u_char                           errstr[NGX_MAX_CONF_ERRSTR];

    ngx_memzero(&dim, sizeof(dim));
    case_sensitive = 1;
    pattern = NULL;

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "type")) {
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "dim")) {
                dim.input = elts[i].value.v.str;
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "pattern")) {
                pattern = &elts[i].value.v.str;
                continue;
            }
            break;

        case NGX_JSON_BOOL:
            if (ngx_str_equals_c(elts[i].key, "case_sensitive")) {
                case_sensitive = elts[i].value.v.boolean;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_regex_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (dim.input.data == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_regex_json: missing \"dim\" key");
        return NGX_BAD_QUERY;
    }

    if (pattern == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_regex_json: missing \"pattern\" key");
        return NGX_BAD_QUERY;
    }


    dim.type = ngx_aggr_query_dim_temp;

    input = ngx_aggr_query_dim_input_get_complex(init, &dim);
    if (input == NULL) {
        return NGX_ERROR;
    }


    ngx_memzero(&rc, sizeof(ngx_regex_compile_t));

    /* the pattern must be null terminated */
    rc.pattern.len = pattern->len;
    rc.pattern.data = ngx_pnalloc(init->temp_pool, rc.pattern.len + 1);
    if (rc.pattern.data == NULL) {
        return NGX_ERROR;
    }

    ngx_memcpy(rc.pattern.data, pattern->data, rc.pattern.len);
    rc.pattern.data[rc.pattern.len] = '\0';

    rc.pool = init->pool;
    rc.options = !case_sensitive ? NGX_REGEX_CASELESS : 0;
    rc.err.len = NGX_MAX_CONF_ERRSTR;
    rc.err.data = errstr;

    if (ngx_regex_compile(&rc) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_regex_json: %V", &rc.err);
        return NGX_BAD_QUERY;
    }


    ctx = ngx_palloc(init->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->re = rc.regex;
    ctx->temp_offset = input->offset;

    offp = ngx_array_push(&init->dim_temp_offs);
    if (offp == NULL) {
        return NGX_ERROR;
    }

    *offp = &ctx->temp_offset;

    *data = ctx;

    return NGX_OK;
}
#endif


static ngx_int_t
ngx_aggr_query_filter_compare_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    double                             value;
    ngx_int_t                          rc;
    ngx_uint_t                         i, n;
    ngx_uint_t                         offset;
    ngx_uint_t                       **offp;
    ngx_json_key_value_t              *elts;
    ngx_aggr_query_metric_t            metric;
    ngx_aggr_query_filter_compare_t   *ctx;

    ngx_memzero(&metric, sizeof(metric));
    value = 0;

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "type")) {
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "metric")) {
                metric.input = elts[i].value.v.str;
                continue;
            }
            break;

        case NGX_JSON_NUMBER:
            if (ngx_str_equals_c(elts[i].key, "value")) {
                value = elts[i].value.v.num;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_compare_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (metric.input.data == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_compare_json: missing \"metric\" key");
        return NGX_BAD_QUERY;
    }


    metric.type = ngx_aggr_query_metric_sum;

    rc = ngx_aggr_query_filter_get_metric_offset(init, &metric, &offset);
    if (rc != NGX_OK) {
        return rc;
    }


    ctx = ngx_palloc(init->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->value = value;
    ctx->offset = offset;

    offp = ngx_array_push(&init->metric_offs);
    if (offp == NULL) {
        return NGX_ERROR;
    }

    *offp = &ctx->offset;

    *data = ctx;

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_query_filter_nest_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    ngx_int_t                 rc;
    ngx_uint_t                i, n;
    ngx_json_object_t        *nest;
    ngx_json_key_value_t     *elts;
    ngx_aggr_query_filter_t  *ctx;

    nest = NULL;

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "type")) {
                continue;
            }
            break;

        case NGX_JSON_OBJECT:
            if (ngx_str_equals_c(elts[i].key, "filter")) {
                nest = &elts[i].value.v.obj;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_nest_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (nest == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_nest_json: missing \"filter\" key");
        return NGX_BAD_QUERY;
    }


    ctx = ngx_palloc(init->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    rc = ngx_aggr_query_filter_json(init, nest, ctx);
    if (rc != NGX_OK) {
        return rc;
    }

    *data = ctx;

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_query_filters_json(ngx_aggr_query_init_t *init,
    ngx_json_array_t *src_arr, ngx_array_t *dst_arr)
{
    ngx_int_t                 rc;
    ngx_array_part_t         *part;
    ngx_json_object_t        *src;
    ngx_aggr_query_filter_t  *dst;

    if (ngx_array_init(dst_arr, init->pool, src_arr->count,
                       sizeof(ngx_aggr_query_filter_t)) != NGX_OK)
    {
        return NGX_ERROR;
    }

    part = &src_arr->part;

    for (src = part->first; ; src++) {

        if ((void *) src >= part->last) {
            if (part->next == NULL) {
                break;
            }

            part = part->next;
            src = part->first;
        }

        dst = ngx_array_push(dst_arr);
        if (dst == NULL) {
            return NGX_ERROR;
        }

        rc = ngx_aggr_query_filter_json(init, src, dst);
        if (rc != NGX_OK) {
            return rc;
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_query_filter_group_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    ngx_int_t                       rc;
    ngx_uint_t                      i, n;
    ngx_json_array_t               *filters;
    ngx_json_key_value_t           *elts;
    ngx_aggr_query_filter_group_t  *ctx;

    filters = NULL;

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "type")) {
                continue;
            }
            break;

        case NGX_JSON_ARRAY:
            if (ngx_str_equals_c(elts[i].key, "filters") &&
                elts[i].value.v.arr.type == NGX_JSON_OBJECT)
            {
                filters = &elts[i].value.v.arr;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_group_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (filters == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_group_json: missing \"filters\" key");
        return NGX_BAD_QUERY;
    }


    ctx = ngx_palloc(init->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    rc = ngx_aggr_query_filters_json(init, filters, &ctx->filters);
    if (rc != NGX_OK) {
        return rc;
    }

    *data = ctx;

    return NGX_OK;
}


static ngx_aggr_query_filter_json_t  ngx_aggr_query_json_filters[] = {

    { ngx_string("in"), ngx_aggr_query_filter_match_json,
        ngx_aggr_filter_in },
    { ngx_string("contains"), ngx_aggr_query_filter_match_json,
        ngx_aggr_filter_contains },
#if (NGX_PCRE)
    { ngx_string("regex"), ngx_aggr_query_filter_regex_json,
        ngx_aggr_filter_regex },
#endif

    { ngx_string("gt"), ngx_aggr_query_filter_compare_json,
        ngx_aggr_filter_gt },
    { ngx_string("lt"), ngx_aggr_query_filter_compare_json,
        ngx_aggr_filter_lt },
    { ngx_string("gte"), ngx_aggr_query_filter_compare_json,
        ngx_aggr_filter_gte },
    { ngx_string("lte"), ngx_aggr_query_filter_compare_json,
        ngx_aggr_filter_lte },

    { ngx_string("and"), ngx_aggr_query_filter_group_json,
        ngx_aggr_filter_and },
    { ngx_string("or"), ngx_aggr_query_filter_group_json,
        ngx_aggr_filter_or },
    { ngx_string("not"), ngx_aggr_query_filter_nest_json,
        ngx_aggr_filter_not },

    { ngx_null_string, NULL, NULL }
};


static ngx_aggr_query_filter_json_t  ngx_aggr_query_json_having[] = {

    { ngx_string("gt"), ngx_aggr_query_filter_compare_json,
        ngx_aggr_filter_gt },
    { ngx_string("lt"), ngx_aggr_query_filter_compare_json,
        ngx_aggr_filter_lt },
    { ngx_string("gte"), ngx_aggr_query_filter_compare_json,
        ngx_aggr_filter_gte },
    { ngx_string("lte"), ngx_aggr_query_filter_compare_json,
        ngx_aggr_filter_lte },

    { ngx_string("and"), ngx_aggr_query_filter_group_json,
        ngx_aggr_filter_and },
    { ngx_string("or"), ngx_aggr_query_filter_group_json,
        ngx_aggr_filter_or },
    { ngx_string("not"), ngx_aggr_query_filter_nest_json,
        ngx_aggr_filter_not },

    { ngx_null_string, NULL, NULL }
};


static ngx_json_value_t *
ngx_aggr_query_json_object_get(ngx_json_object_t *obj, ngx_str_t *name)
{
    ngx_uint_t             i, n;
    ngx_json_key_value_t  *elts;

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {
        if (elts[i].key.len == name->len &&
            ngx_strncmp(elts[i].key.data, name->data, name->len) == 0)
        {
            return &elts[i].value;
        }
    }

    return NULL;
}


static ngx_int_t
ngx_aggr_query_filter_json(ngx_aggr_query_init_t *init, ngx_json_object_t *obj,
    ngx_aggr_query_filter_t *filter)
{
    ngx_int_t                      rc;
    ngx_str_t                     *type;
    ngx_json_value_t              *value;
    ngx_aggr_query_filter_json_t  *cur;

    value = ngx_aggr_query_json_object_get(obj, &ngx_aggr_query_filter_type);
    if (value == NULL || value->type != NGX_JSON_STRING) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_query_filter_json: missing \"type\" property");
        return NGX_BAD_QUERY;
    }

    type = &value->v.str;


    switch (init->ctx) {

    case ngx_aggr_query_ctx_filter:
        cur = ngx_aggr_query_json_filters;
        break;

    case ngx_aggr_query_ctx_having:
        cur = ngx_aggr_query_json_having;
        break;

    default:
        cur = NULL;
    }

    for ( ;; ) {

        if (cur->name.len <= 0) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                "ngx_aggr_query_filter_json: "
                "invalid filter type \"%V\"", type);
            return NGX_BAD_QUERY;
        }

        if (cur->name.len == type->len &&
            ngx_strncmp(cur->name.data, type->data, type->len) == 0)
        {
            break;
        }

        cur++;
    }

    rc = cur->parse(init, obj, &filter->data);
    if (rc != NGX_OK) {
        return rc;
    }

    filter->handler = cur->handler;

    return NGX_OK;
}


/* filter conf */

static ngx_str_hash_t *
ngx_aggr_query_copy_str_list(ngx_pool_t *pool, ngx_str_t *src,
    ngx_uint_t count, ngx_flag_t lower)
{
    ngx_str_t       *end;
    ngx_str_hash_t  *dst;
    ngx_str_hash_t  *list;

    list = ngx_palloc(pool, sizeof(list[0]) * count);
    if (list == NULL) {
        return NULL;
    }

    for (end = src + count, dst = list; src < end; src++, dst++) {
        if (ngx_aggr_query_str_hash(pool, dst, src, lower) != NGX_OK) {
            return NULL;
        }
    }

    return list;
}


static char *
ngx_aggr_query_filter_match_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_str_t                       *value;
    ngx_uint_t                       i, n;
    ngx_uint_t                     **offp;
    ngx_aggr_query_dim_t             dim;
    ngx_aggr_query_init_t           *init;
    ngx_aggr_query_dim_in_t         *input;
    ngx_aggr_query_filter_t         *filter;
    ngx_aggr_query_filter_match_t   *ctx;

    ngx_memzero(&dim, sizeof(dim));

    value = cf->args->elts;
    n = cf->args->nelts;

    for (i = 2; ; i++) {

        if (i >= n) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "invalid number of arguments in \"%V\" directive", value);
            return NGX_CONF_ERROR;
        }

        if (ngx_strncmp(value[i].data, "case_sensitive=o", 16) == 0) {
            if (ngx_strcmp(&value[i].data[16], "n") == 0) {
                dim.lower = 0;

            } else if (ngx_strcmp(&value[i].data[16], "ff") == 0) {
                dim.lower = 1;

            } else {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid param \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        break;
    }

    dim.input = value[1];
    dim.type = ngx_aggr_query_dim_temp;

    init = cf->handler_conf;
    input = ngx_aggr_query_dim_input_get_complex(init, &dim);
    if (input == NULL) {
        return NGX_CONF_ERROR;
    }


    ctx = ngx_palloc(cf->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    ctx->values_len = n - i;
    ctx->values = ngx_aggr_query_copy_str_list(cf->pool, value + i,
        ctx->values_len, dim.lower);
    if (ctx->values == NULL) {
        return NGX_CONF_ERROR;
    }

    ctx->temp_offset = input->offset;

    offp = ngx_array_push(&init->dim_temp_offs);
    if (offp == NULL) {
        return NGX_CONF_ERROR;
    }

    *offp = &ctx->temp_offset;


    filter = ngx_array_push((ngx_array_t *) conf);
    if (filter == NULL) {
        return NGX_CONF_ERROR;
    }

    filter->handler = cmd->post;
    filter->data = ctx;

    return NGX_CONF_OK;
}


#if (NGX_PCRE)
static char *
ngx_aggr_query_filter_regex_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_str_t                       *value;
    ngx_uint_t                       i, n;
    ngx_uint_t                     **offp;
    ngx_flag_t                       case_sensitive;
    ngx_regex_compile_t              rc;
    ngx_aggr_query_dim_t             dim;
    ngx_aggr_query_init_t           *init;
    ngx_aggr_query_dim_in_t         *input;
    ngx_aggr_query_filter_t         *filter;
    ngx_aggr_query_filter_regex_t   *ctx;
    u_char                           errstr[NGX_MAX_CONF_ERRSTR];

    ngx_memzero(&dim, sizeof(dim));
    case_sensitive = 1;

    value = cf->args->elts;
    n = cf->args->nelts;

    for (i = 2; i < n; i++) {

        if (ngx_strncmp(value[i].data, "case_sensitive=o", 16) == 0) {
            if (ngx_strcmp(&value[i].data[16], "n") == 0) {
                case_sensitive = 1;

            } else if (ngx_strcmp(&value[i].data[16], "ff") == 0) {
                case_sensitive = 0;

            } else {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid param \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        break;
    }

    if (i != n - 1) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "invalid number of arguments in \"%V\" directive", value);
        return NGX_CONF_ERROR;
    }

    dim.input = value[1];
    dim.type = ngx_aggr_query_dim_temp;

    init = cf->handler_conf;
    input = ngx_aggr_query_dim_input_get_complex(init, &dim);
    if (input == NULL) {
        return NGX_CONF_ERROR;
    }


    ngx_memzero(&rc, sizeof(ngx_regex_compile_t));
    rc.pattern = value[n - 1];
    rc.pool = init->pool;
    rc.options = !case_sensitive ? NGX_REGEX_CASELESS : 0;
    rc.err.len = NGX_MAX_CONF_ERRSTR;
    rc.err.data = errstr;

    if (ngx_regex_compile(&rc) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "%V", &rc.err);
        return NGX_CONF_ERROR;
    }


    ctx = ngx_palloc(cf->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    ctx->re = rc.regex;
    ctx->temp_offset = input->offset;

    offp = ngx_array_push(&init->dim_temp_offs);
    if (offp == NULL) {
        return NGX_CONF_ERROR;
    }

    *offp = &ctx->temp_offset;


    filter = ngx_array_push((ngx_array_t *) conf);
    if (filter == NULL) {
        return NGX_CONF_ERROR;
    }

    filter->handler = cmd->post;
    filter->data = ctx;

    return NGX_CONF_OK;
}
#endif


static char *
ngx_aggr_query_filter_compare_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    u_char                            *end;
    ngx_str_t                         *value;
    ngx_uint_t                         offset;
    ngx_uint_t                       **offp;
    ngx_aggr_query_init_t             *init;
    ngx_aggr_query_metric_t            metric;
    ngx_aggr_query_filter_t           *filter;
    ngx_aggr_query_filter_compare_t   *ctx;

    value = cf->args->elts;

    ngx_memzero(&metric, sizeof(metric));
    metric.input = value[1];
    metric.type = ngx_aggr_query_metric_sum;

    init = cf->handler_conf;
    if (ngx_aggr_query_filter_get_metric_offset(init, &metric, &offset)
        != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }


    ctx = ngx_palloc(cf->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    ctx->value = strtod((char *) value[2].data, (char **) &end);
    if (end != value[2].data + value[2].len) {
        return "invalid number";
    }

    ctx->offset = offset;

    offp = ngx_array_push(&init->metric_offs);
    if (offp == NULL) {
        return NGX_CONF_ERROR;
    }

    *offp = &ctx->offset;


    filter = ngx_array_push((ngx_array_t *) conf);
    if (filter == NULL) {
        return NGX_CONF_ERROR;
    }

    filter->handler = cmd->post;
    filter->data = ctx;

    return NGX_CONF_OK;
}


static char *
ngx_aggr_query_filter_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char                           *rv, *p;
    ngx_conf_t                      save;
    ngx_aggr_conf_ctx_t            *conf_ctx;
    ngx_aggr_query_init_t          *init;
    ngx_aggr_query_filter_t        *filter;
    ngx_aggr_query_filter_pt        handler;
    ngx_aggr_query_filter_group_t  *ctx;

    ctx = ngx_palloc(cf->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    if (ngx_array_init(&ctx->filters, cf->pool, 1,
                       sizeof(ngx_aggr_query_filter_t))
        != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }

    save = *cf;

    conf_ctx = cf->ctx;
    ngx_aggr_set_filter_ctx(conf_ctx, &ctx->filters, ngx_aggr_query_module);

    if (cf->cmd_type == NGX_AGGR_MAIN_CONF) {

        init = cf->handler_conf;
        if (cmd->offset == offsetof(ngx_aggr_query_t, filter)) {
            cf->cmd_type = NGX_AGGR_FILTER_CONF;
            init->ctx = ngx_aggr_query_ctx_filter;

        } else if (cmd->offset == offsetof(ngx_aggr_query_t, having)) {
            cf->cmd_type = NGX_AGGR_HAVING_CONF;
            init->ctx = ngx_aggr_query_ctx_having;

        } else {
            return "internal error";
        }

        p = conf;
        filter = (ngx_aggr_query_filter_t *) (p + cmd->offset);

    } else {
        filter = ngx_array_push((ngx_array_t *) conf);
        if (filter == NULL) {
            return NGX_CONF_ERROR;
        }
    }

    rv = ngx_conf_parse(cf, NULL);

    *cf = save;

    if (cf->cmd_type == NGX_AGGR_MAIN_CONF) {
        ngx_aggr_set_filter_ctx(conf_ctx, NULL, ngx_aggr_query_module);

    } else {
        ngx_aggr_set_filter_ctx(conf_ctx, conf, ngx_aggr_query_module);
    }

    if (rv != NGX_CONF_OK) {
        return rv;
    }

    handler = cmd->post;

    if (handler == ngx_aggr_filter_not) {
        filter->data = ngx_palloc(cf->pool, sizeof(*filter));
        if (filter->data == NULL) {
            return NGX_CONF_ERROR;
        }

        filter->handler = handler;
        filter = filter->data;

        handler = ngx_aggr_filter_and;
    }

    if (ctx->filters.nelts == 1) {
        *filter = *(ngx_aggr_query_filter_t *) ctx->filters.elts;

    } else {
        filter->data = ctx;
        filter->handler = handler;
    }

    return NGX_CONF_OK;
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

    if (ngx_aggr_variables_init_vars(init) != NGX_OK) {
        return NGX_ERROR;
    }

    hash.key = ngx_hash_key;
    hash.pool = init->pool;
    hash.temp_pool = NULL;

    hash.name = "metrics_hash";
    hash.hash = &query->metrics_hash;
    hash.max_size = query->metrics_hash_max_size;
    hash.bucket_size = query->metrics_hash_bucket_size;

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

    ngx_aggr_query_filter_dims_set_offsets(init);
    ngx_aggr_query_filter_metrics_set_offsets(init);


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

    if (ngx_array_init(&query->dims_complex, init->pool, 1,
                       sizeof(ngx_aggr_query_dim_complex_t)) != NGX_OK)
    {
        return NGX_ERROR;
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

    init->query = query;

    if (ngx_aggr_variables_add_core_vars(init) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


ngx_aggr_query_t *
ngx_aggr_query_block(ngx_conf_t *cf, ngx_flag_t do_init)
{
    char                   *rv;
    ngx_uint_t              m, mi;
    ngx_conf_t              save;
    ngx_aggr_query_t       *query;
    ngx_aggr_module_t      *module;
    ngx_aggr_conf_ctx_t     ctx;
    ngx_aggr_query_init_t   init;

    init.pool = cf->pool;
    init.temp_pool = cf->temp_pool;

    if (ngx_aggr_query_create(&init) != NGX_OK) {
        return NULL;
    }

    query = init.query;

    ngx_aggr_max_module = ngx_count_modules(cf->cycle, NGX_AGGR_MODULE);

    ctx.main_conf = ngx_pcalloc(cf->temp_pool,
                                sizeof(void *) * ngx_aggr_max_module);
    if (ctx.main_conf == NULL) {
        return NULL;
    }

    ctx.filter_conf = ngx_pcalloc(cf->temp_pool,
                                  sizeof(void *) * ngx_aggr_max_module);
    if (ctx.filter_conf == NULL) {
        return NULL;
    }

    ctx.main_conf[ngx_aggr_query_module.ctx_index] = query;

    for (m = 0; cf->cycle->modules[m]; m++) {
        if (cf->cycle->modules[m]->type != NGX_AGGR_MODULE) {
            continue;
        }

        module = cf->cycle->modules[m]->ctx;
        mi = cf->cycle->modules[m]->ctx_index;

        if (module->create_main_conf) {
            ctx.main_conf[mi] = module->create_main_conf(cf);
            if (ctx.main_conf[mi] == NULL) {
                return NGX_CONF_ERROR;
            }
        }
    }

    save = *cf;

    cf->ctx = &ctx;

    cf->module_type = NGX_AGGR_MODULE;
    cf->cmd_type = NGX_AGGR_MAIN_CONF;
    cf->handler_conf = &init;

    rv = ngx_conf_parse(cf, NULL);

    if (rv != NGX_CONF_OK) {
        if (rv != NGX_CONF_ERROR) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "%s", rv);
        }

        *cf = save;
        return NULL;
    }

    if (!do_init) {
        *cf = save;
        return query;
    }

    for (m = 0; cf->cycle->modules[m]; m++) {
        if (cf->cycle->modules[m]->type != NGX_AGGR_MODULE) {
            continue;
        }

        module = cf->cycle->modules[m]->ctx;
        mi = cf->cycle->modules[m]->ctx_index;

        if (module->init_main_conf) {
            rv = module->init_main_conf(cf, ctx.main_conf[mi]);
            if (rv != NGX_CONF_OK) {
                if (rv != NGX_CONF_ERROR) {
                    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "%s", rv);
                }

                *cf = save;
                return NULL;
            }
        }
    }

    *cf = save;

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
                rc = ngx_aggr_query_filter_json(&init, &elts[i].value.v.obj,
                    &query->filter);
                if (rc != NGX_OK) {
                    return rc;
                }
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "having")) {
                init.ctx = ngx_aggr_query_ctx_having;
                rc = ngx_aggr_query_filter_json(&init, &elts[i].value.v.obj,
                    &query->having);
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

    if (ngx_aggr_query_init(&init, temp_pool) != NGX_OK) {
        return NGX_ERROR;
    }

    *result = query;

    return NGX_OK;
}
