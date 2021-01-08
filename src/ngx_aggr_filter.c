#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


typedef ngx_int_t (*ngx_aggr_filter_json_pt)(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data);


typedef struct {
    ngx_str_t                 name;
    ngx_aggr_filter_json_pt   parse;
    ngx_aggr_filter_pt        handler;
} ngx_aggr_filter_json_t;


typedef struct {
    ngx_array_t               filters;
} ngx_aggr_filter_group_t;


typedef struct {
    ngx_uint_t                temp_offset;
    ngx_str_hash_t           *values;
    ngx_uint_t                values_len;
} ngx_aggr_filter_match_t;


#if (NGX_PCRE)
typedef struct {
    ngx_uint_t                temp_offset;
    ngx_regex_t              *re;
} ngx_aggr_filter_regex_t;
#endif


typedef struct {
    ngx_uint_t                offset;
    double                    value;
} ngx_aggr_filter_compare_t;


static char *ngx_aggr_filter_match_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static char *ngx_aggr_filter_regex_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static char *ngx_aggr_filter_compare_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static ngx_flag_t ngx_aggr_filter_in(ngx_aggr_result_t *ar, void *data);

static ngx_flag_t ngx_aggr_filter_contains(ngx_aggr_result_t *ar, void *data);

#if (NGX_PCRE)
static ngx_flag_t ngx_aggr_filter_regex(ngx_aggr_result_t *ar, void *data);
#endif

static ngx_flag_t ngx_aggr_filter_and(ngx_aggr_result_t *ar, void *data);

static ngx_flag_t ngx_aggr_filter_or(ngx_aggr_result_t *ar, void *data);

static ngx_flag_t ngx_aggr_filter_not(ngx_aggr_result_t *ar, void *data);


static ngx_flag_t ngx_aggr_filter_gt(ngx_aggr_result_t *ar, void *data);

static ngx_flag_t ngx_aggr_filter_lt(ngx_aggr_result_t *ar, void *data);

static ngx_flag_t ngx_aggr_filter_gte(ngx_aggr_result_t *ar, void *data);

static ngx_flag_t ngx_aggr_filter_lte(ngx_aggr_result_t *ar, void *data);


static ngx_command_t  ngx_aggr_filter_commands[] = {

    { ngx_string("in"),
      NGX_AGGR_FILTER_CONF|NGX_CONF_2MORE,
      ngx_aggr_filter_match_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_in },

    { ngx_string("contains"),
      NGX_AGGR_FILTER_CONF|NGX_CONF_2MORE,
      ngx_aggr_filter_match_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_contains },

#if (NGX_PCRE)
    { ngx_string("regex"),
      NGX_AGGR_FILTER_CONF|NGX_CONF_2MORE,
      ngx_aggr_filter_regex_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_regex },
#endif

    { ngx_string("gt"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_TAKE2,
      ngx_aggr_filter_compare_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_gt },

    { ngx_string("lt"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_TAKE2,
      ngx_aggr_filter_compare_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_lt },

    { ngx_string("gte"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_TAKE2,
      ngx_aggr_filter_compare_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_gte },

    { ngx_string("lte"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_TAKE2,
      ngx_aggr_filter_compare_conf,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_lte },

    { ngx_string("and"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_filter_block,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_and },

    { ngx_string("or"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_filter_block,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_or },

    { ngx_string("not"),
      NGX_AGGR_FILTER_CONF|NGX_AGGR_HAVING_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
      ngx_aggr_filter_block,
      NGX_AGGR_FILTER_CONF_OFFSET,
      0,
      ngx_aggr_filter_not },

      ngx_null_command
};


static ngx_aggr_module_t  ngx_aggr_filter_module_ctx = {
    NULL,                                  /* create main configuration */
    NULL                                   /* init main configuration */
};


ngx_module_t  ngx_aggr_filter_module = {
    NGX_MODULE_V1,
    &ngx_aggr_filter_module_ctx,           /* module context */
    ngx_aggr_filter_commands,              /* module directives */
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


static ngx_str_t  ngx_aggr_filter_type = ngx_string("type");


/* result */

static ngx_flag_t
ngx_aggr_filter_strstr(ngx_str_t *haystack, ngx_str_t *needle)
{
    size_t   n;
    u_char   c1, c2;
    u_char  *s1, *s1_end;
    u_char  *s2;

    if (needle->len <= 0) {
        return 1;
    }

    if (needle->len > haystack->len) {
        return 0;
    }

    s1 = haystack->data;
    s1_end = s1 + haystack->len - needle->len;

    s2 = needle->data;
    n = needle->len - 1;

    c2 = *s2++;

    do {
        do {
            if (s1 > s1_end) {
                return 0;
            }

            c1 = *s1++;

        } while (c1 != c2);

    } while (ngx_memcmp(s1, s2, n) != 0);

    return 1;
}


static ngx_flag_t
ngx_aggr_filter_in(ngx_aggr_result_t *ar, void *data)
{
    ngx_uint_t                i;
    ngx_str_hash_t           *sh;
    ngx_aggr_filter_match_t  *filter;

    filter = data;
    sh = (ngx_str_hash_t *) (ar->temp_data + filter->temp_offset);

    for (i = 0; i < filter->values_len; i++) {
        if (filter->values[i].hash == sh->hash &&
            filter->values[i].s.len == sh->s.len &&
            ngx_memcmp(filter->values[i].s.data, sh->s.data, sh->s.len) == 0)
        {
            return 1;
        }
    }

    return 0;
}


static ngx_flag_t
ngx_aggr_filter_contains(ngx_aggr_result_t *ar, void *data)
{
    ngx_uint_t                i;
    ngx_str_hash_t           *sh;
    ngx_aggr_filter_match_t  *filter;

    filter = data;
    sh = (ngx_str_hash_t *) (ar->temp_data + filter->temp_offset);

    for (i = 0; i < filter->values_len; i++) {
        if (ngx_aggr_filter_strstr(&sh->s, &filter->values[i].s)) {
            return 1;
        }
    }

    return 0;
}


#if (NGX_PCRE)
static ngx_flag_t
ngx_aggr_filter_regex(ngx_aggr_result_t *ar, void *data)
{
    int                       rc;
    ngx_str_t                 s;
    ngx_str_hash_t           *sh;
    ngx_aggr_filter_regex_t  *filter;

    filter = data;
    sh = (ngx_str_hash_t *) (ar->temp_data + filter->temp_offset);

    s = sh->s;
    if (s.data == NULL) {
        s.data = (u_char *) "";        /* pcre_exec fails on null */
    }

    rc = ngx_regex_exec(filter->re, &s, NULL, 0);

    if (rc == NGX_REGEX_NO_MATCHED) {
        return 0;
    }

    if (rc >= 0) {
        return 1;
    }

    ngx_log_error(NGX_LOG_ALERT, ar->pool->log, 0,
        ngx_regex_exec_n " failed: %i on \"%V\"", rc, &s);

    return 0;
}
#endif


static ngx_flag_t
ngx_aggr_filter_gt(ngx_aggr_result_t *ar, void *data)
{
    double                      value;
    ngx_aggr_filter_compare_t  *filter;

    filter = data;
    value = *(double *) (ar->cur->data + filter->offset);

    return value > filter->value;
}


static ngx_flag_t
ngx_aggr_filter_lt(ngx_aggr_result_t *ar, void *data)
{
    double                      value;
    ngx_aggr_filter_compare_t  *filter;

    filter = data;
    value = *(double *) (ar->cur->data + filter->offset);

    return value < filter->value;
}


static ngx_flag_t
ngx_aggr_filter_gte(ngx_aggr_result_t *ar, void *data)
{
    double                      value;
    ngx_aggr_filter_compare_t  *filter;

    filter = data;
    value = *(double *) (ar->cur->data + filter->offset);

    return value >= filter->value;
}


static ngx_flag_t
ngx_aggr_filter_lte(ngx_aggr_result_t *ar, void *data)
{
    double                      value;
    ngx_aggr_filter_compare_t  *filter;

    filter = data;
    value = *(double *) (ar->cur->data + filter->offset);

    return value <= filter->value;
}


static ngx_flag_t
ngx_aggr_filter_and(ngx_aggr_result_t *ar, void *data)
{
    ngx_uint_t                i, n;
    ngx_aggr_filter_t        *elts;
    ngx_aggr_filter_group_t  *filter = data;

    elts = filter->filters.elts;
    n = filter->filters.nelts;

    for (i = 0; i < n; i++) {
        if (!elts[i].handler(ar, elts[i].data)) {
            return 0;
        }
    }

    return 1;
}


static ngx_flag_t
ngx_aggr_filter_or(ngx_aggr_result_t *ar, void *data)
{
    ngx_uint_t                i, n;
    ngx_aggr_filter_t        *elts;
    ngx_aggr_filter_group_t  *filter = data;

    elts = filter->filters.elts;
    n = filter->filters.nelts;

    for (i = 0; i < n; i++) {
        if (elts[i].handler(ar, elts[i].data)) {
            return 1;
        }
    }

    return 0;
}


static ngx_flag_t
ngx_aggr_filter_not(ngx_aggr_result_t *ar, void *data)
{
    ngx_aggr_filter_t  *filter = data;

    return !filter->handler(ar, filter->data);
}


/* query */

static ngx_int_t
ngx_aggr_filter_str_hash(ngx_pool_t *pool, ngx_str_hash_t *dst, ngx_str_t *src,
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
ngx_aggr_filter_get_metric_offset(ngx_aggr_query_init_t *init,
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
                "ngx_aggr_filter_get_metric_offset: "
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


void
ngx_aggr_filter_dims_set_offsets(ngx_aggr_query_init_t *init)
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


void
ngx_aggr_filter_metrics_set_offsets(ngx_aggr_query_init_t *init)
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
ngx_aggr_filter_copy_json_str_list(ngx_pool_t *pool, ngx_json_array_t *arr,
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

        if (ngx_aggr_filter_str_hash(pool, dst, src, lower) != NGX_OK) {
            return NULL;
        }

        dst++;
    }

    return list;
}


static ngx_int_t
ngx_aggr_filter_match_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    ngx_uint_t                 i, n;
    ngx_uint_t               **offp;
    ngx_json_array_t          *values;
    ngx_json_key_value_t      *elts;
    ngx_aggr_query_dim_t       dim;
    ngx_aggr_query_dim_in_t   *input;
    ngx_aggr_filter_match_t   *ctx;

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
            "ngx_aggr_filter_match_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (dim.input.data == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_filter_match_json: missing \"dim\" key");
        return NGX_BAD_QUERY;
    }

    if (values == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_filter_match_json: missing \"values\" key");
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

    ctx->values = ngx_aggr_filter_copy_json_str_list(init->pool, values,
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
ngx_aggr_filter_regex_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    ngx_str_t                 *pattern;
    ngx_uint_t                 i, n;
    ngx_uint_t               **offp;
    ngx_flag_t                 case_sensitive;
    ngx_regex_compile_t        rc;
    ngx_json_key_value_t      *elts;
    ngx_aggr_query_dim_t       dim;
    ngx_aggr_query_dim_in_t   *input;
    ngx_aggr_filter_regex_t   *ctx;
    u_char                     errstr[NGX_MAX_CONF_ERRSTR];

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
            "ngx_aggr_filter_regex_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (dim.input.data == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_filter_regex_json: missing \"dim\" key");
        return NGX_BAD_QUERY;
    }

    if (pattern == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_filter_regex_json: missing \"pattern\" key");
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
            "ngx_aggr_filter_regex_json: %V", &rc.err);
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
ngx_aggr_filter_compare_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    double                       value;
    ngx_int_t                    rc;
    ngx_uint_t                   i, n;
    ngx_uint_t                   offset;
    ngx_uint_t                 **offp;
    ngx_json_key_value_t        *elts;
    ngx_aggr_query_metric_t      metric;
    ngx_aggr_filter_compare_t   *ctx;

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
            "ngx_aggr_filter_compare_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (metric.input.data == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_filter_compare_json: missing \"metric\" key");
        return NGX_BAD_QUERY;
    }


    metric.type = ngx_aggr_query_metric_sum;

    rc = ngx_aggr_filter_get_metric_offset(init, &metric, &offset);
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
ngx_aggr_filter_nest_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    ngx_int_t              rc;
    ngx_uint_t             i, n;
    ngx_aggr_filter_t     *ctx;
    ngx_json_object_t     *nest;
    ngx_json_key_value_t  *elts;

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
            "ngx_aggr_filter_nest_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (nest == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_filter_nest_json: missing \"filter\" key");
        return NGX_BAD_QUERY;
    }


    ctx = ngx_palloc(init->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    rc = ngx_aggr_filter_json(init, nest, ctx);
    if (rc != NGX_OK) {
        return rc;
    }

    *data = ctx;

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_filters_json(ngx_aggr_query_init_t *init,
    ngx_json_array_t *src_arr, ngx_array_t *dst_arr)
{
    ngx_int_t           rc;
    ngx_array_part_t   *part;
    ngx_json_object_t  *src;
    ngx_aggr_filter_t  *dst;

    if (ngx_array_init(dst_arr, init->pool, src_arr->count,
                       sizeof(ngx_aggr_filter_t)) != NGX_OK)
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

        rc = ngx_aggr_filter_json(init, src, dst);
        if (rc != NGX_OK) {
            return rc;
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_filter_group_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, void **data)
{
    ngx_int_t                 rc;
    ngx_uint_t                i, n;
    ngx_json_array_t         *filters;
    ngx_json_key_value_t     *elts;
    ngx_aggr_filter_group_t  *ctx;

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
            "ngx_aggr_filter_group_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (filters == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_filter_group_json: missing \"filters\" key");
        return NGX_BAD_QUERY;
    }


    ctx = ngx_palloc(init->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    rc = ngx_aggr_filters_json(init, filters, &ctx->filters);
    if (rc != NGX_OK) {
        return rc;
    }

    *data = ctx;

    return NGX_OK;
}


static ngx_aggr_filter_json_t  ngx_aggr_query_json_filters[] = {

    { ngx_string("in"), ngx_aggr_filter_match_json,
        ngx_aggr_filter_in },
    { ngx_string("contains"), ngx_aggr_filter_match_json,
        ngx_aggr_filter_contains },
#if (NGX_PCRE)
    { ngx_string("regex"), ngx_aggr_filter_regex_json,
        ngx_aggr_filter_regex },
#endif

    { ngx_string("gt"), ngx_aggr_filter_compare_json,
        ngx_aggr_filter_gt },
    { ngx_string("lt"), ngx_aggr_filter_compare_json,
        ngx_aggr_filter_lt },
    { ngx_string("gte"), ngx_aggr_filter_compare_json,
        ngx_aggr_filter_gte },
    { ngx_string("lte"), ngx_aggr_filter_compare_json,
        ngx_aggr_filter_lte },

    { ngx_string("and"), ngx_aggr_filter_group_json,
        ngx_aggr_filter_and },
    { ngx_string("or"), ngx_aggr_filter_group_json,
        ngx_aggr_filter_or },
    { ngx_string("not"), ngx_aggr_filter_nest_json,
        ngx_aggr_filter_not },

    { ngx_null_string, NULL, NULL }
};


static ngx_aggr_filter_json_t  ngx_aggr_query_json_having[] = {

    { ngx_string("gt"), ngx_aggr_filter_compare_json,
        ngx_aggr_filter_gt },
    { ngx_string("lt"), ngx_aggr_filter_compare_json,
        ngx_aggr_filter_lt },
    { ngx_string("gte"), ngx_aggr_filter_compare_json,
        ngx_aggr_filter_gte },
    { ngx_string("lte"), ngx_aggr_filter_compare_json,
        ngx_aggr_filter_lte },

    { ngx_string("and"), ngx_aggr_filter_group_json,
        ngx_aggr_filter_and },
    { ngx_string("or"), ngx_aggr_filter_group_json,
        ngx_aggr_filter_or },
    { ngx_string("not"), ngx_aggr_filter_nest_json,
        ngx_aggr_filter_not },

    { ngx_null_string, NULL, NULL }
};


static ngx_json_value_t *
ngx_aggr_filter_json_object_get(ngx_json_object_t *obj, ngx_str_t *name)
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


ngx_int_t
ngx_aggr_filter_json(ngx_aggr_query_init_t *init, ngx_json_object_t *obj,
    ngx_aggr_filter_t *filter)
{
    ngx_int_t                rc;
    ngx_str_t               *type;
    ngx_json_value_t        *value;
    ngx_aggr_filter_json_t  *cur;

    value = ngx_aggr_filter_json_object_get(obj, &ngx_aggr_filter_type);
    if (value == NULL || value->type != NGX_JSON_STRING) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_filter_json: missing \"type\" property");
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
                "ngx_aggr_filter_json: "
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
ngx_aggr_filter_copy_str_list(ngx_pool_t *pool, ngx_str_t *src,
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
        if (ngx_aggr_filter_str_hash(pool, dst, src, lower) != NGX_OK) {
            return NULL;
        }
    }

    return list;
}


static char *
ngx_aggr_filter_match_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_str_t                 *value;
    ngx_uint_t                 i, n;
    ngx_uint_t               **offp;
    ngx_aggr_filter_t         *filter;
    ngx_aggr_query_dim_t       dim;
    ngx_aggr_query_init_t     *init;
    ngx_aggr_query_dim_in_t   *input;
    ngx_aggr_filter_match_t   *ctx;

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
    ctx->values = ngx_aggr_filter_copy_str_list(cf->pool, value + i,
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
ngx_aggr_filter_regex_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_str_t                 *value;
    ngx_uint_t                 i, n;
    ngx_uint_t               **offp;
    ngx_flag_t                 case_sensitive;
    ngx_aggr_filter_t         *filter;
    ngx_regex_compile_t        rc;
    ngx_aggr_query_dim_t       dim;
    ngx_aggr_query_init_t     *init;
    ngx_aggr_query_dim_in_t   *input;
    ngx_aggr_filter_regex_t   *ctx;
    u_char                     errstr[NGX_MAX_CONF_ERRSTR];

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
ngx_aggr_filter_compare_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    u_char                      *end;
    ngx_str_t                   *value;
    ngx_uint_t                   offset;
    ngx_uint_t                 **offp;
    ngx_aggr_filter_t           *filter;
    ngx_aggr_query_init_t       *init;
    ngx_aggr_query_metric_t      metric;
    ngx_aggr_filter_compare_t   *ctx;

    value = cf->args->elts;

    ngx_memzero(&metric, sizeof(metric));
    metric.input = value[1];
    metric.type = ngx_aggr_query_metric_sum;

    init = cf->handler_conf;
    if (ngx_aggr_filter_get_metric_offset(init, &metric, &offset)
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


char *
ngx_aggr_filter_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char                     *rv, *p;
    ngx_conf_t                save;
    ngx_aggr_filter_t        *filter;
    ngx_aggr_filter_pt        handler;
    ngx_aggr_conf_ctx_t      *conf_ctx;
    ngx_aggr_query_init_t    *init;
    ngx_aggr_filter_group_t  *ctx;

    ctx = ngx_palloc(cf->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    if (ngx_array_init(&ctx->filters, cf->pool, 1,
                       sizeof(ngx_aggr_filter_t))
        != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }

    save = *cf;

    conf_ctx = cf->ctx;
    ngx_aggr_set_filter_ctx(conf_ctx, &ctx->filters, ngx_aggr_filter_module);

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
        filter = (ngx_aggr_filter_t *) (p + cmd->offset);

    } else {
        filter = ngx_array_push((ngx_array_t *) conf);
        if (filter == NULL) {
            return NGX_CONF_ERROR;
        }
    }

    rv = ngx_conf_parse(cf, NULL);

    *cf = save;

    if (cf->cmd_type == NGX_AGGR_MAIN_CONF) {
        ngx_aggr_set_filter_ctx(conf_ctx, NULL, ngx_aggr_filter_module);

    } else {
        ngx_aggr_set_filter_ctx(conf_ctx, conf, ngx_aggr_filter_module);
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
        *filter = *(ngx_aggr_filter_t *) ctx->filters.elts;

    } else {
        if (!handler) {
            handler = ngx_aggr_filter_and;
        }

        filter->data = ctx;
        filter->handler = handler;
    }

    return NGX_CONF_OK;
}
