#ifndef _NGX_AGGR_QUERY_H_INCLUDED_
#define _NGX_AGGR_QUERY_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_json_parser.h"
#include "ngx_str_table.h"


#define NGX_ISO8601_TIMESTAMP_LEN  (sizeof("1981-06-13T12:00:00Z") - 1)

#define NGX_AGGR_QUERY_DOUBLE_PRECISION  (14)

/* 7 = 1.<precision>e+100 */
#define NGX_AGGR_QUERY_DOUBLE_LEN  (7 + NGX_AGGR_QUERY_DOUBLE_PRECISION)

#define NGX_BAD_QUERY  NGX_ABORT


enum {
    ngx_aggr_query_fmt_json,
    ngx_aggr_query_fmt_prom,

    ngx_aggr_query_fmts
};


enum {
    ngx_aggr_query_dim_group,
    ngx_aggr_query_dim_select,
    ngx_aggr_query_dim_filter,

    ngx_aggr_query_dim_types
};


enum {
    ngx_aggr_query_metric_sum,
    ngx_aggr_query_metric_max,
};


typedef struct {
    ngx_str_t                  name;        /* must be first */
    ngx_flag_t                 lower;
    ngx_uint_t                 offset;
    ngx_uint_t                 temp_offset;

    /* temp during build */
    ngx_int_t                  type;
    ngx_str_t                  default_value;
} ngx_aggr_query_dim_in_t;


typedef struct {
    ngx_str_t                  name;
    ngx_uint_t                 offset;
} ngx_aggr_query_dim_out_t;


typedef struct {
    ngx_str_t                  name;        /* must be first */
    ngx_uint_t                 offset;
    ngx_int_t                  type;

    /* temp during build */
    double                     default_value;
} ngx_aggr_query_metric_in_t;


typedef struct {
    ngx_str_t                  name;
    ngx_uint_t                 offset;
} ngx_aggr_query_metric_out_t;


typedef struct ngx_aggr_filter_ctx_s  ngx_aggr_filter_ctx_t;


typedef ngx_flag_t(*ngx_aggr_query_filter_pt)(ngx_aggr_filter_ctx_t *ctx,
    void *data);


typedef struct {
    ngx_aggr_query_filter_pt   handler;
    void                      *data;
} ngx_aggr_query_filter_t;


typedef struct {
    ngx_array_t                filters;
} ngx_aggr_query_filter_group_t;


typedef struct {
    ngx_uint_t                 temp_offset;
    ngx_str_hash_t            *values;
    ngx_uint_t                 values_len;
} ngx_aggr_query_filter_match_t;


#if (NGX_PCRE)
typedef struct {
    ngx_uint_t                 temp_offset;
    ngx_regex_t               *re;
} ngx_aggr_query_filter_regex_t;
#endif


typedef struct {
    ngx_uint_t                 offset;
    double                     value;
} ngx_aggr_query_filter_compare_t;


typedef struct {
    ngx_int_t                  fmt;
    ngx_str_t                  time_dim;
    time_t                     granularity;
    ngx_array_t                dims_in;      /* ngx_aggr_query_dim_in_t */
    ngx_array_t                metrics_in;   /* ngx_aggr_query_metric_in_t */
    ngx_array_t                dims_out;     /* ngx_aggr_query_dim_out_t */
    ngx_array_t                metrics_out;  /* ngx_aggr_query_metric_out_t */
    ngx_aggr_query_filter_t    filter;
    ngx_aggr_query_filter_t    having;
    ngx_uint_t                 top_offset;
    ngx_uint_t                 top_count;
    ngx_flag_t                 top_inverted;

    ngx_uint_t                 hash_max_size;
    ngx_uint_t                 hash_bucket_size;
    size_t                     max_event_size;
    size_t                     output_buf_size;

    ngx_hash_t                 dims_hash;    /* <ngx_aggr_query_dim_in_t> */
    ngx_hash_t                 metrics_hash; /* <ngx_aggr_query_metric_in_t> */

    size_t                     size[ngx_aggr_query_dim_types];
    size_t                     event_size;
    size_t                     temp_size;
    u_char                    *temp_default;    /* ngx_str_hash_t[] */
    size_t                     metrics_size;
    u_char                    *metrics_default; /* double[] */

    size_t                     write_size[ngx_aggr_query_fmts];
} ngx_aggr_query_t;


ngx_int_t ngx_aggr_query_json(ngx_pool_t *pool, ngx_pool_t *temp_pool,
    ngx_json_value_t *json, ngx_aggr_query_t *base, ngx_aggr_query_t **result);

ngx_aggr_query_t *ngx_aggr_query_block(ngx_conf_t *cf, ngx_flag_t init);


#endif /* _NGX_AGGR_QUERY_H_INCLUDED_ */
