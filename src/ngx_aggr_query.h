#ifndef _NGX_AGGR_QUERY_H_INCLUDED_
#define _NGX_AGGR_QUERY_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_json_parser.h"


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

    ngx_aggr_query_dim_types
};


enum {
    ngx_aggr_query_metric_sum,
    ngx_aggr_query_metric_max,
};


typedef struct {
    ngx_str_t       name;          /* must be first */
    ngx_flag_t      lower;
    ngx_uint_t      offset;

    /* temp during build */
    ngx_int_t       type;
    ngx_str_t       default_value;
} ngx_aggr_query_dim_in_t;


typedef struct {
    ngx_str_t       name;
    ngx_uint_t      offset;
} ngx_aggr_query_dim_out_t;


typedef struct {
    ngx_str_t       name;          /* must be first */
    ngx_uint_t      offset;
    ngx_int_t       type;

    /* temp during build */
    double          default_value;
} ngx_aggr_query_metric_in_t;


typedef struct {
    ngx_str_t       name;
    ngx_uint_t      offset;
} ngx_aggr_query_metric_out_t;


typedef struct {
    ngx_int_t       fmt;
    ngx_str_t       time_dim;
    time_t          granularity;
    ngx_array_t     dims_in;        /* ngx_aggr_query_dim_in_t */
    ngx_array_t     metrics_in;     /* ngx_aggr_query_metric_in_t */
    ngx_array_t     dims_out;       /* ngx_aggr_query_dim_out_t */
    ngx_array_t     metrics_out;    /* ngx_aggr_query_metric_out_t */

    ngx_uint_t      hash_max_size;
    ngx_uint_t      hash_bucket_size;
    size_t          max_event_size;
    size_t          output_buf_size;

    ngx_hash_t      dims_hash;      /* array<ngx_aggr_query_dim_in_t> */
    ngx_hash_t      metrics_hash;   /* array<ngx_aggr_query_metric_in_t> */
    size_t          size[ngx_aggr_query_dim_types];
    size_t          event_size;
    u_char         *default_event;
    size_t          write_size[ngx_aggr_query_fmts];
} ngx_aggr_query_t;


ngx_int_t ngx_aggr_query_json(ngx_pool_t *pool, ngx_pool_t *temp_pool,
    ngx_json_value_t *json, ngx_aggr_query_t *base, ngx_aggr_query_t **result);

ngx_aggr_query_t *ngx_aggr_query_block(ngx_conf_t *cf, ngx_flag_t init);


#endif /* _NGX_AGGR_QUERY_H_INCLUDED_ */
