#ifndef _NGX_AGGR_RESULT_H_INCLUDED_
#define _NGX_AGGR_RESULT_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr_query.h"


typedef struct ngx_aggr_result_s  ngx_aggr_result_t;

typedef void (*ngx_aggr_event_send_pt)(void *data, void *buf, size_t len,
    void *free_ctx);


ngx_flag_t ngx_aggr_filter_in(ngx_aggr_filter_ctx_t *ctx, void *data);

ngx_flag_t ngx_aggr_filter_contains(ngx_aggr_filter_ctx_t *ctx, void *data);

#if (NGX_PCRE)
ngx_flag_t ngx_aggr_filter_regex(ngx_aggr_filter_ctx_t *ctx, void *data);
#endif

ngx_flag_t ngx_aggr_filter_and(ngx_aggr_filter_ctx_t *ctx, void *data);

ngx_flag_t ngx_aggr_filter_or(ngx_aggr_filter_ctx_t *ctx, void *data);

ngx_flag_t ngx_aggr_filter_not(ngx_aggr_filter_ctx_t *ctx, void *data);


ngx_flag_t ngx_aggr_filter_gt(ngx_aggr_filter_ctx_t *ctx, void *data);

ngx_flag_t ngx_aggr_filter_lt(ngx_aggr_filter_ctx_t *ctx, void *data);

ngx_flag_t ngx_aggr_filter_gte(ngx_aggr_filter_ctx_t *ctx, void *data);

ngx_flag_t ngx_aggr_filter_lte(ngx_aggr_filter_ctx_t *ctx, void *data);


ngx_aggr_result_t *ngx_aggr_result_create(ngx_aggr_query_t *query, time_t t,
    ngx_aggr_result_t *prev);

void ngx_aggr_result_destroy(ngx_aggr_result_t *ar);


ngx_int_t ngx_aggr_result_process(ngx_aggr_result_t *ar, u_char *start,
    size_t size);

ngx_chain_t **ngx_aggr_result_write(ngx_aggr_result_t *ar, ngx_pool_t *pool,
    ngx_chain_t **last, off_t *size);


ngx_int_t ngx_aggr_result_send(ngx_aggr_result_t *ar,
    ngx_aggr_event_send_pt handler, void *data);

void ngx_aggr_result_send_buf_free(void *data);


ngx_buf_t *ngx_aggr_result_get_stats(ngx_pool_t *pool);


#endif /* _NGX_AGGR_RESULT_H_INCLUDED_ */
