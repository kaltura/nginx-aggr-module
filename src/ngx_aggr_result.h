#ifndef _NGX_AGGR_RESULT_H_INCLUDED_
#define _NGX_AGGR_RESULT_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"
#include "ngx_str_table.h"


struct ngx_aggr_event_s {
    ngx_rbtree_node_t           node;
    ngx_aggr_event_t           *next;
    size_t                      group_size;
    u_char                      data[1];

    /*
      data =
    ngx_str_t                  *group_dims[group_size / sizeof(void *)];
    ngx_str_t                  *select_dims[select_size / sizeof(void *)];
    double                      metrics[...];
    */
};


struct ngx_aggr_result_s {
    ngx_pool_t                 *pool;
    ngx_str_table_t            *str_tbl;

    u_char                     *buf;
    size_t                      buf_used;
    size_t                      buf_size;

    ngx_aggr_query_t           *query;
    size_t                      group_size;
    size_t                      select_size;
    size_t                      event_size;
    size_t                      metrics_offset;

    ngx_rbtree_t                rbtree;
    ngx_rbtree_node_t           sentinel;
    ngx_aggr_event_t           *head;
    ngx_aggr_event_t           *cur;
    ngx_uint_t                  count;
    u_char                     *temp_data;

    u_char                      time_buf[NGX_ISO8601_TIMESTAMP_LEN];
    size_t                      time_len;

    ngx_buf_t                   var_temp;

    ngx_aggr_variable_value_t  *variables;
    ngx_uint_t                  variable_depth;

#if (NGX_PCRE)
    ngx_uint_t                  ncaptures;
    int                        *captures;
    u_char                     *captures_data;
#endif
};


typedef void (*ngx_aggr_event_send_pt)(void *data, void *buf, size_t len,
    void *free_ctx);


ngx_aggr_result_t *ngx_aggr_result_create(ngx_aggr_query_t *query,
    ngx_log_t *log, time_t t, ngx_aggr_result_t *prev);

void ngx_aggr_result_destroy(ngx_aggr_result_t *ar);


ngx_int_t ngx_aggr_result_process(ngx_aggr_result_t *ar, u_char *start,
    size_t size);

ngx_chain_t **ngx_aggr_result_write(ngx_aggr_result_t *ar, ngx_pool_t *pool,
    ngx_chain_t **last, off_t *size);


ngx_int_t ngx_aggr_result_send(ngx_aggr_result_t *ar,
    ngx_aggr_event_send_pt handler, void *data);

void ngx_aggr_result_send_buf_free(void *data);


void *ngx_aggr_result_temp_alloc(ngx_aggr_result_t *ar, size_t size);


ngx_buf_t *ngx_aggr_result_get_stats(ngx_pool_t *pool);


#endif /* _NGX_AGGR_RESULT_H_INCLUDED_ */
