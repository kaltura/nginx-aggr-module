#ifndef _NGX_AGGR_WINDOW_H_INCLUDED_
#define _NGX_AGGR_WINDOW_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


typedef struct ngx_aggr_buf_s  ngx_aggr_buf_t;

typedef struct ngx_aggr_window_s  ngx_aggr_window_t;

typedef struct ngx_aggr_bucket_s  ngx_aggr_bucket_t;


struct ngx_aggr_buf_s {
    ngx_aggr_buf_t     *next;
    u_char             *start;
    u_char             *last;
    u_char             *end;
};


typedef struct {
    ngx_str_t           name;
    time_t              interval;
    size_t              buf_size;
    ngx_uint_t          max_buffers;
    size_t              recv_size;
    ngx_int_t           delim;
    ngx_aggr_window_t  *window;
} ngx_aggr_window_conf_t;


typedef void (*ngx_aggr_bucket_handler_pt)(void *data,
    ngx_aggr_bucket_t *bucket);


void ngx_aggr_bucket_add_ref(ngx_aggr_bucket_t *bucket);

void ngx_aggr_bucket_free(ngx_aggr_bucket_t *bucket);

ngx_int_t ngx_aggr_bucket_process(ngx_aggr_bucket_t *bucket,
    ngx_aggr_result_t *ar);

time_t ngx_aggr_bucket_get_time(ngx_aggr_bucket_t *bucket);


void ngx_aggr_window_conf_init(ngx_aggr_window_conf_t *conf);

void ngx_aggr_window_conf_merge(ngx_aggr_window_conf_t *conf,
    ngx_aggr_window_conf_t *prev);


ngx_aggr_window_t *ngx_aggr_window_create(ngx_pool_t *pool,
    ngx_aggr_window_conf_t *conf, ngx_aggr_bucket_handler_pt handler,
    void *data);

ngx_int_t ngx_aggr_window_get_recv_buf(ngx_aggr_window_t *window,
    ngx_aggr_buf_t **result);

ngx_int_t ngx_aggr_window_write(ngx_aggr_window_t *window, u_char *p,
    u_char *last);

ngx_int_t ngx_aggr_window_process(ngx_aggr_window_t *window, ngx_pool_t *pool,
    ngx_aggr_result_t *ar);


char *ngx_conf_set_char_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

#endif /* _NGX_AGGR_WINDOW_H_INCLUDED_ */
