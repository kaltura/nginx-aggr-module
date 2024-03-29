#ifndef _NGX_AGGR_OUTPUT_H_INCLUDED_
#define _NGX_AGGR_OUTPUT_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


typedef struct {
    ngx_queue_t    queue;
    ngx_array_t    outputs;       /* ngx_aggr_output_ctx_t */
    ngx_log_t     *log;
} ngx_aggr_outputs_arr_t;


ngx_int_t ngx_aggr_outputs_init(ngx_conf_t *cf, ngx_aggr_outputs_arr_t *conf);

void ngx_aggr_outputs_push(ngx_aggr_outputs_arr_t *conf,
    ngx_aggr_bucket_t *bucket);


char *ngx_aggr_output_file(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

#if (NGX_HAVE_LIBRDKAFKA)
char *ngx_aggr_output_kafka(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
#endif

#endif /* _NGX_AGGR_OUTPUT_H_INCLUDED_ */
