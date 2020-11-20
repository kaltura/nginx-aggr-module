#ifndef _NGX_AGGR_OUTPUT_H_INCLUDED_
#define _NGX_AGGR_OUTPUT_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


typedef struct {
    ngx_array_t  outputs;       /* ngx_aggr_output_ctx_t */
} ngx_aggr_outputs_conf_t;


ngx_int_t ngx_aggr_outputs_init(ngx_conf_t *cf, ngx_aggr_outputs_conf_t *conf);

ngx_int_t ngx_aggr_outputs_start(ngx_log_t *log,
    ngx_aggr_outputs_conf_t *conf);

void ngx_aggr_outputs_push(ngx_aggr_outputs_conf_t *conf,
    ngx_aggr_bucket_t *bucket);

void ngx_aggr_outputs_close(ngx_cycle_t *cycle);


char * ngx_aggr_output_kafka(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

#endif /* _NGX_AGGR_OUTPUT_H_INCLUDED_ */
