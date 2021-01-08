#ifndef _NGX_AGGR_FILTER_H_INCLUDED_
#define _NGX_AGGR_FILTER_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"
#include "ngx_str_table.h"
#include "ngx_json_parser.h"


typedef ngx_flag_t (*ngx_aggr_filter_pt)(ngx_aggr_result_t *ar, void *data);

typedef struct {
    ngx_aggr_filter_pt   handler;
    void                *data;
} ngx_aggr_filter_t;


void ngx_aggr_filter_dims_set_offsets(ngx_aggr_query_init_t *init);

void ngx_aggr_filter_metrics_set_offsets(ngx_aggr_query_init_t *init);


char *ngx_aggr_filter_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

ngx_int_t ngx_aggr_filter_json(ngx_aggr_query_init_t *init,
    ngx_json_object_t *obj, ngx_aggr_filter_t *filter);

#endif /* _NGX_AGGR_FILTER_H_INCLUDED_ */
