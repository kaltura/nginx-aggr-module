#ifndef _NGX_AGGR_MAP_H_INCLUDED_
#define _NGX_AGGR_MAP_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"
#include "ngx_json_parser.h"


ngx_int_t ngx_aggr_maps_json(ngx_aggr_query_init_t *init,
    ngx_json_array_t *arr);

#endif /* _NGX_AGGR_MAP_H_INCLUDED_ */
