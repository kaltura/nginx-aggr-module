#ifndef _NGX_DGRAM_AGGR_MODULE_H_INCLUDED_
#define _NGX_DGRAM_AGGR_MODULE_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr_query.h"


ngx_chain_t **ngx_dgram_aggr_query(ngx_pool_t *pool, ngx_cycle_t *cycle,
    ngx_str_t *name, ngx_aggr_query_t *query, ngx_chain_t **last, off_t *size);

#endif /* _NGX_DGRAM_AGGR_MODULE_H_INCLUDED_ */
