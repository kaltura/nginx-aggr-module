#ifndef _NGX_AGGR_H_INCLUDED_
#define _NGX_AGGR_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


#define ngx_str_equals(s1, s2)                                              \
    ((s1).len == (s2).len && ngx_memcmp((s1).data, (s2).data, (s1).len) == 0)

#define ngx_str_equals_c(ns, s)                                             \
    ((ns).len == sizeof(s) - 1 &&                                           \
     ngx_strncmp((ns).data, (s), sizeof(s) - 1) == 0)


typedef struct ngx_aggr_query_s       ngx_aggr_query_t;
typedef struct ngx_aggr_query_init_s  ngx_aggr_query_init_t;
typedef struct ngx_aggr_event_s       ngx_aggr_event_t;
typedef struct ngx_aggr_result_s      ngx_aggr_result_t;


#include "ngx_aggr_variables.h"
#include "ngx_aggr_config.h"
#include "ngx_aggr_script.h"
#include "ngx_aggr_filter.h"
#include "ngx_aggr_query.h"
#include "ngx_aggr_result.h"
#include "ngx_aggr_window.h"


ngx_int_t ngx_aggr_add_window(ngx_conf_t *cf, ngx_str_t *name,
    ngx_aggr_window_conf_t *window);

ngx_chain_t **ngx_aggr_query(ngx_pool_t *pool, ngx_cycle_t *cycle,
    ngx_str_t *name, ngx_aggr_query_t *query, ngx_chain_t **last, off_t *size);

#endif /* _NGX_AGGR_H_INCLUDED_ */
