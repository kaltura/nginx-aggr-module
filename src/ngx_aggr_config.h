#ifndef _NGX_AGGR_CONFIG_H_INCLUDED_
#define _NGX_AGGR_CONFIG_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


typedef struct {
    void       *(*create_main_conf)(ngx_aggr_query_init_t *init);
    ngx_int_t   (*init_main_conf)(ngx_aggr_query_init_t *init, void *conf);
} ngx_aggr_module_t;


#define NGX_AGGR_MODULE           0x52474741   /* "AGGR" */

#define NGX_AGGR_MAIN_CONF        0x02000000
#define NGX_AGGR_FILTER_CONF      0x04000000
#define NGX_AGGR_HAVING_CONF      0x08000000
#define NGX_AGGR_GLOBAL_CONF      0x10000000

#define NGX_AGGR_MAIN_CONF_OFFSET                                            \
    offsetof(ngx_aggr_query_init_t, main_conf)
#define NGX_AGGR_FILTER_CONF_OFFSET                                          \
    offsetof(ngx_aggr_query_init_t, filter_conf)


#define ngx_aggr_get_module_main_conf(init, module)                          \
    (init)->main_conf[module.ctx_index]

#define ngx_aggr_conf_get_module_main_conf(cf, module)                       \
    ((ngx_aggr_query_init_t *) cf->ctx)->main_conf[module.ctx_index]

#define ngx_aggr_conf_set_filter_ctx(cf, c, module)                          \
    ((ngx_aggr_query_init_t *) cf->ctx)->filter_conf[module.ctx_index] = c;


#endif /* _NGX_AGGR_CONFIG_H_INCLUDED_ */
