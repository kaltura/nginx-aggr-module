#ifndef _NGX_STR_TABLE_H_INCLUDED_
#define _NGX_STR_TABLE_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


typedef struct ngx_str_table_s  ngx_str_table_t;

struct ngx_str_table_s {
    ngx_pool_t         *pool;
    ngx_rbtree_t        rbtree;
    ngx_rbtree_node_t   sentinel;
};


ngx_str_table_t *ngx_str_table_create(ngx_pool_t *pool);

ngx_str_t *ngx_str_table_get(ngx_str_table_t *tbl, ngx_str_t *val,
    ngx_uint_t hash);


#endif /* _NGX_STR_TABLE_H_INCLUDED_ */
