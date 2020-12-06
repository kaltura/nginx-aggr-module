#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_str_table.h"


ngx_str_table_t *
ngx_str_table_create(ngx_pool_t *pool)
{
    ngx_str_table_t  *tbl;

    tbl = ngx_palloc(pool, sizeof(*tbl));
    if (tbl == NULL) {
        return NULL;
    }

    tbl->pool = pool;

    ngx_rbtree_init(&tbl->rbtree, &tbl->sentinel, ngx_str_rbtree_insert_value);

    return tbl;
}


/* same as ngx_str_rbtree_lookup but with ngx_rbtree_key_t hash */
static ngx_str_node_t *
ngx_str_rbtree_lookup_key(ngx_rbtree_t *rbtree, ngx_str_t *val,
    ngx_rbtree_key_t hash)
{
    ngx_int_t           rc;
    ngx_str_node_t     *n;
    ngx_rbtree_node_t  *node, *sentinel;

    node = rbtree->root;
    sentinel = rbtree->sentinel;

    while (node != sentinel) {

        n = (ngx_str_node_t *) node;

        if (hash != node->key) {
            node = (hash < node->key) ? node->left : node->right;
            continue;
        }

        if (val->len != n->str.len) {
            node = (val->len < n->str.len) ? node->left : node->right;
            continue;
        }

        rc = ngx_memcmp(val->data, n->str.data, val->len);

        if (rc < 0) {
            node = node->left;
            continue;
        }

        if (rc > 0) {
            node = node->right;
            continue;
        }

        return n;
    }

    return NULL;
}


ngx_str_t *
ngx_str_table_get(ngx_str_table_t *tbl, ngx_str_hash_t *sh)
{
    ngx_str_node_t  *sn;

    sn = ngx_str_rbtree_lookup_key(&tbl->rbtree, &sh->s, sh->hash);
    if (sn != NULL) {
        return &sn->str;
    }

    sn = ngx_palloc(tbl->pool, sizeof(*sn));
    if (sn == NULL) {
        return NULL;
    }

    sn->str.data = ngx_pstrdup(tbl->pool, &sh->s);
    if (sn->str.data == NULL) {
        return NULL;
    }

    sn->str.len = sh->s.len;
    sn->node.key = sh->hash;

    ngx_rbtree_insert(&tbl->rbtree, &sn->node);

    return &sn->str;
}
