
/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"

static ngx_aggr_variable_t *ngx_aggr_add_prefix_variable(
    ngx_aggr_query_init_t *init, ngx_str_t *name, ngx_uint_t flags);

static ngx_int_t ngx_aggr_variable_dim(ngx_aggr_result_t *ar,
    ngx_aggr_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_aggr_variable_init_dim(ngx_aggr_query_init_t *init,
    ngx_aggr_variable_t *dst, ngx_aggr_variable_t *src);


static ngx_aggr_variable_t  ngx_aggr_core_variables[] = {

    { ngx_string("dim_"), NULL, ngx_aggr_variable_dim,
      ngx_aggr_variable_init_dim, 0, NGX_AGGR_VAR_PREFIX, 0 },

      ngx_aggr_null_variable
};


ngx_aggr_variable_value_t  ngx_aggr_variable_null_value =
    ngx_aggr_variable("");


ngx_aggr_variable_t *
ngx_aggr_add_variable(ngx_aggr_query_init_t *init, ngx_str_t *name,
    ngx_uint_t flags)
{
    ngx_int_t             rc;
    ngx_uint_t            i;
    ngx_hash_key_t       *key;
    ngx_aggr_query_t     *query;
    ngx_aggr_variable_t  *v;

    if (name->len == 0) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                      "invalid variable name \"$\"");
        return NULL;
    }

    if (flags & NGX_AGGR_VAR_PREFIX) {
        return ngx_aggr_add_prefix_variable(init, name, flags);
    }

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    key = query->variables_keys->keys.elts;
    for (i = 0; i < query->variables_keys->keys.nelts; i++) {
        if (name->len != key[i].key.len
            || ngx_strncasecmp(name->data, key[i].key.data, name->len) != 0)
        {
            continue;
        }

        v = key[i].value;

        if (!(v->flags & NGX_AGGR_VAR_CHANGEABLE)) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                          "the duplicate \"%V\" variable", name);
            return NULL;
        }

        if (!(flags & NGX_AGGR_VAR_WEAK)) {
            v->flags &= ~NGX_AGGR_VAR_WEAK;
        }

        return v;
    }

    v = ngx_palloc(init->pool, sizeof(ngx_aggr_variable_t));
    if (v == NULL) {
        return NULL;
    }

    v->name.len = name->len;
    v->name.data = ngx_pnalloc(init->pool, name->len);
    if (v->name.data == NULL) {
        return NULL;
    }

    ngx_strlow(v->name.data, name->data, name->len);

    v->set_handler = NULL;
    v->get_handler = NULL;
    v->init_handler = NULL;
    v->data = 0;
    v->flags = flags;
    v->index = 0;

    rc = ngx_hash_add_key(query->variables_keys, &v->name, v, 0);

    if (rc == NGX_ERROR) {
        return NULL;
    }

    if (rc == NGX_BUSY) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                      "conflicting variable name \"%V\"", name);
        return NULL;
    }

    return v;
}


static ngx_aggr_variable_t *
ngx_aggr_add_prefix_variable(ngx_aggr_query_init_t *init, ngx_str_t *name,
    ngx_uint_t flags)
{
    ngx_uint_t            i;
    ngx_aggr_query_t     *query;
    ngx_aggr_variable_t  *v;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    v = query->prefix_variables.elts;
    for (i = 0; i < query->prefix_variables.nelts; i++) {
        if (name->len != v[i].name.len
            || ngx_strncasecmp(name->data, v[i].name.data, name->len) != 0)
        {
            continue;
        }

        v = &v[i];

        if (!(v->flags & NGX_AGGR_VAR_CHANGEABLE)) {
            ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                          "the duplicate \"%V\" variable", name);
            return NULL;
        }

        if (!(flags & NGX_AGGR_VAR_WEAK)) {
            v->flags &= ~NGX_AGGR_VAR_WEAK;
        }

        return v;
    }

    v = ngx_array_push(&query->prefix_variables);
    if (v == NULL) {
        return NULL;
    }

    v->name.len = name->len;
    v->name.data = ngx_pnalloc(init->pool, name->len);
    if (v->name.data == NULL) {
        return NULL;
    }

    ngx_strlow(v->name.data, name->data, name->len);

    v->set_handler = NULL;
    v->get_handler = NULL;
    v->init_handler = NULL;
    v->data = 0;
    v->flags = flags;
    v->index = 0;

    return v;
}


ngx_int_t
ngx_aggr_get_variable_index(ngx_aggr_query_init_t *init, ngx_str_t *name)
{
    ngx_uint_t            i;
    ngx_aggr_query_t     *query;
    ngx_aggr_variable_t  *v;

    if (name->len == 0) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
                      "invalid variable name \"$\"");
        return NGX_ERROR;
    }

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    v = query->variables.elts;

    if (v == NULL) {
        if (ngx_array_init(&query->variables, init->pool, 4,
                           sizeof(ngx_aggr_variable_t))
            != NGX_OK)
        {
            return NGX_ERROR;
        }

    } else {
        for (i = 0; i < query->variables.nelts; i++) {
            if (name->len != v[i].name.len
                || ngx_strncasecmp(name->data, v[i].name.data, name->len) != 0)
            {
                continue;
            }

            return i;
        }
    }

    v = ngx_array_push(&query->variables);
    if (v == NULL) {
        return NGX_ERROR;
    }

    v->name.len = name->len;
    v->name.data = ngx_pnalloc(init->pool, name->len);
    if (v->name.data == NULL) {
        return NGX_ERROR;
    }

    ngx_strlow(v->name.data, name->data, name->len);

    v->set_handler = NULL;
    v->get_handler = NULL;
    v->init_handler = NULL;
    v->data = 0;
    v->flags = 0;
    v->index = query->variables.nelts - 1;

    return v->index;
}


ngx_aggr_variable_value_t *
ngx_aggr_get_indexed_variable(ngx_aggr_result_t *ar, ngx_uint_t index)
{
    ngx_aggr_query_t     *query;
    ngx_aggr_variable_t  *v;

    query = ar->query;

    if (query->variables.nelts <= index) {
        ngx_log_error(NGX_LOG_ALERT, ar->pool->log, 0,
                      "unknown variable index: %ui", index);
        return NULL;
    }

    if (ar->variables[index].not_found || ar->variables[index].valid) {
        return &ar->variables[index];
    }

    v = query->variables.elts;

    if (ar->variable_depth == 0) {
        ngx_log_error(NGX_LOG_ERR, ar->pool->log, 0,
                      "cycle while evaluating variable \"%V\"",
                      &v[index].name);
        return NULL;
    }

    ar->variable_depth--;

    if (v[index].get_handler(ar, &ar->variables[index], v[index].data)
        == NGX_OK)
    {
        ar->variable_depth++;

        if (v[index].flags & NGX_AGGR_VAR_NOCACHEABLE) {
            ar->variables[index].no_cacheable = 1;
        }

        return &ar->variables[index];
    }

    ar->variable_depth++;

    ar->variables[index].valid = 0;
    ar->variables[index].not_found = 1;

    return NULL;
}


ngx_aggr_variable_value_t *
ngx_aggr_get_flushed_variable(ngx_aggr_result_t *ar, ngx_uint_t index)
{
    ngx_aggr_variable_value_t  *v;

    v = &ar->variables[index];

    if (v->valid || v->not_found) {
        if (!v->no_cacheable) {
            return v;
        }

        v->valid = 0;
        v->not_found = 0;
    }

    return ngx_aggr_get_indexed_variable(ar, index);
}


static ngx_int_t
ngx_aggr_variable_dim(ngx_aggr_result_t *ar, ngx_aggr_variable_value_t *v,
    uintptr_t data)
{
    ngx_str_hash_t  *sh;

    sh = (ngx_str_hash_t *) ((char *) ar->temp_data + data);

    if (sh->s.data) {
        v->len = sh->s.len;
        v->valid = 1;
        v->no_cacheable = 0;
        v->not_found = 0;
        v->data = sh->s.data;

    } else {
        v->not_found = 1;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_variable_init_dim(ngx_aggr_query_init_t *init,
    ngx_aggr_variable_t *dst, ngx_aggr_variable_t *src)
{
    ngx_uint_t               **offp;
    ngx_aggr_query_dim_t       dim;
    ngx_aggr_query_dim_in_t   *input;

    ngx_memzero(&dim, sizeof(dim));

    dim.input.data = dst->name.data + src->name.len;
    dim.input.len = dst->name.len - src->name.len;
    dim.type = ngx_aggr_query_dim_temp;

    input = ngx_aggr_query_dim_input_get_simple(init, &dim);
    if (input == NULL) {
        return NGX_ERROR;
    }

    dst->data = input->offset;

    offp = ngx_array_push(&init->dim_temp_offs);
    if (offp == NULL) {
        return NGX_ERROR;
    }

    *offp = &dst->data;

    return NGX_OK;
}


void *
ngx_aggr_map_find(ngx_aggr_result_t *ar, ngx_aggr_map_t *map, ngx_str_t *match)
{
    void        *value;
    u_char      *low;
    size_t       len;
    ngx_uint_t   key;

    len = match->len;

    if (len) {
        low = ngx_aggr_result_temp_alloc(ar, len);
        if (low == NULL) {
            return NULL;
        }

    } else {
        low = NULL;
    }

    key = ngx_hash_strlow(low, match->data, len);

    if (map->hash.buckets) {
        value = ngx_hash_find(&map->hash, key, low, len);
        if (value) {
            return value;
        }
    }

#if (NGX_PCRE)

    if (len && map->nregex) {
        ngx_int_t              n;
        ngx_uint_t             i;
        ngx_aggr_map_regex_t  *reg;

        reg = map->regex;

        for (i = 0; i < map->nregex; i++) {

            n = ngx_aggr_regex_exec(ar, reg[i].regex, match);

            if (n == NGX_OK) {
                return reg[i].value;
            }

            if (n == NGX_DECLINED) {
                continue;
            }

            /* NGX_ERROR */

            return NULL;
        }
    }

#endif

    return NULL;
}


#if (NGX_PCRE)

static ngx_int_t
ngx_aggr_variable_not_found(ngx_aggr_result_t *ar, ngx_aggr_variable_value_t *v,
    uintptr_t data)
{
    v->not_found = 1;
    return NGX_OK;
}


ngx_aggr_regex_t *
ngx_aggr_regex_compile(ngx_aggr_query_init_t *init, ngx_regex_compile_t *rc)
{
    u_char                     *p;
    size_t                      size;
    ngx_str_t                   name;
    ngx_uint_t                  i, n;
    ngx_aggr_query_t           *query;
    ngx_aggr_regex_t           *re;
    ngx_aggr_variable_t        *v;
    ngx_aggr_regex_variable_t  *rv;

    rc->pool = init->pool;

    if (ngx_regex_compile(rc) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0, "%V", &rc->err);
        return NULL;
    }

    re = ngx_pcalloc(init->pool, sizeof(ngx_aggr_regex_t));
    if (re == NULL) {
        return NULL;
    }

    re->regex = rc->regex;
    re->ncaptures = rc->captures;
    re->name = rc->pattern;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);
    query->ncaptures = ngx_max(query->ncaptures, re->ncaptures);

    n = (ngx_uint_t) rc->named_captures;

    if (n == 0) {
        return re;
    }

    rv = ngx_palloc(rc->pool, n * sizeof(ngx_aggr_regex_variable_t));
    if (rv == NULL) {
        return NULL;
    }

    re->variables = rv;
    re->nvariables = n;

    size = rc->name_size;
    p = rc->names;

    for (i = 0; i < n; i++) {
        rv[i].capture = 2 * ((p[0] << 8) + p[1]);

        name.data = &p[2];
        name.len = ngx_strlen(name.data);

        v = ngx_aggr_add_variable(init, &name, NGX_AGGR_VAR_CHANGEABLE);
        if (v == NULL) {
            return NULL;
        }

        rv[i].index = ngx_aggr_get_variable_index(init, &name);
        if (rv[i].index == NGX_ERROR) {
            return NULL;
        }

        v->get_handler = ngx_aggr_variable_not_found;

        p += size;
    }

    return re;
}


ngx_int_t
ngx_aggr_regex_exec(ngx_aggr_result_t *ar, ngx_aggr_regex_t *re,
    ngx_str_t *str)
{
    ngx_int_t                   rc, index;
    ngx_uint_t                  i, n, len;
    ngx_aggr_query_t           *query;
    ngx_aggr_variable_value_t  *vv;

    query = ar->query;

    len = re->ncaptures ? query->ncaptures : 0;

    rc = ngx_regex_exec(re->regex, str, ar->captures, len);

    if (rc == NGX_REGEX_NO_MATCHED) {
        return NGX_DECLINED;
    }

    if (rc < 0) {
        ngx_log_error(NGX_LOG_ALERT, ar->pool->log, 0,
                      ngx_regex_exec_n " failed: %i on \"%V\" using \"%V\"",
                      rc, str, &re->name);
        return NGX_ERROR;
    }

    for (i = 0; i < re->nvariables; i++) {

        n = re->variables[i].capture;
        index = re->variables[i].index;
        vv = &ar->variables[index];

        vv->len = ar->captures[n + 1] - ar->captures[n];
        vv->valid = 1;
        vv->no_cacheable = 0;
        vv->not_found = 0;
        vv->data = &str->data[ar->captures[n]];

#if (NGX_DEBUG)
        {
        ngx_aggr_variable_t  *v;

        v = query->variables.elts;

        ngx_log_debug2(NGX_LOG_DEBUG_CORE, ar->pool->log, 0,
                       "aggr regex set $%V to \"%v\"", &v[index].name, vv);
        }
#endif
    }

    ar->ncaptures = rc * 2;
    ar->captures_data = str->data;

    return NGX_OK;
}

#endif


ngx_int_t
ngx_aggr_variables_add_core_vars(ngx_aggr_query_init_t *init)
{
    ngx_aggr_query_t     *query;
    ngx_aggr_variable_t  *cv, *v;

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    query->variables_keys = ngx_pcalloc(init->temp_pool,
                                        sizeof(ngx_hash_keys_arrays_t));
    if (query->variables_keys == NULL) {
        return NGX_ERROR;
    }

    query->variables_keys->pool = init->pool;
    query->variables_keys->temp_pool = init->pool;

    if (ngx_hash_keys_array_init(query->variables_keys, NGX_HASH_SMALL)
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_array_init(&query->prefix_variables, init->pool, 8,
                       sizeof(ngx_aggr_variable_t))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    for (cv = ngx_aggr_core_variables; cv->name.len; cv++) {
        v = ngx_aggr_add_variable(init, &cv->name, cv->flags);
        if (v == NULL) {
            return NGX_ERROR;
        }

        *v = *cv;
    }

    return NGX_OK;
}


ngx_int_t
ngx_aggr_variables_init_vars(ngx_aggr_query_init_t *init)
{
    size_t                len;
    ngx_uint_t            i, n;
    ngx_hash_key_t       *key;
    ngx_hash_init_t       hash;
    ngx_aggr_query_t     *query;
    ngx_aggr_variable_t  *v, *av, *pv;

    /* set the handlers for the indexed aggr variables */

    query = ngx_aggr_get_module_main_conf(init, ngx_aggr_query_module);

    v = query->variables.elts;
    pv = query->prefix_variables.elts;
    key = query->variables_keys->keys.elts;

    for (i = 0; i < query->variables.nelts; i++) {

        for (n = 0; n < query->variables_keys->keys.nelts; n++) {

            av = key[n].value;

            if (v[i].name.len == key[n].key.len
                && ngx_strncmp(v[i].name.data, key[n].key.data, v[i].name.len)
                   == 0)
            {
                v[i].get_handler = av->get_handler;
                v[i].data = av->data;

                av->flags |= NGX_AGGR_VAR_INDEXED;
                v[i].flags = av->flags;

                av->index = i;

                if (av->get_handler == NULL
                    || (av->flags & NGX_AGGR_VAR_WEAK))
                {
                    break;
                }

                goto next;
            }
        }

        len = 0;
        av = NULL;

        for (n = 0; n < query->prefix_variables.nelts; n++) {
            if (v[i].name.len >= pv[n].name.len && v[i].name.len > len
                && ngx_strncmp(v[i].name.data, pv[n].name.data, pv[n].name.len)
                   == 0)
            {
                av = &pv[n];
                len = pv[n].name.len;
            }
        }

        if (av) {
            v[i].get_handler = av->get_handler;
            v[i].data = (uintptr_t) &v[i].name;
            v[i].flags = av->flags;

            if (av->init_handler) {
                if (av->init_handler(init, &v[i], av) != NGX_OK) {
                    return NGX_ERROR;
                }
            }

            goto next;
         }

        if (v[i].get_handler == NULL) {
            ngx_log_error(NGX_LOG_EMERG, init->pool->log, 0,
                          "unknown \"%V\" variable", &v[i].name);
            return NGX_ERROR;
        }

    next:
        continue;
    }


    for (n = 0; n < query->variables_keys->keys.nelts; n++) {
        av = key[n].value;

        if (av->flags & NGX_AGGR_VAR_NOHASH) {
            key[n].key.data = NULL;
        }
    }


    hash.hash = &query->variables_hash;
    hash.key = ngx_hash_key;
    hash.max_size = query->variables_hash_max_size;
    hash.bucket_size = query->variables_hash_bucket_size;
    hash.name = "variables_hash";
    hash.pool = init->pool;
    hash.temp_pool = NULL;

    if (ngx_hash_init(&hash, query->variables_keys->keys.elts,
                      query->variables_keys->keys.nelts)
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    query->variables_keys = NULL;

    return NGX_OK;
}
