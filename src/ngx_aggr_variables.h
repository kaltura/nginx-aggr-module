
/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */


#ifndef _NGX_AGGR_VARIABLES_H_INCLUDED_
#define _NGX_AGGR_VARIABLES_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


typedef ngx_variable_value_t  ngx_aggr_variable_value_t;

#define ngx_aggr_variable(v)     { sizeof(v) - 1, 1, 0, 0, 0, (u_char *) v }

typedef struct ngx_aggr_variable_s  ngx_aggr_variable_t;

typedef void (*ngx_aggr_set_variable_pt) (ngx_aggr_result_t *ar,
    ngx_aggr_variable_value_t *v, uintptr_t data);
typedef ngx_int_t (*ngx_aggr_get_variable_pt) (ngx_aggr_result_t *ar,
    ngx_aggr_variable_value_t *v, uintptr_t data);
typedef ngx_int_t (*ngx_aggr_init_variable_pt) (ngx_aggr_query_init_t *init,
    ngx_aggr_variable_t *dst, ngx_aggr_variable_t *src);

#define NGX_AGGR_VAR_CHANGEABLE   1
#define NGX_AGGR_VAR_NOCACHEABLE  2
#define NGX_AGGR_VAR_INDEXED      4
#define NGX_AGGR_VAR_NOHASH       8
#define NGX_AGGR_VAR_WEAK         16
#define NGX_AGGR_VAR_PREFIX       32


struct ngx_aggr_variable_s {
    ngx_str_t                   name;   /* must be first to build the hash */
    ngx_aggr_set_variable_pt    set_handler;
    ngx_aggr_get_variable_pt    get_handler;
    ngx_aggr_init_variable_pt   init_handler;

    uintptr_t                   data;
    ngx_uint_t                  flags;
    ngx_uint_t                  index;
};

#define ngx_aggr_null_variable  { ngx_null_string, NULL, NULL, NULL, 0, 0, 0 }


ngx_aggr_variable_t *ngx_aggr_add_variable(ngx_aggr_query_init_t *init,
    ngx_str_t *name, ngx_uint_t flags);
ngx_int_t ngx_aggr_get_variable_index(ngx_aggr_query_init_t *init,
    ngx_str_t *name);
ngx_aggr_variable_value_t *ngx_aggr_get_indexed_variable(
    ngx_aggr_result_t *ar, ngx_uint_t index);
ngx_aggr_variable_value_t *ngx_aggr_get_flushed_variable(
    ngx_aggr_result_t *ar, ngx_uint_t index);


#if (NGX_PCRE)

typedef struct {
    ngx_uint_t                  capture;
    ngx_int_t                   index;
} ngx_aggr_regex_variable_t;


typedef struct {
    ngx_regex_t                *regex;
    ngx_uint_t                  ncaptures;
    ngx_aggr_regex_variable_t  *variables;
    ngx_uint_t                  nvariables;
    ngx_str_t                   name;
} ngx_aggr_regex_t;


typedef struct {
    ngx_aggr_regex_t           *regex;
    void                       *value;
} ngx_aggr_map_regex_t;


ngx_aggr_regex_t *ngx_aggr_regex_compile(ngx_aggr_query_init_t *init,
    ngx_regex_compile_t *rc);
ngx_int_t ngx_aggr_regex_exec(ngx_aggr_result_t *ar, ngx_aggr_regex_t *re,
    ngx_str_t *str);

#endif


typedef struct {
    ngx_hash_t                  hash;
#if (NGX_PCRE)
    ngx_aggr_map_regex_t       *regex;
    ngx_uint_t                  nregex;
#endif
} ngx_aggr_map_t;


void *ngx_aggr_map_find(ngx_aggr_result_t *ar, ngx_aggr_map_t *map,
    ngx_str_t *match);


ngx_int_t ngx_aggr_variables_add_core_vars(ngx_aggr_query_init_t *init);
ngx_int_t ngx_aggr_variables_init_vars(ngx_aggr_query_init_t *init);


extern ngx_aggr_variable_value_t  ngx_aggr_variable_null_value;


#endif /* _NGX_AGGR_VARIABLES_H_INCLUDED_ */
