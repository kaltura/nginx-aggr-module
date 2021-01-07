
/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */


#ifndef _NGX_AGGR_SCRIPT_H_INCLUDED_
#define _NGX_AGGR_SCRIPT_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


typedef struct {
    u_char                     *ip;
    u_char                     *pos;
    ngx_aggr_variable_value_t  *sp;

    ngx_str_t                   buf;
    ngx_str_t                   line;

    unsigned                    flushed:1;
    unsigned                    skip:1;

    ngx_aggr_result_t          *ar;
} ngx_aggr_script_engine_t;


typedef struct {
    ngx_aggr_query_init_t      *init;
    ngx_str_t                  *source;

    ngx_array_t               **lengths;
    ngx_array_t               **values;

    ngx_uint_t                  variables;
    ngx_uint_t                  ncaptures;
    ngx_uint_t                  size;

    void                       *main;

    unsigned                    complete_lengths:1;
    unsigned                    complete_values:1;
    unsigned                    zero:1;
} ngx_aggr_script_compile_t;


typedef struct {
    ngx_str_t                   value;
    void                       *lengths;
    void                       *values;

    union {
        size_t                  size;
    } u;
} ngx_aggr_complex_value_t;


typedef struct {
    ngx_aggr_query_init_t      *init;
    ngx_str_t                  *value;
    ngx_aggr_complex_value_t   *complex_value;

    unsigned                    zero:1;
} ngx_aggr_compile_complex_value_t;


typedef void (*ngx_aggr_script_code_pt) (ngx_aggr_script_engine_t *e);
typedef size_t (*ngx_aggr_script_len_code_pt) (ngx_aggr_script_engine_t *e);


typedef struct {
    ngx_aggr_script_code_pt     code;
    uintptr_t                   len;
} ngx_aggr_script_copy_code_t;


typedef struct {
    ngx_aggr_script_code_pt     code;
    uintptr_t                   index;
} ngx_aggr_script_var_code_t;


typedef struct {
    ngx_aggr_script_code_pt     code;
    uintptr_t                   n;
} ngx_aggr_script_copy_capture_code_t;


ngx_int_t ngx_aggr_complex_value(ngx_aggr_result_t *ar,
    ngx_aggr_complex_value_t *val, ngx_str_t *value);
ngx_int_t ngx_aggr_compile_complex_value(
    ngx_aggr_compile_complex_value_t *ccv);

ngx_uint_t ngx_aggr_script_variables_count(ngx_str_t *value);
ngx_int_t ngx_aggr_script_compile(ngx_aggr_script_compile_t *sc);

void *ngx_aggr_script_add_code(ngx_array_t *codes, size_t size, void *code);

size_t ngx_aggr_script_copy_len_code(ngx_aggr_script_engine_t *e);
void ngx_aggr_script_copy_code(ngx_aggr_script_engine_t *e);
size_t ngx_aggr_script_copy_var_len_code(ngx_aggr_script_engine_t *e);
void ngx_aggr_script_copy_var_code(ngx_aggr_script_engine_t *e);
size_t ngx_aggr_script_copy_capture_len_code(ngx_aggr_script_engine_t *e);
void ngx_aggr_script_copy_capture_code(ngx_aggr_script_engine_t *e);

#endif /* _NGX_AGGR_SCRIPT_H_INCLUDED_ */
