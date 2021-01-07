
/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


static ngx_int_t ngx_aggr_script_init_arrays(
    ngx_aggr_script_compile_t *sc);
static ngx_int_t ngx_aggr_script_done(ngx_aggr_script_compile_t *sc);
static ngx_int_t ngx_aggr_script_add_copy_code(
    ngx_aggr_script_compile_t *sc, ngx_str_t *value, ngx_uint_t last);
static ngx_int_t ngx_aggr_script_add_var_code(
    ngx_aggr_script_compile_t *sc, ngx_str_t *name);
#if (NGX_PCRE)
static ngx_int_t ngx_aggr_script_add_capture_code(
    ngx_aggr_script_compile_t *sc, ngx_uint_t n);
#endif


ngx_int_t
ngx_aggr_complex_value(ngx_aggr_result_t *ar,
    ngx_aggr_complex_value_t *val, ngx_str_t *value)
{
    size_t                       len;
    ngx_aggr_script_code_pt      code;
    ngx_aggr_script_engine_t     e;
    ngx_aggr_script_len_code_pt  lcode;

    if (val->lengths == NULL) {
        *value = val->value;
        return NGX_OK;
    }

    ngx_memzero(&e, sizeof(ngx_aggr_script_engine_t));

    e.ip = val->lengths;
    e.ar = ar;
    e.flushed = 1;

    len = 0;

    while (*(uintptr_t *) e.ip) {
        lcode = *(ngx_aggr_script_len_code_pt *) e.ip;
        len += lcode(&e);
    }

    value->len = len;
    value->data = ngx_aggr_result_temp_alloc(ar, len);
    if (value->data == NULL) {
        return NGX_ERROR;
    }

    e.ip = val->values;
    e.pos = value->data;
    e.buf = *value;

    while (*(uintptr_t *) e.ip) {
        code = *(ngx_aggr_script_code_pt *) e.ip;
        code((ngx_aggr_script_engine_t *) &e);
    }

    *value = e.buf;

    return NGX_OK;
}


ngx_int_t
ngx_aggr_compile_complex_value(ngx_aggr_compile_complex_value_t *ccv)
{
    ngx_str_t                  *v;
    ngx_uint_t                  i, n, nv, nc;
    ngx_array_t                 lengths, values, *pl, *pv;
    ngx_aggr_script_compile_t   sc;

    v = ccv->value;

    nv = 0;
    nc = 0;

    for (i = 0; i < v->len; i++) {
        if (v->data[i] == '$') {
            if (v->data[i + 1] >= '1' && v->data[i + 1] <= '9') {
                nc++;

            } else {
                nv++;
            }
        }
    }

    ccv->complex_value->value = *v;
    ccv->complex_value->lengths = NULL;
    ccv->complex_value->values = NULL;

    if (nv == 0 && nc == 0) {
        return NGX_OK;
    }

    n = nv + 1;

    n = nv * (2 * sizeof(ngx_aggr_script_copy_code_t)
                  + sizeof(ngx_aggr_script_var_code_t))
        + sizeof(uintptr_t);

    if (ngx_array_init(&lengths, ccv->init->pool, n, 1) != NGX_OK) {
        return NGX_ERROR;
    }

    n = (nv * (2 * sizeof(ngx_aggr_script_copy_code_t)
                   + sizeof(ngx_aggr_script_var_code_t))
                + sizeof(uintptr_t)
                + v->len
                + sizeof(uintptr_t) - 1)
            & ~(sizeof(uintptr_t) - 1);

    if (ngx_array_init(&values, ccv->init->pool, n, 1) != NGX_OK) {
        return NGX_ERROR;
    }

    pl = &lengths;
    pv = &values;

    ngx_memzero(&sc, sizeof(ngx_aggr_script_compile_t));

    sc.init = ccv->init;
    sc.source = v;
    sc.lengths = &pl;
    sc.values = &pv;
    sc.complete_lengths = 1;
    sc.complete_values = 1;
    sc.zero = ccv->zero;

    if (ngx_aggr_script_compile(&sc) != NGX_OK) {
        return NGX_ERROR;
    }

    ccv->complex_value->lengths = lengths.elts;
    ccv->complex_value->values = values.elts;

    return NGX_OK;
}


ngx_uint_t
ngx_aggr_script_variables_count(ngx_str_t *value)
{
    ngx_uint_t  i, n;

    for (n = 0, i = 0; i < value->len; i++) {
        if (value->data[i] == '$') {
            n++;
        }
    }

    return n;
}


ngx_int_t
ngx_aggr_script_compile(ngx_aggr_script_compile_t *sc)
{
    u_char       ch;
    ngx_str_t    name;
    ngx_uint_t   i, bracket;

    if (ngx_aggr_script_init_arrays(sc) != NGX_OK) {
        return NGX_ERROR;
    }

    for (i = 0; i < sc->source->len; /* void */ ) {

        name.len = 0;

        if (sc->source->data[i] == '$') {

            if (++i == sc->source->len) {
                goto invalid_variable;
            }

            if (sc->source->data[i] >= '1' && sc->source->data[i] <= '9') {
#if (NGX_PCRE)
                ngx_uint_t  n;

                n = sc->source->data[i] - '0';

                if (ngx_aggr_script_add_capture_code(sc, n) != NGX_OK) {
                    return NGX_ERROR;
                }

                i++;

                continue;
#else
                ngx_log_error(NGX_LOG_ERR, sc->init->pool->log, 0,
                              "using variable \"$%c\" requires "
                              "PCRE library", sc->source->data[i]);
                return NGX_ERROR;
#endif
            }

            if (sc->source->data[i] == '{') {
                bracket = 1;

                if (++i == sc->source->len) {
                    goto invalid_variable;
                }

                name.data = &sc->source->data[i];

            } else {
                bracket = 0;
                name.data = &sc->source->data[i];
            }

            for ( /* void */ ; i < sc->source->len; i++, name.len++) {
                ch = sc->source->data[i];

                if (ch == '}' && bracket) {
                    i++;
                    bracket = 0;
                    break;
                }

                if ((ch >= 'A' && ch <= 'Z')
                    || (ch >= 'a' && ch <= 'z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '_')
                {
                    continue;
                }

                break;
            }

            if (bracket) {
                ngx_log_error(NGX_LOG_ERR, sc->init->pool->log, 0,
                                   "the closing bracket in \"%V\" "
                                   "variable is missing", &name);
                return NGX_ERROR;
            }

            if (name.len == 0) {
                goto invalid_variable;
            }

            sc->variables++;

            if (ngx_aggr_script_add_var_code(sc, &name) != NGX_OK) {
                return NGX_ERROR;
            }

            continue;
        }

        name.data = &sc->source->data[i];

        while (i < sc->source->len) {

            if (sc->source->data[i] == '$') {
                break;
            }

            i++;
            name.len++;
        }

        sc->size += name.len;

        if (ngx_aggr_script_add_copy_code(sc, &name, (i == sc->source->len))
            != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    return ngx_aggr_script_done(sc);

invalid_variable:

    ngx_log_error(NGX_LOG_ERR, sc->init->pool->log, 0,
                  "invalid variable name");

    return NGX_ERROR;
}


static ngx_int_t
ngx_aggr_script_init_arrays(ngx_aggr_script_compile_t *sc)
{
    ngx_uint_t   n;

    if (*sc->lengths == NULL) {
        n = sc->variables * (2 * sizeof(ngx_aggr_script_copy_code_t)
                             + sizeof(ngx_aggr_script_var_code_t))
            + sizeof(uintptr_t);

        *sc->lengths = ngx_array_create(sc->init->pool, n, 1);
        if (*sc->lengths == NULL) {
            return NGX_ERROR;
        }
    }

    if (*sc->values == NULL) {
        n = (sc->variables * (2 * sizeof(ngx_aggr_script_copy_code_t)
                              + sizeof(ngx_aggr_script_var_code_t))
                + sizeof(uintptr_t)
                + sc->source->len
                + sizeof(uintptr_t) - 1)
            & ~(sizeof(uintptr_t) - 1);

        *sc->values = ngx_array_create(sc->init->pool, n, 1);
        if (*sc->values == NULL) {
            return NGX_ERROR;
        }
    }

    sc->variables = 0;

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_script_done(ngx_aggr_script_compile_t *sc)
{
    ngx_str_t    zero;
    uintptr_t   *code;

    if (sc->zero) {

        zero.len = 1;
        zero.data = (u_char *) "\0";

        if (ngx_aggr_script_add_copy_code(sc, &zero, 0) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    if (sc->complete_lengths) {
        code = ngx_aggr_script_add_code(*sc->lengths, sizeof(uintptr_t),
                                        NULL);
        if (code == NULL) {
            return NGX_ERROR;
        }

        *code = (uintptr_t) NULL;
    }

    if (sc->complete_values) {
        code = ngx_aggr_script_add_code(*sc->values, sizeof(uintptr_t),
                                        &sc->main);
        if (code == NULL) {
            return NGX_ERROR;
        }

        *code = (uintptr_t) NULL;
    }

    return NGX_OK;
}


void *
ngx_aggr_script_add_code(ngx_array_t *codes, size_t size, void *code)
{
    u_char  *elts, **p;
    void    *new;

    elts = codes->elts;

    new = ngx_array_push_n(codes, size);
    if (new == NULL) {
        return NULL;
    }

    if (code) {
        if (elts != codes->elts) {
            p = code;
            *p += (u_char *) codes->elts - elts;
        }
    }

    return new;
}


static ngx_int_t
ngx_aggr_script_add_copy_code(ngx_aggr_script_compile_t *sc,
    ngx_str_t *value, ngx_uint_t last)
{
    u_char                       *p;
    size_t                        size, len, zero;
    ngx_aggr_script_copy_code_t  *code;

    zero = (sc->zero && last);
    len = value->len + zero;

    code = ngx_aggr_script_add_code(*sc->lengths,
                                    sizeof(ngx_aggr_script_copy_code_t),
                                    NULL);
    if (code == NULL) {
        return NGX_ERROR;
    }

    code->code = (ngx_aggr_script_code_pt) (void *)
                                               ngx_aggr_script_copy_len_code;
    code->len = len;

    size = (sizeof(ngx_aggr_script_copy_code_t) + len + sizeof(uintptr_t) - 1)
            & ~(sizeof(uintptr_t) - 1);

    code = ngx_aggr_script_add_code(*sc->values, size, &sc->main);
    if (code == NULL) {
        return NGX_ERROR;
    }

    code->code = ngx_aggr_script_copy_code;
    code->len = len;

    p = ngx_cpymem((u_char *) code + sizeof(ngx_aggr_script_copy_code_t),
                   value->data, value->len);

    if (zero) {
        *p = '\0';
        sc->zero = 0;
    }

    return NGX_OK;
}


size_t
ngx_aggr_script_copy_len_code(ngx_aggr_script_engine_t *e)
{
    ngx_aggr_script_copy_code_t  *code;

    code = (ngx_aggr_script_copy_code_t *) e->ip;

    e->ip += sizeof(ngx_aggr_script_copy_code_t);

    return code->len;
}


void
ngx_aggr_script_copy_code(ngx_aggr_script_engine_t *e)
{
    u_char                       *p;
    ngx_aggr_script_copy_code_t  *code;

    code = (ngx_aggr_script_copy_code_t *) e->ip;

    p = e->pos;

    if (!e->skip) {
        e->pos = ngx_copy(p, e->ip + sizeof(ngx_aggr_script_copy_code_t),
                          code->len);
    }

    e->ip += sizeof(ngx_aggr_script_copy_code_t)
          + ((code->len + sizeof(uintptr_t) - 1) & ~(sizeof(uintptr_t) - 1));

    ngx_log_debug2(NGX_LOG_DEBUG_CORE, e->ar->pool->log, 0,
                   "aggr script copy: \"%*s\"", e->pos - p, p);
}


static ngx_int_t
ngx_aggr_script_add_var_code(ngx_aggr_script_compile_t *sc, ngx_str_t *name)
{
    ngx_int_t                    index;
    ngx_aggr_script_var_code_t  *code;

    index = ngx_aggr_get_variable_index(sc->init, name);

    if (index == NGX_ERROR) {
        return NGX_ERROR;
    }

    code = ngx_aggr_script_add_code(*sc->lengths,
                                    sizeof(ngx_aggr_script_var_code_t),
                                    NULL);
    if (code == NULL) {
        return NGX_ERROR;
    }

    code->code = (ngx_aggr_script_code_pt) (void *)
                                           ngx_aggr_script_copy_var_len_code;
    code->index = (uintptr_t) index;

    code = ngx_aggr_script_add_code(*sc->values,
                                    sizeof(ngx_aggr_script_var_code_t),
                                    &sc->main);
    if (code == NULL) {
        return NGX_ERROR;
    }

    code->code = ngx_aggr_script_copy_var_code;
    code->index = (uintptr_t) index;

    return NGX_OK;
}


size_t
ngx_aggr_script_copy_var_len_code(ngx_aggr_script_engine_t *e)
{
    ngx_aggr_variable_value_t   *value;
    ngx_aggr_script_var_code_t  *code;

    code = (ngx_aggr_script_var_code_t *) e->ip;

    e->ip += sizeof(ngx_aggr_script_var_code_t);

    if (e->flushed) {
        value = ngx_aggr_get_indexed_variable(e->ar, code->index);

    } else {
        value = ngx_aggr_get_flushed_variable(e->ar, code->index);
    }

    if (value && !value->not_found) {
        return value->len;
    }

    return 0;
}


void
ngx_aggr_script_copy_var_code(ngx_aggr_script_engine_t *e)
{
    u_char                      *p;
    ngx_aggr_variable_value_t   *value;
    ngx_aggr_script_var_code_t  *code;

    code = (ngx_aggr_script_var_code_t *) e->ip;

    e->ip += sizeof(ngx_aggr_script_var_code_t);

    if (!e->skip) {

        if (e->flushed) {
            value = ngx_aggr_get_indexed_variable(e->ar, code->index);

        } else {
            value = ngx_aggr_get_flushed_variable(e->ar, code->index);
        }

        if (value && !value->not_found) {
            p = e->pos;
            e->pos = ngx_copy(p, value->data, value->len);

            ngx_log_debug2(NGX_LOG_DEBUG_CORE,
                           e->ar->pool->log, 0,
                           "aggr script var: \"%*s\"", e->pos - p, p);
        }
    }
}


#if (NGX_PCRE)

static ngx_int_t
ngx_aggr_script_add_capture_code(ngx_aggr_script_compile_t *sc,
    ngx_uint_t n)
{
    ngx_aggr_script_copy_capture_code_t  *code;

    code = ngx_aggr_script_add_code(*sc->lengths,
                                  sizeof(ngx_aggr_script_copy_capture_code_t),
                                  NULL);
    if (code == NULL) {
        return NGX_ERROR;
    }

    code->code = (ngx_aggr_script_code_pt) (void *)
                                       ngx_aggr_script_copy_capture_len_code;
    code->n = 2 * n;


    code = ngx_aggr_script_add_code(*sc->values,
                                  sizeof(ngx_aggr_script_copy_capture_code_t),
                                  &sc->main);
    if (code == NULL) {
        return NGX_ERROR;
    }

    code->code = ngx_aggr_script_copy_capture_code;
    code->n = 2 * n;

    if (sc->ncaptures < n) {
        sc->ncaptures = n;
    }

    return NGX_OK;
}


size_t
ngx_aggr_script_copy_capture_len_code(ngx_aggr_script_engine_t *e)
{
    int                                  *cap;
    ngx_uint_t                            n;
    ngx_aggr_result_t                    *ar;
    ngx_aggr_script_copy_capture_code_t  *code;

    ar = e->ar;

    code = (ngx_aggr_script_copy_capture_code_t *) e->ip;

    e->ip += sizeof(ngx_aggr_script_copy_capture_code_t);

    n = code->n;

    if (n < ar->ncaptures) {
        cap = ar->captures;
        return cap[n + 1] - cap[n];
    }

    return 0;
}


void
ngx_aggr_script_copy_capture_code(ngx_aggr_script_engine_t *e)
{
    int                                  *cap;
    u_char                               *p, *pos;
    ngx_uint_t                            n;
    ngx_aggr_result_t                    *ar;
    ngx_aggr_script_copy_capture_code_t  *code;

    ar = e->ar;

    code = (ngx_aggr_script_copy_capture_code_t *) e->ip;

    e->ip += sizeof(ngx_aggr_script_copy_capture_code_t);

    n = code->n;

    pos = e->pos;

    if (n < ar->ncaptures) {
        cap = ar->captures;
        p = ar->captures_data;
        e->pos = ngx_copy(pos, &p[cap[n]], cap[n + 1] - cap[n]);
    }

    ngx_log_debug2(NGX_LOG_DEBUG_CORE, e->ar->pool->log, 0,
                   "aggr script capture: \"%*s\"", e->pos - pos, pos);
}

#endif
