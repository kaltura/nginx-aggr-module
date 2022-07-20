#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"
#include "ngx_ip2l.h"
#include "ngx_aggr_map_ip2l.h"


typedef struct {
    ngx_aggr_complex_value_t   value;
    ngx_ip2l_file_t           *file;
    ngx_ip2l_field_e           field;
} ngx_aggr_map_ip2l_ctx_t;


static char *ngx_aggr_map_ip2l(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_command_t  ngx_aggr_map_ip2l_commands[] = {

    { ngx_string("map_ip2l"),
      NGX_AGGR_MAIN_CONF|NGX_CONF_TAKE4,
      ngx_aggr_map_ip2l,
      NGX_AGGR_MAIN_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};


static ngx_aggr_module_t  ngx_aggr_map_ip2l_module_ctx = {
    NULL,                                  /* create main configuration */
    NULL                                   /* init main configuration */
};


ngx_module_t  ngx_aggr_map_ip2l_module = {
    NGX_MODULE_V1,
    &ngx_aggr_map_ip2l_module_ctx,         /* module context */
    ngx_aggr_map_ip2l_commands,            /* module directives */
    NGX_AGGR_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_aggr_map_ip2l_variable(ngx_aggr_result_t *ar, ngx_aggr_variable_value_t *v,
    uintptr_t data)
{
    ngx_aggr_map_ip2l_ctx_t  *ctx = (ngx_aggr_map_ip2l_ctx_t *) data;

    ngx_int_t  rc;
    ngx_str_t  val, str;

    if (ngx_aggr_complex_value(ar, &ctx->value, &val) != NGX_OK) {
        return NGX_ERROR;
    }

    rc = ngx_ip2l_file_get_field(ctx->file, &val, ctx->field, ar->pool, &str);
    switch (rc) {

    case NGX_OK:
        v->valid = 1;
        v->no_cacheable = 0;
        v->not_found = 0;
        v->len = str.len;
        v->data = str.data;
        break;

    case NGX_DECLINED:
        v->not_found = 1;
        break;

    default:
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_map_ip2l_add_variable(ngx_aggr_query_init_t *init, ngx_str_t *name,
    ngx_str_t *input, ngx_str_t *file, ngx_str_t *field)
{
    ngx_aggr_variable_t               *var;
    ngx_aggr_map_ip2l_ctx_t           *ctx;
    ngx_aggr_compile_complex_value_t   ccv;

    ctx = ngx_pcalloc(init->pool, sizeof(ngx_aggr_map_ip2l_ctx_t));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ngx_memzero(&ccv, sizeof(ngx_aggr_compile_complex_value_t));

    ccv.init = init;
    ccv.value = input;
    ccv.complex_value = &ctx->value;

    if (ngx_aggr_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_ERROR;
    }

    if (!name->len || name->data[0] != '$') {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_ip2l_add_variable: "
            "invalid variable name \"%V\"", name);
        return NGX_BAD_QUERY;
    }

    name->len--;
    name->data++;

    var = ngx_aggr_add_variable(init, name, NGX_AGGR_VAR_CHANGEABLE);
    if (var == NULL) {
        return NGX_ERROR;
    }

    var->get_handler = ngx_aggr_map_ip2l_variable;
    var->data = (uintptr_t) ctx;

    ctx->file = ngx_ip2l_file_get(init->cycle, file);
    if (ctx->file == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_ip2l_add_variable: "
            "unknown ip2l file \"%V\"", file);
        return NGX_BAD_QUERY;
    }

    ctx->field = ngx_ip2l_parse_field_name(field);
    if (ctx->field == ngx_ip2l_field_count) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_ip2l_add_variable: "
            "unknown ip2l field \"%V\"", field);
        return NGX_BAD_QUERY;
    }

    if (!ngx_ip2l_file_field_exists(ctx->file, ctx->field)) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_ip2l_add_variable: "
            "ip2l field \"%V\" does not exist in file \"%V\"", field, file);
        return NGX_BAD_QUERY;
    }

    return NGX_OK;
}


static char *
ngx_aggr_map_ip2l(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t              *value;
    ngx_aggr_query_init_t  *init;

    init = cf->ctx;
    value = cf->args->elts;

    if (ngx_aggr_map_ip2l_add_variable(init, &value[4], &value[1], &value[2],
                                       &value[3]) != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


ngx_int_t
ngx_aggr_map_ip2l_json(ngx_aggr_query_init_t *init, ngx_str_t *output,
    ngx_json_object_t *obj)
{
    ngx_uint_t             i, n;
    ngx_str_t             *input, *file, *field;
    ngx_json_key_value_t  *elts;

    input = NULL;
    file = NULL;
    field = NULL;

    elts = obj->elts;
    n = obj->nelts;

    for (i = 0; i < n; i++) {

        switch (elts[i].value.type) {

        case NGX_JSON_STRING:
            if (ngx_str_equals_c(elts[i].key, "type")) {
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "input")) {
                input = &elts[i].value.v.str;
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "file")) {
                file = &elts[i].value.v.str;
                continue;

            } else if (ngx_str_equals_c(elts[i].key, "field")) {
                field = &elts[i].value.v.str;
                continue;
            }
            break;
        }

        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_ip2l_json: invalid parameter \"%V\"",
            &elts[i].key);
        return NGX_BAD_QUERY;
    }

    if (input == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_ip2l_json: missing \"input\" key");
        return NGX_BAD_QUERY;
    }

    if (file == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_ip2l_json: missing \"file\" key");
        return NGX_BAD_QUERY;
    }

    if (field == NULL) {
        ngx_log_error(NGX_LOG_ERR, init->pool->log, 0,
            "ngx_aggr_map_ip2l_json: missing \"field\" key");
        return NGX_BAD_QUERY;
    }

    return ngx_aggr_map_ip2l_add_variable(init, output, input, file, field);
}
