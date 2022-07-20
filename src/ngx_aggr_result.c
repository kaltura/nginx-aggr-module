#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_event.h>
#include <ctype.h>
#include "ngx_aggr.h"
#include "ngx_rate_limit.h"


#define NGX_AGGR_RESULT_STATUS_FORMAT                                        \
    "aggr_event_processed %A\n"                                              \
    "aggr_event_parse_ok %A\n"                                               \
    "aggr_event_parse_err %A\n"                                              \
    "aggr_event_created %A\n"


#define NGX_AGGR_EVENT_JSON_ERROR_EXTRACT   (20)

#define NGX_AGGR_EVENT_JSON_ERROR_MSG_SIZE  (128)


typedef struct {
    u_char                   *p;
    u_char                   *err;
    size_t                    err_size;
} ngx_aggr_event_json_ctx_t;


typedef struct {
    ngx_atomic_t              ref_count;
    u_char                    data[1];
} ngx_aggr_result_send_buf_t;


typedef size_t(*ngx_aggr_event_write_size_pt)(ngx_aggr_result_t *ar,
    ngx_aggr_event_t *event);

typedef u_char *(*ngx_aggr_event_write_pt)(u_char *p, ngx_aggr_result_t *ar,
    ngx_aggr_event_t *event);


static ngx_rate_limit_ctx_t  ngx_aggr_event_error_rate =
    ngx_rate_limit_init(1, 4);


/* TODO: move the stats to shared memory */
static ngx_atomic_t   ngx_aggr_event_processed0 = 0;
ngx_atomic_t         *ngx_aggr_event_processed = &ngx_aggr_event_processed0;
static ngx_atomic_t   ngx_aggr_event_parse_ok0 = 0;
ngx_atomic_t         *ngx_aggr_event_parse_ok = &ngx_aggr_event_parse_ok0;
static ngx_atomic_t   ngx_aggr_event_parse_errs0 = 0;
ngx_atomic_t         *ngx_aggr_event_parse_errs = &ngx_aggr_event_parse_errs0;
static ngx_atomic_t   ngx_aggr_event_created0 = 0;
ngx_atomic_t         *ngx_aggr_event_created = &ngx_aggr_event_created0;


static ngx_int_t
ngx_aggr_event_alloc(ngx_aggr_result_t *ar)
{
    ngx_aggr_event_t  *event;

    event = ngx_palloc(ar->pool, ar->event_size);
    if (event == NULL) {
        return NGX_ERROR;
    }

    event->group_size = ar->group_size;
    ar->cur = event;

    return NGX_OK;
}


static int
ngx_aggr_event_compare(ngx_aggr_event_t *event1, ngx_aggr_event_t *event2)
{
    size_t       off;
    ngx_str_t   *s1, *s2;
    ngx_str_t  **s1p, **s2p;

    for (off = 0; off < event1->group_size; off += sizeof(s1)) {

        s1p = (ngx_str_t **) (event1->data + off);
        s2p = (ngx_str_t **) (event2->data + off);

        s1 = *s1p;
        s2 = *s2p;

        /* Note: dims are from string table, can just compare the ptrs */
        if (s1 < s2) {
            return -1;
        }

        if (s1 > s2) {
            return 1;
        }
    }

    return 0;
}


static void
ngx_aggr_event_rbtree_insert_value(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
    ngx_aggr_event_t    *n, *t;
    ngx_rbtree_node_t  **p;

    for ( ;; ) {

        n = (ngx_aggr_event_t *) node;
        t = (ngx_aggr_event_t *) temp;

        if (node->key != temp->key) {
            p = (node->key < temp->key) ? &temp->left : &temp->right;

        } else {
            p = ngx_aggr_event_compare(n, t) < 0 ? &temp->left : &temp->right;
        }

        if (*p == sentinel) {
            break;
        }

        temp = *p;
    }

    *p = node;
    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}


static ngx_aggr_event_t *
ngx_aggr_event_rbtree_lookup(ngx_rbtree_t *rbtree, ngx_aggr_event_t *event)
{
    ngx_int_t           rc;
    ngx_aggr_event_t   *cur;
    ngx_rbtree_node_t  *node, *sentinel;

    node = rbtree->root;
    sentinel = rbtree->sentinel;

    while (node != sentinel) {

        if (event->node.key < node->key) {
            node = node->left;
            continue;
        }

        if (event->node.key > node->key) {
            node = node->right;
            continue;
        }

        cur = (ngx_aggr_event_t *) node;

        rc = ngx_aggr_event_compare(event, cur);

        if (rc < 0) {
            node = node->left;
            continue;
        }

        if (rc > 0) {
            node = node->right;
            continue;
        }

        return cur;
    }

    return NULL;
}


void *
ngx_aggr_result_temp_alloc(ngx_aggr_result_t *ar, size_t size)
{
    u_char  *p;
    size_t   alloc_size;

    p = ar->var_temp.last;

    if (size > (size_t) (ar->var_temp.end - p) || !p) {
        alloc_size = ar->var_temp.end - ar->var_temp.start;
        alloc_size *= 2;
        if (alloc_size < size) {
            alloc_size = size;
        }

        p = ngx_pnalloc(ar->pool, alloc_size);
        if (p == NULL) {
            return NULL;
        }

        ar->var_temp.start = ar->var_temp.last = p;
        ar->var_temp.end = p + alloc_size;
    }

    ar->var_temp.last += size;

    return p;
}


/* Note: change to 1 to support "pretty" jsons */
#define NGX_AGGR_EVENT_JSON_SKIP_SPACES  (0)


#define ngx_aggr_event_json_expect_char(ctx, c)                              \
    if (*ctx->p != c) {                                                      \
        ngx_snprintf(ctx->err, ctx->err_size, "expected '%c'%Z", c);         \
        return NGX_ERROR;                                                    \
    }                                                                        \
    ctx->p++;



#if (NGX_AGGR_EVENT_JSON_SKIP_SPACES)

#define ngx_aggr_event_json_skip_spaces(ctx)                                 \
    for (; isspace(*(ctx)->p) && *(ctx)->p; (ctx)->p++);

#else

#define ngx_aggr_event_json_skip_spaces(ctx)

#endif


static ngx_int_t
ngx_aggr_event_json_skip_str(ngx_aggr_event_json_ctx_t *ctx)
{
    u_char  c;

    for ( ;; ) {

        c = *ctx->p;

        switch (c) {

        case '\\':
            ctx->p++;
            if (*ctx->p) {
                break;
            }

            /* fall through */

        case '\0':
            ngx_snprintf(ctx->err, ctx->err_size, "truncated string%Z");
            return NGX_ERROR;

        case '"':
            ctx->p++;       /* skip the " */
            return NGX_OK;
        }

        ctx->p++;
    }
}


static ngx_int_t
ngx_aggr_event_json_skip_num(ngx_aggr_event_json_ctx_t *ctx)
{
    /* sign */

    if (*ctx->p == '-') {
        ctx->p++;       /* skip the - */
    }

    /* integer */

    if (*ctx->p == '0') {
        ctx->p++;       /* skip the 0 */

    } else if (*ctx->p >= '1' && *ctx->p <= '9') {
        do {
            ctx->p++;       /* skip the digit */
        } while (isdigit(*ctx->p));

    } else {
        ngx_snprintf(ctx->err, ctx->err_size, "expected digit%Z");
        return NGX_ERROR;
    }

    /* fraction */

    if (*ctx->p == '.') {
        ctx->p++;       /* skip the . */

        if (!isdigit(*ctx->p)) {
            ngx_snprintf(ctx->err, ctx->err_size,
                "expected digit after '.'%Z");
            return NGX_ERROR;
        }

        do {
            ctx->p++;       /* skip the digit */
        } while (isdigit(*ctx->p));
    }

    /* exponent */

    if (*ctx->p == 'e' || *ctx->p == 'E') {
        ctx->p++;       /* skip the e */

        if (*ctx->p == '-' || *ctx->p == '+') {
            ctx->p++;       /* skip the -/+ */
        }

        if (!isdigit(*ctx->p)) {
            ngx_snprintf(ctx->err, ctx->err_size,
                "expected digit after 'e'%Z");
            return NGX_ERROR;
        }

        do {
            ctx->p++;       /* skip the digit */
        } while (isdigit(*ctx->p));
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_event_json_skip_value(ngx_aggr_event_json_ctx_t *ctx)
{
    switch (*ctx->p) {

    case '"':
        ctx->p++;       /* skip the " */
        return ngx_aggr_event_json_skip_str(ctx);

    case 'n':
        ctx->p++;       /* skip the n */
        ngx_aggr_event_json_expect_char(ctx, 'u');
        ngx_aggr_event_json_expect_char(ctx, 'l');
        ngx_aggr_event_json_expect_char(ctx, 'l');
        return NGX_OK;

    case 't':
        ctx->p++;       /* skip the t */
        ngx_aggr_event_json_expect_char(ctx, 'r');
        ngx_aggr_event_json_expect_char(ctx, 'u');
        ngx_aggr_event_json_expect_char(ctx, 'e');
        return NGX_OK;

    case 'f':
        ctx->p++;       /* skip the f */
        ngx_aggr_event_json_expect_char(ctx, 'a');
        ngx_aggr_event_json_expect_char(ctx, 'l');
        ngx_aggr_event_json_expect_char(ctx, 's');
        ngx_aggr_event_json_expect_char(ctx, 'e');
        return NGX_OK;

    case '[':
        ngx_snprintf(ctx->err, ctx->err_size,
            "json arrays are unsupported%Z");
        return NGX_ERROR;

    case '{':
        ngx_snprintf(ctx->err, ctx->err_size,
            "nested json objects are unsupported%Z");
        return NGX_ERROR;

    default:
        return ngx_aggr_event_json_skip_num(ctx);
    }
}


static ngx_int_t
ngx_aggr_event_json_str(ngx_aggr_event_json_ctx_t *ctx, ngx_str_hash_t *sh)
{
    u_char  c;

    sh->hash = 0;

    sh->s.data = ctx->p;

    for ( ;; ) {

        c = *ctx->p;

        switch (c) {

        case '\\':
            sh->hash = ngx_hash(sh->hash, c);

            ctx->p++;       /* skip the \ */
            c = *ctx->p;
            if (c) {
                break;
            }

            /* fall through */

        case '\0':
            ngx_snprintf(ctx->err, ctx->err_size, "truncated string%Z");
            return NGX_ERROR;

        case '"':
            sh->s.len = ctx->p - sh->s.data;
            ctx->p++;       /* skip the " */
            return NGX_OK;
        }

        sh->hash = ngx_hash(sh->hash, c);

        ctx->p++;
    }
}


static ngx_int_t
ngx_aggr_event_json_str_lc(ngx_aggr_event_json_ctx_t *ctx, ngx_str_hash_t *sh)
{
    u_char  c;

    sh->hash = 0;

    sh->s.data = ctx->p;

    for ( ;; ) {

        c = *ctx->p;

        switch (c) {

        case '\\':
            sh->hash = ngx_hash(sh->hash, c);

            ctx->p++;       /* skip the \ */
            c = *ctx->p;
            if (c) {
                break;
            }

            /* fall through */

        case '\0':
            ngx_snprintf(ctx->err, ctx->err_size, "truncated string%Z");
            return NGX_ERROR;

        case '"':
            sh->s.len = ctx->p - sh->s.data;
            ctx->p++;       /* skip the " */
            return NGX_OK;

        default:
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;      /* lowercase */
                *ctx->p = c;
            }
            break;
        }

        sh->hash = ngx_hash(sh->hash, c);

        ctx->p++;
    }
}


static ngx_int_t
ngx_aggr_event_json_num(ngx_aggr_event_json_ctx_t *ctx, double *val)
{
    u_char  *start;

    start = ctx->p;
    if (ngx_aggr_event_json_skip_num(ctx) != NGX_OK) {
        return NGX_ERROR;
    }

    *val = atof((char *) start);
    return NGX_OK;
}


static ngx_int_t
ngx_aggr_eval_complex_dims(ngx_aggr_result_t *ar)
{
    ngx_uint_t                     i, n;
    ngx_str_hash_t                *shp;
    ngx_aggr_query_t              *query;
    ngx_aggr_query_dim_complex_t  *complex;

    query = ar->query;

    ngx_memzero(ar->variables, query->variables.nelts
        * sizeof(ngx_aggr_variable_value_t));
    ar->var_temp.last = ar->var_temp.start;

    complex = query->dims_complex.elts;
    n = query->dims_complex.nelts;

    for (i = 0; i < n; i++) {
        shp = (ngx_str_hash_t *) (ar->temp_data + complex[i].temp_offset);
        if (ngx_aggr_complex_value(ar, &complex[i].value, &shp->s) != NGX_OK) {
            return NGX_ERROR;
        }

        shp->hash = ngx_hash_key(shp->s.data, shp->s.len);
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_event_json_parse(ngx_aggr_event_json_ctx_t *ctx,
    ngx_aggr_result_t *ar)
{
    size_t                         dst_offset;
    size_t                         temp_offset;
    double                        *dst_dbl;
    double                         metric_value;
    ngx_str_t                     *dim_value;
    ngx_str_t                    **dim_valuep;
    ngx_uint_t                     i;
    ngx_array_t                   *outputs;
    ngx_str_hash_t                 sh;
    ngx_str_hash_t                *shp;
    ngx_aggr_event_t              *event;
    ngx_aggr_query_t              *query;
    ngx_aggr_query_dim_in_t      **dims;
    ngx_aggr_query_metric_out_t  **metrics;

    query = ar->query;
    event = ar->cur;

    ngx_memcpy(ar->temp_data, query->temp_default, query->temp_size);
    ngx_memcpy(event->data + ar->metrics_offset, query->metrics_default,
        query->metrics_size);

    ngx_aggr_event_json_skip_spaces(ctx);

    ngx_aggr_event_json_expect_char(ctx, '{');

    ngx_aggr_event_json_skip_spaces(ctx);

    if (*ctx->p == '}') {
        ctx->p++;        /* skip } */
        goto done;
    }

    for ( ;; ) {

        ngx_aggr_event_json_expect_char(ctx, '"');

        if (ngx_aggr_event_json_str(ctx, &sh) != NGX_OK) {
            return NGX_ERROR;
        }

        ngx_aggr_event_json_skip_spaces(ctx);

        ngx_aggr_event_json_expect_char(ctx, ':');

        ngx_aggr_event_json_skip_spaces(ctx);

        if (*ctx->p == '"') {

            ctx->p++;       /* skip the " */

            outputs = ngx_hash_find(&query->dims_hash, sh.hash,
                sh.s.data, sh.s.len);
            if (outputs == NULL) {
                if (ngx_aggr_event_json_skip_str(ctx) != NGX_OK) {
                    return NGX_ERROR;
                }

                goto next;
            }

            dims = outputs->elts;

            /* TODO: support mix of lower = 1 & lower = 0 for a single dim */

            if (dims[0]->lower) {
                if (ngx_aggr_event_json_str_lc(ctx, &sh) != NGX_OK) {
                    return NGX_ERROR;
                }

            } else {
                if (ngx_aggr_event_json_str(ctx, &sh) != NGX_OK) {
                    return NGX_ERROR;
                }
            }

            for (i = 0; i < outputs->nelts; i++) {
                shp = (ngx_str_hash_t *) (ar->temp_data +
                    dims[i]->temp_offset);
                *shp = sh;
            }

            goto next;

        } else if (isdigit(*ctx->p) || *ctx->p == '-') {

            outputs = ngx_hash_find(&query->metrics_hash, sh.hash,
                sh.s.data, sh.s.len);
            if (outputs == NULL) {
                if (ngx_aggr_event_json_skip_num(ctx) != NGX_OK) {
                    return NGX_ERROR;
                }

                goto next;
            }

            if (ngx_aggr_event_json_num(ctx, &metric_value) != NGX_OK) {
                return NGX_ERROR;
            }

            metrics = outputs->elts;
            for (i = 0; i < outputs->nelts; i++) {
                dst_dbl = (double *) (event->data + metrics[i]->offset);
                *dst_dbl = metric_value;
            }

            goto next;
        }

        if (ngx_aggr_event_json_skip_value(ctx) != NGX_OK) {
            return NGX_ERROR;
        }

    next:

        ngx_aggr_event_json_skip_spaces(ctx);

        switch (*ctx->p) {

        case ',':
            ctx->p++;        /* skip , */
            ngx_aggr_event_json_skip_spaces(ctx);
            break;

        case '}':
            ctx->p++;        /* skip } */
            goto done;

        default:
            ngx_snprintf(ctx->err, ctx->err_size, "expected '}' or ','%Z");
            return NGX_ERROR;
        }
    }

done:

    ngx_aggr_event_json_skip_spaces(ctx);

    if (*ctx->p) {
        ngx_snprintf(ctx->err, ctx->err_size, "trailing data after json%Z");
        return NGX_ERROR;
    }


    if (ngx_aggr_eval_complex_dims(ar) != NGX_OK) {
        return NGX_ERROR;
    }

    if (query->filter.handler != NULL &&
        !query->filter.handler(ar, query->filter.data))
    {
        return NGX_ABORT;
    }


    /* alloc group dims in str table + calc event hash */
    temp_offset = 0;
    event->node.key = 0;

    for (dst_offset = 0;
        dst_offset < ar->group_size;
        dst_offset += sizeof(ngx_str_t *))
    {
        shp = (ngx_str_hash_t *) (ar->temp_data + temp_offset);
        temp_offset += sizeof(ngx_str_hash_t);

        dim_value = ngx_str_table_get(ar->str_tbl, shp);
        if (dim_value == NULL) {
            return NGX_ERROR;
        }

        dim_valuep = (ngx_str_t **) (event->data + dst_offset);
        *dim_valuep = dim_value;

        event->node.key ^= (ngx_uint_t) dim_value;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_event_copy_select_dims(ngx_aggr_result_t *ar, ngx_aggr_event_t *event)
{
    ngx_str_t        *dim_value;
    ngx_str_t       **dim_valuep;
    ngx_uint_t        dst_offset;
    ngx_uint_t        temp_offset;
    ngx_str_hash_t   *shp;

    dst_offset = ar->group_size;
    temp_offset = dst_offset / sizeof(ngx_str_t *) * sizeof(ngx_str_hash_t);

    for (; dst_offset < ar->metrics_offset; dst_offset += sizeof(ngx_str_t *))
    {
        shp = (ngx_str_hash_t *) (ar->temp_data + temp_offset);
        temp_offset += sizeof(ngx_str_hash_t);

        dim_value = ngx_str_table_get(ar->str_tbl, shp);
        if (dim_value == NULL) {
            return NGX_ERROR;
        }

        dim_valuep = (ngx_str_t **) (event->data + dst_offset);
        *dim_valuep = dim_value;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_aggr_event_update_metrics(ngx_aggr_event_t *dst, ngx_aggr_event_t *src,
    ngx_aggr_result_t *ar)
{
    double                      *src_val, *dst_val;
    ngx_uint_t                   i;
    ngx_flag_t                   update_selects;
    ngx_aggr_query_metric_in_t  *metrics;

    update_selects = 1;

    metrics = ar->query->metrics_in.elts;
    for (i = 0; i < ar->query->metrics_in.nelts; i++) {

        src_val = (double *) (src->data + metrics[i].offset);
        dst_val = (double *) (dst->data + metrics[i].offset);

        switch (metrics[i].type) {

        case ngx_aggr_query_metric_sum:
            *dst_val += *src_val;
            break;

        case ngx_aggr_query_metric_max:
            if (*src_val <= *dst_val) {
                update_selects = 0;
                break;
            }

            *dst_val = *src_val;

            if (!update_selects) {
                break;
            }

            /* update select dims to reflect the highest metric value */
            if (ngx_aggr_event_copy_select_dims(ar, dst) != NGX_OK) {
                return NGX_ERROR;
            }

            update_selects = 0;
            break;
        }
    }

    return NGX_OK;
}


static void
ngx_aggr_event_json_parse_error(ngx_aggr_event_json_ctx_t *ctx, ngx_log_t *log,
    u_char *start)
{
    char       *prefix, *suffix;
    off_t       offset;
    ngx_str_t   extract;

    if (!ngx_rate_limit(&ngx_aggr_event_error_rate)) {
        return;
    }

    ctx->err[ctx->err_size - 1] = '\0';

    offset = ctx->p - start;
    if (offset <= NGX_AGGR_EVENT_JSON_ERROR_EXTRACT) {
        prefix = "";
        extract.data = start;

    } else {
        prefix = "...";
        extract.data = ctx->p - NGX_AGGR_EVENT_JSON_ERROR_EXTRACT;
    }

    extract.len = ctx->p - extract.data;

    if (*ctx->p != '\0') {
        extract.len++;
        suffix = "";

    } else {
        suffix = "\\0";
    }

    ngx_log_error(NGX_LOG_ERR, log, 0,
        "ngx_aggr_event_json_parse_error: "
        "parse error: %s, offset: %O, input: '%s%V%s'",
        ctx->err, offset, prefix, &extract, suffix);
}


static size_t
ngx_aggr_event_dim_size(ngx_aggr_result_t *ar, ngx_aggr_event_t *event)
{
    size_t                      size;
    ngx_str_t                  *str_val;
    ngx_str_t                 **str_valp;
    ngx_uint_t                  i, n;
    ngx_aggr_query_t           *query;
    ngx_aggr_query_dim_out_t   *dims;

    query = ar->query;

    dims = query->dims_out.elts;
    n = query->dims_out.nelts;

    size = 0;
    for (i = 0; i < n; i++) {
        str_valp = (ngx_str_t **) (event->data + dims[i].offset);

        str_val = *str_valp;
        if (str_val == NULL) {
            continue;
        }

        size += str_val->len;
    }

    return size;
}


static size_t
ngx_aggr_event_json_write_size(ngx_aggr_result_t *ar, ngx_aggr_event_t *event)
{
    return ar->query->write_size[ngx_aggr_query_fmt_json] +
        ngx_aggr_event_dim_size(ar, event);
}


static u_char *
ngx_aggr_event_json_write(u_char *p, ngx_aggr_result_t *ar,
    ngx_aggr_event_t *event)
{
    double                         dbl_val;
    double                        *dbl_valp;
    ngx_str_t                     *key;
    ngx_str_t                     *str_val;
    ngx_str_t                    **str_valp;
    ngx_uint_t                     i, n;
    ngx_aggr_query_t              *query;
    ngx_aggr_query_dim_out_t      *dims;
    ngx_aggr_query_metric_out_t   *metrics;

    query = ar->query;

    *p++ = '{';

    if (query->time_dim.len > 0) {
        *p++ = '"';
        p = ngx_copy(p, query->time_dim.data, query->time_dim.len);
        *p++ = '"';
        *p++ = ':';
        *p++ = '"';
        p = ngx_copy(p, ar->time_buf, ar->time_len);
        *p++ = '"';
        *p++ = ',';
    }

    dims = query->dims_out.elts;
    n = query->dims_out.nelts;

    for (i = 0; i < n; i++) {
        str_valp = (ngx_str_t **) (event->data + dims[i].offset);

        str_val = *str_valp;
        if (str_val == NULL) {
            continue;
        }

        *p++ = '"';
        key = &dims[i].name;
        p = ngx_copy(p, key->data, key->len);
        *p++ = '"';
        *p++ = ':';

        *p++ = '"';
        p = ngx_copy(p, str_val->data, str_val->len);
        *p++ = '"';
        *p++ = ',';
    }

    metrics = query->metrics_out.elts;
    n = query->metrics_out.nelts;

    for (i = 0; i < n; i++) {
        *p++ = '"';
        key = &metrics[i].name;
        p = ngx_copy(p, key->data, key->len);
        *p++ = '"';
        *p++ = ':';

        dbl_valp = (double *) (event->data + metrics[i].offset);
        dbl_val = *dbl_valp;

        /* not using ngx_sprintf since it supports only fixed precision */
        p += snprintf((char *) p, NGX_AGGR_QUERY_DOUBLE_LEN,
            "%.*g", NGX_AGGR_QUERY_DOUBLE_PRECISION, dbl_val);
        *p++ = ',';
    }

    if (p[-1] == ',') {
        /* remove the trailing ',' */
        p[-1] = '}';

    } else {
        *p++ = '}';
    }

    return p;
}


static u_char *
ngx_aggr_event_json_write_delim(u_char *p, ngx_aggr_result_t *ar,
    ngx_aggr_event_t *event)
{
    p = ngx_aggr_event_json_write(p, ar, event);
    *p++ = ',';

    return p;
}


static size_t
ngx_aggr_event_prom_write_size(ngx_aggr_result_t *ar, ngx_aggr_event_t *event)
{
    return ar->query->write_size[ngx_aggr_query_fmt_prom] +
        ngx_aggr_event_dim_size(ar, event) * ar->query->metrics_out.nelts;
}


static u_char *
ngx_aggr_event_prom_write(u_char *p, ngx_aggr_result_t *ar,
    ngx_aggr_event_t *event)
{
    u_char                        *start;
    double                         dbl_val;
    double                        *dbl_valp;
    ngx_str_t                     *key;
    ngx_str_t                     *str_val;
    ngx_str_t                    **str_valp;
    ngx_uint_t                     i, j;
    ngx_aggr_query_t              *query;
    ngx_aggr_query_dim_out_t      *dims;
    ngx_aggr_query_metric_out_t   *metrics;

    query = ar->query;
    dims = query->dims_out.elts;
    metrics = query->metrics_out.elts;

    for (i = 0; i < query->metrics_out.nelts; i++) {

        key = &metrics[i].name;
        p = ngx_copy(p, key->data, key->len);

        if (query->dims_out.nelts > 0) {
            *p++ = '{';
            start = p;

            for (j = 0; j < query->dims_out.nelts; j++) {
                str_valp = (ngx_str_t **) (event->data + dims[j].offset);

                str_val = *str_valp;
                if (str_val == NULL) {
                    continue;
                }

                key = &dims[j].name;
                p = ngx_copy(p, key->data, key->len);
                *p++ = '=';

                *p++ = '"';
                p = ngx_copy(p, str_val->data, str_val->len);
                *p++ = '"';
                *p++ = ',';
            }

            if (p > start) {
                p[-1] = '}';    /* overwrite the ',' */

            } else {
                p = start - 1;  /* revert the '{' */
            }
        }

        *p++ = ' ';

        dbl_valp = (double *) (event->data + metrics[i].offset);
        dbl_val = *dbl_valp;

        /* not using ngx_sprintf since it supports only fixed precision */
        p += snprintf((char *) p, NGX_AGGR_QUERY_DOUBLE_LEN,
            "%.*g", NGX_AGGR_QUERY_DOUBLE_PRECISION, dbl_val);
        *p++ = '\n';
    }

    return p;
}


ngx_aggr_result_t *
ngx_aggr_result_create(ngx_aggr_query_t *query, ngx_log_t *log, time_t t,
    ngx_aggr_result_t *prev)
{
    ngx_tm_t            gmt;
    ngx_pool_t         *pool;
    ngx_aggr_result_t  *ar;

    pool = ngx_create_pool(2048, log);
    if (pool == NULL) {
        return NULL;
    }

    ar = ngx_palloc(pool, sizeof(*ar));
    if (ar == NULL) {
        goto failed;
    }

    ar->variables = ngx_pcalloc(pool, query->variables.nelts
                                      * sizeof(ngx_aggr_variable_value_t));
    if (ar->variables == NULL) {
        goto failed;
    }

    ar->captures = ngx_palloc(pool, query->ncaptures * sizeof(int));
    if (ar->captures == NULL) {
        goto failed;
    }

    ar->str_tbl = ngx_str_table_create(pool);
    if (ar->str_tbl == NULL) {
        goto failed;
    }

    ar->buf = ngx_palloc(pool, query->max_event_size);
    if (ar->buf == NULL) {
        goto failed;
    }

    ar->pool = pool;

    ar->query = query;
    ar->group_size = query->size[ngx_aggr_query_dim_group];
    ar->select_size = query->size[ngx_aggr_query_dim_select];
    ar->event_size = offsetof(ngx_aggr_event_t, data) + query->event_size;
    ar->metrics_offset = ar->group_size + ar->select_size;

    ar->temp_data = ngx_palloc(ar->pool, query->temp_size);
    if (ar->temp_data == NULL) {
        goto failed;
    }

    if (ngx_aggr_event_alloc(ar) != NGX_OK) {
        goto failed;
    }

    ngx_gmtime(t, &gmt);

    (void) ngx_sprintf(ar->time_buf, "%4d-%02d-%02dT%02d:%02d:%02dZ",
                       gmt.ngx_tm_year, gmt.ngx_tm_mon,
                       gmt.ngx_tm_mday, gmt.ngx_tm_hour,
                       gmt.ngx_tm_min, gmt.ngx_tm_sec);

    ar->time_len = NGX_ISO8601_TIMESTAMP_LEN;

    ar->head = NULL;
    ar->count = 0;
    ngx_rbtree_init(&ar->rbtree, &ar->sentinel,
        ngx_aggr_event_rbtree_insert_value);

    if (prev != NULL) {
        ar->buf_used = prev->buf_used;
        ngx_memcpy(ar->buf, prev->buf, ar->buf_used);

    } else {
        ar->buf_used = 0;
    }

    ar->buf_size = query->max_event_size;

    ar->variable_depth = 100;

    ar->ncaptures = 0;
    ar->captures_data = NULL;
    ngx_memzero(&ar->var_temp, sizeof(ar->var_temp));

    return ar;

failed:

    ngx_destroy_pool(pool);
    return NULL;
}


void
ngx_aggr_result_destroy(ngx_aggr_result_t *ar)
{
    ngx_destroy_pool(ar->pool);
}


static ngx_aggr_event_t **
ngx_aggr_result_get_top(ngx_aggr_result_t *ar, ngx_uint_t *cnt)
{
    double              cur;
    double             *curp;
    double              value;
    double             *valuep;
    ngx_uint_t          insert;
    ngx_uint_t          count;
    ngx_uint_t          top_offset;
    ngx_aggr_event_t  **top;
    ngx_aggr_query_t   *query;
    ngx_aggr_event_t   *event;

    query = ar->query;

    count = query->top_count + 1;
    if (ar->count < count) {
        count = ar->count;
    }

    top = ngx_palloc(ar->pool, count * sizeof(top[0]));
    if (top == NULL) {
        return NULL;
    }

    top_offset = query->top_offset;

    count = 0;

    for (event = ar->head; event != NULL; event = event->next) {

        if (query->having.handler != NULL) {
            ar->cur = event;
            if (!query->having.handler(ar, query->having.data)) {
                continue;
            }
        }

        valuep = (double *) (event->data + top_offset);
        value = *valuep;

        for (insert = count; insert > 0; insert--) {
            curp = (double *) (top[insert - 1]->data + top_offset);
            cur = *curp;
            if (!query->top_inverted) {
                if (value <= cur) {
                    break;
                }

            } else if (value >= cur) {
                break;
            }

            top[insert] = top[insert - 1];
        }

        top[insert] = event;

        if (count < query->top_count) {
            count++;
        }
    }

    *cnt = count;

    return top;
}


static ngx_chain_t **
ngx_aggr_result_write_top(ngx_aggr_result_t *ar, ngx_pool_t *pool,
    ngx_chain_t **last, off_t *size)
{
    u_char                         *p;
    size_t                          max_size;
    size_t                          buf_size;
    ngx_buf_t                      *b;
    ngx_uint_t                      i;
    ngx_uint_t                      count;
    ngx_chain_t                    *cl;
    ngx_aggr_event_t              **top;
    ngx_aggr_event_t               *event;
    ngx_aggr_query_t               *query;
    ngx_aggr_event_write_pt         write;
    ngx_aggr_event_write_size_pt    get_size;

    top = ngx_aggr_result_get_top(ar, &count);
    if (top == NULL) {
        return NULL;
    }

    query = ar->query;
    buf_size = query->output_buf_size;

    b = ngx_create_temp_buf(pool, buf_size);
    if (b == NULL) {
        return NULL;
    }

    p = b->last;

    switch (query->fmt) {

    case ngx_aggr_query_fmt_json:
        get_size = ngx_aggr_event_json_write_size;
        write = ngx_aggr_event_json_write_delim;
        *p++ = '[';
        break;

    default:    /* ngx_aggr_query_fmt_prom */
        get_size = ngx_aggr_event_prom_write_size;
        write = ngx_aggr_event_prom_write;
        break;
    }

    for (i = 0; i < count; i++) {
        event = top[i];

        max_size = get_size(ar, event);

        if ((size_t) (b->end - p) <= max_size) {

            if (max_size >= buf_size) {
                continue;
            }

            cl = ngx_alloc_chain_link(pool);
            if (cl == NULL) {
                return NULL;
            }

            cl->buf = b;
            *last = cl;
            last = &cl->next;

            b->last = p;
            *size += p - b->pos;

            b = ngx_create_temp_buf(pool, buf_size);
            if (b == NULL) {
                return NULL;
            }

            p = b->last;
        }

        p = write(p, ar, event);
    }

    if (query->fmt == ngx_aggr_query_fmt_json) {
        if (p[-1] == ',') {
            p[-1] = ']';

        } else {
            *p++ = ']';
        }
    }

    if (p > b->pos) {
        cl = ngx_alloc_chain_link(pool);
        if (cl == NULL) {
            return NULL;
        }

        cl->buf = b;
        *last = cl;
        last = &cl->next;

        b->last = p;
        *size += p - b->pos;
    }

    return last;
}


ngx_chain_t **
ngx_aggr_result_write(ngx_aggr_result_t *ar, ngx_pool_t *pool,
    ngx_chain_t **last, off_t *size)
{
    u_char                        *p;
    size_t                         max_size;
    size_t                         buf_size;
    ngx_buf_t                     *b;
    ngx_chain_t                   *cl;
    ngx_aggr_event_t              *event;
    ngx_aggr_query_t              *query;
    ngx_aggr_event_write_pt        write;
    ngx_aggr_event_write_size_pt   get_size;

    query = ar->query;

    if (query->top_count > 0) {
        return ngx_aggr_result_write_top(ar, pool, last, size);
    }

    buf_size = query->output_buf_size;

    b = ngx_create_temp_buf(pool, buf_size);
    if (b == NULL) {
        return NULL;
    }

    p = b->last;

    switch (query->fmt) {

    case ngx_aggr_query_fmt_json:
        get_size = ngx_aggr_event_json_write_size;
        write = ngx_aggr_event_json_write_delim;
        *p++ = '[';
        break;

    default:    /* ngx_aggr_query_fmt_prom */
        get_size = ngx_aggr_event_prom_write_size;
        write = ngx_aggr_event_prom_write;
        break;
    }

    for (event = ar->head; event != NULL; event = event->next) {

        if (query->having.handler != NULL) {
            ar->cur = event;
            if (!query->having.handler(ar, query->having.data)) {
                continue;
            }
        }

        max_size = get_size(ar, event);

        if ((size_t) (b->end - p) <= max_size) {

            if (max_size >= buf_size) {
                continue;
            }

            cl = ngx_alloc_chain_link(pool);
            if (cl == NULL) {
                return NULL;
            }

            cl->buf = b;
            *last = cl;
            last = &cl->next;

            b->last = p;
            *size += p - b->pos;

            b = ngx_create_temp_buf(pool, buf_size);
            if (b == NULL) {
                return NULL;
            }

            p = b->last;
        }

        p = write(p, ar, event);
    }

    if (query->fmt == ngx_aggr_query_fmt_json) {
        if (p[-1] == ',') {
            p[-1] = ']';

        } else {
            *p++ = ']';
        }
    }

    if (p > b->pos) {
        cl = ngx_alloc_chain_link(pool);
        if (cl == NULL) {
            return NULL;
        }

        cl->buf = b;
        *last = cl;
        last = &cl->next;

        b->last = p;
        *size += p - b->pos;
    }

    return last;
}


void
ngx_aggr_result_send_buf_free(void *data)
{
    ngx_aggr_result_send_buf_t  *buf = data;

    if (ngx_atomic_fetch_add(&buf->ref_count, -1) > 1) {
        return;
    }

    ngx_free(buf);
}


ngx_int_t
ngx_aggr_result_send(ngx_aggr_result_t *ar, ngx_aggr_event_send_pt handler,
    void *data)
{
    u_char                      *p;
    u_char                      *end;
    u_char                      *start;
    size_t                       max_size;
    size_t                       buf_size;
    ngx_int_t                    rc;
    ngx_aggr_event_t            *event;
    ngx_aggr_result_send_buf_t  *buf;

    buf_size = ar->query->output_buf_size;

    buf = NULL;
    p = end = NULL;

    for (event = ar->head; event != NULL; event = event->next) {

        max_size = ngx_aggr_event_json_write_size(ar, event);

        if (max_size > (size_t) (end - p)) {

            if (max_size > buf_size - sizeof(*buf)) {
                continue;
            }

            if (buf != NULL) {
                ngx_aggr_result_send_buf_free(buf);
            }

            buf = ngx_alloc(buf_size, ar->pool->log);
            if (buf == NULL) {
                rc = NGX_ERROR;
                goto done;
            }

            buf->ref_count = 1;

            p = buf->data;
            end = (u_char *) buf + buf_size;
        }

        start = p;
        p = ngx_aggr_event_json_write(p, ar, event);

        (void) ngx_atomic_fetch_add(&buf->ref_count, 1);

        handler(data, start, p - start, buf);
    }

    if (buf != NULL) {
        ngx_aggr_result_send_buf_free(buf);
    }

    rc = NGX_OK;

done:

    ngx_destroy_pool(ar->pool);

    return rc;
}


static u_char *
ngx_aggr_result_process_single(ngx_aggr_result_t *ar,
    u_char *start, u_char *end)
{
    size_t                      size;
    u_char                     *buf_end;
    u_char                     *event_end;
    u_char                      err[NGX_AGGR_EVENT_JSON_ERROR_MSG_SIZE];
    ngx_int_t                   rc;
    ngx_aggr_event_t           *new;
    ngx_aggr_event_t           *old;
    ngx_aggr_event_json_ctx_t   ctx;

    err[0] = '\0';
    ctx.err = err;
    ctx.err_size = sizeof(err);

    if (ar->buf_used > 0) {

        event_end = ngx_strlchr(start, end, '\0');
        if (event_end == NULL) {
            event_end = end;
        }

        size = event_end - start;
        if (size >= ar->buf_size - ar->buf_used) {
            if (ngx_rate_limit(&ngx_aggr_event_error_rate)) {
                ngx_log_error(NGX_LOG_ERR, ar->pool->log, 0,
                    "ngx_aggr_result_process_single: "
                    "event size %uz too large", ar->buf_used + size);
            }

            ar->buf_used = 0;
            (void) ngx_atomic_fetch_add(ngx_aggr_event_parse_errs, 1);
            return event_end;
        }

        buf_end = ar->buf + ar->buf_used;
        buf_end = ngx_copy(buf_end, start, size);
        *buf_end = '\0';

        ctx.p = ar->buf;

        rc = ngx_aggr_event_json_parse(&ctx, ar);
        if (rc != NGX_OK) {
            if (rc == NGX_ABORT) {
                ar->buf_used = 0;
                return event_end;
            }

            if (ctx.p == buf_end && event_end == end) {
                /* wait for more data */
                ar->buf_used += size;
                return event_end;
            }

            ngx_aggr_event_json_parse_error(&ctx, ar->pool->log, ar->buf);

            ar->buf_used = 0;
            (void) ngx_atomic_fetch_add(ngx_aggr_event_parse_errs, 1);
            return event_end;
        }

        ar->buf_used = 0;
        ctx.p = event_end;

    } else {

        ctx.p = start;

        rc = ngx_aggr_event_json_parse(&ctx, ar);
        if (rc != NGX_OK) {
            if (rc == NGX_ABORT) {
                return ctx.p;
            }

            size = end - start;
            if (ctx.p == end && size < ar->buf_size) {
                /* wait for more data */
                ngx_memcpy(ar->buf, start, size);
                ar->buf_used = size;
                return ctx.p;
            }

            if (*ctx.p == '\0' && ctx.p == start) {
                /* empty event */
                return ctx.p;
            }

            ngx_aggr_event_json_parse_error(&ctx, ar->pool->log, start);

            (void) ngx_atomic_fetch_add(ngx_aggr_event_parse_errs, 1);
            return ctx.p;
        }
    }

    (void) ngx_atomic_fetch_add(ngx_aggr_event_parse_ok, 1);

    new = ar->cur;
    old = ngx_aggr_event_rbtree_lookup(&ar->rbtree, new);
    if (old != NULL) {
        if (ngx_aggr_event_update_metrics(old, new, ar) != NGX_OK) {
            return NULL;
        }
        return ctx.p;
    }

    if (ngx_aggr_event_copy_select_dims(ar, new) != NGX_OK) {
        return NULL;
    }

    ngx_rbtree_insert(&ar->rbtree, &new->node);

    new->next = ar->head;
    ar->head = new;
    ar->count++;

    (void) ngx_atomic_fetch_add(ngx_aggr_event_created, 1);


    if (ngx_aggr_event_alloc(ar) != NGX_OK) {
        return NULL;
    }

    return ctx.p;
}


ngx_int_t
ngx_aggr_result_process(ngx_aggr_result_t *ar, u_char *start, size_t size)
{
    u_char  *end;

    (void) ngx_atomic_fetch_add(ngx_aggr_event_processed, size);

    end = start + size;
    while (start < end) {

        start = ngx_aggr_result_process_single(ar, start, end);
        if (start == NULL) {
            return NGX_ERROR;
        }

        if (start >= end) {
            break;
        }

        if (*start != '\0') {
            start = ngx_strlchr(start, end, '\0');
            if (start == NULL) {
                break;
            }
        }

        start++;
    }

    return NGX_OK;
}


ngx_buf_t *
ngx_aggr_result_get_stats(ngx_pool_t *pool)
{
    ngx_buf_t  *b;

    b = ngx_create_temp_buf(pool, sizeof(NGX_AGGR_RESULT_STATUS_FORMAT) +
        NGX_ATOMIC_T_LEN * 4);
    if (b == NULL) {
        return NULL;
    }

    b->last = ngx_sprintf(b->last, NGX_AGGR_RESULT_STATUS_FORMAT,
        *ngx_aggr_event_processed,
        *ngx_aggr_event_parse_ok,
        *ngx_aggr_event_parse_errs,
        *ngx_aggr_event_created);

    return b;
}
