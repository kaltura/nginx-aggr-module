#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_ip2l.h"


#define NGX_IP2L_IPV4_SIZE   (4)
#define NGX_IP2L_IPV6_SIZE   (16)

#define NGX_IP2L_INDEX_SIZE  (sizeof(ngx_ip2l_fmt_index_t) << 16)


/* file format */

typedef struct {
    u_char                  type;
    u_char                  cols;
    u_char                  year;
    u_char                  month;
    u_char                  day;
} ngx_ip2l_fmt_header_t;

typedef struct {
    uint32_t                ipv4_count;
    uint32_t                ipv4_off;
    uint32_t                ipv6_count;
    uint32_t                ipv6_off;
    uint32_t                ipv4_index_off;
    uint32_t                ipv6_index_off;
} ngx_ip2l_fmt_layout_t;

typedef struct {
    uint32_t                left;
    uint32_t                right;
} ngx_ip2l_fmt_index_t;


/* file object */

typedef struct {
    uint32_t                row_size;
    uint32_t                count;
    u_char                 *start;
    ngx_ip2l_fmt_index_t   *index;
} ngx_ip2l_db_t;

typedef struct {
    u_char                  cols;
    char                    fields[ngx_ip2l_field_count];
} ngx_ip2l_file_spec_t;

struct ngx_ip2l_file_s {
    ngx_str_t               name;
    u_char                 *data;
    uint32_t                size;
    ngx_ip2l_file_spec_t    spec;
    ngx_ip2l_db_t           ipv4;
    ngx_ip2l_db_t           ipv6;
};

typedef struct {
    ngx_array_t             files;      /* ngx_ip2l_file_t * */
} ngx_ip2l_conf_t;


typedef ngx_int_t (*ngx_ip2l_get_field_pt) (ngx_ip2l_file_t *file,
    ngx_pool_t *pool, void *in, ngx_str_t *out);


static void *ngx_ip2l_create_conf(ngx_cycle_t *cycle);
static char *ngx_ip2l_file(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);


static ngx_command_t  ngx_ip2l_commands[] = {

    { ngx_string("ip2l_file"),
      NGX_MAIN_CONF|NGX_DIRECT_CONF|NGX_CONF_TAKE2,
      ngx_ip2l_file,
      0,
      0,
      NULL },

      ngx_null_command
};


static ngx_core_module_t  ngx_ip2l_module_ctx = {
    ngx_string("ip2l"),
    ngx_ip2l_create_conf,
    NULL,
};


ngx_module_t  ngx_ip2l_module = {
    NGX_MODULE_V1,
    &ngx_ip2l_module_ctx,                  /* module context */
    ngx_ip2l_commands,                     /* module directives */
    NGX_CORE_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_ip2l_file_spec_t  ngx_ip2l_file_types[] = {
    /*     CC RG CT IS LA LO DO ZP TZ NS ID AC WC WN MC MN MB EL UT */
    { 2,  { 0,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 3,  { 0,-1,-1, 1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 4,  { 0, 1, 2,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 5,  { 0, 1, 2, 3,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 6,  { 0, 1, 2,-1, 3, 4,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 7,  { 0, 1, 2, 5, 3, 4,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 6,  { 0, 1, 2, 3,-1,-1, 4,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 8,  { 0, 1, 2, 5, 3, 4, 6,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 7,  { 0, 1, 2,-1, 3, 4,-1, 5,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 9,  { 0, 1, 2, 6, 3, 4, 7, 5,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 8,  { 0, 1, 2,-1, 3, 4,-1, 5, 6,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 10, { 0, 1, 2, 7, 3, 4, 8, 5, 6,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 8,  { 0, 1, 2,-1, 3, 4,-1,-1, 5, 6,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 11, { 0, 1, 2, 7, 3, 4, 8, 5, 6, 9,-1,-1,-1,-1,-1,-1,-1,-1,-1 } },
    { 10, { 0, 1, 2,-1, 3, 4,-1, 5, 6,-1, 7, 8,-1,-1,-1,-1,-1,-1,-1 } },
    { 13, { 0, 1, 2, 7, 3, 4, 8, 5, 6, 9,10,11,-1,-1,-1,-1,-1,-1,-1 } },
    { 10, { 0, 1, 2,-1, 3, 4,-1,-1, 5, 6,-1,-1, 7, 8,-1,-1,-1,-1,-1 } },
    { 15, { 0, 1, 2, 7, 3, 4, 8, 5, 6, 9,10,11,12,13,-1,-1,-1,-1,-1 } },
    { 11, { 0, 1, 2, 5, 3, 4, 6,-1,-1,-1,-1,-1,-1,-1, 7, 8, 9,-1,-1 } },
    { 18, { 0, 1, 2, 7, 3, 4, 8, 5, 6, 9,10,11,12,13,14,15,16,-1,-1 } },
    { 11, { 0, 1, 2,-1, 3, 4,-1, 5, 6,-1, 7, 8,-1,-1,-1,-1,-1, 9,-1 } },
    { 19, { 0, 1, 2, 7, 3, 4, 8, 5, 6, 9,10,11,12,13,14,15,16,17,-1 } },
    { 12, { 0, 1, 2, 5, 3, 4, 6,-1,-1,-1,-1,-1,-1,-1, 7, 8, 9,-1,10 } },
    { 20, { 0, 1, 2, 7, 3, 4, 8, 5, 6, 9,10,11,12,13,14,15,16,17,18 } },
};


/* must match ngx_ip2l_field_e in order */

static ngx_str_t  ngx_ip2l_field_names[] = {
    ngx_string("country_code"),
    ngx_string("region"),
    ngx_string("city"),
    ngx_string("isp"),
    ngx_string("latitude"),
    ngx_string("longitude"),
    ngx_string("domain"),
    ngx_string("zipcode"),
    ngx_string("timezone"),
    ngx_string("netspeed"),
    ngx_string("iddcode"),
    ngx_string("areacode"),
    ngx_string("weatherstationcode"),
    ngx_string("weatherstationname"),
    ngx_string("mcc"),
    ngx_string("mnc"),
    ngx_string("mobilebrand"),
    ngx_string("elevation"),
    ngx_string("usagetype"),
    ngx_string("-"),    /* count */
    ngx_string("country"),

    ngx_null_string
};


static void *
ngx_ip2l_create_conf(ngx_cycle_t *cycle)
{
    ngx_ip2l_conf_t  *icf;

    icf = ngx_pcalloc(cycle->pool, sizeof(ngx_ip2l_conf_t));
    if (icf == NULL) {
        return NULL;
    }

    if (ngx_array_init(&icf->files, cycle->pool, 1,
                       sizeof(ngx_ip2l_file_t *))
        != NGX_OK)
    {
        return NULL;
    }

    return icf;
}


static void
ngx_ip2l_db_ipv6_swap_addr(void *ip)
{
    uint32_t   t;
    uint32_t  *l, *h;

    l = ip;
    h = l + 3;

    t = htonl(*l);
    *l = htonl(*h);
    *h = t;

    l++;
    h--;

    t = htonl(*l);
    *l = htonl(*h);
    *h = t;
}


static ngx_int_t
ngx_ip2l_db_ipv6_compare(void *ip1, void *ip2)
{
    uint64_t   n1, n2;
    uint64_t  *p1, *p2;

    p1 = ip1;
    p2 = ip2;

    n1 = p1[1];
    n2 = p2[1];
    if (n1 != n2) {
        return n1 < n2 ? -1 : 1;
    }

    n1 = p1[0];
    n2 = p2[0];
    if (n1 != n2) {
        return n1 < n2 ? -1 : 1;
    }

    return 0;
}


static void *
ngx_ip2l_db_ipv6_lookup(ngx_ip2l_db_t *db, u_char *ip)
{
    u_char                *pcur;
    u_char                *start;
    int32_t                left, right, mid;
    ngx_int_t              rc;
    ngx_ip2l_fmt_index_t   index;

    if (!db->count) {
        return NULL;
    }

    if (db->index) {
        index = db->index[(ip[15] << 8) | ip[14]];

        left = index.left;
        right = index.right;
        if (left > right || (uint32_t) right >= db->count) {
            return NULL;
        }

    } else {
        left = 0;
        right = db->count - 1;
    }

    start = db->start;

    while (left < right) {
        mid = (left + right + 1) / 2;

        pcur = start + mid * db->row_size;
        rc = ngx_ip2l_db_ipv6_compare(ip, pcur);

        if (rc < 0) {
            right = mid - 1;

        } else if (rc > 0) {
            left = mid;

        } else {
            return pcur + NGX_IP2L_IPV6_SIZE;
        }
    }

    pcur = start + left * db->row_size;
    return pcur + NGX_IP2L_IPV6_SIZE;
}


static void *
ngx_ip2l_db_ipv4_lookup(ngx_ip2l_db_t *db, uint32_t ip)
{
    u_char                *start;
    int32_t                left, right, mid;
    uint32_t              *pcur, cur;
    ngx_ip2l_fmt_index_t   index;

    if (!db->count) {
        return NULL;
    }

    if (db->index) {
        index = db->index[ip >> 16];

        left = index.left;
        right = index.right;
        if (left > right || (uint32_t) right >= db->count) {
            return NULL;
        }

    } else {
        left = 0;
        right = db->count - 1;
    }

    start = db->start;

    while (left < right) {
        mid = (left + right + 1) / 2;

        pcur = (void *) (start + mid * db->row_size);
        cur = *pcur;

        if (ip < cur) {
            right = mid - 1;

        } else if (ip > cur) {
            left = mid;

        } else {
            return pcur + 1;
        }
    }

    pcur = (void *) (start + left * db->row_size);
    return pcur + 1;
}


static void *
ngx_ip2l_file_lookup(ngx_ip2l_file_t *file, ngx_str_t *ip)
{
    in_addr_t         inaddr;
#if (NGX_HAVE_INET6)
    u_char           *p;
    struct in6_addr   inaddr6;
#endif

    inaddr = ngx_inet_addr(ip->data, ip->len);

    if (inaddr != INADDR_NONE) {
        inaddr = ntohl(inaddr);
        return ngx_ip2l_db_ipv4_lookup(&file->ipv4, inaddr);

#if (NGX_HAVE_INET6)
    } else if (ngx_inet6_addr(ip->data, ip->len, inaddr6.s6_addr) == NGX_OK) {

        if (IN6_IS_ADDR_V4MAPPED(&inaddr6)) {
            p = inaddr6.s6_addr;
            inaddr = p[12] << 24;
            inaddr |= p[13] << 16;
            inaddr |= p[14] << 8;
            inaddr |= p[15];

            return ngx_ip2l_db_ipv4_lookup(&file->ipv4, inaddr);
        }

        ngx_ip2l_db_ipv6_swap_addr(inaddr6.s6_addr);
        return ngx_ip2l_db_ipv6_lookup(&file->ipv6, inaddr6.s6_addr);

#endif
    } else {
        return NULL;
    }
}


ngx_ip2l_field_e
ngx_ip2l_parse_field_name(ngx_str_t *name)
{
    ngx_str_t  *cur;

    for (cur = ngx_ip2l_field_names; cur->len; cur++) {
        if (cur->len == name->len
            && ngx_strncmp(cur->data, name->data, cur->len) == 0)
        {
            return cur - ngx_ip2l_field_names;
        }
    }

    return ngx_ip2l_field_count;
}


static ngx_int_t
ngx_ip2l_file_get_string(ngx_ip2l_file_t *file, ngx_pool_t *pool,
    void *in, ngx_str_t *out)
{
    uint32_t  off, *offp;

    offp = in;
    off = *offp;

    if (off >= file->size) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_ip2l_file_get_string: "
            "invalid offset: %uD, size: %uD", off, file->size);
        return NGX_ERROR;
    }

    out->len = file->data[off];
    off++;

    if (out->len > file->size - off) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_ip2l_file_get_string: "
            "invalid len %uz, offset: %uD, size: %uD",
            out->len, off, file->size);
        return NGX_ERROR;
    }

    out->data = &file->data[off];

    return NGX_OK;
}

static ngx_int_t
ngx_ip2l_file_get_country_long(ngx_ip2l_file_t *file, ngx_pool_t *pool,
    void *in, ngx_str_t *out)
{
    uint32_t  off, *offp;

    offp = in;
    off = *offp;

    off += 3;       /* skip country short */

    if (off >= file->size) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_ip2l_file_get_country_long: "
            "invalid offset: %uD, size: %uD", off, file->size);
        return NGX_ERROR;
    }

    out->len = file->data[off];
    off++;

    if (out->len > file->size - off) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "ngx_ip2l_file_get_country_long: "
            "invalid len %uz, offset: %uD, size: %uD",
            out->len, off, file->size);
        return NGX_ERROR;
    }

    out->data = &file->data[off];

    return NGX_OK;
}

static ngx_int_t
ngx_ip2l_file_get_float(ngx_ip2l_file_t *file, ngx_pool_t *pool,
    void *in, ngx_str_t *out)
{
    float   val, *valp;
    u_char  val_str[64];

    valp = in;
    val = *valp;

    out->len = ngx_snprintf(val_str, sizeof(val_str), "%.6f", (double) val)
        - val_str;

    out->data = ngx_pnalloc(pool, out->len);
    if (out->data == NULL) {
        return NGX_ERROR;
    }

    ngx_memcpy(out->data, val_str, out->len);

    return NGX_OK;
}


ngx_int_t
ngx_ip2l_file_get_field(ngx_ip2l_file_t *file, ngx_str_t *ip,
    ngx_uint_t field, ngx_pool_t *pool, ngx_str_t *value)
{
    char                    field_idx;
    u_char                 *row;
    u_char                 *bin_value;
    ngx_ip2l_get_field_pt   getter;

    switch (field) {

    case ngx_ip2l_field_country_long:
        field = ngx_ip2l_field_country;
        getter = ngx_ip2l_file_get_country_long;
        break;

    case ngx_ip2l_field_latitude:
    case ngx_ip2l_field_longitude:
        getter = ngx_ip2l_file_get_float;
        break;

    default:
        if (field >= ngx_ip2l_field_count) {
            ngx_log_error(NGX_LOG_ALERT, pool->log, 0,
                "ngx_ip2l_file_get_field: invalid field %ui", field);
            return NGX_ERROR;
        }

        getter = ngx_ip2l_file_get_string;
    }

    field_idx = file->spec.fields[field];
    if (field_idx < 0) {
        ngx_log_debug1(NGX_LOG_DEBUG_CORE, pool->log, 0,
            "ngx_ip2l_file_get_field: "
            "field \"%V\" does not exist in db", &ngx_ip2l_field_names[field]);
        return NGX_DECLINED;
    }

    row = ngx_ip2l_file_lookup(file, ip);
    if (row == NULL) {
        return NGX_DECLINED;
    }

    bin_value = row + field_idx * sizeof(uint32_t);

    return getter(file, pool, bin_value, value);
}


static ngx_int_t
ngx_ip2l_file_init(ngx_conf_t *cf, ngx_ip2l_file_t *file)
{
    size_t                  size;
    u_char                 *data;
    ngx_ip2l_fmt_layout_t   layout;
    ngx_ip2l_fmt_header_t  *header;

    size = file->size;
    if (size < sizeof(*header) + sizeof(layout)) {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
            "ngx_ip2l_file_init: invalid file size %uz", size);
        return NGX_ERROR;
    }

    data = file->data;
    header = (void *) data;
    if (header->type < 1 || header->type
        > sizeof(ngx_ip2l_file_types) / sizeof(ngx_ip2l_file_types[0]))
    {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
            "ngx_ip2l_file_init: invalid file type %uD",
            (uint32_t) header->type);
        return NGX_ERROR;
    }

    file->spec = ngx_ip2l_file_types[header->type - 1];

    if (header->cols < file->spec.cols) {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
            "ngx_ip2l_file_init: invalid column count %uD, expected: %uD",
            (uint32_t) header->cols, (uint32_t) file->spec.cols);
        return NGX_ERROR;
    }

    ngx_memcpy(&layout, header + 1, sizeof(layout));

    /* ipv4 */

    layout.ipv4_off--;      /* 1-based */
    if (layout.ipv4_off >= size) {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
            "ngx_ip2l_file_init: invalid ipv4 offset %uD, size: %uz",
            layout.ipv4_off, size);
        return NGX_ERROR;
    }

    file->ipv4.row_size = NGX_IP2L_IPV4_SIZE + (header->cols - 1)
        * sizeof(uint32_t);

    if (layout.ipv4_count > (size - layout.ipv4_off) / file->ipv4.row_size) {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
            "ngx_ip2l_file_init: "
            "invalid ipv4 count %uD, offset: %uD, size: %uz",
            layout.ipv4_count, layout.ipv4_off, size);
        return NGX_ERROR;
    }

    file->ipv4.count = layout.ipv4_count;
    file->ipv4.start = data + layout.ipv4_off;

    if (layout.ipv4_index_off > 0) {
        layout.ipv4_index_off--;      /* 1-based */

        if (size < NGX_IP2L_INDEX_SIZE 
            || layout.ipv4_index_off > size - NGX_IP2L_INDEX_SIZE)
        {
            ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
                "ngx_ip2l_file_init: invalid ipv4 index offset %uD, size: %uz",
                layout.ipv4_index_off, size);
            return NGX_ERROR;
        }

        file->ipv4.index = (void *) (data + layout.ipv4_index_off);
    }

    /* ipv6 */

    layout.ipv6_off--;      /* 1-based */
    if (layout.ipv6_off >= size) {
        return NGX_ERROR;
    }

    file->ipv6.row_size = NGX_IP2L_IPV6_SIZE + (header->cols - 1)
        * sizeof(uint32_t);

    if (layout.ipv6_count > (size - layout.ipv6_off) / file->ipv6.row_size) {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
            "ngx_ip2l_file_init: "
            "invalid ipv6 count %uD, offset: %uD, size: %uz",
            layout.ipv6_count, layout.ipv6_off, size);
        return NGX_ERROR;
    }

    file->ipv6.count = layout.ipv6_count;
    file->ipv6.start = data + layout.ipv6_off;

    if (layout.ipv6_index_off > 0) {
        layout.ipv6_index_off--;      /* 1-based */

        if (size < NGX_IP2L_INDEX_SIZE
            || layout.ipv6_index_off > size - NGX_IP2L_INDEX_SIZE)
        {
            ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
                "ngx_ip2l_file_init: invalid ipv6 index offset %uD, size: %uz",
                layout.ipv6_index_off, size);
            return NGX_ERROR;
        }

        file->ipv6.index = (void *) (data + layout.ipv6_index_off);
    }

    return NGX_OK;
}


static ngx_int_t
ngx_ip2l_file_open(ngx_conf_t *cf, ngx_str_t *name, ngx_ip2l_file_t *ipf)
{
    u_char           *base;
    size_t            size;
    ssize_t           n;
    ngx_err_t         err;
    ngx_int_t         rc;
    ngx_file_t        file;
    ngx_file_info_t   fi;

    ngx_memzero(&file, sizeof(ngx_file_t));
    file.name = *name;
    file.log = cf->log;

    file.fd = ngx_open_file(name->data, NGX_FILE_RDONLY, NGX_FILE_OPEN, 0);

    if (file.fd == NGX_INVALID_FILE) {
        err = ngx_errno;
        if (err != NGX_ENOENT) {
            ngx_conf_log_error(NGX_LOG_CRIT, cf, err,
                               ngx_open_file_n " \"%s\" failed", name->data);
        }
        return NGX_DECLINED;
    }

    if (ngx_fd_info(file.fd, &fi) == NGX_FILE_ERROR) {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, ngx_errno,
                           ngx_fd_info_n " \"%s\" failed", name->data);
        goto failed;
    }

    size = (size_t) ngx_file_size(&fi);

    base = ngx_palloc(cf->pool, size);
    if (base == NULL) {
        goto failed;
    }

    n = ngx_read_file(&file, base, size, 0);

    if (n == NGX_ERROR) {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, ngx_errno,
                           ngx_read_file_n " \"%s\" failed", name->data);
        goto failed;
    }

    if ((size_t) n != size) {
        ngx_conf_log_error(NGX_LOG_CRIT, cf, 0,
            ngx_read_file_n " \"%s\" returned only %z bytes instead of %z",
            name->data, n, size);
        goto failed;
    }

    ipf->data = base;
    ipf->size = size;

    if (ngx_ip2l_file_init(cf, ipf) != NGX_OK) {
        goto failed;
    }

    ngx_conf_log_error(NGX_LOG_NOTICE, cf, 0,
                       "using ip2location file \"%s\"", name->data);

    rc = NGX_OK;

    goto done;

failed:

    rc = NGX_DECLINED;

done:

    if (ngx_close_file(file.fd) == NGX_FILE_ERROR) {
        ngx_log_error(NGX_LOG_ALERT, cf->log, ngx_errno,
                      ngx_close_file_n " \"%s\" failed", name->data);
    }

    return rc;
}


ngx_flag_t
ngx_ip2l_file_field_exists(ngx_ip2l_file_t *file, ngx_uint_t field)
{
    if (field == ngx_ip2l_field_country_long) {
        field = ngx_ip2l_field_country;

    } else if (field >= ngx_ip2l_field_count) {
        return 0;
    }

    return file->spec.fields[field] >= 0;
}


ngx_ip2l_file_t *
ngx_ip2l_file_get(ngx_cycle_t *cycle, ngx_str_t *name)
{
    ngx_uint_t         i;
    ngx_ip2l_file_t  **files;
    ngx_ip2l_conf_t   *icf;

    icf = (ngx_ip2l_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_ip2l_module);

    files = icf->files.elts;

    for (i = 0; i < icf->files.nelts; i++) {

        if (files[i]->name.len == name->len
            && ngx_strncmp(files[i]->name.data, name->data, name->len) == 0)
        {
            return files[i];
        }
    }

    return NULL;
}


static ngx_ip2l_file_t *
ngx_ip2l_file_add(ngx_conf_t *cf, ngx_str_t *name)
{
    ngx_ip2l_file_t  *file, **filep;
    ngx_ip2l_conf_t  *icf;

    file = ngx_pcalloc(cf->pool, sizeof(ngx_ip2l_file_t));
    if (file == NULL) {
        return NULL;
    }

    ngx_memzero(file, sizeof(*file));
    file->name = *name;

    icf = (ngx_ip2l_conf_t *) ngx_get_conf(cf->cycle->conf_ctx,
        ngx_ip2l_module);

    filep = ngx_array_push(&icf->files);
    if (filep == NULL) {
        return NULL;
    }

    *filep = file;

    return file;
}


static char *
ngx_ip2l_file(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t        *value;
    ngx_ip2l_file_t  *file;

    value = cf->args->elts;

    if (ngx_ip2l_file_get(cf->cycle, &value[1]) != NULL) {
        return "is duplicate";
    }

    file = ngx_ip2l_file_add(cf, &value[1]);
    if (file == NULL) {
        return NGX_CONF_ERROR;
    }

    if (ngx_ip2l_file_open(cf, &value[2], file) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}
