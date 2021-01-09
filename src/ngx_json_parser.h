#ifndef _NGX_JSON_PARSER_H_INCLUDED_
#define _NGX_JSON_PARSER_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


enum {
    NGX_JSON_NULL,
    NGX_JSON_BOOL,
    NGX_JSON_NUMBER,
    NGX_JSON_STRING,
    NGX_JSON_ARRAY,
    NGX_JSON_OBJECT,
};

enum {
    NGX_JSON_OK             = 0,
    NGX_JSON_BAD_DATA       = -1,
    NGX_JSON_ALLOC_FAILED   = -2,
};


typedef ngx_int_t ngx_json_status_t;

typedef struct ngx_array_part_s {
    void                     *first;
    void                     *last;
    size_t                    count;
    struct ngx_array_part_s  *next;
} ngx_array_part_t;

typedef struct {
    int                       type;
    size_t                    count;
    ngx_array_part_t          part;
} ngx_json_array_t;

typedef ngx_array_t ngx_json_object_t;

typedef struct {
    int                       type;
    union {
        ngx_flag_t            boolean;
        double                num;
        ngx_str_t             str;  /* Note: the string may be json escaped */
        ngx_json_array_t      arr;
        ngx_json_object_t     obj;  /* ngx_json_key_value_t */
    } v;
} ngx_json_value_t;

typedef struct {
    ngx_uint_t                key_hash;
    ngx_str_t                 key;
    ngx_json_value_t          value;
} ngx_json_key_value_t;



ngx_json_status_t ngx_json_parse(ngx_pool_t *pool, u_char *string,
    ngx_json_value_t *result, u_char *error, size_t error_size);

#endif /*_NGX_JSON_PARSER_H_INCLUDED_ */
