#ifndef _NGX_IP2L_H_INCLUDED_
#define _NGX_IP2L_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


typedef struct ngx_ip2l_file_s  ngx_ip2l_file_t;


typedef enum {
    ngx_ip2l_field_country,
    ngx_ip2l_field_region,
    ngx_ip2l_field_city,
    ngx_ip2l_field_isp,
    ngx_ip2l_field_latitude,
    ngx_ip2l_field_longitude,
    ngx_ip2l_field_domain,
    ngx_ip2l_field_zipcode,
    ngx_ip2l_field_timezone,
    ngx_ip2l_field_netspeed,
    ngx_ip2l_field_iddcode,
    ngx_ip2l_field_areacode,
    ngx_ip2l_field_weatherstationcode,
    ngx_ip2l_field_weatherstationname,
    ngx_ip2l_field_mcc,
    ngx_ip2l_field_mnc,
    ngx_ip2l_field_mobilebrand,
    ngx_ip2l_field_elevation,
    ngx_ip2l_field_usagetype,

    ngx_ip2l_field_count,

    ngx_ip2l_field_country_long,        /* bundled with country short */
} ngx_ip2l_field_e;


ngx_ip2l_field_e ngx_ip2l_parse_field_name(ngx_str_t *name);

ngx_ip2l_file_t *ngx_ip2l_file_get(ngx_cycle_t *cycle, ngx_str_t *name);

ngx_flag_t ngx_ip2l_file_field_exists(ngx_ip2l_file_t *file, ngx_uint_t field);

ngx_int_t ngx_ip2l_file_get_field(ngx_ip2l_file_t *file, ngx_str_t *ip,
    ngx_uint_t field, ngx_pool_t *pool, ngx_str_t *value);

#endif /* _NGX_IP2L_H_INCLUDED_ */
