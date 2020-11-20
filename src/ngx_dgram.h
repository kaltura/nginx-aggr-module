#ifndef _NGX_DGRAM_H_INCLUDED_
#define _NGX_DGRAM_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_event.h>


typedef struct ngx_dgram_session_s  ngx_dgram_session_t;


#include "ngx_dgram_variables.h"
#include "ngx_dgram_script.h"


typedef struct {
    void                         **main_conf;
    void                         **srv_conf;
} ngx_dgram_conf_ctx_t;


typedef struct {
    struct sockaddr               *sockaddr;
    socklen_t                      socklen;
    ngx_str_t                      addr_text;

    /* server ctx */
    ngx_dgram_conf_ctx_t          *ctx;

    unsigned                       bind:1;
    unsigned                       wildcard:1;
#if (NGX_HAVE_INET6)
    unsigned                       ipv6only:1;
#endif
    unsigned                       reuseport:1;
    unsigned                       so_keepalive:2;
#if (NGX_HAVE_KEEPALIVE_TUNABLE)
    int                            tcp_keepidle;
    int                            tcp_keepintvl;
    int                            tcp_keepcnt;
#endif
    int                            backlog;
    int                            rcvbuf;
    int                            sndbuf;
    int                            type;
} ngx_dgram_listen_t;


typedef void (*ngx_dgram_content_handler_pt)(ngx_dgram_session_t *s);


typedef struct {
    ngx_array_t                    servers;     /* ngx_dgram_core_srv_conf_t */
    ngx_array_t                    listening;   /* ngx_dgram_listen_t */

    ngx_hash_t                     variables_hash;

    ngx_array_t                    variables;        /* ngx_dgram_variable_t */
    ngx_array_t                    prefix_variables; /* ngx_dgram_variable_t */
    ngx_uint_t                     ncaptures;

    ngx_uint_t                     variables_hash_max_size;
    ngx_uint_t                     variables_hash_bucket_size;

    ngx_hash_keys_arrays_t        *variables_keys;
} ngx_dgram_core_main_conf_t;


typedef struct {
    ngx_dgram_content_handler_pt   handler;

    ngx_dgram_conf_ctx_t          *ctx;

    ngx_array_t                   *listen;      /* ngx_dgram_listen_t */

    u_char                        *file_name;
    ngx_uint_t                     line;

    ngx_flag_t                     tcp_nodelay;

    ngx_log_t                     *error_log;

    ngx_msec_t                     accept_timeout;
    ngx_msec_t                     recv_timeout;
} ngx_dgram_core_srv_conf_t;


struct ngx_dgram_session_s {
    uint32_t                       signature;         /* "DGRM" */

    ngx_connection_t              *connection;

    off_t                          received;
    time_t                         start_sec;
    ngx_msec_t                     start_msec;

    ngx_log_handler_pt             log_handler;

    void                         **ctx;
    void                         **main_conf;
    void                         **srv_conf;

    ngx_dgram_variable_value_t    *variables;

#if (NGX_PCRE)
    ngx_uint_t                     ncaptures;
    int                           *captures;
    u_char                        *captures_data;
#endif
};


typedef struct {
    ngx_int_t                    (*preconfiguration)(ngx_conf_t *cf);
    ngx_int_t                    (*postconfiguration)(ngx_conf_t *cf);

    void                        *(*create_main_conf)(ngx_conf_t *cf);
    char                        *(*init_main_conf)(ngx_conf_t *cf, void *conf);

    void                        *(*create_srv_conf)(ngx_conf_t *cf);
    char                        *(*merge_srv_conf)(ngx_conf_t *cf, void *prev,
                                                   void *conf);
} ngx_dgram_module_t;


#define NGX_DGRAM_MODULE       0x4d524744     /* "DGRM" */

#define NGX_DGRAM_MAIN_CONF    0x02000000
#define NGX_DGRAM_SRV_CONF     0x04000000
#define NGX_DGRAM_UPS_CONF     0x08000000


#define NGX_DGRAM_MAIN_CONF_OFFSET  offsetof(ngx_dgram_conf_ctx_t, main_conf)
#define NGX_DGRAM_SRV_CONF_OFFSET   offsetof(ngx_dgram_conf_ctx_t, srv_conf)


#define ngx_dgram_get_module_ctx(s, module)   (s)->ctx[module.ctx_index]
#define ngx_dgram_set_ctx(s, c, module)       s->ctx[module.ctx_index] = c;
#define ngx_dgram_delete_ctx(s, module)       s->ctx[module.ctx_index] = NULL;


#define ngx_dgram_get_module_main_conf(s, module)                              \
    (s)->main_conf[module.ctx_index]
#define ngx_dgram_get_module_srv_conf(s, module)                               \
    (s)->srv_conf[module.ctx_index]

#define ngx_dgram_conf_get_module_main_conf(cf, module)                        \
    ((ngx_dgram_conf_ctx_t *) cf->ctx)->main_conf[module.ctx_index]
#define ngx_dgram_conf_get_module_srv_conf(cf, module)                         \
    ((ngx_dgram_conf_ctx_t *) cf->ctx)->srv_conf[module.ctx_index]

#define ngx_dgram_cycle_get_module_main_conf(cycle, module)                    \
    (cycle->conf_ctx[ngx_dgram_module.index] ?                                 \
        ((ngx_dgram_conf_ctx_t *) cycle->conf_ctx[ngx_dgram_module.index])     \
            ->main_conf[module.ctx_index]:                                     \
        NULL)


extern ngx_module_t  ngx_dgram_module;
extern ngx_uint_t    ngx_dgram_max_module;
extern ngx_module_t  ngx_dgram_core_module;


static ngx_inline ssize_t
ngx_dgram_recv(ngx_connection_t *c, u_char *buf, size_t size)
{
    c->read->available = 1;     /* make ngx_unix_recv call recv */

    return c->recv(c, buf, size);
}


#endif /* _NGX_DGRAM_H_INCLUDED_ */
