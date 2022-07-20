#ifndef _NGX_RATE_LIMIT_H_INCLUDED_
#define _NGX_RATE_LIMIT_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


#define ngx_rate_limit_init(interval, limit) { interval, limit, 0, 0 }


typedef struct {
    ngx_int_t  interval;
    ngx_int_t  limit;

    time_t     reset_time;
    ngx_int_t  left;
} ngx_rate_limit_ctx_t;


static ngx_inline ngx_flag_t
ngx_rate_limit(ngx_rate_limit_ctx_t *ctx)
{
    time_t  now;

    now = ngx_time();
    if (now < ctx->reset_time || now >= ctx->reset_time + ctx->interval) {
        ctx->reset_time = now;
        ctx->left = ctx->limit;
    }

    if (ctx->left <= 0) {
        return 0;
    }

    ctx->left--;
    return 1;
}


#endif /* _NGX_RATE_LIMIT_H_INCLUDED_ */
