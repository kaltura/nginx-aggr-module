#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr_window.h"


struct ngx_aggr_bucket_s {
    ngx_aggr_bucket_t            *next;
    ngx_aggr_window_t            *window;
    ngx_atomic_t                  ref_count;

    time_t                        time;
    ngx_aggr_buf_t               *head;
    ngx_aggr_buf_t              **last;
};


struct ngx_aggr_window_s {
    ngx_pool_t                   *pool;
    ngx_aggr_window_conf_t        conf;

    ngx_aggr_bucket_handler_pt    handler;
    void                         *data;

    ngx_atomic_t                  lock;

    /* locked fields */
    ngx_aggr_bucket_t            *used_head;
    ngx_aggr_bucket_t           **used_tail;
    ngx_aggr_bucket_t            *free;
    ngx_aggr_buf_t               *free_bufs;

    /* used only by receiver */
    ngx_uint_t                    bufs_left;
    ngx_aggr_bucket_t            *bucket;
    ngx_aggr_buf_t               *buf;
};


static void
ngx_aggr_bucket_do_free_locked(ngx_aggr_bucket_t *bucket)
{
    ngx_aggr_window_t  *window;

    window = bucket->window;

    *bucket->last = window->free_bufs;
    window->free_bufs = bucket->head;

    bucket->next = window->free;
    window->free = bucket;
}


static void
ngx_aggr_bucket_free_locked(ngx_aggr_bucket_t *bucket)
{
    if (ngx_atomic_fetch_add(&bucket->ref_count, -1) > 1) {
        return;
    }

    ngx_aggr_bucket_do_free_locked(bucket);
}


static ngx_aggr_bucket_t *
ngx_aggr_bucket_create(ngx_aggr_window_t *window, time_t now)
{
    ngx_aggr_bucket_t  *next;
    ngx_aggr_bucket_t  *bucket;

    /* free old buckets*/

    ngx_spinlock(&window->lock, 1, 2048);

    for (bucket = window->used_head; ; bucket = next) {

        if (bucket == NULL) {
            window->used_tail = &window->used_head;
            break;
        }

        if (bucket->time + window->conf.interval > now) {
            break;
        }

        next = bucket->next;

        ngx_aggr_bucket_free_locked(bucket);
    }

    window->used_head = bucket;

    /* create the new bucket */

    if (window->free != NULL) {
        bucket = window->free;
        window->free = bucket->next;

    } else {
        bucket = NULL;
    }

    ngx_memory_barrier();

    ngx_unlock(&window->lock);

    if (bucket == NULL) {
        bucket = ngx_palloc(window->pool, sizeof(*bucket));
        if (bucket == NULL) {
            return NULL;
        }
    }

    bucket->next = NULL;
    bucket->window = window;
    bucket->ref_count = 1;
    bucket->time = now;
    bucket->head = NULL;
    bucket->last = &bucket->head;

    return bucket;
}


void
ngx_aggr_bucket_free(ngx_aggr_bucket_t *bucket)
{
    ngx_aggr_window_t  *window;

    if (ngx_atomic_fetch_add(&bucket->ref_count, -1) > 1) {
        return;
    }

    window = bucket->window;

    ngx_spinlock(&window->lock, 1, 2048);

    ngx_aggr_bucket_do_free_locked(bucket);

    ngx_memory_barrier();

    ngx_unlock(&window->lock);
}


void
ngx_aggr_bucket_add_ref(ngx_aggr_bucket_t *bucket)
{
    (void) ngx_atomic_fetch_add(&bucket->ref_count, 1);
}


static void
ngx_aggr_bucket_send(ngx_aggr_bucket_t *bucket)
{
    ngx_aggr_window_t  *window;

    window = bucket->window;

    *bucket->last = NULL;

    window->handler(window->data, bucket);

    ngx_spinlock(&window->lock, 1, 2048);

    if (window->conf.interval > 0) {
        *window->used_tail = bucket;
        window->used_tail = &bucket->next;

    } else {
        ngx_aggr_bucket_free_locked(bucket);
    }

    ngx_memory_barrier();

    ngx_unlock(&window->lock);
}


static ngx_aggr_bucket_t *
ngx_aggr_bucket_get(ngx_aggr_window_t *window)
{
    time_t              now;
    ngx_aggr_bucket_t  *bucket;

    now = ngx_time();

    bucket = window->bucket;
    if (bucket != NULL) {
        if (bucket->time == now) {
            return bucket;
        }

        if (bucket->last == &bucket->head->next &&
            bucket->head->last == bucket->head->start)
        {
            bucket->time = now;
            return bucket;
        }

        ngx_aggr_bucket_send(bucket);
    }

    bucket = ngx_aggr_bucket_create(window, now);

    window->bucket = bucket;
    window->buf = NULL;

    return bucket;
}


ngx_int_t
ngx_aggr_bucket_process(ngx_aggr_bucket_t *bucket, ngx_aggr_result_t *ar)
{
    ngx_aggr_buf_t  *cur;

    for (cur = bucket->head; cur != NULL; cur = cur->next) {

        if (ngx_aggr_result_process(ar, cur->start, cur->last - cur->start)
            != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


time_t
ngx_aggr_bucket_get_time(ngx_aggr_bucket_t *bucket)
{
    return bucket->time;
}


void
ngx_aggr_window_conf_init(ngx_aggr_window_conf_t *conf)
{
    conf->interval = NGX_CONF_UNSET;
    conf->buf_size = NGX_CONF_UNSET_SIZE;
    conf->max_buffers = NGX_CONF_UNSET_UINT;
    conf->recv_size = NGX_CONF_UNSET_SIZE;
}


void
ngx_aggr_window_conf_merge(ngx_aggr_window_conf_t *conf,
    ngx_aggr_window_conf_t *prev)
{
    ngx_conf_merge_sec_value(conf->interval, prev->interval, 10);
    ngx_conf_merge_size_value(conf->buf_size, prev->buf_size, 65536);
    ngx_conf_merge_uint_value(conf->max_buffers, prev->max_buffers, 4096);
    ngx_conf_merge_size_value(conf->recv_size, prev->recv_size, 4096);
}


static void
ngx_aggr_window_detach(void *data)
{
    ngx_aggr_window_conf_t  *conf = data;

    conf->window = NULL;
}


ngx_aggr_window_t *
ngx_aggr_window_create(ngx_pool_t *pool, ngx_aggr_window_conf_t *conf,
    ngx_aggr_bucket_handler_pt handler, void *data)
{
    ngx_aggr_window_t   *window;
    ngx_pool_cleanup_t  *cln;

    if (conf->window != NULL) {
        ngx_log_error(NGX_LOG_ALERT, pool->log, 0,
            "ngx_aggr_window_create: already attached");
        return NULL;
    }

    window = ngx_pcalloc(pool, sizeof(*window));
    if (window == NULL) {
        return NULL;
    }

    cln = ngx_pool_cleanup_add(pool, 0);
    if (cln == NULL) {
        return NULL;
    }

    window->pool = pool;
    window->conf = *conf;
    window->bufs_left = conf->max_buffers;

    window->handler = handler;
    window->data = data;

    window->used_tail = &window->used_head;

    conf->window = window;

    cln->handler = ngx_aggr_window_detach;
    cln->data = conf;

    return window;
}


ngx_int_t
ngx_aggr_window_get_recv_buf(ngx_aggr_window_t *window,
    ngx_aggr_buf_t **result)
{
    ngx_aggr_buf_t     *buf;
    ngx_aggr_bucket_t  *bucket;

    bucket = ngx_aggr_bucket_get(window);
    if (bucket == NULL) {
        return NGX_ERROR;
    }

    buf = window->buf;
    if (buf != NULL &&
        (size_t) (buf->end - buf->last) > window->conf.recv_size)
    {
        *result = buf;
        return NGX_OK;
    }

    ngx_spinlock(&window->lock, 1, 2048);

    if (window->free_bufs != NULL) {
        buf = window->free_bufs;
        window->free_bufs = buf->next;

    } else {
        buf = NULL;
    }

    ngx_memory_barrier();

    ngx_unlock(&window->lock);

    if (buf == NULL) {
        if (window->bufs_left <= 0) {
            ngx_log_error(NGX_LOG_ERR, window->pool->log, 0,
                "ngx_aggr_window_get_recv_buf: no free bufs");
            return NGX_AGAIN;
        }

        buf = ngx_palloc(window->pool, sizeof(*buf) + window->conf.buf_size);
        if (buf == NULL) {
            return NGX_ERROR;
        }

        buf->start = (void *)(buf + 1);
        buf->end = buf->start + window->conf.buf_size;
    }

    buf->last = buf->start;

    *bucket->last = buf;
    bucket->last = &buf->next;

    window->buf = buf;

    *result = buf;
    return NGX_OK;
}


ngx_int_t
ngx_aggr_window_process(ngx_aggr_window_t *window, ngx_pool_t *pool,
    ngx_aggr_result_t *ar)
{
    ngx_int_t            rc;
    ngx_uint_t           i, n;
    ngx_aggr_bucket_t   *bucket;
    ngx_aggr_bucket_t  **buckets;

    buckets = ngx_palloc(pool, sizeof(buckets[0]) * window->conf.interval);
    if (buckets == NULL) {
        return NGX_ERROR;
    }

    ngx_spinlock(&window->lock, 1, 2048);

    n = 0;
    for (bucket = window->used_head; bucket != NULL; bucket = bucket->next) {

        if (n >= (ngx_uint_t) window->conf.interval) {
            break;
        }

        ngx_aggr_bucket_add_ref(bucket);
        buckets[n++] = bucket;
    }

    ngx_memory_barrier();

    ngx_unlock(&window->lock);

    for (i = 0; i < n; i++) {
        if (ngx_aggr_bucket_process(buckets[i], ar) != NGX_OK) {
            rc = NGX_ERROR;
            goto done;
        }
    }

    rc = NGX_OK;

done:

    for (i = 0; i < n; i++) {
        ngx_aggr_bucket_free(buckets[i]);
    }

    return rc;
}
