#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_aggr.h"


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

        if (bucket->time + window->conf.interval >= now) {
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
ngx_aggr_bucket_replace_delim(ngx_aggr_bucket_t *bucket)
{
    u_char           delim;
    u_char          *p, *last;
    ngx_aggr_buf_t  *cur;

    delim = bucket->window->conf.delim;

    for (cur = bucket->head; cur != NULL; cur = cur->next) {
        last = cur->last;
        for (p = cur->start; p < last; p++) {
            if (*p == delim) {
                *p = '\0';
            }
        }
    }
}


static void
ngx_aggr_bucket_send(ngx_aggr_bucket_t *bucket)
{
    ngx_aggr_window_t  *window;

    window = bucket->window;

    *bucket->last = NULL;

    if (window->conf.delim != '\0') {
        ngx_aggr_bucket_replace_delim(bucket);
    }

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
    conf->delim = NGX_CONF_UNSET;
}


void
ngx_aggr_window_conf_merge(ngx_aggr_window_conf_t *conf,
    ngx_aggr_window_conf_t *prev)
{
    ngx_conf_merge_sec_value(conf->interval, prev->interval, 10);
    ngx_conf_merge_size_value(conf->buf_size, prev->buf_size, 65536);
    ngx_conf_merge_uint_value(conf->max_buffers, prev->max_buffers, 4096);
    ngx_conf_merge_size_value(conf->recv_size, prev->recv_size, 4096);
    ngx_conf_merge_value(conf->delim, prev->delim, 0);
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
    if (buf != NULL) {
        if ((size_t) (buf->end - buf->last) > window->conf.recv_size) {
            *result = buf;
            return NGX_OK;
        }

        *buf->last = '\0';
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

        buf->start = (void *) (buf + 1);
        buf->end = buf->start + window->conf.buf_size - 1;  /* room for null */
    }

    buf->last = buf->start;

    *bucket->last = buf;
    bucket->last = &buf->next;

    window->buf = buf;

    *result = buf;
    return NGX_OK;
}


ngx_int_t
ngx_aggr_window_write(ngx_aggr_window_t *window, u_char *p, u_char *last)
{
    size_t           in_left, out_left;
    ngx_int_t        rc;
    ngx_aggr_buf_t  *buf;

    for ( ;; ) {

        rc = ngx_aggr_window_get_recv_buf(window, &buf);
        if (rc != NGX_OK) {
            return rc;
        }

        in_left = last - p;
        out_left = buf->end - buf->last;

        if (in_left <= out_left) {
            buf->last = ngx_copy(buf->last, p, in_left);
            break;
        }

        buf->last = ngx_copy(buf->last, p, out_left);
        p += out_left;
    }

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


char *
ngx_conf_set_char_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char  *p = conf;

    ngx_int_t        *field;
    ngx_str_t        *value;
    ngx_conf_post_t  *post;

    field = (ngx_int_t *) (p + cmd->offset);

    if (*field != NGX_CONF_UNSET) {
        return "is duplicate";
    }

    value = cf->args->elts;
    if (value[1].len != 1) {
        return "has invalid length";
    }

    *field = value[1].data[0];

    if (cmd->post) {
        post = cmd->post;
        return post->post_handler(cf, post, field);
    }

    return NGX_CONF_OK;
}
