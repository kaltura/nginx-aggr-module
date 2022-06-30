#include <ngx_config.h>
#include <ngx_core.h>
#include <pthread.h>
#include <zlib.h>
#if (NGX_HAVE_LIBRDKAFKA)
#include "ngx_kafka_producer.h"
#endif
#include "ngx_aggr.h"
#include "ngx_aggr_window.h"
#include "ngx_aggr_output.h"


static void *ngx_aggr_outputs_create_conf(ngx_cycle_t *cycle);

static ngx_int_t ngx_aggr_outputs_start(ngx_cycle_t *cycle);
static void ngx_aggr_outputs_close(ngx_cycle_t *cycle);


typedef void (*ngx_aggr_output_poll_pt)(void *data);

typedef ngx_int_t (*ngx_aggr_output_init_pt)(void *data, ngx_log_t *log,
    time_t t);

typedef void (*ngx_aggr_output_close_pt)(void *data);


typedef struct {
    ngx_uint_t                    queue_size;
} ngx_aggr_output_conf_t;


typedef struct {
    ngx_aggr_query_t             *query;
    ngx_uint_t                    queue_size;
    ngx_log_t                     log;

    ngx_aggr_bucket_t           **queue;
    volatile ngx_uint_t           read_pos;
    volatile ngx_uint_t           write_pos;

    ngx_log_handler_pt            log_error;
    ngx_aggr_output_poll_pt       poll;
    ngx_aggr_output_init_pt       init;
    ngx_aggr_event_send_pt        handler;
    ngx_aggr_output_close_pt      close;
    void                         *data;
} ngx_aggr_output_ctx_t;


typedef struct {
    ngx_str_t                     path_format;
    ngx_str_t                     delim;
    gzFile                        file;
} ngx_aggr_output_file_ctx_t;


typedef struct {
    ngx_queue_t                   queue;   /* ngx_aggr_outputs_arr_t */
} ngx_aggr_outputs_conf_t;


static ngx_core_module_t  ngx_aggr_output_module_ctx = {
    ngx_string("aggr_output"),
    ngx_aggr_outputs_create_conf,
    NULL
};


ngx_module_t  ngx_aggr_output_module = {
    NGX_MODULE_V1,
    &ngx_aggr_output_module_ctx,           /* module context */
    NULL,                                  /* module directives */
    NGX_CORE_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_aggr_outputs_start,                /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_aggr_outputs_close,                /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_atomic_t   aggr_threads = 0;
ngx_atomic_t         *ngx_aggr_threads = &aggr_threads;


static char *
ngx_aggr_output_init_conf(ngx_conf_t *cf, ngx_aggr_output_conf_t *output_conf,
    ngx_uint_t required_args)
{
    ngx_str_t    tmp;
    ngx_str_t   *value;
    ngx_uint_t   i;

    value = cf->args->elts;

    output_conf->queue_size = 64;

    for (i = required_args + 1; i < cf->args->nelts; ) {

        if (ngx_strncmp(value[i].data, "queue_size=", 11) == 0) {

            output_conf->queue_size = ngx_atoi(value[i].data + 11,
                value[i].len - 11);

            if (output_conf->queue_size == (ngx_uint_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid \"queue_size\" value \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

        } else {
            i++;
            continue;
        }

        cf->args->nelts--;

        tmp = value[i];
        value[i] = value[cf->args->nelts];
        value[cf->args->nelts] = tmp;
    }

    return NGX_CONF_OK;
}


static void
ngx_aggr_output_push(ngx_aggr_output_ctx_t *output, ngx_aggr_bucket_t *bucket)
{
    ngx_uint_t  next_pos;

    next_pos = (output->write_pos + 1) % output->queue_size;
    if (next_pos == output->read_pos) {
        ngx_log_error(NGX_LOG_ERR, &output->log, 0,
            "ngx_aggr_output_push: bucket queue full");
        return;
    }

    ngx_aggr_bucket_add_ref(bucket);
    output->queue[output->write_pos] = bucket;

    ngx_memory_barrier();

    output->write_pos = next_pos;
}


static ngx_int_t
ngx_aggr_output_send(ngx_aggr_output_ctx_t *output, ngx_aggr_result_t *ar,
    time_t t)
{
    ngx_int_t  rc;

    if (output->init) {
        if (output->init(output->data, &output->log, t) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    rc = ngx_aggr_result_send(ar, output->handler, output->data);

    if (output->close) {
        output->close(output->data);
    }

    return rc;
}


static void *
ngx_aggr_output_cycle(void *data)
{
    time_t                  granularity;
    ngx_uint_t              period;
    ngx_uint_t              last_period;
    ngx_aggr_query_t       *query;
    ngx_aggr_bucket_t      *bucket;
    ngx_aggr_result_t      *ar, *new_ar;
    ngx_aggr_output_ctx_t  *output = data;

    (void) ngx_atomic_fetch_add(ngx_aggr_threads, 1);

    ngx_log_error(NGX_LOG_INFO, &output->log, 0,
        "ngx_aggr_output_cycle: thread started");

    query = output->query;
    granularity = query->granularity;

    ar = NULL;
    last_period = 0;

    while (!ngx_terminate && !ngx_exiting) {

        if (output->poll) {
            output->poll(output->data);
        }

        if (output->read_pos == output->write_pos) {
            ngx_msleep(500);
            continue;
        }

        bucket = output->queue[output->read_pos];

        ngx_memory_barrier();

        output->read_pos = (output->read_pos + 1) % output->queue_size;

        period = ngx_aggr_bucket_get_time(bucket) / granularity;

        if (last_period != period) {

            new_ar = ngx_aggr_result_create(query, &output->log,
                period * granularity, ar);
            if (new_ar == NULL) {
                break;
            }

            if (ar != NULL) {
                (void) ngx_aggr_output_send(output, ar,
                    last_period * granularity);
            }

            ar = new_ar;

            last_period = period;
        }

        if (ngx_aggr_bucket_process(bucket, ar) != NGX_OK) {
            break;
        }

        ngx_aggr_bucket_free(bucket);
    }

    if (ar != NULL) {
        ngx_aggr_result_destroy(ar);
    }

    ngx_log_error(NGX_LOG_INFO, &output->log, 0,
        "ngx_aggr_output_cycle: thread done");

    (void) ngx_atomic_fetch_add(ngx_aggr_threads, -1);

    return NULL;
}


static ngx_int_t
ngx_aggr_output_thread_init(ngx_log_t *log, ngx_aggr_output_ctx_t *output)
{
    int             err;
    pthread_t       tid;
    pthread_attr_t  attr;

    err = pthread_attr_init(&attr);
    if (err) {
        ngx_log_error(NGX_LOG_ALERT, log, err,
            "pthread_attr_init() failed");
        return NGX_ERROR;
    }

    err = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (err) {
        ngx_log_error(NGX_LOG_ALERT, log, err,
            "pthread_attr_setdetachstate() failed");
        (void) pthread_attr_destroy(&attr);
        return NGX_ERROR;
    }

    output->log = *log;
    if (output->log_error) {
        output->log.handler = output->log_error;
        output->log.data = output->data;
    }

    err = pthread_create(&tid, &attr, ngx_aggr_output_cycle, output);
    if (err) {
        ngx_log_error(NGX_LOG_ALERT, log, err,
            "pthread_create() failed");
        (void) pthread_attr_destroy(&attr);
        return NGX_ERROR;
    }

    (void) pthread_attr_destroy(&attr);

    return NGX_OK;
}


ngx_int_t
ngx_aggr_outputs_init(ngx_conf_t *cf, ngx_aggr_outputs_arr_t *conf)
{
    ngx_aggr_outputs_conf_t  *ocf;

    if (ngx_array_init(&conf->outputs, cf->pool, 1,
                       sizeof(ngx_aggr_output_ctx_t))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    ocf = (void *) ngx_get_conf(cf->cycle->conf_ctx, ngx_aggr_output_module);

    ngx_queue_insert_tail(&ocf->queue, &conf->queue);

    return NGX_OK;
}


static ngx_aggr_output_ctx_t *
ngx_aggr_outputs_add(ngx_conf_t *cf, ngx_aggr_outputs_arr_t *conf,
    ngx_aggr_output_conf_t *output_conf)
{
    ngx_aggr_output_ctx_t  *output;

    output = ngx_array_push(&conf->outputs);
    if (output == NULL) {
        return NULL;
    }

    ngx_memzero(output, sizeof(*output));

    output->query = ngx_aggr_query_block(cf, 1);
    if (output->query == NULL) {
        return NULL;
    }

    output->queue_size = output_conf->queue_size;

    output->queue = ngx_palloc(cf->pool, sizeof(output->queue[0]) *
        output->queue_size);
    if (output->queue == NULL) {
        return NULL;
    }

    return output;
}


void
ngx_aggr_outputs_push(ngx_aggr_outputs_arr_t *conf, ngx_aggr_bucket_t *bucket)
{
    ngx_uint_t              i, n;
    ngx_aggr_output_ctx_t  *elts;

    n = conf->outputs.nelts;
    elts = conf->outputs.elts;

    for (i = 0; i < n; i++) {
        ngx_aggr_output_push(&elts[i], bucket);
    }
}


static ngx_int_t
ngx_aggr_outputs_start(ngx_cycle_t *cycle)
{
    ngx_uint_t                i, n;
    ngx_queue_t              *q;
    ngx_aggr_output_ctx_t    *elts;
    ngx_aggr_outputs_arr_t   *conf;
    ngx_aggr_outputs_conf_t  *ocf;

    ocf = (void *) ngx_get_conf(cycle->conf_ctx, ngx_aggr_output_module);

    for (q = ngx_queue_head(&ocf->queue);
        q != ngx_queue_sentinel(&ocf->queue);
        q = ngx_queue_next(q))
    {
        conf = ngx_queue_data(q, ngx_aggr_outputs_arr_t, queue);

        n = conf->outputs.nelts;
        elts = conf->outputs.elts;

        for (i = 0; i < n; i++) {
            if (ngx_aggr_output_thread_init(conf->log, &elts[i]) != NGX_OK) {
                return NGX_ERROR;
            }
        }
    }

    return NGX_OK;
}


static void
ngx_aggr_outputs_close(ngx_cycle_t *cycle)
{
    ngx_uint_t  i;

    for (i = 0; i < 50; i++) {

        if (*ngx_aggr_threads <= 0) {
            ngx_log_debug0(NGX_LOG_DEBUG_CORE, cycle->log, 0,
                "ngx_aggr_outputs_close: all threads finished");
            return;
        }

        ngx_msleep(100);
    }

    ngx_log_error(NGX_LOG_ALERT, cycle->log, 0,
        "ngx_aggr_outputs_close: timed out waiting for threads to quit");
}


static void *
ngx_aggr_outputs_create_conf(ngx_cycle_t *cycle)
{
    ngx_aggr_outputs_conf_t  *ocf;

    ocf = ngx_pcalloc(cycle->pool, sizeof(ngx_aggr_outputs_conf_t));
    if (ocf == NULL) {
        return NULL;
    }

    ngx_queue_init(&ocf->queue);

    return ocf;
}


static u_char *
ngx_aggr_output_file_log_error(ngx_log_t *log, u_char *buf, size_t len)
{
    u_char                      *p;
    ngx_aggr_output_file_ctx_t  *ctx;

    p = buf;

    ctx = log->data;

    p = ngx_snprintf(buf, len, ", path_format: %V", &ctx->path_format);

    return p;
}


static ngx_int_t
ngx_aggr_output_file_init(void *data, ngx_log_t *log, time_t t)
{
    struct tm                    tm;
    ngx_aggr_output_file_ctx_t  *ctx = data;
    char                         path[NGX_MAX_PATH];

    ngx_libc_gmtime(t, &tm);

    if (strftime(path, sizeof(path), (char *) ctx->path_format.data, &tm)
        == 0)
    {
        ngx_log_error(NGX_LOG_ERR, log, 0,
            "ngx_aggr_output_file_init: strftime failed");
        return NGX_ERROR;
    }

    ctx->file = gzopen(path, "wb");
    if (!ctx->file) {
        ngx_log_error(NGX_LOG_ERR, log, ngx_errno,
            "ngx_aggr_output_file_init: gzopen(%s) failed", path);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static void
ngx_aggr_output_file_write(void *data, void *buf, size_t len, void *free_ctx)
{
    ngx_aggr_output_file_ctx_t  *ctx = data;

    gzwrite(ctx->file, buf, len);
    gzwrite(ctx->file, ctx->delim.data, ctx->delim.len);

    ngx_aggr_result_send_buf_free(free_ctx);
}


static void
ngx_aggr_output_file_close(void *data)
{
    ngx_aggr_output_file_ctx_t  *ctx = data;

    gzclose(ctx->file);
    ctx->file = NULL;
}


char *
ngx_aggr_output_file(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char  *p = conf;

    char                        *rv;
    ngx_str_t                   *value;
    ngx_uint_t                   i;
    ngx_aggr_output_ctx_t       *output;
    ngx_aggr_output_conf_t       output_conf;
    ngx_aggr_outputs_arr_t      *ocf;
    ngx_aggr_output_file_ctx_t  *ctx;

    value = cf->args->elts;

    ocf = (ngx_aggr_outputs_arr_t *) (p + cmd->offset);

    rv = ngx_aggr_output_init_conf(cf, &output_conf, 1);
    if (rv != NGX_CONF_OK) {
        return rv;
    }

    ctx = ngx_pcalloc(cf->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    ctx->path_format = value[1];
    ngx_str_set(&ctx->delim, "\n");

    for (i = 2; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "delim=", 6) == 0) {
            ctx->delim.data = value[i].data + 6;
            ctx->delim.len = value[i].len - 6;
            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }


    output = ngx_aggr_outputs_add(cf, ocf, &output_conf);
    if (output == NULL) {
        return NGX_CONF_ERROR;
    }

    output->log_error = ngx_aggr_output_file_log_error;
    output->init = ngx_aggr_output_file_init;
    output->handler = ngx_aggr_output_file_write;
    output->close = ngx_aggr_output_file_close;
    output->data = ctx;

    return NGX_CONF_OK;
}


#if (NGX_HAVE_LIBRDKAFKA)
char *
ngx_aggr_output_kafka(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char  *p = conf;

    char                        *rv;
    ngx_command_t                empty_cmd;
    ngx_aggr_output_ctx_t       *output;
    ngx_aggr_output_conf_t       output_conf;
    ngx_aggr_outputs_arr_t      *ocf;
    ngx_kafka_producer_topic_t  *kpt;

    ocf = (ngx_aggr_outputs_arr_t *) (p + cmd->offset);

    rv = ngx_aggr_output_init_conf(cf, &output_conf, 2);
    if (rv != NGX_CONF_OK) {
        return rv;
    }

    ngx_memzero(&empty_cmd, sizeof(empty_cmd));
    kpt = NGX_CONF_UNSET_PTR;

    rv = ngx_kafka_producer_topic_slot(cf, &empty_cmd, &kpt,
        ngx_aggr_result_send_buf_free);
    if (rv != NGX_CONF_OK) {
        return rv;
    }

    output = ngx_aggr_outputs_add(cf, ocf, &output_conf);
    if (output == NULL) {
        return NGX_CONF_ERROR;
    }

    output->log_error = ngx_kafka_producer_topic_log_error;
    output->poll = (ngx_aggr_output_poll_pt) ngx_kafka_producer_topic_poll;
    output->handler = (ngx_aggr_event_send_pt)
        ngx_kafka_producer_topic_produce;
    output->data = kpt;

    return NGX_CONF_OK;
}
#endif
