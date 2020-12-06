#include <ngx_config.h>
#include <ngx_core.h>
#if (NGX_HAVE_LIBRDKAFKA)
#include "ngx_kafka_producer.h"
#endif
#include "ngx_aggr_result.h"
#include "ngx_aggr_window.h"
#include "ngx_aggr_output.h"


typedef void (*ngx_aggr_output_poll_pt)(void *data);


typedef struct {
    ngx_uint_t                 queue_size;
} ngx_aggr_output_conf_t;


typedef struct {
    ngx_aggr_query_t          *query;
    ngx_uint_t                 queue_size;
    ngx_log_t                 *log;

    ngx_aggr_bucket_t        **queue;
    volatile ngx_uint_t        read_pos;
    volatile ngx_uint_t        write_pos;

    ngx_aggr_output_poll_pt    poll;
    ngx_aggr_event_send_pt     handler;
    void                      *data;
} ngx_aggr_output_ctx_t;


static ngx_atomic_t   aggr_threads = 0;
ngx_atomic_t         *ngx_aggr_threads = &aggr_threads;


#if (NGX_HAVE_LIBRDKAFKA)    /* TODO: remove when adding more output types */
static char *
ngx_aggr_output_init_conf(ngx_conf_t *cf, ngx_aggr_output_conf_t *output_conf)
{
    ngx_str_t    tmp;
    ngx_str_t   *value;
    ngx_uint_t   i;

    value = cf->args->elts;

    output_conf->queue_size = 64;

    for (i = 3; i < cf->args->nelts; ) {

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
#endif


static void
ngx_aggr_output_push(ngx_aggr_output_ctx_t *output, ngx_aggr_bucket_t *bucket)
{
    ngx_uint_t  next_pos;

    next_pos = (output->write_pos + 1) % output->queue_size;
    if (next_pos == output->read_pos) {
        ngx_log_error(NGX_LOG_ERR, output->log, 0,
            "ngx_aggr_output_push: bucket queue full");
        return;
    }

    ngx_aggr_bucket_add_ref(bucket);
    output->queue[output->write_pos] = bucket;

    ngx_memory_barrier();

    output->write_pos = next_pos;
}


static void *
ngx_aggr_output_cycle(void *data)
{
    ngx_uint_t              period;
    ngx_uint_t              last_period;
    ngx_aggr_query_t       *query;
    ngx_aggr_bucket_t      *bucket;
    ngx_aggr_result_t      *ar, *new_ar;
    ngx_aggr_output_ctx_t  *output = data;

    (void) ngx_atomic_fetch_add(ngx_aggr_threads, 1);

    ngx_log_debug0(NGX_LOG_DEBUG_CORE, output->log, 0,
        "ngx_aggr_output_cycle: thread started");

    query = output->query;

    ar = NULL;
    last_period = 0;

    while (!ngx_terminate && !ngx_exiting) {

        output->poll(output->data);

        if (output->read_pos == output->write_pos) {
            ngx_msleep(500);
            continue;
        }

        bucket = output->queue[output->read_pos];

        ngx_memory_barrier();

        output->read_pos = (output->read_pos + 1) % output->queue_size;

        period = ngx_aggr_bucket_get_time(bucket) / query->granularity;

        if (last_period != period) {

            new_ar = ngx_aggr_result_create(query, period * query->granularity,
                ar);
            if (new_ar == NULL) {
                break;
            }

            if (ar != NULL) {
                if (ngx_aggr_result_send(ar, output->handler, output->data)
                    != NGX_OK)
                {
                    break;
                }
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

    ngx_log_debug0(NGX_LOG_DEBUG_CORE, output->log, 0,
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

    output->log = log;

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
ngx_aggr_outputs_init(ngx_conf_t *cf, ngx_aggr_outputs_conf_t *conf)
{
    if (ngx_array_init(&conf->outputs, cf->pool, 1,
                       sizeof(ngx_aggr_output_ctx_t))
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    return NGX_OK;
}


#if (NGX_HAVE_LIBRDKAFKA)    /* TODO: remove when adding more output types */
static ngx_aggr_output_ctx_t *
ngx_aggr_outputs_add(ngx_conf_t *cf, ngx_aggr_outputs_conf_t *conf,
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
#endif


void
ngx_aggr_outputs_push(ngx_aggr_outputs_conf_t *conf, ngx_aggr_bucket_t *bucket)
{
    ngx_uint_t              i, n;
    ngx_aggr_output_ctx_t  *elts;

    n = conf->outputs.nelts;
    elts = conf->outputs.elts;

    for (i = 0; i < n; i++) {
        ngx_aggr_output_push(&elts[i], bucket);
    }
}


ngx_int_t
ngx_aggr_outputs_start(ngx_log_t *log, ngx_aggr_outputs_conf_t *conf)
{
    ngx_uint_t              i, n;
    ngx_aggr_output_ctx_t  *elts;

    n = conf->outputs.nelts;
    elts = conf->outputs.elts;

    for (i = 0; i < n; i++) {
        if (ngx_aggr_output_thread_init(log, &elts[i]) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


void
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


#if (NGX_HAVE_LIBRDKAFKA)
char *
ngx_aggr_output_kafka(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char  *p = conf;

    char                        *rv;
    ngx_command_t                empty_cmd;
    ngx_aggr_output_ctx_t       *output;
    ngx_aggr_output_conf_t       output_conf;
    ngx_aggr_outputs_conf_t     *ocf;
    ngx_kafka_producer_topic_t  *kpt;

    ocf = (ngx_aggr_outputs_conf_t *) (p + cmd->offset);

    rv = ngx_aggr_output_init_conf(cf, &output_conf);
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

    output->poll = (ngx_aggr_output_poll_pt) ngx_kafka_producer_topic_poll;
    output->handler = (ngx_aggr_event_send_pt)
        ngx_kafka_producer_topic_produce;
    output->data = kpt;

    return NGX_CONF_OK;
}
#endif
