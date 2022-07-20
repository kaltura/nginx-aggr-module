#include <ngx_config.h>
#include <ngx_core.h>
#include <librdkafka/rdkafka.h>
#include "ngx_rate_limit.h"
#include "ngx_kafka_producer.h"


#define NGX_KAFKA_PRODUCER_ERROR_MSG_LEN  (1024)


typedef struct {
    ngx_array_t                  producers;      /* ngx_kafka_producer_t * */
} ngx_kafka_producer_conf_t;


typedef struct {
    ngx_str_t                    name;
    ngx_str_t                    brokers;
    ngx_str_t                    client_id;
    ngx_str_t                    compression;
    ngx_str_t                    debug;
    ngx_uint_t                   log_level;
    ngx_uint_t                   buffer_max_msgs;
    ngx_uint_t                   buffer_max_ms;
    ngx_uint_t                   max_retries;
    ngx_msec_t                   backoff_ms;

    ngx_queue_t                  topics;

    rd_kafka_t                  *rk;
    rd_kafka_conf_t             *rkc;
} ngx_kafka_producer_t;


struct ngx_kafka_producer_topic_s {
    ngx_str_t                    name;
    ngx_int_t                    partition;
    ngx_kafka_producer_free_pt   free;

    ngx_kafka_producer_t        *kp;
    ngx_queue_t                  queue;

    rd_kafka_topic_t            *rkt;
    rd_kafka_topic_conf_t       *rktc;
};


static char *ngx_kafka_producer(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static void *ngx_kafka_producer_create_conf(ngx_cycle_t *cycle);

static ngx_int_t ngx_kafka_producer_init_worker(ngx_cycle_t *cycle);
static void ngx_kafka_producer_exit_worker(ngx_cycle_t *cycle);


static ngx_command_t  ngx_kafka_producer_commands[] = {

    { ngx_string("kafka_producer"),
      NGX_MAIN_CONF|NGX_DIRECT_CONF|NGX_CONF_1MORE,
      ngx_kafka_producer,
      0,
      0,
      NULL },

      ngx_null_command
};


static ngx_core_module_t  ngx_kafka_producer_module_ctx = {
    ngx_string("kafka_producer"),
    ngx_kafka_producer_create_conf,
    NULL,
};


ngx_module_t  ngx_kafka_producer_module = {
    NGX_MODULE_V1,
    &ngx_kafka_producer_module_ctx,        /* module context */
    ngx_kafka_producer_commands,           /* module directives */
    NGX_CORE_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_kafka_producer_init_worker,        /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_kafka_producer_exit_worker,        /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_str_t             ngx_kafka_producer_default_client_id =
    ngx_string("nginx");

static ngx_str_t             ngx_kafka_producer_default_compression =
    ngx_string("snappy");

static ngx_rate_limit_ctx_t  ngx_kakfa_producer_error_rate =
    ngx_rate_limit_init(1, 4);


static char *
ngx_kafka_producer_str_dup(ngx_pool_t *pool, ngx_str_t *src)
{
    char  *dst;

    dst = ngx_pnalloc(pool, src->len + 1);
    if (dst == NULL) {
        return NULL;
    }

    ngx_memcpy(dst, src->data, src->len);
    dst[src->len] = '\0';
    return dst;
}


static ngx_int_t
ngx_kafka_producer_conf_set_int(ngx_pool_t *pool, rd_kafka_conf_t *rkc,
    const char *key, ngx_int_t num)
{
    char                 value[NGX_INT64_LEN + 1];
    rd_kafka_conf_res_t  rc;

    char  errstr[NGX_KAFKA_PRODUCER_ERROR_MSG_LEN];

    ngx_sprintf((u_char *) value, "%i%Z", num);

    rc = rd_kafka_conf_set(rkc, key, value, errstr, sizeof(errstr));
    if (rc != RD_KAFKA_CONF_OK) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "rd_kafka_conf_set failed, key:%s, value:%s, err:%s",
            key, value, errstr);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_kafka_producer_conf_set_str(ngx_pool_t *pool, rd_kafka_conf_t *rkc,
    const char *key, ngx_str_t *str)
{
    char                 *value;
    rd_kafka_conf_res_t   rc;

    char  errstr[NGX_KAFKA_PRODUCER_ERROR_MSG_LEN];

    value = ngx_kafka_producer_str_dup(pool, str);
    if (value == NULL) {
        return NGX_ERROR;
    }

    rc = rd_kafka_conf_set(rkc, key, value, errstr, sizeof(errstr));
    if (rc != RD_KAFKA_CONF_OK) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "rd_kafka_conf_set failed, key:%s, value:%s, err:%s",
            key, value, errstr);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static rd_kafka_conf_res_t
ngx_kafka_producer_topic_conf_set_str(ngx_pool_t *pool,
    rd_kafka_topic_conf_t *rktc, const char *key, ngx_str_t *str)
{
    char                 *value;
    rd_kafka_conf_res_t   rc;

    char  errstr[NGX_KAFKA_PRODUCER_ERROR_MSG_LEN];

    value = ngx_kafka_producer_str_dup(pool, str);
    if (value == NULL) {
        return NGX_ERROR;
    }

    rc = rd_kafka_topic_conf_set(rktc, key, value, errstr, sizeof(errstr));
    if (rc != RD_KAFKA_CONF_OK) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "rd_kafka_topic_conf_set failed, key:%s, value:%s, err:%s",
            key, value, errstr);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_kafka_producer_topic_init(ngx_kafka_producer_topic_t *kpt,
    ngx_pool_t *pool)
{
    static ngx_str_t  zero_str = ngx_string("0");

    char  *name;

    kpt->rktc = rd_kafka_topic_conf_new();
    if (kpt->rktc == NULL) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "rd_kafka_topic_conf_new failed");
        return NGX_ERROR;
    }

    if (ngx_kafka_producer_topic_conf_set_str(pool, kpt->rktc,
        "request.required.acks", &zero_str) != NGX_OK)
    {
        return NGX_ERROR;
    }

    name = ngx_kafka_producer_str_dup(pool, &kpt->name);
    if (name == NULL) {
        return NGX_ERROR;
    }


    rd_kafka_topic_conf_set_opaque(kpt->rktc, kpt);


    kpt->rkt = rd_kafka_topic_new(kpt->kp->rk, name, kpt->rktc);
    if (kpt->rkt == NULL) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "rd_kafka_topic_new failed \"%s\"", name);
        return NGX_ERROR;
    }

    kpt->rktc = NULL;   /* ownership passed to librdkafka */

    return NGX_OK;
}


static void
ngx_kafka_producer_topic_destroy(ngx_kafka_producer_topic_t *kpt)
{
    if (kpt->rkt != NULL) {
        rd_kafka_topic_destroy(kpt->rkt);
        kpt->rkt = NULL;
    }

    if (kpt->rktc != NULL) {
        rd_kafka_topic_conf_destroy(kpt->rktc);
        kpt->rktc = NULL;
    }
}


static void
ngx_kafka_producer_dr_msg_cb(rd_kafka_t *rk,
    const rd_kafka_message_t *rkmessage, void *opaque)
{
    void                        *free_ctx;
    ngx_kafka_producer_topic_t  *kpt;

    kpt = rd_kafka_topic_opaque(rkmessage->rkt);
    free_ctx = rkmessage->_private;

    kpt->free(free_ctx);

    if (!rkmessage->err) {
        return;
    }

    if (!ngx_rate_limit(&ngx_kakfa_producer_error_rate)) {
        return;
    }

    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
        "kafka message delivery failed, err:%s",
        rd_kafka_err2str(rkmessage->err));
}


static void
ngx_kafka_producer_error_cb(rd_kafka_t *rk, int err, const char *reason,
    void *opaque)
{
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
        "kafka error: %s", reason);
}


static ngx_int_t
ngx_kafka_producer_init(ngx_kafka_producer_t *kp, ngx_pool_t *pool)
{
    ngx_queue_t                 *q;
    ngx_kafka_producer_topic_t  *kpt;

    char errstr[NGX_KAFKA_PRODUCER_ERROR_MSG_LEN];

    kp->rkc = rd_kafka_conf_new();
    if (kp->rkc == NULL) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "rd_kafka_conf_new failed");
        return NGX_ERROR;
    }


    if (ngx_kafka_producer_conf_set_str(pool, kp->rkc,
        "bootstrap.servers", &kp->brokers) != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_kafka_producer_conf_set_str(pool, kp->rkc,
        "client.id", &kp->client_id) != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_kafka_producer_conf_set_str(pool, kp->rkc,
        "compression.codec", &kp->compression) != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (kp->debug.len != 0) {
        if (ngx_kafka_producer_conf_set_str(pool, kp->rkc,
            "debug", &kp->debug) != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    if (ngx_kafka_producer_conf_set_int(pool, kp->rkc,
        "log_level", kp->log_level) != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_kafka_producer_conf_set_int(pool, kp->rkc,
        "queue.buffering.max.messages", kp->buffer_max_msgs) != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_kafka_producer_conf_set_int(pool, kp->rkc,
        "queue.buffering.max.ms", kp->buffer_max_ms) != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_kafka_producer_conf_set_int(pool, kp->rkc,
        "message.send.max.retries", kp->max_retries) != NGX_OK)
    {
        return NGX_ERROR;
    }

    if (ngx_kafka_producer_conf_set_int(pool, kp->rkc,
        "retry.backoff.ms", kp->backoff_ms) != NGX_OK)
    {
        return NGX_ERROR;
    }


    rd_kafka_conf_set_dr_msg_cb(kp->rkc, ngx_kafka_producer_dr_msg_cb);

    rd_kafka_conf_set_error_cb(kp->rkc, ngx_kafka_producer_error_cb);


    kp->rk = rd_kafka_new(RD_KAFKA_PRODUCER, kp->rkc, errstr, sizeof(errstr));
    if (kp->rk == NULL) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
            "rd_kafka_new failed, err:%s", errstr);
        return NGX_ERROR;
    }

    kp->rkc = NULL;   /* ownership passed to librdkafka */


    for (q = ngx_queue_head(&kp->topics);
        q != ngx_queue_sentinel(&kp->topics);
        q = ngx_queue_next(q))
    {
        kpt = ngx_queue_data(q, ngx_kafka_producer_topic_t, queue);

        if (ngx_kafka_producer_topic_init(kpt, pool) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static void
ngx_kafka_producer_destroy(ngx_kafka_producer_t *kp)
{
    ngx_queue_t                 *q;
    ngx_kafka_producer_topic_t  *kpt;

    for (q = ngx_queue_head(&kp->topics);
        q != ngx_queue_sentinel(&kp->topics);
        q = ngx_queue_next(q))
    {
        kpt = ngx_queue_data(q, ngx_kafka_producer_topic_t, queue);

        ngx_kafka_producer_topic_destroy(kpt);
    }

    if (kp->rk != NULL) {
        rd_kafka_destroy(kp->rk);
        kp->rk = NULL;
    }

    if (kp->rkc != NULL) {
        rd_kafka_conf_destroy(kp->rkc);
        kp->rkc = NULL;
    }
}


static void *
ngx_kafka_producer_create_conf(ngx_cycle_t *cycle)
{
    ngx_kafka_producer_conf_t  *kpcf;

    kpcf = ngx_pcalloc(cycle->pool, sizeof(ngx_kafka_producer_conf_t));
    if (kpcf == NULL) {
        return NULL;
    }

    if (ngx_array_init(&kpcf->producers, cycle->pool, 4,
                       sizeof(ngx_kafka_producer_t *))
        != NGX_OK)
    {
        return NULL;
    }

    return kpcf;
}


static ngx_kafka_producer_t *
ngx_kafka_producer_get(ngx_cycle_t *cycle, ngx_str_t *name)
{
    ngx_uint_t                   i;
    ngx_kafka_producer_t       **tpp;
    ngx_kafka_producer_conf_t   *kpcf;

    kpcf = (ngx_kafka_producer_conf_t *) ngx_get_conf(cycle->conf_ctx,
        ngx_kafka_producer_module);

    tpp = kpcf->producers.elts;

    for (i = 0; i < kpcf->producers.nelts; i++) {

        if (tpp[i]->name.len == name->len
            && ngx_strncmp(tpp[i]->name.data, name->data, name->len) == 0)
        {
            return tpp[i];
        }
    }

    return NULL;
}


static ngx_kafka_producer_t *
ngx_kafka_producer_add(ngx_conf_t *cf, ngx_str_t *name)
{
    ngx_kafka_producer_t       *kp, **tpp;
    ngx_kafka_producer_conf_t  *kpcf;

    kp = ngx_kafka_producer_get(cf->cycle, name);

    if (kp) {
        return kp;
    }

    kp = ngx_pcalloc(cf->pool, sizeof(ngx_kafka_producer_t));
    if (kp == NULL) {
        return NULL;
    }

    ngx_queue_init(&kp->topics);

    kp->name = *name;

    kpcf = (ngx_kafka_producer_conf_t *) ngx_get_conf(cf->cycle->conf_ctx,
        ngx_kafka_producer_module);

    tpp = ngx_array_push(&kpcf->producers);
    if (tpp == NULL) {
        return NULL;
    }

    *tpp = kp;

    return kp;
}


static char *
ngx_kafka_producer(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t             *value, v;
    ngx_uint_t             i;
    ngx_kafka_producer_t  *kp;

    value = cf->args->elts;

    kp = ngx_kafka_producer_add(cf, &value[1]);

    if (kp == NULL) {
        return NGX_CONF_ERROR;
    }

    if (kp->brokers.len) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "duplicate kafka producer \"%V\"", &kp->name);
        return NGX_CONF_ERROR;
    }

    kp->client_id = ngx_kafka_producer_default_client_id;
    kp->compression = ngx_kafka_producer_default_compression;
    kp->log_level = 6;
    kp->buffer_max_msgs = 100000;
    kp->buffer_max_ms = 50;
    kp->backoff_ms = 10;

    for (i = 2; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "brokers=", 8) == 0) {

            kp->brokers.data = value[i].data + 8;
            kp->brokers.len = value[i].len - 8;

            continue;
        }

        if (ngx_strncmp(value[i].data, "client_id=", 10) == 0) {

            kp->client_id.data = value[i].data + 10;
            kp->client_id.len = value[i].len - 10;

            continue;
        }

        if (ngx_strncmp(value[i].data, "compression=", 12) == 0) {

            kp->compression.data = value[i].data + 12;
            kp->compression.len = value[i].len - 12;

            continue;
        }

        if (ngx_strncmp(value[i].data, "debug=", 6) == 0) {

            kp->debug.data = value[i].data + 6;
            kp->debug.len = value[i].len - 6;

            continue;
        }

        if (ngx_strncmp(value[i].data, "log_level=", 10) == 0) {

            kp->log_level = ngx_atoi(value[i].data + 10, value[i].len - 10);

            if (kp->log_level == (ngx_uint_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid log_level value \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "buffer_max_msgs=", 16) == 0) {

            kp->buffer_max_msgs = ngx_atoi(value[i].data + 16,
                value[i].len - 16);

            if (kp->buffer_max_msgs == (ngx_uint_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid buffer_max_msgs value \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "buffer_max_ms=", 14) == 0) {

            kp->buffer_max_ms = ngx_atoi(value[i].data + 14,
                value[i].len - 14);

            if (kp->buffer_max_ms == (ngx_uint_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid buffer_max_ms value \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "max_retries=", 12) == 0) {

            kp->max_retries = ngx_atoi(value[i].data + 12, value[i].len - 12);

            if (kp->max_retries == (ngx_uint_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid max_retries value \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "backoff_ms=", 11) == 0) {

            v.data = value[i].data + 11;
            v.len = value[i].len - 11;

            kp->backoff_ms = ngx_parse_time(&v, 0);
            if (kp->backoff_ms == (ngx_msec_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid backoff_ms value \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    if (kp->brokers.len == 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "\"%V\" must have \"brokers\" parameter",
            &cmd->name);
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


char *
ngx_kafka_producer_topic_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf,
    ngx_kafka_producer_free_pt free)
{
    char  *p = conf;

    ngx_str_t                    *value;
    ngx_uint_t                    i;
    ngx_kafka_producer_t         *kp;
    ngx_kafka_producer_topic_t  **kptp, *kpt;

    kptp = (ngx_kafka_producer_topic_t **) (p + cmd->offset);

    if (*kptp != NGX_CONF_UNSET_PTR) {
        return "is duplicate";
    }

    value = cf->args->elts;

    kp = ngx_kafka_producer_get(cf->cycle, &value[1]);

    if (kp == NULL) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "kafka producer \"%V\" not found", &value[1]);
        return NGX_CONF_ERROR;
    }

    kpt = ngx_pcalloc(cf->pool, sizeof(ngx_kafka_producer_topic_t));
    if (kpt == NULL) {
        return NULL;
    }

    kpt->partition = RD_KAFKA_PARTITION_UA;
    kpt->free = free;

    for (i = 3; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "partition=", 10) == 0) {

            kpt->partition = ngx_atoi(value[i].data + 10, value[i].len - 10);

            if (kpt->partition == NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "invalid partition value \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    kpt->kp = kp;
    kpt->name = value[2];
    ngx_queue_insert_tail(&kp->topics, &kpt->queue);

    *kptp = kpt;

    return NGX_CONF_OK;
}


void
ngx_kafka_producer_topic_poll(ngx_kafka_producer_topic_t *kpt)
{
    if (kpt->kp->rk == NULL) {
        /* can happen due to a race condition on startup */
        return;
    }

    (void) rd_kafka_poll(kpt->kp->rk, 0);
}


u_char *
ngx_kafka_producer_topic_log_error(ngx_log_t *log, u_char *buf, size_t len)
{
    u_char                      *p;
    ngx_kafka_producer_topic_t  *kpt;

    p = buf;

    kpt = log->data;

    p = ngx_snprintf(buf, len, ", kafka: %V, topic: %V",
        &kpt->kp->name, &kpt->name);

    return p;
}


void
ngx_kafka_producer_topic_produce(ngx_kafka_producer_topic_t *kpt,
    void *buf, size_t len, void *free_ctx)
{
    int          err;
    const char  *errstr;

    if (kpt->rkt == NULL) {
        /* can happen due to a race condition on startup */
        kpt->free(free_ctx);
        return;
    }

    err = rd_kafka_produce(kpt->rkt, kpt->partition, 0, buf, len, NULL, 0,
        free_ctx);
    if (err == 0) {
        return;
    }

    kpt->free(free_ctx);

    if (!ngx_rate_limit(&ngx_kakfa_producer_error_rate)) {
        return;
    }

    errstr = rd_kafka_err2str(rd_kafka_last_error());

    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
        "produce failed, topic:%V, partition:%i, err:%s",
        &kpt->name, kpt->partition, errstr);
}


static ngx_int_t
ngx_kafka_producer_init_worker(ngx_cycle_t *cycle)
{
    ngx_uint_t                   i;
    ngx_kafka_producer_t       **tpp;
    ngx_kafka_producer_conf_t   *kpcf;

    if (ngx_process != NGX_PROCESS_WORKER
        && ngx_process != NGX_PROCESS_SINGLE)
    {
        return NGX_OK;
    }

    kpcf = (ngx_kafka_producer_conf_t *) ngx_get_conf(cycle->conf_ctx,
        ngx_kafka_producer_module);

    if (kpcf == NULL) {
        return NGX_OK;
    }

    tpp = kpcf->producers.elts;

    for (i = 0; i < kpcf->producers.nelts; i++) {
        if (ngx_kafka_producer_init(tpp[i], cycle->pool) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static void
ngx_kafka_producer_exit_worker(ngx_cycle_t *cycle)
{
    ngx_uint_t                   i;
    ngx_kafka_producer_t       **tpp;
    ngx_kafka_producer_conf_t   *kpcf;

    if (ngx_process != NGX_PROCESS_WORKER
        && ngx_process != NGX_PROCESS_SINGLE)
    {
        return;
    }

    kpcf = (ngx_kafka_producer_conf_t *) ngx_get_conf(cycle->conf_ctx,
        ngx_kafka_producer_module);

    if (kpcf == NULL) {
        return;
    }

    tpp = kpcf->producers.elts;

    for (i = 0; i < kpcf->producers.nelts; i++) {
        ngx_kafka_producer_destroy(tpp[i]);
    }
}
