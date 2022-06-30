#ifndef _NGX_KAFKA_PRODUCER_H_INCLUDED_
#define _NGX_KAFKA_PRODUCER_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


typedef void (*ngx_kafka_producer_free_pt)(void *free_ctx);

typedef struct ngx_kafka_producer_topic_s  ngx_kafka_producer_topic_t;


char *ngx_kafka_producer_topic_slot(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf, ngx_kafka_producer_free_pt free);

u_char *ngx_kafka_producer_topic_log_error(ngx_log_t *log,
    u_char *buf, size_t len);

void ngx_kafka_producer_topic_produce(ngx_kafka_producer_topic_t *kpt,
    void *buf, size_t len, void *free_ctx);

void ngx_kafka_producer_topic_poll(ngx_kafka_producer_topic_t *kpt);

#endif /* _NGX_KAFKA_PRODUCER_H_INCLUDED_ */
