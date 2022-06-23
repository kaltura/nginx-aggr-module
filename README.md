# Nginx Aggregation Module

Aggregates JSON events received over UDP/TCP.
The aggregated events can be pushed to a Kafka topic, gzip files or pulled over HTTP.


## Build

To link statically against Nginx, cd to Nginx source directory and execute:

    ./configure --add-module=/path/to/nginx-aggr-module

To compile as a dynamic module (Nginx 1.9.11+), use:

    ./configure --add-dynamic-module=/path/to/nginx-aggr-module

In this case, the `load_module` directive should be used in nginx.conf to load the module.

### Profile Guided Optimization

From our experience, PGO can provide a significant performance boost to this module - ~30% reduction in query time on our environment.
You can follow the steps below in order to use PGO:

* configure nginx with `--with-cc-opt="-O3 -fprofile-generate"` and `--with-ld-opt=-lgcov`
* make install
* add `daemon off` and `master_process off` to nginx.conf
* restart nginx
* apply load simulating the expected production load
* stop nginx orderly - `nginx -s stop`
* edit the makefile - `vi ./objs/Makefile`
    * replace `-fprofile-generate` with `-fprofile-use -fprofile-correction`
    * remove `-lgcov`
* delete all object files - `find objs/ -type f -name '*.o' -delete`
* make install
* remove `dameon off` and `master_process off` from nginx.conf
* restart nginx

## Configuration

### Sample configuration

```
kafka_producer kafka brokers=192.168.0.1:9092;

dgram {

    server {

        listen 6005;

        aggr_input name=stats;

        aggr_output_kafka kafka stats_topic {

            granularity 10s;

            dim time type=time;
            dim event_type;
            dim user_id lower;
            dim country type=select;

            metric count default=1;
            metric total_time input=time;
            metric max_time type=max input=time;
        }
    }
}

http {

    aggr_thread_pool query_thread_pool;

    server {

        listen 80;
        server_name localhost;

        location /query {
            aggr_dynamic stats;
        }

        location /metrics {
            aggr_static stats {
                format prom;
                dim user_id lower;
                metric time_by_user input=time;
            }

            aggr_static stats {
                format prom;
                dim country;
                metric length_by_country input=length;
            }

            aggr_status;
        }
    }
}
```

### aggr core directives

#### aggr
* **syntax**: `aggr { ... }`
* **default**: `-`
* **context**: `main`

Provides the configuration file context in which the global aggr directives are specified.

#### windows_hash_max_size
* **syntax**: `windows_hash_max_size size;`
* **default**: `512`
* **context**: `aggr`

Sets the maximum size of the event windows hash table.

#### windows_hash_bucket_size
* **syntax**: `windows_hash_bucket_size size;`
* **default**: `64`
* **context**: `aggr`

Sets the bucket size for the event windows hash table.

### dgram core directives

#### dgram
* **syntax**: `dgram { ... }`
* **default**: `-`
* **context**: `main`

Provides the configuration file context in which the dgram server directives are specified.
The dgram module supports raw UDP/TCP input (similar to the Nginx `stream` module),
except that it uses blocking IO and worker threads, instead of sharing the main Nginx event loop.

#### server
* **syntax**: `server { ... }`
* **default**: `-`
* **context**: `dgram`

Sets the configuration for a server.

#### listen
* **syntax**: `listen address:port [tcp] [backlog=number] [rcvbuf=size] [sndbuf=size] [bind] [ipv6only=on|off] [reuseport] [so_keepalive=on|off|[keepidle]:[keepintvl]:[keepcnt]];`
* **default**: `-`
* **context**: `server`

Sets the address and port for the socket on which the server will accept connections.

The `tcp` parameter configures a listening socket for working with streams (the default is UDP).

See the documentation of the listen directive of the Nginx `stream` module for more details on the optional parameters supported by this directive.

#### tcp_nodelay
* **syntax**: `tcp_nodelay on|off;`
* **default**: `on`
* **context**: `dgram, server`

Enables or disables the use of the `TCP_NODELAY` option.

#### variables_hash_max_size
* **syntax**: `variables_hash_max_size size;`
* **default**: `1024`
* **context**: `dgram`

Sets the maximum size of the variables hash table.

#### variables_hash_bucket_size
* **syntax**: `variables_hash_bucket_size size;`
* **default**: `64`
* **context**: `dgram`

Sets the bucket size for the variables hash table.

#### error_log
* **syntax**: `error_log file [level];`
* **default**: `logs/error.log error`
* **context**: `dgram, server`

Configures logging, see the documentation of the Nginx core `error_log` directive for more details.


### dgram aggregation directives

#### aggr_input
* **syntax**: `aggr_input [name=string];`
* **default**: `-`
* **context**: `server`

Enables aggregation input on the enclosing `server`.
The module keeps a window of the events received in the last X seconds.
See [Input event JSON](#input-event-json) below for more details on the required input format.

The optional `name` parameter is required when referencing the events window in directives such as `aggr_static` and `aggr_dynamic`.

#### aggr_input_window
* **syntax**: `aggr_input_window time;`
* **default**: `10s`
* **context**: `dgram, server`

The duration in seconds of the window of events that are kept in memory.

If this parameter is set to 0, events are disposed as soon as they are pushed (e.g. to Kafka).
In this case, it is not possible to query the events using `aggr_static` and `aggr_dynamic`.

#### aggr_input_buf_size
* **syntax**: `aggr_input_buf_size size;`
* **default**: `64k`
* **context**: `dgram, server`

Sets the size of the buffers used for keeping the events in the window.

#### aggr_input_max_buffers
* **syntax**: `aggr_input_max_buffers number;`
* **default**: `4096`
* **context**: `dgram, server`

Sets the maximum number of buffers that can be allocated per window.
This parameter can be used to limit the amount of memory allocated by the window (=max_buffers x buf_size).
If the limit is reached, the module will stop receiving events until some buffers are released.

#### aggr_input_recv_size
* **syntax**: `aggr_input_recv_size size;`
* **default**: `4k`
* **context**: `dgram, server`

Sets the minimum receive buffer size. The value of this parameter must be smaller than `aggr_input_buf_size`.
It is recommended to set `aggr_input_buf_size` to be several times larger than `aggr_input_recv_size`,
in order to reduce memory fragmentation - up to recv_size bytes can be wasted per buffer.

When using UDP, the value should be large enough to contain the largest expected event, to avoid reassembly.

#### kafka_producer
* **syntax**: `kafka_producer name brokers=list [client_id=string] [compression=string] [debug=list] [log_level=number] [buffer_max_msgs=number] [buffer_max_ms=number] [max_retries=number] [backoff_ms=number];`
* **default**: `-`
* **context**: `main`

Defines a named Kafka connection that can used as aggregation output.
The `brokers` parameter sets the initial list of brokers, a comma separated list of `host` or `host:port`.

The following optional parameters can be specified:
* `client_id` - sets the Kafka client id, the default is `nginx`.
* `compression` - sets the message compression format, the default is `snappy`.
* `debug` - sets the list of librdkafka debug contexts, a comma separated list.
* `log_level` - sets the logging level of librdkafka, the default level is 6.
* `buffer_max_msgs` - maximum number of messages allowed on the producer queue, the default is 100000.
* `buffer_max_ms` - the delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches, the default is 50.
* `max_retries` - defines how many times to retry sending a failed message set, the default is 0.
* `backoff_ms` - the back off time in milliseconds before retrying a message send, the default is 10.

#### aggr_output_kafka
* **syntax**: `aggr_output_kafka name topic_name [partition=number] [queue_size=number] { ... }`
* **default**: `-`
* **context**: `server`

Adds a Kafka output to the enclosing `server`.
The `name` parameter must point to a Kafka connection created using the `kafka_producer` directive.
The `topic_name` parameter specifies the Kafka topic to which the aggregated events will be written.

The following optional parameters can be specified:
* `partition` - can be used to write to a specific partition, by default the module will write to a random partition.
* `queue_size` - the size of queue that the output thread uses to receive events.
    Each slot in the queue holds a bucket of 1 sec, the default queue size is 64.

The block contains the aggregation query that should be applied to the events, see [Aggregation query directives](#aggregation-query-directives) below.

#### aggr_output_file
* **syntax**: `aggr_output_file file_format [queue_size=number] { ... }`
* **default**: `-`
* **context**: `server`

Adds a gzip file output to the enclosing `server`. Each line in the output files generated by this directive is an aggregated JSON event.
The `file_format` parameter defines the file name, and supports strftime format specifiers (for example, %Y = year incl. century).

The following optional parameters can be specified:
* `queue_size` - the size of queue that the output thread uses to receive events.
    Each slot in the queue holds a bucket of 1 sec, the default queue size is 64.

The block contains the aggregation query that should be applied to the events, see [Aggregation query directives](#aggregation-query-directives) below.

### http directives

#### aggr_dynamic
* **syntax**: `aggr_dynamic window_name;`
* **default**: `-`
* **context**: `server, location`

Enables serving of queries sent as HTTP POST in the enclosing `location`.
The requests must have `Content-Type: application/json`, see [Aggregation query JSON](#aggregation-query-json) below for details about the format of JSON.

The `window_name` parameter should evaluate to a named events window, defined using the `aggr_input` directive.
This parameter can contain variables.

#### aggr_dynamic_block
* **syntax**: `aggr_dynamic_block window_name { ... }`
* **default**: `-`
* **context**: `server, location`

Enables serving of queries sent as HTTP POST in the enclosing `location`.
The requests must have `Content-Type: application/json`, see [Aggregation query JSON](#aggregation-query-json) below for details about the format of JSON.

The `window_name` parameter should evaluate to a named events window, defined using the `aggr_input` directive.
This parameter can contain variables.

The block contains additional parameters for the aggregation query (for example, `format`), see [Aggregation query directives](#aggregation-query-directives) below.

#### aggr_static
* **syntax**: `aggr_static window_name { ... }`
* **default**: `-`
* **context**: `server, location`

Adds a pre-configured aggregation to the enclosing `location, the result can be pulled using HTTP GET requests.
Multiple `aggr_static` directives can be specified on a single `location`, and their results will be concatenated.

The `window_name` parameter should evaluate to a named events window, defined using the `aggr_input` directive.
This parameter can contain variables.

The block contains the aggregation query that should be applied to the events, see [Aggregation query directives](#aggregation-query-directives) below.

#### aggr_static
* **syntax**: `aggr_status;`
* **default**: `-`
* **context**: `server, location`

Outputs metrics on the aggregations performed by the Nginx worker process serving the request.
This directive can be combined with `aggr_static` on the same `location`, the response will contain all metrics concatenated.

#### aggr_thread_pool
* **syntax**: `aggr_thread_pool name;`
* **default**: `-`
* **context**: `http, server, location`

Sets a thread pool that should be used for serving aggregation HTTP queries.
The thread pool must be defined using the Nginx `thread_pool` directive.
This directive is supported only on Nginx 1.7.11 or newer when compiling with --add-threads.

### Aggregation query directives

#### dim
* **syntax**: `dim output_name [type=time|group|select] [input=string] [default=string] [lower];`
* **default**: `-`
* **context**: `query`

Adds a dimension to group by/select.
`output_name` specifies the name of the dimension in the result set.

The following optional parameters can be specified:
* `type` - sets the type of the dimension - `time`/`group`/`select`, the default is `group`.
* `input` - sets the name of the key in the input JSON, the default is `output_name`. The parameter value can be a complex expression containing variables (e.g. `$dim_name`).
* `default` - sets a default value for the dimension, the default will be used if the dimension does not appear in the input JSON.
* `lower` - if set, the value of the dimension will be lower-cased.

#### metric
* **syntax**: `metric output_name [type=sum|max] [input=string] [default=number] [top=number];`
* **default**: `-`
* **context**: `query`

Adds a metric to the query.
`output_name` specifies the name of the metric in the result set.

The following optional parameters can be specified:
* `type` - sets the type of the metric - `sum`/`max`, the default is `max`.
* `input` - sets the name of the key in the input JSON, the default is `output_name`.
* `default` - sets a default value for the metric, the default will be used if the metric does not appear in the input JSON.
* `top` - number, include in the output only the events that have the top N values of the metric. If the provided number is negative, the output will include only the events that have the bottom N values of the metric. This property can be applied to one metric at the most.

#### filter
* **syntax**: `filter { ... }`
* **default**: `-`
* **context**: `query`

Sets the filter of the query, see [Aggregation filter directives](#aggregation-filter-directives) below.
The filter is applied to all input events during aggregation, events that don't match the filter are ignored.
If multiple filters are defined inside the block, they are combined with AND.

#### having
* **syntax**: `having { ... }`
* **default**: `-`
* **context**: `query`

Sets the filter of the query, see [Aggregation filter directives](#aggregation-filter-directives) below.
The filter is applied to all output events post aggregation, events that don't match the filter are not included in the output.
If multiple filters are defined inside the block, they are combined with AND.

#### map
* **syntax**: `map string $variable { ... }`
* **default**: `-`
* **context**: `query`

Similar to nginx's builtin `map` directive - creates a new variable whose value depends on the complex expression specified in the first parameter.
* Parameters inside the `map` block specify a mapping between source and resulting values.
* Source values are specified as strings or regular expressions.
* Strings are matched while ignoring the case.
* A regular expression should either start with `"~"` for case-sensitive matching, or with `"~*"` for case-insensitive matching.
* A regular expression can contain named and positional captures that can later be used in other directives along with the resulting variable.
* The resulting value can contain text, variables, and their combination.

The following special parameters are also supported:
* `default` value - sets the resulting value if the source value doesn't match matches any of the specified variants. When default is not specified, the default resulting value will be an empty string.

If a source value matches one of the names of special parameters described above, it should be prefixed with the `"\"` symbol.

#### map_hash_max_size
* **syntax**: `map_hash_max_size size;`
* **default**: `2048`
* **context**: `query`

Sets the maximum size of the map hash table.

#### map_hash_bucket_size
* **syntax**: `map_hash_bucket_size size;`
* **default**: `32|64|128`
* **context**: `query`

Sets the bucket size for the map hash table. The default value depends on the processor’s cache line size.

#### map_metric
* **syntax**: `map_metric name $variable { ... }`
* **default**: `-`
* **context**: `query`

Creates a new variable whose value depends on the value of the metric specified in the first parameter.
* Parameters inside the `map_metric` block specify a mapping between ranges of the metric value and resulting values.
* Source values have the format `min:max` where min and max are numbers.
* The range includes `min` but does not include `max` - `[min,max)`.
* The min/max values in each range are optional, if one of them is omitted, it is treated as -inf/+inf respectively.
* The resulting value can contain text, variables, and their combination.

The following special parameters are also supported:
* `default` value - sets the resulting value if the source value doesn't match matches any of the specified ranges. When default is not specified, the default resulting value will be an empty string.

#### format
* **syntax**: `format json|prom;`
* **default**: `-`
* **context**: `query`

Sets the output format of the result, the default is `json`.

#### granularity
* **syntax**: `granularity interval;`
* **default**: `30s`
* **context**: `query`

Sets the time granularity of the query, relevant only in output blocks (e.g. `aggr_output_kafka`).

#### dims_hash_max_size
* **syntax**: `dims_hash_max_size size;`
* **default**: `512`
* **context**: `query`

Sets the maximum size of the dimensions hash table.

#### dims_hash_bucket_size
* **syntax**: `dims_hash_bucket_size size;`
* **default**: `64`
* **context**: `query`

Sets the bucket size for the dimensions hash table.

#### metrics_hash_max_size
* **syntax**: `metrics_hash_max_size size;`
* **default**: `512`
* **context**: `query`

Sets the maximum size of the metrics hash table.

#### metrics_hash_bucket_size
* **syntax**: `metrics_hash_bucket_size size;`
* **default**: `64`
* **context**: `query`

Sets the bucket size for the metrics hash table.

#### max_event_size
* **syntax**: `max_event_size size;`
* **default**: `2k`
* **context**: `query`

Sets the size of the input events reassembly buffer.
Events larger than this size cannot be reassembled if split across multiple `recv` calls.

#### output_buf_size
* **syntax**: `output_buf_size size;`
* **default**: `64k`
* **context**: `query`

Sets the size of the buffers allocated for holding the result set.

### Aggregation filter directives

#### in
* **syntax**: `in dim [case_sensitive=on|off] value1 [value2 ...];`
* **context**: `filter`

Checks whether the value of an input dimension is in the provided list of values.
The dim parameter can be a complex expression containing variables (e.g. `$dim_name`).

#### contains
* **syntax**: `contains dim [case_sensitive=on|off] value1 [value2 ...];`
* **context**: `filter`

Checks whether any of the provided values is contained in the value of an input dimension.
The dim parameter can be a complex expression containing variables (e.g. `$dim_name`).

#### regex
* **syntax**: `regex dim [case_sensitive=on|off] pattern;`
* **context**: `filter`

Checks whether the value of an input dimension matches the provided regular expression pattern.
The dim parameter can be a complex expression containing variables (e.g. `$dim_name`).

#### gt
* **syntax**: `gt metric value;`
* **context**: `filter, having`

Checks whether the value of a metric is greater than the provided reference value.

#### lt
* **syntax**: `lt metric value;`
* **context**: `filter, having`

Checks whether the value of a metric is less than the provided reference value.

#### gte
* **syntax**: `gte metric value;`
* **context**: `filter, having`

Checks whether the value of a metric is greater than or equal to the provided reference value.

#### lte
* **syntax**: `lte metric value;`
* **context**: `filter, having`

Checks whether the value of a metric is less than or equal to the provided reference value.

#### and
* **syntax**: `and { ... }`
* **context**: `filter, having`

Applies a logical AND to the results of all the filters defined in the block.

#### or
* **syntax**: `or { ... }`
* **context**: `filter, having`

Applies a logical OR to the results of all the filters defined in the block.

#### not
* **syntax**: `not { ... }`
* **context**: `filter, having`

Applies a logical NOT to the result of the filter defined in the block.
If multiple filters are defined in the block, their results are AND'ed before applying NOT.

## Input event JSON

### Sample JSON

```
{"event_type":"click","user_id":"Joe","country":"United States","time":0.123}
```

### Requirements

* When using TCP, input events must be separated by `\0`. When using UDP, multiple events can be sent in a single UDP packet by delimiting them with `\0`.
* Each event must be a flat JSON object, containing only simple types - nested objects/arrays are not supported.
* Keys used as dimensions must have a string value, while keys used as metrics must have a number value.
* By default, the module does not support spaces in the JSON (other than spaces contained in string values).
    To enable support for spaces, compile the module with `NGX_AGGR_EVENT_JSON_SKIP_SPACES` set to 1.


## Aggregation query JSON

### Sample JSON

```
{
    "dims": {
        "event_type": {},
        "user_id": {
            "lower": true
        },
        "first_name": {
            "input": "$first_name"
        },
        "country": {
            "type": "select"
        }
    },
    "metrics": {
        "count": {
            "default": 1
        },
        "total_time": {
            "input": "time"
        },
        "max_time": {
            "type": "max",
            "input": "time"
        }
    },
    "maps": {
        "$first_name": {
            "input": "$dim_full_name",
            "values": {
                "~([^ ]+) ": "$1",
                "default": "Unknown"
            }
        }
    },
    "filter": {
        "type": "in",
        "dim": "domain",
        "values": ["example.com"]
    },
    "having": {
        "type": "gt",
        "metric": "count",
        "value": 100
    }
}
```

#### dims
* **syntax**: `"dims": { ... }`
* **default**: `-`
* **context**: `query`

An object containing the dimensions of the query.
The keys hold the output name of the dimension, the values hold the dimension properties.

The following optional properties can be specified:
* `type` - string, sets the type of the dimension - `time`/`group`/`select`, the default is `group`.
* `input` - string, sets the name of the key in the input JSON, the default is `output_name`. The property value can be a complex expression containing variables (e.g. `$dim_name`).
* `default` - string, sets a default value for the dimension, the default will be used if the dimension does not appear in the input JSON.
* `lower` - boolean, if set to `true`, the value of the dimension will be lower-cased.

#### metrics
* **syntax**: `"metrics": { ... }`
* **default**: `-`
* **context**: `query`

An object containing the metrics of the query.
The keys hold the output name of the metric, the values hold the metric properties.

The following optional properties can be specified:
* `type` - string, sets the type of the metric - `sum`/`max`, the default is `max`.
* `input` - string, sets the name of the key in the input JSON, the default is `output_name`.
* `default` - number, sets a default value for the metric, the default will be used if the metric does not appear in the input JSON.
* `top` - number, include in the output only the events that have the top N values of the metric. If the provided number is negative, the output will include only the events that have the bottom N values of the metric. This property can be applied to one metric at the most.

#### maps
* **syntax**: `"maps": { ... }`
* **default**: `-`
* **context**: `query`

An object containing map objects, each map object creates a variable by mapping one or more input dimensions. The variables can then be used as dimensions / in filters.
The keys of the maps object are the names of the variables, and must start with `"$"`.

The following properties are required for each map object in the array:
* `input` - string, a complex expression containing variables (e.g. `$dim_name`) that is matched against the map `values`.
* `values` - object, holds the input values and the resulting values. The keys are either simple strings, or regular expressions (prefixed with `"~"`). The values are strings, and can be either simple strings or complex expressions containing variables. See the description of the map directive above for more details.

#### filter
* **syntax**: `"filter": { ... }`
* **default**: `-`
* **context**: `query`

A filter object, see [Filter objects](#filter-objects) below for more details. The filter is applied to all input events during aggregation, events that don't match the filter are ignored.

#### having
* **syntax**: `"having": { ... }`
* **default**: `-`
* **context**: `query`

A filter object, see [Filter objects](#filter-objects) below for more details. The filter is applied to all output events post aggregation, events that don't match the filter are not included in the output.

### Filter objects

#### In filter

Checks whether the value of an input dimension is in the provided string array.

The following properties are required:
* `type` - string, must be `"in"`.
* `dim` - string, the name of the dimension key in the event JSON. The dim value can be a complex expression containing variables (e.g. `$dim_name`).
* `values` - string array, the list of values to compare against.

The following optional properties are defined:
* `case_sensitive` - boolean, if set to false, the string comparison will ignore case (default is `true`).

#### Contains filter

Checks whether any of the provided strings is contained in the value of an input dimension.

The following properties are required:
* `type` - string, must be `"contains"`.
* `dim` - string, the name of the dimension key in the event JSON. The dim value can be a complex expression containing variables (e.g. `$dim_name`).
* `values` - string array, the list of substrings to search for.

The following optional properties are defined:
* `case_sensitive` - boolean, if set to false, the string comparison will ignore case (default is `true`).

#### Regex filter

Checks whether the value of an input dimension matches a provided regular expression pattern.

The following properties are required:
* `type` - string, must be `"regex"`.
* `dim` - string, the name of the dimension key in the event JSON. The dim value can be a complex expression containing variables (e.g. `$dim_name`).
* `pattern` - string, a PCRE regular expression pattern.

The following optional properties are defined:
* `case_sensitive` - boolean, if set to false, the comparison will ignore case (default is `true`).

#### Compare filter

Checks whether the value of a metric is greater/less than the provided reference value.
The filter can be applied during aggregation to input metrics (when included in the query `filter` object), or post aggregation to output metrics (when included in the query `having` object).

The following properties are required:
* `type` - string, one of the following:
    * `"gt"` - greater than
    * `"lt"` - less than
    * `"gte"` - greater than or equals
    * `"lte"` - less than or equals
* `metric` - string, the name of the metric key. When used in `filter` context, name is the input name of a metric in the event JSON. When used in `having` context, name is the output name of some aggregation metric.

The following optional properties are defined:
* `value` - number, the value to compare against (default is `0`)

#### And filter

Applies a logical AND to the results of multiple filters.

The following properties are required:
* `type` - string, must be `"and"`.
* `filters` - object array, the filter objects.

#### Or filter

Applies a logical OR to the results of multiple filters.

The following properties are required:
* `type` - string, must be `"or"`.
* `filters` - object array, the filter objects.

#### Not filter

Applies a logical NOT to the result of another filter.

The following properties are required:
* `type` - string, must be `"not"`.
* `filter` - object, a filter object.


## Embedded Variables

The following built-in variables are available for use in `query` context -
* `dim_name` - arbitrary dimension, the last part of the variable name is the name of the dimension in the input JSON.


## Copyright & License

All code in this project is released under the [AGPLv3 license](http://www.gnu.org/licenses/agpl-3.0.html) unless a different license for a particular library is specified in the applicable library path.

Copyright © Kaltura Inc. All rights reserved.
