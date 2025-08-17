#!/bin/bash

clickhouse-client --queries-file <(
    cat <<EOL
CREATE TABLE sensor_output
(
    sensorId String,
    windowStart UInt64,
    windowEnd UInt64,
    averageValue Float64
)
ENGINE = MergeTree ORDER BY (windowStart, sensorId);

CREATE TABLE sensor_output_queue
(
    sensorId String,
    windowStart UInt64,
    windowEnd UInt64,
    averageValue Float64
)
   ENGINE = Kafka()
    SETTINGS kafka_thread_per_consumer = 0,
     kafka_num_consumers = 1,
     kafka_broker_list = 'broker:29092',
     kafka_topic_list = 'sensor-output',
     kafka_group_name = 'clickhouse',
     kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW sensor_output_mv TO sensor_output AS
SELECT *
FROM sensor_output_queue;
EOL
)
