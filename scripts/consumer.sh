#/bin/bash

docker exec broker /usr/bin/kafka-console-consumer \
    --bootstrap-server broker:29092 "$@"
