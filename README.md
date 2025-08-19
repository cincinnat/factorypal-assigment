FactoryPal home assigment
===

To create the environment run
```bash
docker compose up -d
```

Once all containers are running Spark Jobs UI will be available at
http://localhost:4040.

After one minute (the aggreagation window) the aggreaged data be data should
apprear in Clickhouse and can be request as follows.
```bash
docker run -it --rm --network=container:clickhouse \
    --entrypoint clickhouse-client clickhouse/clickhouse-server \
    --format=PrettySpace \
    -q 'select * from sensor_output limit 10'
```

The outbut should be similar to:
```
     sensorId       windowStart       windowEnd         averageValue

 1.  sensor-123   1755548940000   1755549000000   56.733000000000004
 2.  sensor-321   1755548940000   1755549000000            48.531875
 3.  sensor-456   1755548940000   1755549000000    53.80785714285715
 4.  sensor-789   1755548940000   1755549000000   56.110833333333325
 5.  sensor-123   1755549000000   1755549060000    53.72380952380953
 6.  sensor-321   1755549000000   1755549060000    73.95076923076923
 7.  sensor-456   1755549000000   1755549060000   45.329333333333345
 8.  sensor-789   1755549000000   1755549060000                54.25
 9.  sensor-123   1755549060000   1755549120000    54.60333333333333
10.  sensor-321   1755549060000   1755549120000    55.11222222222222
```
