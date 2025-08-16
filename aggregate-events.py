from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as fn


bootstrap_servers = 'broker:29092'
checkpoint_location = '/tmp'
input_topic = 'sensor-input'
outpu_topic = 'sensor-output'


spark = (SparkSession
    .builder
    .appName("aggregate-events")
    .config('spark.sql.shuffle.partitions', 10)
    .getOrCreate()
)


schema = (StructType() 
    .add("timestamp", "long")
    .add("sensorId", "string")
    .add("value", "double")
)

df = (spark
    .readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', bootstrap_servers)
    .option('subscribe', input_topic)
    .option('startingOffsets', 'earliest')
    .option('maxOffsetsPerTrigger', 1000)
    .load()

    .selectExpr('cast(value as string) as value')
    .select(fn.from_json(fn.col('value'), schema).alias('value'))
    .select('value.*')

    .withColumn('ts', fn.timestamp_millis(fn.col('timestamp')))

    .withWatermark('ts', '2 minutes')
    .groupBy(fn.window(fn.col('ts'), '1 minute'), fn.col('sensorId'))
    .agg(fn.avg('value').alias('averageValue'))

    .selectExpr(
        'sensorId',
        'unix_millis(window.start) as windowStart',
        'unix_millis(window.end) as windowEnd',
        'averageValue',
    )
    .selectExpr('to_json(struct(*)) as value')

    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers', bootstrap_servers)
    .option('topic', outpu_topic)
    .option('checkpointLocation', checkpoint_location)
    .outputMode('append')
    .trigger(processingTime='5 seconds')
    .start()

    .awaitTermination()
)
