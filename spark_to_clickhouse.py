from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

# Define the schema for the IoT data
schema = StructType(
    [
        StructField("device_id", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
    ]
)

# Initialize Spark session with explicit master configuration
spark = (
    SparkSession.builder.appName("KafkaToClickHouse").master("local[*]").getOrCreate()
)  # Or your cluster manager URL (Here we dont since we are running locally with docker...)

# Read data from Kafka with error handling and startingOffsets
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "iot_data")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON with enhanced error handling
json_df = df.selectExpr("CAST(value AS STRING)")
iot_df = (
    json_df.withColumn("data", from_json(col("value"), schema))
    .filter(col("data").isNotNull())
    .select("data.*")
)


# Write to ClickHouse with secure connection and batch size
def foreach_batch_function(df, epoch_id):
    df.write.format("jdbc").option(
        "url", "jdbc:clickhouse://localhost:8123/default"
    ).option("driver", "ru.yandex.clickhouse.ClickHouseDriver").option(
        "user", "default"
    ).option(
        "password", "password"
    ).option(
        "dbtable", "iot_data"
    ).option(
        "batchsize", "100"
    ).mode(
        "append"
    ).save()


iot_df.writeStream.foreachBatch(foreach_batch_function).trigger(
    processingTime="10 seconds"
).start().awaitTermination()


# Run the Spark job 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars clickhouse-jdbc-0.3.1.jar spark_to_clickhouse.py