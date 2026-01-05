# Aviation-Insights-A-Databricks-Lakehouse-Data-Engineering-Project

query = (
    stream_df.writeStream
        .outputMode("append")
        .option(
        "checkpointLocation",
        "abfss://bronze@revtraining.dfs.core.windows.net/Tables/streaming/Checkpointing"
    )
        .trigger(availableNow=True)
        .toTable("aviation_project.bronze.flights_stream")
)



----------------


stream_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("header", "true")
         .option("cloudFiles.schemaLocation",
            "abfss://bronze@revtraining.dfs.core.windows.net/schema/flights_stream")
         .schema(schema)
         .load("abfss://bronze@revtraining.dfs.core.windows.net/dataset/Streaming/")
)



############################################

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType
)

schema = StructType([
    StructField("YEAR", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY", IntegerType(), True),
    StructField("DAY_OF_WEEK", IntegerType(), True),
    StructField("AIRLINE", StringType(), True),
    StructField("FLIGHT_NUMBER", IntegerType(), True),
    StructField("TAIL_NUMBER", StringType(), True),
    StructField("ORIGIN_AIRPORT", StringType(), True),
    StructField("DESTINATION_AIRPORT", StringType(), True),
    StructField("SCHEDULED_DEPARTURE", IntegerType(), True),
    StructField("DEPARTURE_TIME", IntegerType(), True),
    StructField("DEPARTURE_DELAY", IntegerType(), True),
    StructField("TAXI_OUT", IntegerType(), True),
    StructField("WHEELS_OFF", IntegerType(), True),
    StructField("SCHEDULED_TIME", IntegerType(), True),
    StructField("ELAPSED_TIME", IntegerType(), True),
    StructField("AIR_TIME", IntegerType(), True),
    StructField("DISTANCE", IntegerType(), True),
    StructField("WHEELS_ON", IntegerType(), True),
    StructField("TAXI_IN", IntegerType(), True),
    StructField("SCHEDULED_ARRIVAL", IntegerType(), True),
    StructField("ARRIVAL_TIME", IntegerType(), True),
    StructField("ARRIVAL_DELAY", IntegerType(), True),
    StructField("DIVERTED", IntegerType(), True),
    StructField("CANCELLED", IntegerType(), True),
    StructField("CANCELLATION_REASON", StringType(), True),
    StructField("AIR_SYSTEM_DELAY", IntegerType(), True),
    StructField("SECURITY_DELAY", IntegerType(), True),
    StructField("AIRLINE_DELAY", IntegerType(), True),
    StructField("LATE_AIRCRAFT_DELAY", IntegerType(), True),
    StructField("WEATHER_DELAY", IntegerType(), True)
])
