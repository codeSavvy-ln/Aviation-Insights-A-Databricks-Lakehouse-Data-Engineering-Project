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
