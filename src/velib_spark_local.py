import os
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, to_timestamp, hour, dayofweek, avg

MONGO_URI = os.getenv("MONGO_URI_LOCAL", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "velib")


def get_spark(app: str = "velib-spark") -> SparkSession:
    return SparkSession.builder.master("local[*]").appName(app).getOrCreate()


def read_mongo_as_spark(spark: SparkSession, collection: str, limit: int = 0) -> DataFrame:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    cur = db[collection].find()
    if limit and limit > 0:
        cur = cur.limit(limit)
    docs = list(cur)
    if not docs:
        return spark.createDataFrame([], schema=None)

    for d in docs:
        if "_id" in d:
            d["_id"] = str(d["_id"])
        if isinstance(d.get("ts"), datetime):
            d["ts"] = d["ts"].isoformat()

    return spark.createDataFrame(pd.DataFrame(docs))


def write_spark_to_mongo(df: DataFrame, collection: str, mode: str = "overwrite") -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    if mode == "overwrite":
        db[collection].drop()
    rows = [r.asDict(recursive=True) for r in df.collect()]
    if rows:
        db[collection].insert_many(rows)


def build_kpi_station(availability: DataFrame) -> DataFrame:
    a = (availability
         .withColumn("capacity", col("num_bikes_available") + col("num_docks_available"))
         .withColumn("availability_rate",
                     when(col("capacity") > 0, col("num_bikes_available") / col("capacity")).otherwise(None))
         )
    return (a.groupBy("station_id")
            .agg(avg("availability_rate").alias("avg_availability_rate"),
                 avg("num_bikes_available").alias("avg_bikes"),
                 avg("num_docks_available").alias("avg_docks")))


def build_ml_dataset(availability: DataFrame) -> DataFrame:
    a = (availability
         .withColumn("ts_parsed", to_timestamp(col("timestamp")))
         .withColumn("hour", hour(col("ts_parsed")))
         .withColumn("dow", dayofweek(col("ts_parsed")))
         .select("station_id", "timestamp", "hour", "dow",
                 "num_bikes_available", "num_docks_available"))
    return a
