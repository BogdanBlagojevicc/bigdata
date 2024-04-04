from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import os

spark = SparkSession \
    .builder \
    .appName("Concumer") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .getOrCreate()

def load_df(topic):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker1:19092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value as string) as value")
    return df.withColumn("value", from_json(df["value"], schema))\
        .select("value.*") \
        .withColumn("timestamp", current_timestamp())

def space():
    print()
    print()
    print()
    print()
    print()
    print()
    print()
    print()
    print()
    print()

schema = StructType([
    StructField("wind_mph", FloatType(), True),
    StructField("wind_kph", FloatType(), True),
    StructField("wind_degree", IntegerType(), True),
    StructField("wind_dir", StringType(), True),
    StructField("pressure_mb", FloatType(), True),
    StructField("pressure_in", FloatType(), True),
    StructField("precip_mm", FloatType(), True),
    StructField("precip_in", FloatType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("cloud", IntegerType(), True),
    StructField("feelslike_c", FloatType(), True),
    StructField("feelslike_f", FloatType(), True),
    StructField("vis_km", FloatType(), True),
    StructField("vis_miles", FloatType(), True),
    StructField("uv", FloatType(), True),
    StructField("gust_mph", FloatType(), True),
    StructField("gust_kph", FloatType(), True),
    StructField("temp_c", FloatType(), True),
    StructField("temp_f", FloatType(), True),
    StructField("is_day", IntegerType(), True),
])

serbia_df = load_df("serbia_info")
usa_df = load_df("usa_info")
japan_df = load_df("japan_info")

streaming_df = serbia_df\
    .union(usa_df)\
    .union(japan_df)

# Upit1 - Prikazati najveci uv index gde je pblacnost bar 50%

max_uv_cloud_gt_50_df = streaming_df.filter(col("uv").isNotNull() & col("cloud").isNotNull() & (col("cloud") >= 50)) \
                          .groupBy(window("timestamp", "10 seconds")) \
                          .agg(max("uv").alias("max_uv"))
def write_max_uv_with_message(row):
    max_uv = row["max_uv"]
    message = f"Max UV when cloud >= 50 from is: {max_uv}"
    space()
    print(message)
    space()

query_max_uv_cloud_gt_50 = max_uv_cloud_gt_50_df \
    .writeStream \
    .foreach(write_max_uv_with_message) \
    .outputMode("complete") \
    .start()

query_max_uv_cloud_gt_50.awaitTermination()

# Upit2 - Prikazazati prosecnu temperaturu izrazenu u celzijusima
# def write_with_message(row):
#     avg_temp_c = row["avg_temp_c"]
#     message = f"Average temperature is: {avg_temp_c} °C"
#     space()
#     print(message)
#     space()

# avg_temp_c_df = streaming_df.filter(col("temp_c").isNotNull()) \
#                   .groupBy(window("timestamp", "10 seconds")) \
#                   .agg(round(avg("temp_c"), 2).alias("avg_temp_c"))

# query = avg_temp_c_df \
#     .writeStream \
#     .foreach(write_with_message) \
#     .outputMode("complete") \
#     .start()

# query.awaitTermination()

# Upit3 - Prikazati najvecu vidljivost izrazenu u kilometrima
# def write_max_vis_km_with_message(row):
#     max_vis_km = row["max_vis_km"]
#     message = f"Max visibility is: {max_vis_km} km"
#     space()
#     print(message)
#     space()

# max_vis_km_df = streaming_df.filter(col("vis_km").isNotNull()) \
#                   .groupBy(window("timestamp", "10 seconds")) \
#                   .agg(max("vis_km").alias("max_vis_km"))

# query_max_vis_km = max_vis_km_df \
#     .writeStream \
#     .foreach(write_max_vis_km_with_message) \
#     .outputMode("complete") \
#     .start()

# query_max_vis_km.awaitTermination()

# Upit4 - Prikazati majvecu razliku izmedju stvarne temperature i subjektivnog osecaja temperature

# diff_df = streaming_df.filter(col("feelslike_c").isNotNull() & col("temp_c").isNotNull()) \
#             .withColumn("diff", abs(col("feelslike_c") - col("temp_c")))

# max_diff_df = diff_df.groupBy(window("timestamp", "10 seconds")) \
#                      .agg(max("diff").alias("max_diff"))

# def write_max_diff_with_message(row):
#     max_diff = row["max_diff"]
#     message = f"Maximum difference between feelslike_c and temp_c from is: {max_diff} °C"
#     space()
#     print(message)
#     space()

# query_max_diff = max_diff_df \
#     .writeStream \
#     .foreach(write_max_diff_with_message) \
#     .outputMode("complete") \
#     .start()

# query_max_diff.awaitTermination()

# Upit5 - Prikazati procesni pritisak izracen u incima gde je vlaznost manja ili jedanaka od 70

# avg_pressure_in_humidity_le_70_df = streaming_df.filter(col("pressure_in").isNotNull() & col("humidity").isNotNull() & (col("humidity") <= 70)) \
#                                       .groupBy(window("timestamp", "10 seconds")) \
#                                       .agg(round(avg("pressure_in"), 2).alias("avg_pressure_in"))

# def write_avg_pressure_in_with_message(row):
#     avg_pressure_in = row["avg_pressure_in"]
#     message = f"Average pressure_in when humidity <= 70 is: {avg_pressure_in} inches"
#     space()
#     print(message)
#     space()

# query_avg_pressure_in_humidity_le_70 = avg_pressure_in_humidity_le_70_df \
#     .writeStream \
#     .foreach(write_avg_pressure_in_with_message) \
#     .outputMode("complete") \
#     .start()

# query_avg_pressure_in_humidity_le_70.awaitTermination()
