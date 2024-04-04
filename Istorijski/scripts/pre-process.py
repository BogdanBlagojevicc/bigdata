import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


SOURCE_DIR_PATH = "hdfs://namenode:9000/weather"
HIVE_METASTORE_URIS = "thrift://hive-metastore:9083"

conf = SparkConf().setAppName("pre-processing").setMaster("spark://spark-master:7077")

conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
weather = session.read \
    .option("delimiter", ",") \
    .csv("{}/WeatherEvents_Jan2016-Dec2022.csv".format(SOURCE_DIR_PATH), header=True, inferSchema=True) \
    
df = weather.select("EventId", "Type", "Severity", "StartTime(UTC)", "EndTime(UTC)", "Precipitation(in)", "TimeZone", "AirportCode", "LocationLat", "LocationLng", "City", "County", "State", "ZipCode")

## DIM_SEVERITY - RADI

window_spec_dim_severity = Window.orderBy("name")

dim_severity = weather.select("Severity").distinct() \
                      .withColumnRenamed("Severity", "name") \
                      .withColumn("id_severity", row_number().over(window_spec_dim_severity))
#dim_severity.show()

#upis u HIVE bazu
dim_severity.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("dim_severity")

## DIM_TIME_ZONE - RADI

window_spec_dim_time_zone = Window.orderBy("name")

dim_time_zone = weather.select("TimeZone").distinct() \
                      .withColumnRenamed("TimeZone", "name") \
                      .withColumn("id_tz", row_number().over(window_spec_dim_time_zone))
#dim_time_zone.show()

dim_time_zone.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("dim_time_zone")

## DIM_TYEAR - RADI

window_spec_dim_tyear = Window.orderBy("year")

dim_tyear = weather.select(year("StartTime(UTC)").alias("year")).distinct() \
                    .withColumnRenamed("StartTime(UTC)", 'year') \
                    .withColumn("id_tyear", row_number().over(window_spec_dim_tyear))
#dim_tyear.show()

dim_tyear.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("dim_tyear")


## DIM_TMONTH - RADI

# dim_tmonth = df.alias("dim_tmonth")
# dim_tmonth_with_year_month = dim_tmonth.withColumn("Year", substring(col("StartTime(UTC)"), 1, 4)) \
#                                       .withColumn("Month", substring(col("StartTime(UTC)"), 6, 2))

# dim_tmonth_with_year_month_renamed = dim_tmonth_with_year_month \
#     .withColumn("Year", when(col("Year") == "2016", lit(1))
#                           .when(col("Year") == "2017", lit(2))
#                           .when(col("Year") == "2018", lit(3))
#                           .when(col("Year") == "2019", lit(4))
#                           .when(col("Year") == "2020", lit(5))
#                           .when(col("Year") == "2021", lit(6))
#                           .when(col("Year") == "2022", lit(7))) \
#     .withColumn("Month", when(col("Month") == "01", lit("January"))
#                            .when(col("Month") == "02", lit("February"))
#                            .when(col("Month") == "03", lit("March"))
#                            .when(col("Month") == "04", lit("April"))
#                            .when(col("Month") == "05", lit("May"))
#                            .when(col("Month") == "06", lit("June"))
#                            .when(col("Month") == "07", lit("July"))
#                            .when(col("Month") == "08", lit("August"))
#                            .when(col("Month") == "09", lit("September"))
#                            .when(col("Month") == "10", lit("October"))
#                            .when(col("Month") == "11", lit("November"))
#                            .when(col("Month") == "12", lit("December")))

# window_spec_dim_tmonth_with_year_month_renamed = Window.orderBy("dim_tyear_id_tyear")

# dim_tmonth_final = dim_tmonth_with_year_month_renamed.select('Month', 'Year') \
#                     .withColumnRenamed("Month", 'month') \
#                     .withColumnRenamed("Year", 'dim_tyear_id_tyear') \
#                     .withColumn("id_tmonth", row_number().over(window_spec_dim_tmonth_with_year_month_renamed))

# dim_tmonth_final.show()

# dim_tmonth_final.write \
#     .mode(saveMode="overwrite") \
#     .saveAsTable("dim_tmonth")

## DIM_TYPE - RADI

window_spec_dim_type = Window.orderBy("name")

dim_type = weather.select("Type").distinct() \
                      .withColumnRenamed("Type", "name") \
                      .withColumn("id_type", row_number().over(window_spec_dim_type))
#dim_type.show()

dim_type.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("dim_type")

## FACT_WEATHER - RADI

window_spec_fact_weather = Window.orderBy("event_id")
fact_weather = weather.select("EventId", "Precipitation(in)", "AirportCode", "LocationLat", "LocationLng", "City", "County", "State", "ZipCode", "Type", "Severity", "StartTime(UTC)", "EndTime(UTC)", "TimeZone") \
                      .withColumnRenamed("EventId", "event_id") \
                      .withColumnRenamed("Precipitation(in)", "precipitation") \
                      .withColumnRenamed("AirportCode", "airport_code") \
                      .withColumnRenamed("LocationLat", "location_lat") \
                      .withColumnRenamed("LocationLng", "location_lng") \
                      .withColumnRenamed("City", "city") \
                      .withColumnRenamed("County", "county") \
                      .withColumnRenamed("State", "state") \
                      .withColumn("start_time", substring(col("StartTime(UTC)"), 6, 19)) \
                      .withColumn("end_time", substring(col("EndTime(UTC)"), 6, 19)) \
                      .withColumnRenamed("ZipCode", "zip_code") \
                      .withColumn("id_weather", row_number().over(window_spec_fact_weather)) \
                      .withColumn("Year", substring(col("StartTime(UTC)"), 1, 4)) \
                      .withColumn("dim_tyear_id_tyear", when(col("Year") == "2016", lit(1))
                            .when(col("Year") == "2017", lit(2))
                            .when(col("Year") == "2018", lit(3))
                            .when(col("Year") == "2019", lit(4))
                            .when(col("Year") == "2020", lit(5))
                            .when(col("Year") == "2021", lit(6))
                            .when(col("Year") == "2022", lit(7))) \
                      .withColumn("dim_type_id_type", when(col("Type") == "Cold", lit(1))
                            .when(col("Type") == "Fog", lit(2))
                            .when(col("Type") == "Hail", lit(3))
                            .when(col("Type") == "Precipitation", lit(4))
                            .when(col("Type") == "Rain", lit(5))
                            .when(col("Type") == "Snow", lit(6))
                            .when(col("Type") == "Storm", lit(7))) \
                      .withColumn("dim_time_zone_id_tz", when(col("TimeZone") == "US/Central", lit(1))
                            .when(col("TimeZone") == "US/Eastern", lit(2))
                            .when(col("TimeZone") == "US/Mountain", lit(3))
                            .when(col("TimeZone") == "US/Pacific", lit(4))
                            .when(col("TimeZone") == "Storm", lit(7))) \
                      .withColumn("dim_severity_id_severity", when(col("Severity") == "Heavy", lit(1))
                            .when(col("Severity") == "Light", lit(2))
                            .when(col("Severity") == "Moderate", lit(3))
                            .when(col("Severity") == "Other", lit(4))
                            .when(col("Severity") == "Severe", lit(5))
                            .when(col("Severity") == "UNK", lit(6)))
                            
fact_weather_final = fact_weather.select("event_id", "precipitation", "airport_code", "location_lat", "location_lng", "city", "county", "state", "zip_code", "start_time", "end_time", "id_weather", 
                                         "dim_tyear_id_tyear", "dim_type_id_type", "dim_time_zone_id_tz", "dim_severity_id_severity")
                      
# fact_weather_final.show()

fact_weather_final.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("fact_weather")