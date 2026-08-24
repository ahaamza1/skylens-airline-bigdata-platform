import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ.get('PATH', '') + ';C:\\hadoop\\bin'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. إنشاء Spark Session
spark = SparkSession.builder \
    .appName("FlightStreamingAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema
schema = StructType() \
    .add("flight_id", StringType()) \
    .add("airline", StringType()) \
    .add("origin", StringType()) \
    .add("destination", StringType()) \
    .add("status", StringType()) \
    .add("delay_minutes", IntegerType()) \
    .add("event_time", StringType())

# 3. قراءة البيانات
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight-events") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. فك الـ JSON وتحويل نوع الوقت (هنا الحل)
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("event_time"))) # السطر اللي بيحول النص لوقت

# 5. الدالة المعدلة
def write_to_postgres(batch_df, batch_id):
    print(f"--- Processing Batch {batch_id} ---")
    batch_df.show() # هنطبع الداتا عشان نتأكد
    
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/airline_db") \
        .option("dbtable", "live_flight_status") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 6. التشغيل
query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()