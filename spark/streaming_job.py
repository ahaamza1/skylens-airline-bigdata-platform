from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. إنشاء Spark Session مع ربط Kafka Connector
spark = SparkSession.builder \
    .appName("FlightStreamingAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. تحديد Schema للـ JSON اللي جاي من البروديوسر
schema = StructType() \
    .add("flight_id", StringType()) \
    .add("airline", StringType()) \
    .add("origin", StringType()) \
    .add("destination", StringType()) \
    .add("status", StringType()) \
    .add("delay_minutes", IntegerType()) \
    .add("event_time", StringType())

# 3. قراءة البيانات كـ Stream من Kafka Topic (flight-events)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight-events") \
    .option("startingOffsets", "latest") \
    .load()

# 4. تحويل الـ Binary Value إلى String ثم فك الـ JSON باستخدام الـ Schema
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. عرض البيانات المعالجة مباشرة على الشاشة (Console)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()