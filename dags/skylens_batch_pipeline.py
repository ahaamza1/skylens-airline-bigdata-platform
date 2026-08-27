"""
Skylens Batch Pipeline
----------------------
Orchestrates the batch flow for the Skylens project:
1. Check that raw flight data exists in HDFS
2. Run a Spark job that reads raw data and writes a summary to processed
3. Clean raw CSV data and write valid / rejected Parquet splits
4. Add derived columns and write enriched Parquet (partitioned by Year, Month)
5. Compute five aggregated analytics tables from the enriched dataset
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "skylens",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def check_raw_data():
    from hdfs import InsecureClient

    client = InsecureClient("http://namenode:9870", user="root")
    files = client.list("/skylens/raw/flights")
    print(f"Found {len(files)} files in /skylens/raw/flights")
    if len(files) == 0:
        raise ValueError("No files found in /skylens/raw/flights - stopping pipeline")


def spark_row_count():
    from pyspark.sql import SparkSession
    from hdfs import InsecureClient

    spark = (
        SparkSession.builder
        .appName("SkylensRowCountCheck")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    df = spark.read.option("header", "true").csv(
        "hdfs://namenode:9000/skylens/raw/flights"
    )

    # Lightweight check: read just 1 row instead of counting all 20M+ rows.
    # Confirms Spark can connect to HDFS and parse the CSVs without the
    # cost of a full scan.
    sample = df.limit(1).collect()
    check_passed = len(sample) > 0
    print(f"Sample row read successfully: {check_passed}")
    if not check_passed:
        raise ValueError("No rows could be read from /skylens/raw/flights")

    spark.stop()

    # Write the summary directly via the hdfs client, avoiding
    # spark.createDataFrame (hits a cloudpickle/Python 3.11 incompatibility)
    client = InsecureClient("http://namenode:9870", user="root")
    summary_content = "dataset,check_status\nskylens_raw_flights,connection_ok\n"
    client.write(
        "/skylens/processed/flights/row_count_check/summary.csv",
        data=summary_content,
        overwrite=True,
        encoding="utf-8",
    )
    print("Summary written to /skylens/processed/flights/row_count_check/summary.csv")

def clean_flight_data():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    spark = (
        SparkSession.builder
        .appName("SkylensCleanFlightData")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    # Explicit schema for the core columns we need
    # (avoids slow inferSchema=true on 8.7GB and guarantees correct types)
    schema_fields = [
        ("FlightDate", StringType()),
        ("Reporting_Airline", StringType()),
        ("Tail_Number", StringType()),
        ("Flight_Number_Reporting_Airline", StringType()),
        ("Origin", StringType()),
        ("OriginCityName", StringType()),
        ("OriginState", StringType()),
        ("Dest", StringType()),
        ("DestCityName", StringType()),
        ("DestState", StringType()),
        ("CRSDepTime", StringType()),
        ("DepTime", StringType()),
        ("DepDelay", DoubleType()),
        ("DepDelayMinutes", DoubleType()),
        ("CRSArrTime", StringType()),
        ("ArrTime", StringType()),
        ("ArrDelay", DoubleType()),
        ("ArrDelayMinutes", DoubleType()),
        ("CRSElapsedTime", DoubleType()),
        ("ActualElapsedTime", DoubleType()),
        ("AirTime", DoubleType()),
        ("Distance", DoubleType()),
        ("Cancelled", DoubleType()),
        ("CancellationCode", StringType()),
        ("Diverted", DoubleType()),
        ("CarrierDelay", DoubleType()),
        ("WeatherDelay", DoubleType()),
        ("NASDelay", DoubleType()),
        ("SecurityDelay", DoubleType()),
        ("LateAircraftDelay", DoubleType()),
    ]
    wanted_columns = [name for name, _ in schema_fields]
    numeric_columns = [name for name, dtype in schema_fields if isinstance(dtype, DoubleType)]

    # Read raw CSVs (header row already present in every file)
    df_raw = (
        spark.read
        .option("header", "true")
        .csv("hdfs://namenode:9000/skylens/raw/flights")
    )

    # Some yearly files may have slightly different extra columns -
    # only select the ones we actually defined and that exist in the data
    available_columns = [c for c in wanted_columns if c in df_raw.columns]
    df = df_raw.select(*available_columns)

    # Cast numeric columns explicitly (they arrive as strings from CSV)
    for col_name in numeric_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    total_raw = df.count()

    # --- Validity check: separate clean rows from rejected rows ---
    valid_condition = (
        F.col("Origin").isNotNull() & (F.col("Origin") != "") &
        F.col("Dest").isNotNull() & (F.col("Dest") != "") &
        F.col("FlightDate").isNotNull() &
        (F.col("Distance").isNull() | (F.col("Distance") > 0))
    )

    df_valid = df.filter(valid_condition)
    df_rejected = df.filter(~valid_condition)

    # --- Deduplicate valid rows on a logical flight key ---
    dedup_keys = [
        c for c in [
            "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
            "Origin", "Dest", "CRSDepTime",
        ]
        if c in df_valid.columns
    ]
    df_valid = df_valid.dropDuplicates(dedup_keys)

    total_valid = df_valid.count()
    total_rejected = df_rejected.count()

    print(f"Raw rows read: {total_raw}")
    print(f"Valid rows after cleaning: {total_valid}")
    print(f"Rejected rows: {total_rejected}")

    # --- Write outputs as Parquet (efficient columnar format) ---
    df_valid.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/skylens/processed/flights/cleaned"
    )
    df_rejected.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/skylens/rejected/flights"
    )

    spark.stop()
    print("Cleaning complete: valid -> /skylens/processed/flights/cleaned, "
          "rejected -> /skylens/rejected/flights")

def add_derived_columns():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder
        .appName("SkylensAddDerivedColumns")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    df = spark.read.parquet("hdfs://namenode:9000/skylens/processed/flights/cleaned")

    # --- Parse FlightDate into a real DateType ---
    df = df.withColumn("FlightDateParsed", F.to_date(F.col("FlightDate"), "yyyy-MM-dd"))

    # --- Time-based derived columns ---
    df = df.withColumn("Year", F.year("FlightDateParsed"))
    df = df.withColumn("Month", F.month("FlightDateParsed"))
    df = df.withColumn("DayOfWeek", F.dayofweek("FlightDateParsed"))  # 1=Sunday ... 7=Saturday
    df = df.withColumn(
        "IsWeekend",
        F.when(F.col("DayOfWeek").isin(1, 7), F.lit(True)).otherwise(F.lit(False))
    )
    df = df.withColumn(
        "Season",
        F.when(F.col("Month").isin(12, 1, 2), "Winter")
         .when(F.col("Month").isin(3, 4, 5), "Spring")
         .when(F.col("Month").isin(6, 7, 8), "Summer")
         .otherwise("Fall")
    )

    # --- Delay-based derived columns ---
    df = df.withColumn(
        "IsDelayed",
        F.when(F.col("ArrDelayMinutes") >= 15, F.lit(True)).otherwise(F.lit(False))
    )
    df = df.withColumn(
        "DelayCategory",
        F.when(F.col("Cancelled") == 1, "Cancelled")
         .when(F.col("ArrDelayMinutes").isNull(), "Unknown")
         .when(F.col("ArrDelayMinutes") < 15, "OnTime")
         .when(F.col("ArrDelayMinutes") < 60, "Minor")
         .when(F.col("ArrDelayMinutes") < 180, "Major")
         .otherwise("Severe")
    )

    # --- Route identifier ---
    df = df.withColumn("RouteID", F.concat_ws("-", F.col("Origin"), F.col("Dest")))

    # --- Primary delay reason: the delay-cause column holding the highest value ---
    delay_reason_cols = [
        "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"
    ]
    df = df.withColumn(
        "PrimaryDelayReason",
        F.when(
            F.greatest(*[F.coalesce(F.col(c), F.lit(0.0)) for c in delay_reason_cols]) == 0,
            F.lit("None")
        ).otherwise(
            F.array(
                *[F.when(F.coalesce(F.col(c), F.lit(0.0)) ==
                         F.greatest(*[F.coalesce(F.col(cc), F.lit(0.0)) for cc in delay_reason_cols]),
                         F.lit(c.replace("Delay", "")))
                  for c in delay_reason_cols]
            ).getItem(0)
        )
    )

    total_rows = df.count()
    print(f"Rows after adding derived columns: {total_rows}")

    df.write.mode("overwrite").partitionBy("Year", "Month").parquet(
        "hdfs://namenode:9000/skylens/processed/flights/enriched"
    )

    spark.stop()
    print("Derived columns added, output written to /skylens/processed/flights/enriched (partitioned by Year, Month)")


def generate_aggregated_tables():
    """
    Reads the enriched Parquet dataset from HDFS and computes five aggregated
    datasets, persisting each back to HDFS in Parquet format:

      1. airline_performance   - per-airline flight & delay/cancellation KPIs
      2. route_performance     - per-route distance and delay averages
      3. airport_performance   - unified departure + arrival stats per airport
      4. monthly_trends        - monthly / seasonal volume and delay trends
      5. delay_reasons         - incident counts and total minutes per delay type

    Designed to run as an Airflow PythonOperator after add_derived_columns.
    No Python UDFs or collect() are used; all logic is expressed via
    standard pyspark.sql.functions to maximize Catalyst optimisation.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    # ------------------------------------------------------------------
    # 1. Spark session
    # ------------------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("SkylensAggregations")
        .master("spark://spark-master:7077")
        # 200 shuffle partitions is a sensible default for ~20 M rows;
        # increase if you observe spill warnings in the Spark UI.
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    INPUT_PATH  = "hdfs://namenode:9000/skylens/processed/flights/enriched"
    OUTPUT_BASE = "hdfs://namenode:9000/skylens/processed/aggregations"

    # ------------------------------------------------------------------
    # 2. Read enriched dataset once — reused across all five aggregations.
    #    Spark will push partition pruning down through the Parquet reader.
    # ------------------------------------------------------------------
    df = spark.read.parquet(INPUT_PATH)

    # ------------------------------------------------------------------
    # Helper: safe percentage expressed purely in Catalyst (no UDF)
    # ------------------------------------------------------------------
    def _pct(numerator_col, denominator_col):
        """(numerator / denominator) * 100, rounded to 2 dp, safe vs 0."""
        return F.round(
            F.when(denominator_col > 0, (numerator_col / denominator_col) * 100)
             .otherwise(F.lit(0.0)),
            2
        )

    # ==================================================================
    # TABLE 1 — airline_performance
    # Grain: one row per Reporting_Airline
    # Columns: total_flights, delayed_flights, cancelled_flights,
    #          avg_arr_delay_min, avg_dep_delay_min,
    #          delay_rate_pct, cancellation_rate_pct
    # ==================================================================
    airline_perf = (
        df.groupBy("Reporting_Airline")
          .agg(
              F.count("*")
               .alias("total_flights"),
              F.sum(F.when(F.col("IsDelayed") == True, 1).otherwise(0))
               .alias("delayed_flights"),
              F.sum(F.when(F.col("Cancelled") == 1.0, 1).otherwise(0))
               .alias("cancelled_flights"),
              F.round(F.avg("ArrDelayMinutes"), 2)
               .alias("avg_arr_delay_min"),
              F.round(F.avg("DepDelayMinutes"), 2)
               .alias("avg_dep_delay_min"),
          )
    )
    airline_perf = (
        airline_perf
        .withColumn(
            "delay_rate_pct",
            _pct(F.col("delayed_flights"), F.col("total_flights"))
        )
        .withColumn(
            "cancellation_rate_pct",
            _pct(F.col("cancelled_flights"), F.col("total_flights"))
        )
    )
    airline_perf.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/airline_performance")
    print("[1/5] airline_performance written.")

    # ==================================================================
    # TABLE 2 — route_performance
    # Grain: one row per (RouteID, Origin, OriginCityName, Dest, DestCityName)
    # Columns: total_flights, delayed_flights,
    #          avg_arr_delay_min, avg_dep_delay_min, avg_distance_miles
    # ==================================================================
    route_perf = (
        df.groupBy("RouteID", "Origin", "OriginCityName", "Dest", "DestCityName")
          .agg(
              F.count("*")
               .alias("total_flights"),
              F.sum(F.when(F.col("IsDelayed") == True, 1).otherwise(0))
               .alias("delayed_flights"),
              F.round(F.avg("ArrDelayMinutes"), 2)
               .alias("avg_arr_delay_min"),
              F.round(F.avg("DepDelayMinutes"), 2)
               .alias("avg_dep_delay_min"),
              F.round(F.avg("Distance"), 2)
               .alias("avg_distance_miles"),
          )
    )
    route_perf.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/route_performance")
    print("[2/5] route_performance written.")

    # ==================================================================
    # TABLE 3 — airport_performance
    # Strategy: compute departure stats (keyed on Origin) and arrival stats
    # (keyed on Dest) separately, then FULL OUTER JOIN on AirportCode so
    # that airports appearing only as origin or only as destination are
    # still represented.
    # Columns: AirportCode, dep_total_flights, dep_delayed_flights,
    #          dep_cancelled_flights, avg_dep_delay_min,
    #          arr_total_flights, arr_delayed_flights, avg_arr_delay_min,
    #          total_flights, total_delayed_flights, overall_delay_rate_pct
    # ==================================================================
    dep_stats = (
        df.groupBy(F.col("Origin").alias("AirportCode"))
          .agg(
              F.count("*")
               .alias("dep_total_flights"),
              F.sum(F.when(F.col("IsDelayed") == True, 1).otherwise(0))
               .alias("dep_delayed_flights"),
              F.sum(F.when(F.col("Cancelled") == 1.0, 1).otherwise(0))
               .alias("dep_cancelled_flights"),
              F.round(F.avg("DepDelayMinutes"), 2)
               .alias("avg_dep_delay_min"),
          )
    )

    arr_stats = (
        df.groupBy(F.col("Dest").alias("AirportCode"))
          .agg(
              F.count("*")
               .alias("arr_total_flights"),
              F.sum(F.when(F.col("IsDelayed") == True, 1).otherwise(0))
               .alias("arr_delayed_flights"),
              F.round(F.avg("ArrDelayMinutes"), 2)
               .alias("avg_arr_delay_min"),
          )
    )

    airport_perf = (
        dep_stats.join(arr_stats, on="AirportCode", how="full")
                 .withColumn(
                     "total_flights",
                     F.coalesce(F.col("dep_total_flights"), F.lit(0))
                     + F.coalesce(F.col("arr_total_flights"), F.lit(0))
                 )
                 .withColumn(
                     "total_delayed_flights",
                     F.coalesce(F.col("dep_delayed_flights"), F.lit(0))
                     + F.coalesce(F.col("arr_delayed_flights"), F.lit(0))
                 )
                 .withColumn(
                     "overall_delay_rate_pct",
                     _pct(
                         F.coalesce(F.col("dep_delayed_flights"), F.lit(0))
                         + F.coalesce(F.col("arr_delayed_flights"), F.lit(0)),
                         F.coalesce(F.col("dep_total_flights"),    F.lit(0))
                         + F.coalesce(F.col("arr_total_flights"),  F.lit(0))
                     )
                 )
    )
    airport_perf.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/airport_performance")
    print("[3/5] airport_performance written.")

    # ==================================================================
    # TABLE 4 — monthly_trends
    # Grain: one row per (Year, Month, Season)
    # Columns: total_flights, total_delayed, total_cancelled,
    #          avg_arr_delay_min, avg_dep_delay_min, delay_rate_pct
    # ==================================================================
    monthly_trends = (
        df.groupBy("Year", "Month", "Season")
          .agg(
              F.count("*")
               .alias("total_flights"),
              F.sum(F.when(F.col("IsDelayed") == True, 1).otherwise(0))
               .alias("total_delayed"),
              F.sum(F.when(F.col("Cancelled") == 1.0, 1).otherwise(0))
               .alias("total_cancelled"),
              F.round(F.avg("ArrDelayMinutes"), 2)
               .alias("avg_arr_delay_min"),
              F.round(F.avg("DepDelayMinutes"), 2)
               .alias("avg_dep_delay_min"),
          )
    )
    monthly_trends = monthly_trends.withColumn(
        "delay_rate_pct",
        _pct(F.col("total_delayed"), F.col("total_flights"))
    )
    monthly_trends.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/monthly_trends")
    print("[4/5] monthly_trends written.")

    # ==================================================================
    # TABLE 5 — delay_reasons
    # Filter: only rows where PrimaryDelayReason != 'None'
    # Grain: one row per (PrimaryDelayReason, Year)
    # Columns: incident_count, total_delay_minutes
    #
    # total_delay_minutes maps each reason label back to its source column
    # using a CASE expression inside sum() — zero UDFs, zero collect().
    # ==================================================================
    delay_reasons = (
        df.filter(F.col("PrimaryDelayReason") != "None")
          .groupBy("PrimaryDelayReason", "Year")
          .agg(
              F.count("*")
               .alias("incident_count"),
              F.round(
                  F.sum(
                      F.coalesce(
                          F.when(F.col("PrimaryDelayReason") == "Carrier",
                                 F.col("CarrierDelay"))
                           .when(F.col("PrimaryDelayReason") == "Weather",
                                 F.col("WeatherDelay"))
                           .when(F.col("PrimaryDelayReason") == "NAS",
                                 F.col("NASDelay"))
                           .when(F.col("PrimaryDelayReason") == "Security",
                                 F.col("SecurityDelay"))
                           .when(F.col("PrimaryDelayReason") == "LateAircraft",
                                 F.col("LateAircraftDelay")),
                          F.lit(0.0)
                      )
                  ), 2
              )
              .alias("total_delay_minutes"),
          )
          .orderBy("Year", F.col("incident_count").desc())
    )
    delay_reasons.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/delay_reasons")
    print("[5/5] delay_reasons written.")

    # ------------------------------------------------------------------
    # Release Spark resources
    # ------------------------------------------------------------------
    spark.stop()
    print(
        "generate_aggregated_tables() complete. "
        "All 5 tables written to hdfs://namenode:9000/skylens/processed/aggregations/"
    )

def load_to_postgres():
    from pyspark.sql import SparkSession

    JDBC_JAR_PATH = "/opt/spark-apps/postgresql-42.7.3.jar"
    JDBC_URL = "jdbc:postgresql://skylens-dw:5432/skylens_dw"
    JDBC_PROPERTIES = {
        "user": "skylens",
        "password": "skylens_pass",
        "driver": "org.postgresql.Driver",
    }

    spark = (
        SparkSession.builder
        .appName("SkylensLoadToPostgres")
        .master("spark://spark-master:7077")
        .config("spark.jars", JDBC_JAR_PATH)
        .config("spark.driver.extraClassPath", JDBC_JAR_PATH)
        .config("spark.executor.extraClassPath", JDBC_JAR_PATH)
        .getOrCreate()
    )

    INPUT_BASE = "hdfs://namenode:9000/skylens/processed/aggregations"

    # Maps HDFS folder name -> destination table name in PostgreSQL
    tables = {
        "airline_performance": "agg_airline_performance",
        "route_performance": "agg_route_performance",
        "airport_performance": "agg_airport_performance",
        "monthly_trends": "agg_monthly_trends",
        "delay_reasons": "agg_delay_reasons",
    }

    for i, (hdfs_folder, table_name) in enumerate(tables.items(), start=1):
        df = spark.read.parquet(f"{INPUT_BASE}/{hdfs_folder}")
        row_count = df.count()

        (
            df.write
            .mode("overwrite")
            .jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPERTIES)
        )

        print(f"[{i}/5] {table_name} loaded to PostgreSQL ({row_count} rows).")

    spark.stop()
    print("load_to_postgres() complete. All 5 tables loaded into skylens_dw.")


with DAG(
    dag_id="skylens_batch_pipeline",
    description="Skylens batch pipeline: HDFS -> Spark -> PostgreSQL",
    default_args=default_args,
    schedule=None,  # manual trigger for now
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["skylens", "batch"],
) as dag:

    t1 = PythonOperator(
        task_id="check_raw_data",
        python_callable=check_raw_data,
    )

    t2 = PythonOperator(
        task_id="spark_row_count",
        python_callable=spark_row_count,
    )

    t3 = PythonOperator(
        task_id="clean_flight_data",
        python_callable=clean_flight_data,
    )

    t4 = PythonOperator(
        task_id="add_derived_columns",
        python_callable=add_derived_columns,
    )

    t5 = PythonOperator(
        task_id="generate_aggregated_tables",
        python_callable=generate_aggregated_tables,
    )

    t6 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
