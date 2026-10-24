"""
Customer 360 pipeline in PySpark.

Steps (each one is a small function so it can be unit tested):
  1. load      - read raw CSVs into Spark DataFrames
  2. clean     - normalize emails and phone numbers
  3. resolve   - identity resolution: merge duplicate records of the same person
  4. analytics - per-customer spend, order count, recency, segment
  5. write     - save results as Parquet (the standard big-data file format)

Run:  python src/pipeline.py
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_spark(app_name="customer-360"):
    # local[*] = run on this laptop using all CPU cores. On AWS EMR/Glue the
    # exact same code runs on a cluster - only this line changes.
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small data -> few partitions
        .getOrCreate()
    )


# ---------- 1. LOAD ----------
def load(spark, customers_path, orders_path):
    customers = spark.read.csv(customers_path, header=True, inferSchema=True)
    orders = spark.read.csv(orders_path, header=True, inferSchema=True)
    return customers, orders


# ---------- 2. CLEAN ----------
def clean_customers(df: DataFrame) -> DataFrame:
    """'  AVA.Patel0@Example.com ' -> 'ava.patel0@example.com'
       '+1 (217) 555-0001'         -> '2175550001'"""
    phone_digits = F.regexp_replace(F.col("phone"), r"\D", "")  # strip non-digits
    return (
        df.withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn(
            "phone",
            # drop a leading US country code '1' if the number has 11 digits
            F.when(F.length(phone_digits) == 11, F.substring(phone_digits, 2, 10))
            .otherwise(phone_digits),
        )
        .withColumn("signup_date", F.to_date("signup_date"))
    )


# ---------- 3. IDENTITY RESOLUTION ----------
def resolve_identities(df: DataFrame) -> DataFrame:
    """Give every record a `customer_id`. Records with the same cleaned email
    are the same person. The id is the smallest record_id in the group.

    Returns a mapping table: record_id -> customer_id (+ customer profile)."""
    w = Window.partitionBy("email")
    return (
        df.withColumn("customer_id", F.min("record_id").over(w))
        .withColumn("first_seen", F.min("signup_date").over(w))
        .withColumn("sources", F.collect_set("source").over(w))
        .select("record_id", "customer_id", "first_name", "last_name",
                "email", "phone", "first_seen", "sources")
    )


# ---------- 4. ANALYTICS ----------
def customer_metrics(identities: DataFrame, orders: DataFrame) -> DataFrame:
    """One row per real customer with spend metrics and a simple segment."""
    profiles = identities.dropDuplicates(["customer_id"]).drop("record_id")

    order_stats = (
        orders.join(identities.select("record_id", "customer_id"), "record_id")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("amount"), 2).alias("total_spend"),
            F.max(F.to_date("order_date")).alias("last_order_date"),
        )
    )

    return (
        profiles.join(order_stats, "customer_id", "left")  # left: keep customers with 0 orders
        .fillna({"order_count": 0, "total_spend": 0.0})
        .withColumn(
            "segment",
            F.when(F.col("total_spend") >= 1000, "VIP")
            .when(F.col("order_count") >= 3, "Regular")
            .when(F.col("order_count") >= 1, "Occasional")
            .otherwise("Inactive"),
        )
    )


def segment_summary(metrics: DataFrame) -> DataFrame:
    return (
        metrics.groupBy("segment")
        .agg(F.count("*").alias("customers"),
             F.round(F.avg("total_spend"), 2).alias("avg_spend"))
        .orderBy(F.desc("avg_spend"))
    )


# ---------- 5. RUN EVERYTHING ----------
def run(customers_path="data/customers.csv", orders_path="data/orders.csv", out="output"):
    spark = get_spark()
    customers, orders = load(spark, customers_path, orders_path)

    identities = resolve_identities(clean_customers(customers))
    metrics = customer_metrics(identities, orders)

    metrics.write.mode("overwrite").parquet(f"{out}/customer_metrics")

    raw, unique = customers.count(), metrics.count()
    print(f"\nRaw records: {raw}  ->  Unique customers: {unique}  "
          f"({raw - unique} duplicates merged)\n")
    segment_summary(metrics).show()
    metrics.orderBy(F.desc("total_spend")).select(
        "customer_id", "email", "order_count", "total_spend", "segment", "sources"
    ).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    run()
