from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable
from pyspark.sql import Window


def create_spark_session():
    """Create and configure Spark session with Delta Lake support"""
    return (
        SparkSession.builder.appName("ClientsETL")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.2.0",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000")
        .config("spark.hadoop.fs.s3a.access.key", "MinioAdmin123")
        .config("spark.hadoop.fs.s3a.secret.key", "MinioAdmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.debug.maxToStringFields", "1000000")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def read_landing_data(spark, source):
    """Read data from landing zone based on source"""
    if source == "cloud_x":
        return spark.read.parquet(f"s3a://landing-zone/dataway/{source}/clients")
    elif source == "protheus":
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ",")
            .csv(f"s3a://landing-zone/dataway/{source}/clients")
        )
    elif source == "sap":
        return spark.read.option("inferSchema", "true").json(
            f"s3a://landing-zone/dataway/{source}/clients"
        )
    else:
        raise ValueError(f"Unknown source: {source}")


def process_bronze_layer(df, source):
    """Process data for bronze layer"""
    processed_df = df.withColumn("system", F.lit(source)).withColumn(
        "processed_at", F.current_timestamp()
    )
    print(f"[{source}] Landing zone count: {df.count()}")
    print(f"[{source}] Bronze layer count: {processed_df.count()}")
    return processed_df


def save_bronze_layer(df, source):
    """Save data to bronze layer"""
    (df.write.mode("overwrite").parquet(f"s3a://bronze-zone/dataway/{source}/clients"))


def upsert_silver_layer(spark, df_source):
    """Upsert data to silver layer Delta table"""
    print(f"[{df_source.select('system').first()[0]}] Pre-deduplicated count: {df_source.count()}")
    
    delta_path = "s3a://silver-zone/dataway/clients"

    # Deduplicate source data by keeping the latest record per email
    df_source = (
        df_source
        .withColumn("row_number", F.row_number().over(
            Window.partitionBy("email").orderBy(F.col("processed_at").desc())
        ))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )
    
    print(f"[{df_source.select('system').first()[0]}] Post-deduplicated count: {df_source.count()}")

    # Create Delta table if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, delta_path):
        (df_source.write.format("delta").mode("overwrite").save(delta_path))
        return

    # Perform upsert if table exists
    delta_table = DeltaTable.forPath(spark, delta_path)
    (
        delta_table.alias("target")
        .merge(df_source.alias("source"), "target.email = source.email")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def create_gold_report(spark):
    """Create report in gold layer"""
    delta_path = "s3a://silver-zone/dataway/clients"
    clients_df = spark.read.format("delta").load(delta_path)
    print(f"Total records in Silver layer: {clients_df.count()}")

    # Create summary report
    report_df = (
        clients_df.groupBy("system")
        .agg(
            F.count("*").alias("total_clients"),
            F.count("email").alias("total_with_email"),
            F.count("phone_number").alias("total_with_phone"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )
    
    print("Gold layer summary:")
    report_df.show()


def main():
    """Main ETL process"""
    spark = create_spark_session()
    sources = ["cloud_x", "protheus", "sap"]

    # Process each source
    for source in sources:
        # Landing to Bronze
        df_landing = read_landing_data(spark, source)
        df_bronze = process_bronze_layer(df_landing, source)
        save_bronze_layer(df_bronze, source)

        # Bronze to Silver (upsert)
        df_bronze = spark.read.parquet(f"s3a://bronze-zone/dataway/{source}/clients")
        upsert_silver_layer(spark, df_bronze)

    # Create Gold report
    create_gold_report(spark)

    spark.stop()


if __name__ == "__main__":
    main()
