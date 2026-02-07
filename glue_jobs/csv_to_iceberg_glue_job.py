import sys
import uuid
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# -------------------------------------------------
# LOGGER
# -------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# ARG PARSER
# -------------------------------------------------

def parse_args():
    args = {}
    for arg in sys.argv[1:]:
        key, value = arg.split("=")
        args[key.replace("--", "")] = value
    return args

# -------------------------------------------------
# VALIDATION
# -------------------------------------------------

def validate_csv_path(path):
    if not path.lower().endswith(".csv") and not path.endswith("/"):
        raise ValueError("Input must be CSV")

def detect_header(line):
    return any(char.isalpha() for char in line)

# -------------------------------------------------
# SPARK SESSION
# -------------------------------------------------

def create_spark_session(catalog, warehouse):
    return (
        SparkSession.builder
        .appName("DynamicCSVToIcebergGlue")
        .config(
            f"spark.sql.catalog.{catalog}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{catalog}.warehouse",
            warehouse
        )
        .config(
            f"spark.sql.catalog.{catalog}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        .config(
            f"spark.sql.catalog.{catalog}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .getOrCreate()
    )

# -------------------------------------------------
# READ CSV DYNAMIC
# -------------------------------------------------

def read_csv_dynamic(spark, csv_path):
    rdd = spark.sparkContext.textFile(csv_path)

    first_line = rdd.first()
    has_header = detect_header(first_line)

    rows = (
        rdd.zipWithIndex()
           .filter(lambda x: x[1] > 0 if has_header else True)
           .map(lambda x: x[0].split(","))
    )

    max_cols = rows.map(len).max()

    padded_rows = rows.map(
        lambda r: r + ["0"] * (max_cols - len(r))
    )

    if has_header:
        header = first_line.split(",")
        header += [f"col_{i+1}" for i in range(len(header), max_cols)]
        columns = header
    else:
        columns = [f"col_{i+1}" for i in range(max_cols)]

    schema = StructType([
        StructField(c, StringType(), True) for c in columns
    ])

    return spark.createDataFrame(padded_rows, schema)

# -------------------------------------------------
# CLEANSING
# -------------------------------------------------

def cleanse_dataframe(df):
    for c in df.columns:
        df = df.withColumn(c, trim(col(c)))

    df = df.dropDuplicates()
    return df

# -------------------------------------------------
# INGESTION METADATA
# -------------------------------------------------

def add_ingestion_metadata(df, source_file):
    return (
        df.withColumn("ingestion_id", lit(str(uuid.uuid4())))
          .withColumn("ingestion_ts", current_timestamp())
          .withColumn("source_file", lit(source_file))
    )

# -------------------------------------------------
# SAFE MERGE
# -------------------------------------------------

def merge_into_iceberg(spark, df, catalog, database, table):
    df.createOrReplaceTempView("staging_data")

    spark.sql(f"""
        MERGE INTO {catalog}.{database}.{table} t
        USING staging_data s
        ON t.source_file = s.source_file
        WHEN NOT MATCHED THEN INSERT *
    """)

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    args = parse_args()

    validate_csv_path(args["csv_path"])

    spark = create_spark_session(
        args["iceberg_catalog"],
        args["warehouse"],
    )

    df = read_csv_dynamic(spark, args["csv_path"])
    df = cleanse_dataframe(df)
    df = add_ingestion_metadata(df, args["csv_path"])

    merge_into_iceberg(
        spark,
        df,
        args["iceberg_catalog"],
        args["iceberg_db"],
        args["iceberg_table"],
    )

    spark.stop()
    logger.info("Glue job completed successfully")

if __name__ == "__main__":
    main()