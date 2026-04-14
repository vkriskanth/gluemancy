"""PySpark ingestion demo — AWS Glue version.

Adapts spark_ingest_demo.py to run on AWS Glue 4.0 (Spark 3.3, Python 3.10).

Changes from the local version
--------------------------------
- GlueContext / Job boilerplate replaces SparkSession.builder.master("local[*]")
- Output is written to S3 (--output_path job parameter) instead of a local path
- Decimal values are cast to DOUBLE before CSV write (Glue/S3 CSV quirk)
- Job commit at the end signals success to Glue

Usage (Glue Job parameters)
----------------------------
--output_path   s3://your-bucket/output/spark_demo_csv/
--num_records   10000   (optional, default 10000)
"""

import random
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Glue boilerplate — must come before any Spark operations
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "output_path",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

OUTPUT_PATH = args["output_path"].rstrip("/")
NUM_RECORDS = int(args.get("num_records", 10_000)) if "num_records" in args else 10_000
RANDOM_SEED = 42

# ---------------------------------------------------------------------------
# Schema — identical to the local version
# ---------------------------------------------------------------------------
ADDRESS_SCHEMA = StructType(
    [
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip_code", StringType(), True),
    ]
)

SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", DoubleType(), True),
        StructField("rating", FloatType(), True),
        StructField("balance", DecimalType(15, 2), True),
        StructField("is_active", BooleanType(), True),
        StructField("birth_date", DateType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("address", ADDRESS_SCHEMA, True),
        StructField("metadata", MapType(StringType(), StringType()), True),
        StructField("category_id", ShortType(), True),
        StructField("byte_flag", ByteType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------
_TAGS = ["electronics", "clothing", "food", "sports", "home", "books", "toys", "beauty"]
_CITIES = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "Dallas",
]
_STREETS = [
    "Main St",
    "Oak Ave",
    "Maple Dr",
    "Cedar Ln",
    "Pine Rd",
    "Elm Blvd",
    "Washington St",
    "Park Ave",
]

_START_DATE = date(1960, 1, 1)
_END_DATE = date(2005, 12, 31)
_DATE_RANGE = (_END_DATE - _START_DATE).days

_START_TS = datetime(2020, 1, 1)
_END_TS = datetime(2026, 4, 13)
_TS_RANGE = int((_END_TS - _START_TS).total_seconds())


# ---------------------------------------------------------------------------
# Row generation — unchanged
# ---------------------------------------------------------------------------
def _rand_date(rng: random.Random) -> date:
    return _START_DATE + timedelta(days=rng.randint(0, _DATE_RANGE))


def _rand_ts(rng: random.Random) -> datetime:
    return _START_TS + timedelta(seconds=rng.randint(0, _TS_RANGE))


def generate_rows(n: int, seed: int = RANDOM_SEED) -> list[tuple]:
    rng = random.Random(seed)
    rows: list[tuple] = []
    for i in range(n):
        address = {
            "street": rng.choice(_STREETS),
            "city": rng.choice(_CITIES),
            "zip_code": str(rng.randint(10000, 99999)),
        }
        metadata = {
            f"key_{j}": f"val_{rng.randint(1, 100)}" for j in range(rng.randint(1, 3))
        }
        rows.append(
            (
                i + 1,
                f"user_{i + 1:05d}",
                rng.randint(18, 80),
                round(rng.uniform(0.0, 100.0), 6),
                float(round(rng.uniform(1.0, 5.0), 2)),
                Decimal(str(round(rng.uniform(-10_000.0, 50_000.0), 2))),
                rng.choice([True, False]),
                _rand_date(rng),
                _rand_ts(rng),
                rng.sample(_TAGS, rng.randint(1, 4)),
                address,
                metadata,
                rng.randint(1, 100),
                rng.randint(0, 127),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
print(f"\n[1/4]  Generating {NUM_RECORDS:,} mock rows …")
rows = generate_rows(NUM_RECORDS)
raw_df = spark.createDataFrame(rows, schema=SCHEMA)

raw_df.createOrReplaceTempView("raw_users")
print("[2/4]  Spark SQL view created: raw_users")

print("[3/4]  Applying transformations …")
transformed_df = spark.sql(
    """
    SELECT
        id,
        name,
        age,
        CASE
            WHEN age BETWEEN 18 AND 30 THEN 'young'
            WHEN age BETWEEN 31 AND 50 THEN 'middle'
            ELSE                             'senior'
        END                                             AS age_bucket,
        ROUND(score, 2)                                 AS score,
        CASE
            WHEN score <  33.33 THEN 'low'
            WHEN score <  66.67 THEN 'medium'
            ELSE                     'high'
        END                                             AS score_category,
        rating,
        CAST(balance AS DOUBLE)                         AS balance,
        is_active,
        birth_date,
        DATEDIFF(CURRENT_DATE(), birth_date)            AS days_since_birth,
        DATE_FORMAT(created_at, 'yyyy-MM-dd HH:mm:ss') AS created_at,
        SIZE(tags)                                      AS tag_count,
        ARRAY_JOIN(tags, ', ')                          AS tags_str,
        address.street                                  AS street,
        address.city                                    AS city,
        address.zip_code                                AS zip_code,
        MAP_KEYS(metadata)                              AS metadata_keys,
        category_id,
        byte_flag
    FROM raw_users
    WHERE is_active = TRUE
       OR score > 60.0
    """
)

transformed_df = transformed_df.withColumn(
    "high_value",
    F.when(
        (F.col("balance") > 30_000) & (F.col("score_category") == "high"),
        F.lit(True),
    ).otherwise(F.lit(False)),
)

transformed_df.createOrReplaceTempView("transformed_users")
print("[4/4]  Spark SQL view created: transformed_users")

# ---------------------------------------------------------------------------
# Write output to S3 as CSV
# NOTE: metadata_keys is an ArrayType — cast to string for CSV compatibility
# ---------------------------------------------------------------------------
(
    transformed_df.withColumn(
        "metadata_keys", F.col("metadata_keys").cast(StringType())
    )
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(OUTPUT_PATH)
)

print(f"\nOutput written to: {OUTPUT_PATH}")

# ---------------------------------------------------------------------------
# Glue job commit — required to mark the job run as SUCCEEDED
# ---------------------------------------------------------------------------
job.commit()
