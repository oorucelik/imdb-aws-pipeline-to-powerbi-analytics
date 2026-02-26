"""
Glue Job: imdb_build_fact_popularity.py

Purpose:
    Builds the fact_popularity table from daily TMDB popularity snapshots.
    Compares today's rankings with yesterday's to compute rank changes,
    identify new joiners (first appearance) and leavers (dropped off).

Input:
    - s3://oruc-imdb-lake/raw/stg_popularity/   (today's snapshot)
    - s3://oruc-imdb-lake/gold/fact_popularity/  (historical data)

Output:
    - s3://oruc-imdb-lake/gold/fact_popularity/  (partitioned by loadDate)

Schema:
    - loadDate:      DATE      — snapshot date (partition key)
    - content_id:    STRING    — IMDB ID
    - rank:          INT       — daily popularity rank (1 = best)
    - popularity:    DECIMAL   — TMDB popularity score
    - rank_change:   INT       — delta vs previous day (negative = improved)
    - is_new_joiner: BOOLEAN   — true if new to the top list today
    - is_leaver:     BOOLEAN   — true if dropped off the list today

Trigger:
    Called by Step Functions after imdb_generate_dim_bridge_tables completes.
"""

import sys
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Initialize
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# S3 Paths
STG_POPULARITY_PATH = "s3://oruc-imdb-lake/raw/stg_popularity/"
FACT_POPULARITY_PATH = "s3://oruc-imdb-lake/gold/fact_popularity/"

# ----------------------------
# 1. Read Today's Snapshot
# ----------------------------
stg_popularity = spark.read.parquet(STG_POPULARITY_PATH)

# Parse loadDate and cast popularity
stg_popularity = stg_popularity.withColumn(
    "loadDate",
    F.to_date(F.col("loadDate"))
).withColumn(
    "popularity",
    F.col("popularity").cast("decimal(10,2)")
)

# Get execution date (today)
today = stg_popularity.select(F.max("loadDate")).collect()[0][0]

# Filter today's data and add rank
today_data = stg_popularity.filter(F.col("loadDate") == today)

# Rank by popularity (higher popularity = better rank)
window_spec = Window.orderBy(F.col("popularity").desc())
today_ranked = today_data.withColumn(
    "rank",
    F.row_number().over(window_spec)
).select(
    "loadDate",
    "id",  # content_id
    "rank",
    "popularity"
)

# ----------------------------
# 2. Read Historical Fact Table (Previous Days)
# ----------------------------
try:
    fact_existing = spark.read.parquet(FACT_POPULARITY_PATH)

    # Get yesterday's date
    yesterday = today - timedelta(days=1)

    # Get yesterday's data for comparison
    yesterday_data = fact_existing.filter(
        F.col("loadDate") == yesterday
    ).select(
        F.col("content_id"),
        F.col("rank").alias("prev_rank")
    )

except Exception as e:
    yesterday_data = None

# ----------------------------
# 3. Calculate Metrics
# ----------------------------
if yesterday_data is not None:
    # Join today with yesterday
    comparison = today_ranked.alias("today").join(
        yesterday_data.alias("yesterday"),
        F.col("today.id") == F.col("yesterday.content_id"),
        how="left"
    )

    # Calculate metrics
    fact_today = comparison.select(
        F.col("today.loadDate"),
        F.col("today.id").alias("content_id"),
        F.col("today.rank"),
        F.col("today.popularity"),
        # rank_change: negative = rank improved, positive = rank worsened
        # For new joiners = 0, otherwise = today.rank - yesterday.rank
        F.when(
            F.col("yesterday.prev_rank").isNull(),
            F.lit(0)).otherwise(
                F.col("today.rank") - F.col("yesterday.prev_rank")
                ).alias("rank_change"),
        # is_new_joiner: was not in yesterday's list
        F.when(F.col("yesterday.prev_rank").isNull(), True).otherwise(False).alias("is_new_joiner"),
        # is_leaver: will be calculated separately (was in yesterday, not in today)
        F.lit(False).alias("is_leaver")
    )

    # Find leavers (in yesterday but not in today)
    today_ids = today_ranked.select(F.col("id")).distinct()
    leavers = yesterday_data.alias("yesterday").join(
        today_ids.alias("today"),
        F.col("yesterday.content_id") == F.col("today.id"),
        how="left_anti"
    ).select(
        F.lit(today).alias("loadDate"),
        F.col("content_id"),
        F.lit(None).cast("int").alias("rank"),       # No rank (dropped out)
        F.lit(0.0).cast("decimal(10,2)").alias("popularity"),
        F.lit(999).alias("rank_change"),              # Large positive = dropped
        F.lit(False).alias("is_new_joiner"),
        F.lit(True).alias("is_leaver")
    )

    # Union active and leavers
    fact_today_final = fact_today.union(leavers)

else:
    # First run — all are new joiners
    fact_today_final = today_ranked.select(
        F.col("loadDate"),
        F.col("id").alias("content_id"),
        F.col("rank"),
        F.col("popularity"),
        F.lit(0).alias("rank_change"),
        F.lit(True).alias("is_new_joiner"),
        F.lit(False).alias("is_leaver")
    )

# ----------------------------
# 4. Write to Gold Layer (Dynamic Partition Overwrite)
# ----------------------------
fact_today_final.write.mode("overwrite").option(
    "partitionOverwriteMode", "dynamic"
).partitionBy("loadDate").parquet(FACT_POPULARITY_PATH)

job.commit()