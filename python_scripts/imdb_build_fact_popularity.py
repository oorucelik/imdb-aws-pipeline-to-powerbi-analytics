"""
Glue Job: imdb_build_fact_popularity.py
Purpose: Build fact_popularity table from daily snapshots

Schema:
- loadDate: DATE
- content_id: STRING
- rank: INT
- popularity: DECIMAL
- rank_change: INT (vs previous day)
- is_new_joiner: BOOLEAN (new to top list)
- is_leaver: BOOLEAN (dropped from top list)
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

print("Building Fact Popularity Table")
# ----------------------------
# 1. Read Today's Snapshot
# ----------------------------
print("\n📥 Reading today's popularity snapshot from staging...")

stg_popularity = spark.read.parquet(STG_POPULARITY_PATH)

# Parse loadDate and add rank
stg_popularity = stg_popularity.withColumn(
    "loadDate", 
    F.to_date(F.col("loadDate"))
).withColumn(
    "popularity",
    F.col("popularity").cast("decimal(10,2)")
)

# Get execution date (today)
today = stg_popularity.select(F.max("loadDate")).collect()[0][0]
print(f"   Today's date: {today}")

# Filter today's data and add rank
today_data = stg_popularity.filter(F.col("loadDate") == today)

# Rank by popularity (higher = better rank)
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

print(f"   Today's snapshot count: {today_ranked.count()}")
today_ranked.show(10, truncate=False)

# ----------------------------
# 2. Read Historical Fact Table (Previous Days)
# ----------------------------
print("\n📚 Reading historical fact_popularity...")

try:
    fact_existing = spark.read.parquet(FACT_POPULARITY_PATH)
    print(f"   Historical records found: {fact_existing.count()}")
    
    # Get yesterday's date
    yesterday = today - timedelta(days=1)
    print(f"   Yesterday's date: {yesterday}")
    
    # Get yesterday's data for comparison
    yesterday_data = fact_existing.filter(
        F.col("loadDate") == yesterday
    ).select(
        F.col("content_id"),
        F.col("rank").alias("prev_rank")
    )
    print(f"   Yesterday's record count: {yesterday_data.count()}")
    
except Exception as e:
    print(f"   No historical data found (first run): {e}")
    yesterday_data = None

# ----------------------------
# 3. Calculate Metrics
# ----------------------------
print("\n🔢 Calculating rank_change, is_new_joiner, is_leaver...")
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
            #For new joiners = 0, otherwise = today.rank - yesterday.rank        
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
        F.lit(None).cast("int").alias("rank"),  # No rank (dropped out)
        F.lit(0.0).cast("decimal(10,2)").alias("popularity"),
        F.lit(999).alias("rank_change"),  # Large positive = dropped
        F.lit(False).alias("is_new_joiner"),
        F.lit(True).alias("is_leaver")
    )
    
    # Union active and leavers
    fact_today_final = fact_today.union(leavers)
    
else:
    # First run - all are new joiners
    print("   First run detected - marking all as new joiners")
    fact_today_final = today_ranked.select(
        F.col("loadDate"),
        F.col("id").alias("content_id"),
        F.col("rank"),
        F.col("popularity"),
        F.lit(0).alias("rank_change"),
        F.lit(True).alias("is_new_joiner"),
        F.lit(False).alias("is_leaver")
    )

print(f"   Final fact records for today: {fact_today_final.count()}")
# Show summary statistics
print("\n📊 Today's Summary:")
fact_today_final.groupBy("is_new_joiner", "is_leaver").count().show()
print("\nTop 10 Gainers (biggest rank improvements):")
fact_today_final.filter(
    (F.col("rank_change") < 0) & (~F.col("is_new_joiner"))
).orderBy("rank_change").show(10, truncate=False)
print("\nTop 10 Decliners (biggest rank drops):")
fact_today_final.filter(
    (F.col("rank_change") > 0) & (~F.col("is_leaver"))
).orderBy(F.col("rank_change").desc()).show(10, truncate=False)

# ----------------------------
# 4. Write to Gold Layer (Append Mode)
# ----------------------------
print(f"\n💾 Writing to {FACT_POPULARITY_PATH} (append mode)...")
fact_today_final.write.mode("overwrite").option("partitionOverwriteMode","dynamic").partitionBy("loadDate").parquet(FACT_POPULARITY_PATH)
print("✅ Fact Popularity table updated successfully!")

# ----------------------------
# Job Summary
# ----------------------------
print("JOB SUMMARY")
print(f"Load Date: {today}")
print(f"Total Records Written: {fact_today_final.count()}")
fact_today_final.groupBy("is_new_joiner", "is_leaver").count().show()
job.commit()