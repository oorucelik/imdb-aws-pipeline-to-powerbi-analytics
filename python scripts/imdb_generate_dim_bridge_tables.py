"""
Glue Job: imdb_generate_dim_bridge_tables.py

Purpose:
    Reads enriched staging tables from S3, builds dimension and bridge tables,
    and writes them to the gold layer using a full-overwrite strategy.

Input (s3://oruc-imdb-lake/stg/):
    - content_detail/      → dim_content
    - content_person/      → dim_person, bridge_content_person
    - content_genre/       → dim_genre, bridge_content_genre
    - content_production/  → dim_production_company, bridge_content_company
    - content_interest/    → dim_interest, bridge_content_interest (optional)
    - content_network/     → dim_network, bridge_content_network (optional
    - content_season/      → dim_season
    - content_episode/     → dim_episode

Output (s3://oruc-imdb-lake/gold/):
    Dimension tables: dim_content, dim_person, dim_genre, dim_production_company,
                      dim_interest, dim_network, dim_season, dim_episode
    Bridge tables:    bridge_content_genre, bridge_content_person,
                      bridge_content_company, bridge_content_interest,
                      bridge_content_network

Trigger:
    Called by Step Functions after imdb_enrich_content_metadata completes.
"""

import sys
from datetime import datetime

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import concat, lit, lpad, col
from pyspark.sql import functions as F

# ----------------------------
# Glue boilerplate
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RUN_TS = datetime.utcnow()

# ----------------------------
# Helper Functions
# ----------------------------
def read_parquet_or_none(path: str):
    """Try to read a Parquet path from S3. Returns None if it doesn't exist."""
    try:
        return spark.read.parquet(path)
    except Exception:
        return None

# ----------------------------
# S3 Paths - STG inputs
# ----------------------------
STG_CONTENT_DETAIL = "s3://oruc-imdb-lake/stg/content_detail/"
STG_CONTENT_PERSON = "s3://oruc-imdb-lake/stg/content_person/"
STG_CONTENT_GENRE = "s3://oruc-imdb-lake/stg/content_genre/"
STG_CONTENT_PROD = "s3://oruc-imdb-lake/stg/content_production/"
STG_CONTENT_INTEREST = "s3://oruc-imdb-lake/stg/content_interest/"
STG_CONTENT_NETWORK = "s3://oruc-imdb-lake/stg/content_network/"
STG_CONTENT_SEASON = "s3://oruc-imdb-lake/stg/content_season/"
STG_CONTENT_EPISODE = "s3://oruc-imdb-lake/stg/content_episode/"
# ----------------------------
# S3 Paths - GOLD outputs
# ----------------------------
DIM_CONTENT_PATH = "s3://oruc-imdb-lake/gold/dim_content/"
DIM_PERSON_PATH = "s3://oruc-imdb-lake/gold/dim_person/"
DIM_GENRE_PATH = "s3://oruc-imdb-lake/gold/dim_genre/"
DIM_INTEREST_PATH = "s3://oruc-imdb-lake/gold/dim_interest/"
DIM_NETWORK_PATH = "s3://oruc-imdb-lake/gold/dim_network/"
DIM_COMPANY_PATH = "s3://oruc-imdb-lake/gold/dim_production_company/"
DIM_SEASON_PATH = "s3://oruc-imdb-lake/gold/dim_season/"
DIM_EPISODE_PATH = "s3://oruc-imdb-lake/gold/dim_episode/"

BRIDGE_CONTENT_GENRE_PATH = "s3://oruc-imdb-lake/gold/bridge_content_genre/"
BRIDGE_CONTENT_PERSON_PATH = "s3://oruc-imdb-lake/gold/bridge_content_person/"
BRIDGE_CONTENT_COMPANY_PATH = "s3://oruc-imdb-lake/gold/bridge_content_company/"
BRIDGE_CONTENT_INTEREST_PATH = "s3://oruc-imdb-lake/gold/bridge_content_interest/"
BRIDGE_CONTENT_NETWORK_PATH = "s3://oruc-imdb-lake/gold/bridge_content_network/"

# ============================================================================
# LOAD STAGING DATA
# ============================================================================

stg_content = spark.read.parquet(STG_CONTENT_DETAIL)
stg_person = spark.read.parquet(STG_CONTENT_PERSON)
stg_genre = spark.read.parquet(STG_CONTENT_GENRE)
stg_prod = spark.read.parquet(STG_CONTENT_PROD)
stg_interest = read_parquet_or_none(STG_CONTENT_INTEREST)
stg_network = read_parquet_or_none(STG_CONTENT_NETWORK)
stg_season = read_parquet_or_none(STG_CONTENT_SEASON)
stg_episode = read_parquet_or_none(STG_CONTENT_EPISODE)

# ============================================================================
# DIMENSION TABLES - FULL OVERWRITE
# ============================================================================

# ----------------------------
# 1. DIM_CONTENT
# ----------------------------
dim_content = stg_content.select(
    F.col("content_id"),
    F.col("content_type"),
    F.col("primary_title"),
    F.col("original_title"),
    F.col("overview"),
    F.col("release_date"),
    F.col("runtime_minutes"),
    F.col("status"),
    F.col("original_language"),
    F.col("trailer"),
    F.col("content_poster"),
    F.col("average_rating"),
    F.col("vote_count"),
    F.col("content_homepage"),
    F.col("tagline"),
    F.col("budget"),
    F.col("revenue"),
    F.col("created_at"),
    F.col("updated_at")
).dropDuplicates(["content_id"])

dim_content_count = dim_content.count()

# ----------------------------
# 2. DIM_PERSON
# ----------------------------
dim_person = stg_person.select(
    F.col("person_id"),
    F.col("person_name"),
    F.col("person_homepage"),
    F.col("person_poster")
).dropDuplicates(["person_id"])

dim_person_count = dim_person.count()

# ----------------------------
# 3. DIM_GENRE
# ----------------------------
dim_genre = stg_genre.select(
    F.col("genre_name")
).dropDuplicates(["genre_name"])

dim_genre_count = dim_genre.count()

# ----------------------------
# 4. DIM_EPISODE
# ----------------------------
dim_episode = stg_episode.select(
    F.col("content_id"),
    F.col("season_number"),
    F.col("episode_number"),
    F.col("primary_title"),
    F.col("original_title"),
    F.col("runtime_minutes"),
    F.col("average_rating"),
    F.col("vote_count"),
    F.col("still_path"),
    F.col("episode_type"),
    F.col("air_date")
).dropDuplicates(["content_id","season_number","episode_number"])

# Generate composite keys for season/episode relationships
dim_episode = dim_episode.withColumn(
    "season_key",
    concat(
        col("content_id"),
        lit("_S"),
        lpad(col("season_number"), 2, "0")
    )
).withColumn(
    "episode_key",
    concat(
        col("content_id"),
        lit("_S"),
        lpad(col("season_number"), 2, "0"),
        lit("_E"),
        lpad(col("episode_number"), 2, "0")
    )
)

dim_episode_count = dim_episode.count()

# ----------------------------
# 5. DIM_SEASON
# ----------------------------
dim_season = stg_season.select(
  F.col("content_id"),
  F.col("season_number"),
  F.col("air_date"),
  F.col("episode_count"),
  F.col("name"),
  F.col("overview"),
  F.col("poster_path")
).dropDuplicates(["content_id", "season_number"])

# Generate composite key for season
dim_season = dim_season.withColumn(
    "season_key",
    concat(
        col("content_id"),
        lit("_S"),
        lpad(col("season_number"), 2, "0")
    )
)
dim_season_count = dim_season.count()

# ----------------------------
# 6. DIM_PRODUCTION_COMPANY
# ----------------------------
dim_company = stg_prod.select(
    F.col("company_id"),
    F.col("company_name"),
    F.col("company_poster")
).dropDuplicates(["company_id"])

dim_company_count = dim_company.count()

# ----------------------------
# 7. DIM_INTEREST (Optional)
# ----------------------------
if stg_interest is not None:
    dim_interest = stg_interest.select(
        F.col("interest_name")
    ).dropDuplicates(["interest_name"])
    dim_interest_count = dim_interest.count()
else:
    dim_interest = None

# ----------------------------
# 8. DIM_NETWORK (Optional)
# ----------------------------
if stg_network is not None:
    cols = stg_network.columns
    if "network_id" in cols:
        dim_network = stg_network.select(
            F.col("network_id"),
            F.col("network_name"),
            F.col("network_poster")
        ).dropDuplicates(["network_id"])
    else:
        dim_network = stg_network.select(
            F.col("network_name"),
            F.col("network_poster")
        ).dropDuplicates(["network_name"])
    dim_network_count = dim_network.count()
else:
    dim_network = None

# ============================================================================
# WRITE DIMENSION TABLES
# ============================================================================

dim_content.write.mode("overwrite").parquet(DIM_CONTENT_PATH)
dim_person.write.mode("overwrite").parquet(DIM_PERSON_PATH)
dim_genre.write.mode("overwrite").parquet(DIM_GENRE_PATH)
dim_company.write.mode("overwrite").parquet(DIM_COMPANY_PATH)
dim_episode.write.mode("overwrite").parquet(DIM_EPISODE_PATH)
dim_season.write.mode("overwrite").parquet(DIM_SEASON_PATH)

if dim_interest is not None:
    dim_interest.write.mode("overwrite").parquet(DIM_INTEREST_PATH)

if dim_network is not None:
    dim_network.write.mode("overwrite").parquet(DIM_NETWORK_PATH)

# ============================================================================
# BRIDGE TABLES - FULL OVERWRITE
# ============================================================================

# ----------------------------
# 1. BRIDGE: Content ↔ Genre
# ----------------------------
bridge_content_genre = stg_genre.select(
    F.col("content_id"),
    F.col("genre_name")
).join(
    dim_content.select("content_id"),
    on="content_id",
    how="inner"
).join(
    dim_genre.select("genre_name"),
    on="genre_name",
    how="inner"
).select(
    F.col("content_id"),
    F.col("genre_name")
).distinct()

bridge_content_genre_count = bridge_content_genre.count()

# ----------------------------
# 2. BRIDGE: Content ↔ Person
# ----------------------------
bridge_content_person = stg_person.select(
    F.col("content_id"),
    F.col("person_id"),
    F.col("role_type"),
    F.col("character_names"),
    F.col("order_no")
).join(
    dim_content.select("content_id"),
    on="content_id",
    how="inner"
).join(
    dim_person.select("person_id"),
    on="person_id",
    how="inner"
).select(
    F.col("content_id"),
    F.col("person_id"),
    F.col("role_type"),
    F.col("character_names"),
    F.col("order_no")
).distinct()

bridge_content_person_count = bridge_content_person.count()

# ----------------------------
# 3. BRIDGE: Content ↔ Company
# ----------------------------
bridge_content_company = stg_prod.select(
    F.col("content_id"),
    F.col("company_id")
).join(
    dim_content.select("content_id"),
    on="content_id",
    how="inner"
).join(
    dim_company.select("company_id"),
    on="company_id",
    how="inner"
).select(
    F.col("content_id"),
    F.col("company_id")
).distinct()

bridge_content_company_count = bridge_content_company.count()

# ----------------------------
# 4. BRIDGE: Content ↔ Interest (Optional)
# ----------------------------
if stg_interest is not None and dim_interest is not None:
    bridge_content_interest = stg_interest.select(
        F.col("content_id"),
        F.col("interest_name")
    ).join(
        dim_content.select("content_id"),
        on="content_id",
        how="inner"
    ).join(
        dim_interest.select("interest_name"),
        on="interest_name",
        how="inner"
    ).select(
        F.col("content_id"),
        F.col("interest_name")
    ).distinct()
    bridge_content_interest_count = bridge_content_interest.count()
else:
    bridge_content_interest = None

# ----------------------------
# 5. BRIDGE: Content ↔ Network (Optional)
# ----------------------------
if stg_network is not None and dim_network is not None:
    if "network_id" in stg_network.columns:
        bridge_content_network = stg_network.select(
            F.col("content_id"),
            F.col("network_id")
        ).join(
            dim_content.select("content_id"),
            on="content_id",
            how="inner"
        ).join(
            dim_network.select("network_id"),
            on="network_id",
            how="inner"
        ).select(
            F.col("content_id"),
            F.col("network_id")
        ).distinct()
    else:
        bridge_content_network = stg_network.select(
            F.col("content_id"),
            F.col("network_name")
        ).join(
            dim_content.select("content_id"),
            on="content_id",
            how="inner"
        ).join(
            dim_network.select("network_name"),
            on="network_name",
            how="inner"
        ).select(
            F.col("content_id"),
            F.col("network_name")
        ).distinct()
    bridge_content_network_count = bridge_content_network.count()
else:
    bridge_content_network = None

# ============================================================================
# WRITE BRIDGE TABLES
# ============================================================================

bridge_content_genre.write.mode("overwrite").parquet(BRIDGE_CONTENT_GENRE_PATH)
bridge_content_person.write.mode("overwrite").parquet(BRIDGE_CONTENT_PERSON_PATH)
bridge_content_company.write.mode("overwrite").parquet(BRIDGE_CONTENT_COMPANY_PATH)

if bridge_content_interest is not None:
    bridge_content_interest.write.mode("overwrite").parquet(BRIDGE_CONTENT_INTEREST_PATH)

if bridge_content_network is not None:
    bridge_content_network.write.mode("overwrite").parquet(BRIDGE_CONTENT_NETWORK_PATH)

job.commit()