# Glue Job: imdb_generate_dim_bridge_tables.py
# Purpose: Full overwrite approach for dimension and bridge table generation

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
    """
    Try to read parquet, return None if doesn't exist
    """
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

#print("Loading staging data from S3...")

stg_content = spark.read.parquet(STG_CONTENT_DETAIL)
stg_person = spark.read.parquet(STG_CONTENT_PERSON)
stg_genre = spark.read.parquet(STG_CONTENT_GENRE)
stg_prod = spark.read.parquet(STG_CONTENT_PROD)
stg_interest = read_parquet_or_none(STG_CONTENT_INTEREST)
stg_network = read_parquet_or_none(STG_CONTENT_NETWORK)
stg_season = read_parquet_or_none(STG_CONTENT_SEASON)  
stg_episode = read_parquet_or_none(STG_CONTENT_EPISODE)

#print("✅ Staging data loaded")

# ============================================================================
# DIMENSION TABLES - FULL OVERWRITE
# ============================================================================
#print("BUILDING DIMENSION TABLES")
# ----------------------------
# 1. DIM_CONTENT
# ----------------------------
#print("📊 Building dim_content...")

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
#print(f"   ✅ dim_content: {dim_content_count} records")

# ----------------------------
# 2. DIM_PERSON
# ----------------------------
#print("\n📊 Building dim_person...")

dim_person = stg_person.select(
    F.col("person_id"),
    F.col("person_name"),
    F.col("person_homepage"),
    F.col("person_poster")
).dropDuplicates(["person_id"])

dim_person_count = dim_person.count()
#print(f"   ✅ dim_person: {dim_person_count} records")

# ----------------------------
# 3. DIM_GENRE
# ----------------------------
#print("\n📊 Building dim_genre...")

dim_genre = stg_genre.select(
    F.col("genre_name")
).dropDuplicates(["genre_name"])

dim_genre_count = dim_genre.count()
#print(f"   ✅ dim_genre: {dim_genre_count} records")

# ----------------------------
# 4. DIM_EPISODE
# ----------------------------
#print("\n📊 Building dim_episode...")

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

# Episode için key oluşturma
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
        lpad(col("episode_number"), 2, "0")  # 01, 02, ... formatında
    )
)

dim_episode_count = dim_episode.count()
#print(f"   ✅ dim_episode: {dim_episode_count} records")

# ----------------------------
# 5. DIM_SEASON
# ----------------------------
#print("\n📊 Building dim_season...")

dim_season = stg_season.select(
  F.col("content_id"), 
  F.col("season_number"), 
  F.col("air_date"), 
  F.col("episode_count"), 
  F.col("name"), 
  F.col("overview"), 
  F.col("poster_path")
).dropDuplicates(["content_id", "season_number"])
# Season için key oluşturma
dim_season = dim_season.withColumn(
    "season_key",
    concat(
        col("content_id"),
        lit("_S"),
        lpad(col("season_number"), 2, "0")  # 01, 02, ... formatında
    )
)
dim_season_count = dim_season.count()
#print(f"   ✅ dim_season: {dim_season_count} records")

# ----------------------------
# 6. DIM_PRODUCTION_COMPANY
# ----------------------------
#print("\n📊 Building dim_production_company...")

dim_company = stg_prod.select(
    F.col("company_id"),
    F.col("company_name"),
    F.col("company_poster")
).dropDuplicates(["company_id"])

dim_company_count = dim_company.count()
#print(f"   ✅ dim_production_company: {dim_company_count} records")



# ----------------------------
# 7. DIM_INTEREST (Optional)
# ----------------------------
if stg_interest is not None:
    #print("\n📊 Building dim_interest...")
    
    dim_interest = stg_interest.select(
        F.col("interest_name")
    ).dropDuplicates(["interest_name"])
    
    dim_interest_count = dim_interest.count()
    #print(f"   ✅ dim_interest: {dim_interest_count} records")
else:
    dim_interest = None
    #print("\n⚠️  dim_interest: Skipped (no staging data)")

# ----------------------------
# 8. DIM_NETWORK (Optional)
# ----------------------------
if stg_network is not None:
    #print("\n📊 Building dim_network...")
    
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
    #print(f"   ✅ dim_network: {dim_network_count} records")
else:
    dim_network = None
    #print("\n⚠️  dim_network: Skipped (no staging data)")

# ============================================================================
# WRITE DIMENSION TABLES
# ============================================================================

#print("\n" + "="*80)
#print("WRITING DIMENSION TABLES TO S3")
#print("="*80)

dim_content.write.mode("overwrite").parquet(DIM_CONTENT_PATH)
#print(f"✅ {DIM_CONTENT_PATH}")

dim_person.write.mode("overwrite").parquet(DIM_PERSON_PATH)
#print(f"✅ {DIM_PERSON_PATH}")

dim_genre.write.mode("overwrite").parquet(DIM_GENRE_PATH)
#print(f"✅ {DIM_GENRE_PATH}")

dim_company.write.mode("overwrite").parquet(DIM_COMPANY_PATH)
#print(f"✅ {DIM_COMPANY_PATH}")

dim_episode.write.mode("overwrite").parquet(DIM_EPISODE_PATH)
#print(f"✅ {DIM_EPISODE_PATH}")

dim_season.write.mode("overwrite").parquet(DIM_SEASON_PATH)
#print(f"✅ {DIM_SEASON_PATH}")

if dim_interest is not None:
    dim_interest.write.mode("overwrite").parquet(DIM_INTEREST_PATH)
    #print(f"✅ {DIM_INTEREST_PATH}")

if dim_network is not None:
    dim_network.write.mode("overwrite").parquet(DIM_NETWORK_PATH)
    #print(f"✅ {DIM_NETWORK_PATH}")

# ============================================================================
# BRIDGE TABLES - FULL OVERWRITE
# ============================================================================
#print("BUILDING BRIDGE TABLES")
# ----------------------------
# 1. BRIDGE: Content ↔ Genre
# ----------------------------
#print("\n🔗 Building bridge_content_genre...")

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
#print(f"   ✅ {bridge_content_genre_count} relationships")

# ----------------------------
# 2. BRIDGE: Content ↔ Person
# ----------------------------
#print("\n🔗 Building bridge_content_person...")

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
#print(f"   ✅ {bridge_content_person_count} relationships")

# ----------------------------
# 3. BRIDGE: Content ↔ Company
# ----------------------------
#print("\n🔗 Building bridge_content_company...")

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
#print(f"   ✅ {bridge_content_company_count} relationships")

# ----------------------------
# 4. BRIDGE: Content ↔ Interest (Optional)
# ----------------------------
if stg_interest is not None and dim_interest is not None:
    #print("\n🔗 Building bridge_content_interest...")
    
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
    #print(f"   ✅ {bridge_content_interest_count} relationships")
else:
    bridge_content_interest = None
    #print("\n⚠️  bridge_content_interest: Skipped")

# ----------------------------
# 5. BRIDGE: Content ↔ Network (Optional)
# ----------------------------
if stg_network is not None and dim_network is not None:
    #print("\n🔗 Building bridge_content_network...")
    
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
    #print(f"   ✅ {bridge_content_network_count} relationships")
else:
    bridge_content_network = None
    #print("\n⚠️  bridge_content_network: Skipped")

# ============================================================================
# WRITE BRIDGE TABLES
# ============================================================================

#print("\n" + "="*80)
#print("WRITING BRIDGE TABLES TO S3")
#print("="*80)

bridge_content_genre.write.mode("overwrite").parquet(BRIDGE_CONTENT_GENRE_PATH)
#print(f"✅ {BRIDGE_CONTENT_GENRE_PATH}")

bridge_content_person.write.mode("overwrite").parquet(BRIDGE_CONTENT_PERSON_PATH)
#print(f"✅ {BRIDGE_CONTENT_PERSON_PATH}")

bridge_content_company.write.mode("overwrite").parquet(BRIDGE_CONTENT_COMPANY_PATH)
#print(f"✅ {BRIDGE_CONTENT_COMPANY_PATH}")

if bridge_content_interest is not None:
    bridge_content_interest.write.mode("overwrite").parquet(BRIDGE_CONTENT_INTEREST_PATH)
    #print(f"✅ {BRIDGE_CONTENT_INTEREST_PATH}")

if bridge_content_network is not None:
    bridge_content_network.write.mode("overwrite").parquet(BRIDGE_CONTENT_NETWORK_PATH)
    #print(f"✅ {BRIDGE_CONTENT_NETWORK_PATH}")

# ============================================================================
# JOB SUMMARY
# ============================================================================

#print("📊 Dimension Tables:")
#print(f"   - dim_content: {dim_content_count}")
#print(f"   - dim_person: {dim_person_count}")
#print(f"   - dim_genre: {dim_genre_count}")
#print(f"   - dim_production_company: {dim_company_count}")
#print(f"   - dim_episode: {dim_episode_count}")
#print(f"   - dim_season: {dim_season_count}")
#print(f"   - dim_interest: {dim_interest_count}")
#print(f"   - dim_network: {dim_network_count}")

#print("\n🔗 Bridge Tables:")
#print(f"   - bridge_content_genre: {bridge_content_genre_count}")
#print(f"   - bridge_content_person: {bridge_content_person_count}")
#print(f"   - bridge_content_company: {bridge_content_company_count}")
#print(f"   - bridge_content_interest: {bridge_content_interest_count}")
#print(f"   - bridge_content_network: {bridge_content_network_count}")

#print("\n✅ Job completed successfully!")
job.commit()