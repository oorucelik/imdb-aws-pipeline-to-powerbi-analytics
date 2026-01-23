import json
import sys
import time
import boto3
import requests
from datetime import datetime

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import Row

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------- Constants ----------------
IMDB_API_HOST = "https://imdb236.p.rapidapi.com/api/imdb"
TMDB_API_HOST = "https://api.themoviedb.org/3"

IMDB_SECRET_ARN = (
    "arn:aws:secretsmanager:eu-north-1:XXXXXXXXXXXX:secret:"
    "events!connection/IMDB_API_CONNECTION/0c5b5d72-bfe0-40c3-b9c1-8f9e27caa807-8MWTDP"
)
TMDB_SECRET_ARN = (
    "arn:aws:secretsmanager:eu-north-1:XXXXXXXXXXXX:secret:"
    "events!connection/TMDB_API_CONNECTION/5788f53d-4460-45ed-90a8-088fb5c580ff-X79n8u"
)


def load_secret_json(secret_arn):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    secret_value = response.get("SecretString")
    if not secret_value:
        raise ValueError(f"SecretString missing for {secret_arn}")
    return json.loads(secret_value)


imdb_secret = load_secret_json(IMDB_SECRET_ARN)
imdb_api_key = imdb_secret.get("api_key_value")
if not imdb_api_key:
    raise ValueError("IMDB API key missing from secret payload")

tmdb_secret = load_secret_json(TMDB_SECRET_ARN)
tmdb_api_key = tmdb_secret.get("api_key_value")
if not tmdb_api_key:
    raise ValueError("TMDB token missing from secret payload")

IMDB_HEADERS = {
    "X-RapidAPI-Key": imdb_api_key,
    "X-RapidAPI-Host": "imdb236.p.rapidapi.com",
}

TMDB_HEADERS = {
    "Authorization": tmdb_api_key,
    "accept": "application/json",
}

content_df = spark.read.parquet("s3://oruc-imdb-lake/raw/stg_contentIDs/")
content_df.printSchema()

print("Reading input from S3...")
print("content_df count:", content_df.count())
content_df.show(truncate=False)


# ---------------- Helper Functions - API Calls ----------------
def fetch_imdb_content(session, imdb_id, retries=5):
    url = f"{IMDB_API_HOST}/{imdb_id}"
    for attempt in range(retries):
        try:
            response = session.get(url, headers=IMDB_HEADERS, timeout=20)
        except requests.RequestException:
            response = None
        if response is not None and response.status_code == 200:
            return response.json()
        time.sleep(1 + attempt * 0.5)
    return None


def fetch_tmdb_content(session, tmdb_id, retries=5):
    url = f"{TMDB_API_HOST}/{tmdb_id}"
    for attempt in range(retries):
        try:
            response = session.get(url, headers=TMDB_HEADERS, timeout=20)
        except requests.RequestException:
            response = None
        if response is not None and response.status_code == 200:
            return response.json()
        time.sleep(1 + attempt * 0.5)
    return None
    
def normalize_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def enrich_partition(rows):
    session = requests.Session()
    now_ts = datetime.utcnow()
    for row in rows:
        imdb_id = row["id"]
        tmdb_id = row["tmdb_id"]
        content_type = row["type"]

        imdb_data = fetch_imdb_content(session, imdb_id)
        if not imdb_data:
            print("IMDB DATA EMPTY FOR:", imdb_id)
            continue
        tmdb_data = fetch_tmdb_content(session, tmdb_id)
        if not tmdb_data:
            print("TMDB DATA EMPTY FOR:", imdb_id)
            tmdb_data = {}

        yield (
            "detail",
            {
                "content_id": imdb_id,
                "content_type": content_type,
                "primary_title": imdb_data.get("primaryTitle"),
                "original_title": imdb_data.get("originalTitle"),
                "release_date": imdb_data.get("releaseDate"),
                "trailer": imdb_data.get("trailer"),
                "runtime_minutes": imdb_data.get("runtimeMinutes"),
                "content_poster": imdb_data.get("primaryImage"),
                "average_rating": normalize_float(imdb_data.get("averageRating")),
                "vote_count": imdb_data.get("numVotes"),
                "content_homepage": tmdb_data.get("homepage"),
                "overview": tmdb_data.get("overview"),
                "original_language": tmdb_data.get("original_language"),
                "status": tmdb_data.get("status"),
                "tagline": tmdb_data.get("tagline"),
                "budget": tmdb_data.get("budget"),
                "revenue": tmdb_data.get("revenue"),
                "created_at": now_ts,
                "updated_at": now_ts,
            },
        )

        for pc in tmdb_data.get("production_companies", []):
            yield (
                "production",
                {
                    "content_id": imdb_id,
                    "company_id": pc.get("id"),
                    "company_name": pc.get("name"),
                    "company_poster": (
                        f"https://image.tmdb.org/t/p/w92{pc.get('logo_path')}"
                        if pc.get("logo_path")
                        else None
                    ),
                },
            )

        for g in imdb_data.get("genres", []):
            yield ("genre", {"content_id": imdb_id, "genre_name": g})

        for interest in imdb_data.get("interests", []):
            yield ("interest", {"content_id": imdb_id, "interest_name": interest})

        for d in imdb_data.get("directors", []):
            yield (
                "person",
                {
                    "content_id": imdb_id,
                    "person_id": d.get("id"),
                    "person_name": d.get("fullName"),
                    "person_homepage": d.get("url"),
                    "person_poster": None,
                    "role_type": "director",
                    "character_names": None,
                    "order_no": None,
                },
            )

        for idx, c in enumerate(imdb_data.get("cast", [])):
            yield (
                "person",
                {
                    "content_id": imdb_id,
                    "person_id": c.get("id"),
                    "person_name": c.get("fullName"),
                    "person_homepage": c.get("url"),
                    "person_poster": c.get("primaryImage"),
                    "role_type": c.get("job"),
                    "character_names": c.get("characters") or [],
                    "order_no": idx + 1,
                },
            )

        for idx, cc in enumerate(tmdb_data.get("created_by", [])):
            yield (
                "person",
                {
                    "content_id": imdb_id,
                    "person_id": cc.get("id"),
                    "person_name": cc.get("name"),
                    "person_homepage": None,
                    "person_poster": None,
                    "role_type": "creator",
                    "character_names": None,
                    "order_no": idx + 1,
                },
            )

        for n in tmdb_data.get("networks", []):
            yield (
                "network",
                {
                    "content_id": imdb_id,
                    "network_id": n.get("id"),
                    "network_poster": (
                        f"https://image.tmdb.org/t/p/w92{n.get('logo_path')}"
                        if n.get("logo_path")
                        else None
                    ),
                    "network_name": n.get("name"),
                },
            )

        # TV Series: Seasons and Episodes
        if content_type == "tvSeries" and tmdb_data.get("seasons"):
            for season in tmdb_data.get("seasons", []):
                season_number = season.get("season_number")
                
                # Yield season details
                yield (
                    "season",
                    {
                        "content_id": imdb_id,
                        "season_number": season_number,
                        "air_date": season.get("air_date"),
                        "episode_count": season.get("episode_count"),
                        "name": season.get("name"),
                        "overview": season.get("overview"),
                        "poster_path": (
                            f"https://image.tmdb.org/t/p/w92{season.get('poster_path')}"
                            if season.get("poster_path")
                            else None
                        ),
                    },
                )
                
                # Fetch season details from TMDB API
                season_data = fetch_tmdb_content(session, f"{tmdb_id}/season/{season_number}")
                if not season_data:
                    continue
                
                # Process episodes
                for tmdb_episode in season_data.get("episodes", []):
                    episode_number = tmdb_episode.get("episode_number")
                    
                    # Find matching episode from IMDB data
                    imdb_episode = None
                    for ep in imdb_data.get("episodes", []):
                        if (ep.get("seasonNumber") == season_number and 
                            ep.get("episodeNumber") == episode_number):
                            imdb_episode = ep
                            break
                    
                    # Yield episode details (combining IMDB and TMDB data)
                    yield (
                        "episode",
                        {
                            "content_id": imdb_id,
                            "season_number": season_number,
                            "episode_number": episode_number,
                            # From IMDB
                            "primary_title": imdb_episode.get("primaryTitle") if imdb_episode else None,
                            "original_title": imdb_episode.get("originalTitle") if imdb_episode else None,
                            "runtime_minutes": imdb_episode.get("runtimeMinutes") if imdb_episode else None,
                            "average_rating": normalize_float(imdb_episode.get("averageRating")) if imdb_episode else None,
                            "vote_count": imdb_episode.get("numVotes") if imdb_episode else None,
                            # From TMDB
                            "still_path": (
                                f"https://image.tmdb.org/t/p/w92{tmdb_episode.get('still_path')}"
                                if tmdb_episode.get("still_path")
                                else None
                            ),
                            "episode_type": tmdb_episode.get("episode_type"),
                            "air_date": tmdb_episode.get("air_date"),
                        },
                    )


# ---------------- Main Enrichment Loop ----------------
result_rdd = content_df.rdd.mapPartitions(enrich_partition).cache()

content_detail = result_rdd.filter(lambda item: item[0] == "detail").map(
    lambda item: Row(**item[1])
)
content_person = result_rdd.filter(lambda item: item[0] == "person").map(
    lambda item: Row(**item[1])
)
content_production = result_rdd.filter(lambda item: item[0] == "production").map(
    lambda item: Row(**item[1])
)
content_genre = result_rdd.filter(lambda item: item[0] == "genre").map(
    lambda item: Row(**item[1])
)
content_network = result_rdd.filter(lambda item: item[0] == "network").map(
    lambda item: Row(**item[1])
)
content_interest = result_rdd.filter(lambda item: item[0] == "interest").map(
    lambda item: Row(**item[1])
)
content_season = result_rdd.filter(lambda item: item[0] == "season").map(
    lambda item: Row(**item[1])
)
content_episode = result_rdd.filter(lambda item: item[0] == "episode").map(
    lambda item: Row(**item[1])
)

print("=== COLLECTOR COUNTS ===")
print("content_detail:", content_detail.count())
print("content_person:", content_person.count())
print("content_production:", content_production.count())
print("content_genre:", content_genre.count())
print("content_network:", content_network.count())
print("content_interest:", content_interest.count())
print("content_season:", content_season.count())
print("content_episode:", content_episode.count())

# ---------------- Spark DataFrame ==> S3 (STAGING) ----------------
BUCKET = "s3://oruc-imdb-lake/stg/"

if not content_detail.isEmpty():
    df = spark.createDataFrame(content_detail)
    print("content_detail df count:", df.count())
    df.write.mode("overwrite").parquet(f"{BUCKET}content_detail/")
    print("content_detail written")
if not content_person.isEmpty():
    df = spark.createDataFrame(content_person)
    print("content_person df count:", df.count())
    df.write.mode("overwrite").parquet(f"{BUCKET}content_person/")
    print("content_person written")
if not content_production.isEmpty():
    df = spark.createDataFrame(content_production)
    print("content_production df count:", df.count())
    df.write.mode("overwrite").parquet(f"{BUCKET}content_production/")
    print("content_production written")
if not content_genre.isEmpty():
    df = spark.createDataFrame(content_genre)
    print("content_genre df count:", df.count())
    df.write.mode("overwrite").parquet(f"{BUCKET}content_genre/")
    print("content_genre written")
if not content_network.isEmpty():
    df = spark.createDataFrame(content_network)
    print("content_network df count:", df.count())
    df.write.mode("overwrite").parquet(f"{BUCKET}content_network/")
    print("content_network written")
if not content_interest.isEmpty():
    df = spark.createDataFrame(content_interest)
    print("content_interest df count:", df.count())
    df.write.mode("overwrite").parquet(f"{BUCKET}content_interest/")
    print("content_interest written")
if not content_season.isEmpty():
    df = spark.createDataFrame(content_season)
    print("content_season df count:", df.count())
    df.write.mode("overwrite").parquet(f"{BUCKET}content_season/")
    print("content_season written")
if not content_episode.isEmpty():
    df = spark.createDataFrame(content_episode)
    print("content_episode df count:", df.count())
    df.write.mode("overwrite").parquet(f"{BUCKET}content_episode/")
    print("content_episode written")

job.commit()