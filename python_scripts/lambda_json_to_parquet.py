"""
AWS Lambda Function: JSON to Parquet Converter

Converts a JSON array (from Step Functions or S3) into Parquet format
and writes it to S3. Used for both stg_contentIDs and stg_popularity.

Dependencies (Lambda Layer):
    - pandas
    - pyarrow
"""

import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Accepts records either inline or from an S3 JSON file, converts to
    Parquet, and writes to the specified S3 location.

    Event format (inline records):
    {
        "bucket": "oruc-imdb-lake",
        "output_key": "raw/stg_contentIDs/data.parquet",
        "records": [
            {"id": "tt1234567", "type": "movie", "tmdb_id": "123"},
            {"id": "tt7654321", "type": "tv", "tmdb_id": "456"}
        ]
    }

    Event format (read from S3):
    {
        "bucket": "oruc-imdb-lake",
        "input_key": "raw/stg_contentIDs/data.json",
        "output_key": "raw/stg_contentIDs/data.parquet"
    }
    """
    
    try:
        # Extract parameters
        bucket = event.get('bucket', 'oruc-imdb-lake')
        output_key = event.get('output_key')
        input_key = event.get('input_key')
        
        if not output_key:
            return {
                'statusCode': 400,
                'body': json.dumps('output_key is required')
            }
        
        # Get records from event payload or from S3
        if input_key:
            response = s3_client.get_object(Bucket=bucket, Key=input_key)
            json_content = response['Body'].read().decode('utf-8')
            records = json.loads(json_content)
        else:
            records = event.get('records', [])
        
        if not records:
            return {
                'statusCode': 400,
                'body': json.dumps('No records found in input')
            }
        
        # Flatten nested arrays (Step Functions may send nested arrays from Map states)
        flat_records = []
        for item in records:
            if isinstance(item, list):
                flat_records.extend(item)
            else:
                flat_records.append(item)
        
        # Convert to Parquet in memory
        df = pd.DataFrame(flat_records)
        table = pa.Table.from_pandas(df)
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer, compression='snappy')
        
        # Write to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully wrote parquet file',
                'location': f's3://{bucket}/{output_key}',
                'record_count': len(flat_records)
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
