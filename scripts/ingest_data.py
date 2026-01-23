import requests
import json
import os
from minio import Minio
from datetime import datetime
from io import BytesIO
from config import *

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def ingest_data_to_minio(execution_date):
    # 1. Cấu hình MinIO
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    bucket_name = "data-raw"
    # Tạo bucket nếu chưa có
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # 2. Lấy data từ CheapShark API
    url = "https://www.cheapshark.com/api/1.0/deals"
    params = {
        "onSale": 1,
        "sortBy": "dealRating",
        "pageSize": 60,
        "pageNumber": 0
    }
    response = requests.get(url, params=params)
    data = response.json()

    # 3. Định nghĩa đường dẫn lưu file: data_raw/YYYY-MM-DD/deals.json
    date_str = execution_date  # Airflow sẽ truyền ngày vào đây
    object_name = f"{date_str}/deals.json"
    
    # 4. Upload lên MinIO
    data_bytes = json.dumps(data, indent=4, ensure_ascii=False).encode('utf-8')
    data_stream = BytesIO(data_bytes)
    
    client.put_object(
        bucket_name,
        object_name,
        data_stream,
        length=len(data_bytes),
        content_type='application/json'
    )
    print(f"Uploaded {object_name} to bucket {bucket_name}")

    game_ids = list(set([deal['gameID'] for deal in data]))
    batches = list(chunks(game_ids, 25))

    results = []
    for batch in batches:
        ids_param = ",".join(str(i) for i in batch)
        url_game = f"https://www.cheapshark.com/api/1.0/games"
        params = {
            "ids": ids_param,
            "format": "array"
        }
        resp = requests.get(url_game, params=params)
        results.extend(resp.json())

    game_object_name = f"{date_str}/games.json"
    game_data_bytes = json.dumps(results, indent=4, ensure_ascii=False).encode('utf-8')
    game_data_stream = BytesIO(game_data_bytes)
    
    client.put_object(
        bucket_name,
        game_object_name,
        game_data_stream,
        length=len(game_data_bytes),
        content_type='application/json'
    )
    print(f"Uploaded {game_object_name} to bucket {bucket_name}")

    store_url = "https://www.cheapshark.com/api/1.0/stores"
    store_resp = requests.get(store_url)
    store_data = store_resp.json()
    store_object_name = f"{date_str}/stores.json"

    store_data_bytes = json.dumps(store_data, indent=4, ensure_ascii=False).encode('utf-8')
    store_data_stream = BytesIO(store_data_bytes)

    client.put_object(
        bucket_name,
        store_object_name,
        store_data_stream,
        length=len(store_data_bytes),
        content_type='application/json'
    )
    print(f"Uploaded {store_object_name} to bucket {bucket_name}")

if __name__ == "__main__":
    ingest_data_to_minio(datetime.now().strftime('%Y-%m-%d'))