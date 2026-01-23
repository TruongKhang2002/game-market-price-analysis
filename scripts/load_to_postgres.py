import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
import io
import os
from config import *

def load_data_to_postgres():
    # 1. Kết nối MinIO
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # 2. Kết nối Postgres (Data Warehouse)
    engine = create_engine(f'postgresql+psycopg2://{POSTGRES_DW_USER}:{POSTGRES_DW_PASSWORD}@{POSTGRES_DW_HOST}:{POSTGRES_DW_PORT}/{POSTGRES_DW_DB}')

    # Lấy danh sách deal_id và date đã tồn tại trong Postgres để tránh trùng lặp
    existing_keys = set()
    try:
        print("Fetching existing keys from Postgres...")
        query = "SELECT deal_id, date FROM game_prices"
        df_existing = pd.read_sql(query, engine)
        # Chuyển sang string để so sánh đồng nhất
        df_existing['deal_id'] = df_existing['deal_id'].astype(str)
        df_existing['date'] = df_existing['date'].astype(str)
        existing_keys = set(zip(df_existing['deal_id'], df_existing['date']))
        print(f"Found {len(existing_keys)} existing records.")
    except Exception as e:
        print(f"Could not fetch existing data (Table might not exist yet): {e}")

    bucket_name = "data-process"
    prefix = "game_prices/" # Thư mục chứa các file csv part

    # 3. List toàn bộ file CSV trong bucket đã xử lý
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)

    for obj in objects:
        if obj.object_name.endswith('.csv'):
            print(f"Loading {obj.object_name}...")
            
            # Đọc file từ MinIO vào Pandas
            response = minio_client.get_object(bucket_name, obj.object_name)
            df = pd.read_csv(io.BytesIO(response.read()))
            
            # Chuẩn hóa kiểu dữ liệu để so sánh
            df['deal_id'] = df['deal_id'].astype(str)
            df['date'] = df['date'].astype(str)

            # Lọc dữ liệu: Chỉ lấy những dòng có (deal_id, date) chưa tồn tại trong existing_keys
            # Tạo cột tuple tạm để kiểm tra
            df['key_check'] = list(zip(df['deal_id'], df['date']))
            df_new = df[~df['key_check'].isin(existing_keys)].drop(columns=['key_check'])

            if not df_new.empty:
                # 4. Insert vào Postgres
                df_new.to_sql('game_prices', engine, if_exists='append', index=False, chunksize=1000)
                print(f"Loaded {len(df_new)} new rows to Postgres.")
                
                # Cập nhật existing_keys với dữ liệu vừa thêm
                existing_keys.update(set(zip(df_new['deal_id'], df_new['date'])))
            else:
                print("No new rows to load.")
            
if __name__ == "__main__":
    load_data_to_postgres()