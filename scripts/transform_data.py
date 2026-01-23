from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit, to_date
from functools import reduce
import sys
import os
import traceback
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import *

try:
    # Cấu hình Spark Session kết nối MinIO
    jar_dir = "/opt/airflow/jars-custom" # Đường dẫn trong container Spark
    jars = [
        f"{jar_dir}/hadoop-aws-3.3.4.jar",
        f"{jar_dir}/aws-java-sdk-bundle-1.12.262.jar"
    ]

    spark = SparkSession.builder \
        .appName("GamePriceETL") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # 1. Xác định ngày mới nhất trong Master File hiện tại
    target_dir = "s3a://data-process/game_prices"
    target_file_name = "master_game_prices.csv"
    full_target_path = f"{target_dir}/{target_file_name}"

    # Sử dụng Hadoop FileSystem API để thao tác file
    sc = spark.sparkContext
    uri = sc._jvm.java.net.URI(target_dir)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, sc._jsc.hadoopConfiguration())
    target_path_obj = sc._jvm.org.apache.hadoop.fs.Path(full_target_path)

    latest_date = "1900-01-01" # Giá trị mặc định nếu chưa có file master
    df_existing = None

    if fs.exists(target_path_obj):
        print(f"Found existing master file at {full_target_path}. Checking latest date...")
        df_existing = spark.read.option("header", "true").csv(full_target_path)
        # Lấy ngày lớn nhất trong file cũ
        try:
            row = df_existing.agg({"date": "max"}).collect()[0]
            if row[0]:
                latest_date = row[0]
        except Exception as e:
            print(f"Warning: Could not determine max date from existing file: {e}")
        print(f"Latest date in master: {latest_date}")
    else:
        print("No existing master file found. Will process all available data.")

    # 2. Quét bucket data_raw để lấy danh sách các ngày cần xử lý
    raw_bucket_path = "s3a://data-raw/"
    raw_path_obj = sc._jvm.org.apache.hadoop.fs.Path(raw_bucket_path)
    
    # Lấy FileSystem riêng cho bucket data-raw vì fs hiện tại đang trỏ vào data-process
    raw_uri = sc._jvm.java.net.URI(raw_bucket_path)
    raw_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(raw_uri, sc._jsc.hadoopConfiguration())
    file_statuses = raw_fs.listStatus(raw_path_obj)

    dates_to_process = []
    for status in file_statuses:
        if status.isDirectory():
            dir_name = status.getPath().getName()
            # Logic: Chỉ xử lý nếu ngày thư mục > ngày mới nhất trong master
            if dir_name > latest_date:
                dates_to_process.append(dir_name)

    dates_to_process.sort()
    print(f"Dates to process: {dates_to_process}")

    if not dates_to_process:
        print("No new data to process. Exiting.")
        spark.stop()
        sys.exit(0)

    new_dfs = []

    # 3. Lặp và xử lý từng ngày
    for execution_date in dates_to_process:
        print(f"Processing data for date: {execution_date}")
        
        deal_raw_path = f"s3a://data-raw/{execution_date}/deals.json"
        game_raw_path = f"s3a://data-raw/{execution_date}/games.json"
        store_raw_path = f"s3a://data-raw/{execution_date}/stores.json"

        deal_df = spark.read.option("multiline", "true").json(deal_raw_path)
        game_df = spark.read.option("multiline", "true").json(game_raw_path)
        store_df = spark.read.option("multiline", "true").json(store_raw_path)
        
        # Transform
        store_dim = store_df.select(
            col("storeID").alias("store_id"),
            col("storeName").alias("store_name"),
        )

        game_dim = game_df.select(
            col("info.gameID").alias("game_id"),
            col("cheapestPriceEver.price").alias("cheapest_price_ever"),
            col("cheapestPriceEver.date").cast("timestamp").cast("date").alias("cheapest_price_ever_date")
        )

        deal_dim = deal_df.select(
            col("dealID").alias("deal_id"),
            col("gameID").alias("game_id"),
            col("title").alias("game_title"),
            col("isOnSale").cast("boolean").alias("is_on_sale"),
            col("normalPrice").cast("float").alias("original_price"),
            col("salePrice").cast("float").alias("sale_price"),
            col("metacriticScore").cast("int").alias("metacritic_score"),
            col("steamRatingText").alias("rating_text"),
            col("steamRatingPercent").cast("int").alias("rating"),
            col("dealRating").cast("float").alias("deal_score"),
            col("releaseDate").cast("timestamp").cast("date").alias("release_date"),
            col("storeID").alias("store_id"),
            col("thumb").alias("thumbnail_url")
        )

        fact_df = deal_dim.join(game_dim, "game_id", "left").join(store_dim, "store_id", "left")

        # Thêm cột ngày xử lý
        df_final = fact_df.withColumn("date", to_date(lit(execution_date), "yyyy-MM-dd"))
        
        # Cast sang String để tương thích với CSV master và thêm vào list
        df_final_str = df_final.select([col(c).cast("string") for c in df_final.columns])
        new_dfs.append(df_final_str)

    # 4. Gộp dữ liệu và ghi file
    if new_dfs:
        # Gộp tất cả các ngày mới lại với nhau
        df_new_combined = reduce(lambda df1, df2: df1.unionByName(df2), new_dfs)
        
        # Gộp với dữ liệu cũ (nếu có)
        if df_existing:
            print("Appending new data to existing master file...")
            df_combined = df_existing.unionByName(df_new_combined)
        else:
            print("Creating new master file...")
            df_combined = df_new_combined
    
    # Ghi ra thư mục tạm
    temp_dir = f"s3a://data-process/temp/game_prices_update_{dates_to_process[-1]}"
    # Ghi ra thư mục tạm (coalesce(1) để đảm bảo chỉ sinh ra 1 file part)
    df_combined.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(temp_dir)
    
    # Tìm file part vừa sinh ra trong thư mục tạm
    part_file = fs.globStatus(sc._jvm.org.apache.hadoop.fs.Path(f"{temp_dir}/part-*.csv"))[0].getPath()
    
    # Xóa file master cũ (nếu có) và đổi tên file tạm thành file master
    if fs.exists(target_path_obj):
        fs.delete(target_path_obj, True)
        
    fs.rename(part_file, target_path_obj)
    
    # Dọn dẹp thư mục tạm
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(temp_dir), True)
    
    print(f"Successfully updated master file at {full_target_path}")

except Exception as e:
    print(f"Error processing data: {e}")
    traceback.print_exc()
    sys.exit(1)

spark.stop()