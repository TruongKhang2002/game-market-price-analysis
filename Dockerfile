FROM apache/airflow:2.10.2

USER root

# Cài đặt OpenJDK-17 và procps (cần thiết cho Spark driver)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH=/home/airflow/.local/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Tạo thư mục chứa jars (Spark/Hadoop thường đọc ở đây)
# Thay đổi đường dẫn để khớp với cấu hình trong DAG và script (/opt/airflow/jars-custom)
RUN mkdir -p /opt/airflow/jars-custom

# Tải AWS + Hadoop S3 connector
RUN curl -L -o /opt/airflow/jars-custom/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -L -o /opt/airflow/jars-custom/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && chown -R airflow:root /opt/airflow/jars-custom

# (Tuỳ chọn) add vào CLASSPATH để Spark / Hadoop / Airflow thấy
ENV CLASSPATH=/opt/airflow/jars-custom/*

USER airflow

# Copy file requirements và cài đặt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PATH=/home/airflow/.local/bin:$PATH