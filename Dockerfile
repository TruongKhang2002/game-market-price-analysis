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


USER airflow

# Copy file requirements và cài đặt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PATH=/home/airflow/.local/bin:$PATH