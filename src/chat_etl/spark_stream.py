from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from urllib.parse import unquote
import os
import sys
ds_nodash = sys.argv[1]

print("========"*100)
print(ds_nodash)

spark = (
    SparkSession
    .builder
    .appName("kafka_streaming")
    .master("local[*]")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
        )


topic_name = "message-topic"
#bootstrap_server = "3.36.52.201:9092"
bootstrap_server = "localhost:9092"
#group_id = "chat-group"

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrap_server) \
  .option("subscribe", topic_name) \
  .load()

#.option("startingOffsets", "latest") \
# JSON 스키마 정의
json_schema = StructType([
    StructField("traceId", StringType(), True),
    StructField("clientIp", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("path", StringType(), True),
    StructField("method", StringType(), True),
    StructField("request", StructType([
        StructField("user_uuid", StringType(), True),
        StructField("message", StringType(), True)
    ])),
    StructField("response", StructType([])),  # 빈 객체 스키마
    StructField("statusCode", StringType(), True),
    StructField("elapsedTimeMillis", IntegerType(), True)
])

# JSON을 DataFrame으로 변환
df = df \
    .withColumn("json_data", from_json(col("value").cast("string"), json_schema)) \
    .selectExpr("json_data.*")  # JSON 필드를 개별 컬럼으로 추출

print(df.count())
df.show()

# 임시로 response 컬럼 추가
df = df.withColumn("response", expr("'tmp'"))  # 모든 행에 'tmp' 값을 추가


# 공통 접두사 구하는 함수
def find_common_prefix(paths):
    return os.path.commonprefix(paths)

# 공통 접두사 추출
common_part = find_common_prefix(df.select("path").rdd.flatMap(lambda x: x).collect())

# 공통된 접두사를 새로운 컬럼 'base_path'로 저장하고, 나머지 부분을 'specific_path'로 분리
df = df.withColumn("base_path", expr(f"'{common_part}'")) \
       .withColumn("specific_path", regexp_replace(col("path"), common_part, ""))

#df = df.drop("path")
print(df.count())
# DataFrame 내용 출력
df.show(truncate=False)

# Parquet 파일로 저장, 'specific_path'를 파티션 컬럼으로 설정
save_dir = f"/home/ubuntu/data/tmp/{ds_nodash}/{common_part}"  # 저장할 디렉토리 경로

df.write.partitionBy("specific_path").parquet(save_dir, mode="overwrite")  # 'overwrite' 모드로 저장
