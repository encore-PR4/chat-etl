from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, expr
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
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
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrap_server) \
  .option("assign", '{"message-topic":[0]}') \
  .load()

#.option("startingOffsets", "latest") \
# JSON 스키마 정의
json_schema = StructType([
    StructField("traceId", StringType(), True),
    StructField("clientId", StringType(), True),
    StructField("time", ArrayType(IntegerType()), True),
    StructField("path", StringType(), True),
    StructField("method", StringType(), True),
    StructField("request", StructType([
        StructField("content", StringType(), True),
        StructField("senderId", IntegerType(), True),
        StructField("chatRoomId", IntegerType(), True)
    ]), True),
    StructField("response", StructType([
        StructField("id", IntegerType(), True),
        StructField("content", StringType(), True),
        StructField("senderId", IntegerType(), True),
        StructField("senderName", StringType(), True),
#        StructField("senderAvatar", StringType(), True),
        StructField("timestamp", ArrayType(IntegerType()), True)
    ]), True),
    StructField("status", IntegerType(), True)
])

# JSON을 DataFrame으로 변환
df = df \
    .withColumn("json_data", from_json(col("value").cast("string"), json_schema)) \
    .selectExpr("json_data.*")  # JSON 필드를 개별 컬럼으로 추출

# 예시 DataFrame (df)에 새로운 칼럼 추가
df = df.withColumn("request_content", df["request"]["content"]) \
       .withColumn("request_senderId", df["request"]["senderId"]) \
       .withColumn("request_chatRoomId", df["request"]["chatRoomId"]) \
       .withColumn("response_id", df["response"]["id"]) \
       .withColumn("response_content", df["response"]["content"]) \
       .withColumn("response_senderId", df["response"]["senderId"]) \
       .withColumn("response_senderName", df["response"]["senderName"]) \
       .withColumn("response_timestamp", df["response"]["timestamp"]) 

# request와 response 칼럼 삭제
df = df.drop("request", "response")



# time 배열의 모든 요소를 결합하여 datetime으로 변환하기
df = df.withColumn(
    "time",
    F.to_timestamp(
        F.concat_ws("-", 
            F.col("time").getItem(0),  # 연도
            F.lpad(F.col("time").getItem(1), 2, '0'),  # 월을 두 자리로 맞추기
            F.lpad(F.col("time").getItem(2), 2, '0'),  # 일을 두 자리로 맞추기
            F.lpad(F.col("time").getItem(3), 2, '0'),  # 시를 두 자리로 맞추기
            F.lpad(F.col("time").getItem(4), 2, '0'),  # 분을 두 자리로 맞추기
            F.lpad(F.col("time").getItem(5), 2, '0')   # 초를 두 자리로 맞추기
        ), 'yyyy-MM-dd-HH-mm-ss')
)

# response_timestamp 배열의 모든 요소에 lpad를 적용하여 datetime으로 변환하기
df = df.withColumn(
    "response_timestamp",
    F.to_timestamp(
        F.concat_ws("-",
            F.col("response_timestamp").getItem(0),  # 연도
            F.lpad(F.col("response_timestamp").getItem(1), 2, '0'),  # 월을 두 자리로 맞추기
            F.lpad(F.col("response_timestamp").getItem(2), 2, '0'),  # 일을 두 자리로 맞추기
            F.lpad(F.col("response_timestamp").getItem(3), 2, '0'),  # 시를 두 자리로 맞추기
            F.lpad(F.col("response_timestamp").getItem(4), 2, '0'),  # 분을 두 자리로 맞추기
            F.lpad(F.col("response_timestamp").getItem(5), 2, '0')   # 초를 두 자리로 맞추기
        ), 'yyyy-MM-dd-HH-mm-ss')
)


print("======="*100)
df.show()
print(df.count())

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

df.write.partitionBy("request_senderId").parquet(save_dir, mode="overwrite")  # 'overwrite' 모드로 저장
