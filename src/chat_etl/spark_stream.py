from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = (
    SparkSession 
    .builder 
    .appName("kafka_streaming") 
    .master("local[*]") 
    .getOrCreate()
	)


topic_name = "chat"
#bootstrap_server = "3.36.52.201:9092"
bootstrap_server = "localhost:9092"
#group_id = "chat-group"  

df = spark \
  .read \
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
