from kafka import KafkaProducer
from json import dumps
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x, default=str).encode('utf-8')
)

# JSON 파일 경로
json_file_path = '/home/kim1/data/custom_sample_data.json'  # JSON 파일 경로로 변경

# JSON 파일을 읽고 메시지를 Kafka로 전송
try:
    with open(json_file_path, 'r') as file:
        data = json.load(file)  # JSON 파일 읽기
        for _ in range(100):
            # 각 항목을 Kafka로 전송
            for item in data:
                producer.send('chat', value=item)  # 'your_topic_name'을 적절한 토픽 이름으로 변경
            
                # 전송 후 약간의 지연을 추가할 수 있습니다 (선택 사항)
                time.sleep(0.1)  # 0.1초 대기
        producer.flush()  # 모든 메시지가 전송되도록 강제
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()  # Producer를 종료
