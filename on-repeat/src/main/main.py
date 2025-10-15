from kafka import KafkaConsumer, KafkaProducer
import json

KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC_NAME = 'playlists.auto-generation.tracks'
OUTPUT_TOPIC_NAME = 'playlists.auto-generation.images'

consumer = KafkaConsumer(
    INPUT_TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='playlists.auto.images'
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

IMAGES = [
    {
        "url": "https://pickasso.spotifycdn.com/image/ab67c0de0000deef/dt/v1/img/repeat/or/en",
        "width": "600",
        "height": "600"
    }
]

try:
    for message in consumer:
        if message.value["type"] != 'ON_REPEAT':
            pass

        message.value["images"] = IMAGES
        producer.send(OUTPUT_TOPIC_NAME, message.value)
finally:
    consumer.close()
