from confluent_kafka import Producer
conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(conf)

topic = "python_test"

def delivery_report(err, msg):
    if err is not None:
        print(f"Error sending message: {err}")
    else:
        print(f"Message produced to topic '{msg.topic()}' at offset {msg.offset()}")

for i in range(1, 11):
  msg = f"Order with id #{i}"
  producer.produce(
        topic, 
        value=msg.encode('utf-8'), 
        on_delivery=delivery_report
    )
  producer.poll(0)

producer.flush()
