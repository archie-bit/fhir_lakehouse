from confluent_kafka import Consumer, KafkaError

conf= {
    'bootstrap.servers': 'localhost:9092',
    'group.id': "test_consume_group",
    'auto.offset.reset': 'earliest'
}
topic_name= 'python_test'

consumer= Consumer(conf)
consumer.subscribe([topic_name])

try:
    while True:
        # 4. The Poll Trigger
        # This waits for up to 1.0 second for a new message
        msg = consumer.poll(1.0)

        if msg is None:
            # No message received within the timeout
            continue
        
        if msg.error():
            # Handle errors (like the topic being deleted)
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        # 5. Process the message
        # msg.value() returns the bytes, we decode them to a string
        data = msg.value().decode('utf-8')
        print(f"Received: {data} from Partition: {msg.partition()} at Offset: {msg.offset()}")

except KeyboardInterrupt:
    pass
finally:
    # 6. Clean up
    # This leaves the group cleanly and saves your final offsets
    consumer.close()