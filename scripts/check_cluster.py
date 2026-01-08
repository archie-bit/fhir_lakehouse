from confluent_kafka.admin import AdminClient

conf = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(conf)

topic_name = "python_test"
metadata = admin_client.list_topics(topic=topic_name, timeout=10)

if topic_name in metadata.topics:
    topic_info = metadata.topics[topic_name]
    num_partitions = len(topic_info.partitions)
    print(f"Topic '{topic_name}' has {num_partitions} partition(s).")
    
    # You can even see which broker is the 'leader' for each partition
    for p_id, p_info in topic_info.partitions.items():
        print(f"  - Partition {p_id} leader is Broker {p_info.leader}")
else:
    print("Topic not found.")