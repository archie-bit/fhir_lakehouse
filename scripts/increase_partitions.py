from confluent_kafka.admin import AdminClient, NewPartitions

conf = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(conf)

topic_name = "python_test"
new_total_count = 2

partitions_request = [NewPartitions(topic_name, new_total_count)]

fs = admin_client.create_partitions(partitions_request)

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic '{topic}' now has {new_total_count} partitions.")
    except Exception as e:
        print(f"Failed to add partitions to '{topic}': {e}")