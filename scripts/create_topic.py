from confluent_kafka.admin import AdminClient, NewTopic

def CreateTopic(bootstrap_server, topics):
    print(topics)
    conf= {"bootstrap.servers": bootstrap_server}
    admin= AdminClient(conf)

    existing_metadata = admin.list_topics(timeout=10)
    existing_topics = existing_metadata.topics.keys()

    new_topics_to_create = [
        NewTopic(topic, num_partitions=1, replication_factor=1) 
        for topic in topics if topic not in existing_topics
    ]

    if new_topics_to_create:
        t= admin.create_topics(new_topics_to_create)

        for topic, future in t.items():
            try:
                future.result()
                print(f"topic '{topic}' created successfully")
            except Exception as e:
                print(f"failed to create topic '{topic}' due to: {e}")
    else:
        print("topics already exist")

    # for topic in topics:
    #     if topic not in admin.list_topics().topics.keys():
    #         admin.create_topics([topic])
    #     else:
    #         print(f"{topic} already exits")

# if __name__ == "__main__":
    # bootstrap_servers= "localhost:19092"
    # conf= {"bootstrap.servers": bootstrap_servers}
    # admin= AdminClient(conf)
