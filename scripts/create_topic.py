from confluent_kafka.admin import AdminClient

def CreateTopic(bootstrap_server, *topics):
    print(topics)
    for topic in topics:
        if topic not in admin.list_topics().topics.keys():
            admin.create_topics([topic])
        else:
            print(f"{topic} already exits")

if __name__ == "__main__":
    bootstrap_servers= "localhost:19092"
    conf= {"bootstrap.servers": bootstrap_servers}
    admin= AdminClient(conf)
