from confluent_kafka import Consumer
import time
import json
import os
import pandas as pd


def save_to_bronze(batch_data, batch_id):
    """
    Docstring for save_to_bronze
    
    :param batch_data: Description
    :param batch_id: Description
    """
    if not batch_data:
        return
    df= pd.DataFrame(batch_data)
    filename = f"data/bronze/fhir_batch_{batch_id}_{int(time.time())}.parquet"
    df["FILENAME"]= filename
    df= df.astype(str)
    df.to_parquet(filename, index=False)
    print(f"Saved {len(batch_data)} resources to {filename}")

def consumer_loop(consumer:object, topic_list:list, batchsize= 100, timeout=10.0):
    """
    Docstring for consumer_loop
    
    :param consumer : consumer class
    :param topic_list: list of topice consumer subscribes to
    :param bucketsize: maximum size of items 
    :param timeout: maximum size of idle time 
    """
    consumer.subscribe(topic_list)
    
    batch_data=[]
    batch_count = 0
    last_flush = time.time()

    try:
        while True:


            msg = consumer.poll(1.0)

            if msg is None:
                if batch_data and (time.time() - last_flush) > timeout:
                    save_to_bronze(batch_data, batch_count)
                    batch_data = []
                    batch_count += 1
                continue

            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            
            resource = json.loads(msg.value().decode('utf-8'))
            header= msg.headers()
            batch_data.append({"RESOURCE_TYPE": header[0][1],
                               "RAW_JSON":resource,
                               "INGESTED_AT": msg.timestamp()[1],
                               })
        
            if len(batch_data) >= batchsize:
                save_to_bronze(batch_data, batch_count)
                batch_data = []
                batch_count += 1
                last_flush = time.time()

    except KeyboardInterrupt:
        pass
    finally: 
        consumer.close()

if __name__ == "__main__":
    os.makedirs('data/bronze', exist_ok=True)

    bootstrap_servers= 'localhost:19092'
    consumer_group= 'patinent_info'
    topic_list= ['test0']
    config= {'bootstrap.servers': bootstrap_servers,
           'group.id': consumer_group,
           'auto.offset.reset': 'earliest'}

    consumer= Consumer(config)

    # print(type(consumer))
    consumer_loop(consumer, topic_list)
    
