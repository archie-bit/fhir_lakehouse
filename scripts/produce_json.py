from confluent_kafka import Producer
import random
import os
import json


def get_patient_key(resource):
    if resource.get('resourceType') == 'Patient':
        return resource.get('id')
    
    ref_obj = resource.get('patient') or resource.get('subject')
    if isinstance(ref_obj, dict):
        ref = ref_obj.get("reference")
        if ref:
            return ref.split(':')[-1].split('/')[-1]
    
    return None 

def init_producer(config):
    """
    Creates an instance of a producer
    """
    return Producer(config)

def produce_message(producer, topic, message):
    """
    Docstring for produce_message
    
    :param topic: name of topic
    :param message: message to be produced
    """
    pkey = get_patient_key(message)
    resource_type = [('resource_type', message.get('resourceType', '').encode('utf-8'))]
    while True:
        try: 
            producer.produce(topic, 
                            value= json.dumps(message).encode("utf-8"),
                            key=pkey,
                            headers=resource_type,)
            break
        except BufferError:
            producer.poll(1)
        except Exception as e:
            print(e)
            break
    producer.poll(0)

if __name__ == '__main__':
    bootstrap_server= 'localhost:19092'
    config= {'bootstrap.servers': bootstrap_server,
             'compression.type': 'gzip',
             }
    producer= init_producer(config)
    topic= 'test0'


    files_path='./data/raw/'
    files=[os.path.join(files_path, file) for file in os.listdir(files_path)
           if file.endswith('.json')]

    try:
        while True:
            rand_file= random.choice(files)
            print(rand_file)
            with open(rand_file, 'r') as f:
                f=json.load(f)
                if f.get("resourceType") == "Bundle":
                    print(f'{len(f["entry"])} entries')
                    entries= f.get('entry', [])
                    # print(random_entry.get('resource'))
                    # print(entries[0])
                    # for entry in entries:
                    patient= entries[0].get('resource')
                    random_entry= random.choice(entries)
                    produce_message(producer, topic, patient)

                    resource = random_entry.get('resource')
                    produce_message(producer, topic, resource)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()



