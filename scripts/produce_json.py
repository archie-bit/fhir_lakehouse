import os
import json
from confluent_kafka import Producer, error

conf= {
    'bootstrap.servers': 'localhost:9092',
    'message.max.bytes': 50485760,
    'compression.type': 'gzip',
    # 'queue.buffering.max.messages': 100000,
    # 'queue.buffering.max.kbytes': 1048576,
}
topic_name= "fhir_test"
file_path= 'data/raw/'
counter = 0
l= [os.path.join(file_path, file) for file in os.listdir(file_path) if file.endswith('.json')]

producer= Producer(conf)

def get_patient_key(resource):
    if resource.get('resourceType') == 'Patient':
        return resource.get('id')
    
    # if resource.get('resourceType') in ['Claim','ExplanationOfBenefit']:
    #     resource.get('patient', {})
    # else:    
    #     subject = resource.get('subject', {})
    #     ref = subject.get('reference')
    ref_obj = resource.get('patient') or resource.get('subject')
    if isinstance(ref_obj, dict):
        ref = ref_obj.get("reference")
        if ref:
            return ref.split(':')[-1].split('/')[-1]
    
    return None 


def delivery_report(err, msg):
    if err is not None:
        print(f"Error sending message: {err}")
    else:
        pass


def send_to_kafka(resource):
    pkey = get_patient_key(resource)
    resource_type = [('resource_type', resource.get('resourceType', '').encode('utf-8'))]
    while True:
        try:
            producer.produce(
                topic_name,
                key=pkey,
                value=json.dumps(resource).encode("utf-8"),
                on_delivery=delivery_report,
                headers= resource_type
            )
            break
        except BufferError:
            producer.poll(1)
        except Exception as e:
            print(e)
            break

    producer.poll(0)

for i in range(3):
    with open(l[i], 'r') as f:
        bundle = json.load(f)
        
        if bundle.get('resourceType') == 'Bundle':
            print(f"Processing Bundle: {l[i]} with {len(bundle['entry'])} resources")
            entries = bundle.get('entry', [])
            for entry in entries:
                resource = entry.get('resource')
                if resource:
                    send_to_kafka(resource)
        else:
            print(f"Processing Single Resource: {l[i]}")
            send_to_kafka(bundle)
        
producer.flush()
print("Finished producing all messages.")