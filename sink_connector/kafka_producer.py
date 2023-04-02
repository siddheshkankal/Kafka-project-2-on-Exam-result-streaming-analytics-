


# A simple example demonstrating use of JSONSerializer.

import argparse
from msilib.schema import Registry
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = r"I:\notes\kafka\Kafka_project2\exams.csv"
columns=['gender','group','parental level of education','lunch','test preparation course','math score','reading score','writing score']


API_KEY = 'UWNWP4ER6EPLO356'
ENDPOINT_SCHEMA_URL  = 'https://psrc-mw2k1.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'A1G/Q/R0NK9Tyo+PC8xfmngQuZHn/FJM+XHZW1NBOhE3NUgNf8QITq3h15pO6ThE'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'UJ3LZ3TPAF3IGBTC'
SCHEMA_REGISTRY_API_SECRET = 'qfY+TKn+k7cjxcMEzkWMvz67B1a7pSnaePFMQBwsOcJiPhjR5nwniN+aVg4I4FAZ'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Exam:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_exam(data:dict,ctx):
        return Exam(record=data)

    def __str__(self):
        return f"{self.record}"


def get_exam_instance(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,:]
    exams:List[Exam]=[]
    for data in df.values:
        exam=Exam(dict(zip(columns,data)))
        exams.append(exam)
        yield exam

def exam_to_dict(exam:Exam, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return exam.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
#*************************** start schema is hardcoded *************************** 
#     schema_str = """
#     {
#   "$id": "http://example.com/myURI.schema.json",
#   "$schema": "http://json-schema.org/draft-07/schema#",
#   "additionalProperties": false,
#   "description": "Sample schema to help you get started.",
#   "properties": {
#     "Item Name": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "gender": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "group": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "parental level of education": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "lunch": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "test preparation course": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "math score": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "reading score": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "writing score": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     }
#   },
#   "title": "SampleRecord",
#   "type": "object"
# }
#     """

#*************************** end  hardcoded  schema *************************** 

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

#********************  first approact to get schema from schema Registry using subjects  ********************
    # subjects = schema_registry_client.get_subjects()
    # for sub in subjects:
    #     if sub == 'student-exams-data-value':
    #         schema = schema_registry_client.get_latest_version(sub)
    #         schema_str = schema.schema.schema_str

#********************  second  approact to get schema from schema Registry using schema_id  ********************    
    schema_str = schema_registry_client.get_schema(schema_id = 100004).schema_str


    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, exam_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)

    # info --> counter is used to know how many records published to the topic 
    counter = 0
    try:
        for exam in get_exam_instance(file_path=FILE_PATH):
        
            print(exam)
            producer.produce(topic=topic,
                                key=string_serializer(str(uuid4()), exam_to_dict),
                                value=json_serializer(exam, SerializationContext(topic, MessageField.VALUE)),
                                on_delivery=delivery_report)
            counter += 1

            # info --> loop is break when counter is 5 because we want to publish only 5 records as of now
            if counter == 1:
                break

        # info --> printing at the end how many records got published successfully 
        print(f'totoal number of recorded published are : {counter}')

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, disrestaurantding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("student-exams-data")
