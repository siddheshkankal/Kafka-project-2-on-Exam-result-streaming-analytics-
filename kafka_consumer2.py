import argparse
import os.path
import csv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


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

# *************** function to write data export to abn excel file which ever data subscribed by consumer ***************
def export_data_excel(header,data,path):
    file_exists = os.path.isfile(path)

    with open(path,'a', newline = '') as f:
        w = csv.DictWriter(f,fieldnames= header)
        if not file_exists:
            w.writeheader()
        w.writerow(data)
        
def main(topic):
# info -->******************  hardcoded schema ******************

#     schema_str = """
# {
#   "$id": "http://example.com/myURI.schema.json",
#   "$schema": "http://json-schema.org/draft-07/schema#",
#   "additionalProperties": false,
#   "description": "Sample schema to help you get started.",
#   "properties": {  					
#     "Order Number": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Order Date": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "Item Name": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "Quantity": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Product Price": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Total products": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     }
#   },
#   "title": "SampleRecord",
#   "type": "object"
# }
#     """

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Info --> ****************** approch 1 to get schema from schema registry using subject ******************
    # subjects = schema_registry_client.get_subjects()
    # for sub in subjects:
    #     if sub == 'student-exams-data-value':
    #         schema = schema_registry_client.get_latest_version(sub)
    #         schema_str = schema.schema.schema_str

# Info --> ****************** approch 2 to get schema from schema registry using schema id ******************

    schema_str = schema_registry_client.get_schema(schema_id = 100004).schema_str       
    
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Exam.dict_to_exam)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group2',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    counter = 0
    # path = './second_consumer/output.csv'
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            exam = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if exam is not None:
                print("User record {}: exam: {}\n"
                      .format(msg.key(), exam))
                header = list(exam.record.keys())
                data = (exam.record)
                # 'math score','reading score','writing score'
                if exam.record['group'] == 'group A':
                    if exam.record['math score'] >60 and exam.record['reading score'] > 60 and  exam.record['writing score'] > 60:
                        export_data_excel(header,data,'./second_consumer/group_A_passed_students.csv')
                    else:
                        export_data_excel(header,data,'./second_consumer/group_A_failed_students.csv')
   
                if exam.record['group'] == 'group B':
                    if exam.record['math score'] >60 and exam.record['reading score'] > 60 and  exam.record['writing score'] > 60:
                        export_data_excel(header,data,'./second_consumer/group_B_passed_students.csv')
                    else:
                        export_data_excel(header,data,'./second_consumer/group_B_failed_students.csv')
                       
                if exam.record['group'] == 'group C':
                    if exam.record['math score'] >60 and exam.record['reading score'] > 60 and  exam.record['writing score'] > 60:
                        export_data_excel(header,data,'./second_consumer/group_C_passed_students.csv')
                    else:
                         export_data_excel(header,data,'./second_consumer/group_C_failed_students.csv')
               
                if exam.record['group'] == 'group D':
                    if exam.record['math score'] >60 and exam.record['reading score'] > 60 and  exam.record['writing score'] > 60:
                        export_data_excel(header,data,'./second_consumer/group_D_passed_students.csv')
                    else:
                        export_data_excel(header,data,'./second_consumer/group_D_failed_students.csv')

                if exam.record['group'] == 'group E':
                    if exam.record['math score'] >60 and exam.record['reading score'] > 60 and  exam.record['writing score'] > 60:
                        export_data_excel(header,data,'./second_consumer/group_E_passed_students.csv')
                    else:
                        export_data_excel(header,data,'./second_consumer/group_E_failed_students.csv')

                
                counter+= 1
            print(f'totoal number of recorded subscribed are : {counter}')

        except KeyboardInterrupt:
            break

    consumer.close()

main("student-exams-data")