# Written in Python 3.7
# LibrdKafka 1.4.1
# Confluent-Kafka-Python

import random
import argparse
import multiprocessing
import sys
from protobuf import FoodPreferences_pb2
from uuid import uuid4
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


def generateRecord():
    names = ["Abraham","Valeria","Jay","Laurent","Gemma","Scott"]
    foods = ["Hot Dog","Pizza","Hamburger","Pasta","Chicken","Noodles","Ramen","Sushi"]
    singlePerson = FoodPreferences_pb2.PersonFood(
        name=str(names[random.randint(0, len(names)-1)]),
        favoriteFood=str(foods[random.randint(0, len(foods)-1)]),
        timesEaten=int(random.randint(1, 100))
    )

    return singlePerson

def getConfigs():

    sr_client_props = {
        'url': '<CCLOUD_SR_DNS>',
        'basic.auth.user.info': '<CCLOUD_SR_KEY>:<CCLOUD_SR_SECRET>'
    }

    sr_client = SchemaRegistryClient(sr_client_props)
    value_serializer = ProtobufSerializer(FoodPreferences_pb2.PersonFood, sr_client)

    configs = {
        'bootstrap.servers': '<CCLOUD_DNS>',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '<API_KEY>',
        'sasl.password': '<API_SECRET>',
        'client.id': 'pythonProduction',
        'compression.type': 'zstd',
        'retries': '10',
        'linger.ms': '5',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': value_serializer
    }

    return configs

def getReport(err,msg):
    if err is not None:
        print("Delivery failed for a record with error {}".format(err.str()))
    else:
        print("Successfully produced a record")

    return

# Production loop that allows for multi-processing calls.
def produce (goal):
    count = 0
    reusableProducer = SerializingProducer(getConfigs())
    while (count < goal):
        try:
            reusableProducer.produce(topic='myprototopic',
            key=str(uuid4()),
                value=generateRecord(),
                      on_delivery=getReport)
            # print("In process:{}".format(multiprocessing.current_process().name))
            reusableProducer.poll(0.0)
        except KeyboardInterrupt:
            break
        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): flushing...\n' %
                             len(reusableProducer))
            reusableProducer.flush()

    print("Flushing one producer thread")
    reusableProducer.flush()

if __name__ == '__main__':
    consoleParser = argparse.ArgumentParser(description='Parsing options for Python producer')
    consoleParser.add_argument('-g', dest="goal", required=True,help="How many records do you want to produce?")
    consoleParser.add_argument('-t', dest="threads", required=True, help="How many threads do you want to allocate? NOTE:"
                                                                         "this utilizes multiprocessing, which is capped by the "
                                                                         "amount of cores are in the machine." +
                               "Your core count is:" + str(multiprocessing.cpu_count()))

    try:
        noOfThreads = int(consoleParser.parse_args().threads)
        splitGoal = int(consoleParser.parse_args().goal) / noOfThreads
    except:
        print("You need to specify -g (goal number of records to produce) and -t (number of cores to use, max: {}) ".format(str(multiprocessing.cpu_count())))

    elasticProcessingList =[]

    # Start producing through multiple cores
    for i in range(noOfThreads):
        thread = multiprocessing.Process(target=produce,args=(splitGoal,), name=str(i))
        elasticProcessingList.append(thread)
        thread.start()

    for i in elasticProcessingList:
        i.join()







