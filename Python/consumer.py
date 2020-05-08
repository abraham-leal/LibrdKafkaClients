# Written in Python 3.7
# LibrdKafka 1.4.1
# Confluent-Kafka-Python

import argparse
import multiprocessing
from protobuf import FoodPreferences_pb2
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer



def getConfigs():
    value_deserializer = ProtobufDeserializer(FoodPreferences_pb2.PersonFood)

    configs = {
        'bootstrap.servers': '<CCLOUD_DNS>',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '<API_KEY>',
        'sasl.password': '<API_SECRET>',
        'group.id': 'consumingPythonWorld',
        'client.id': 'pythonConsumption',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': value_deserializer
    }

    return configs

# Production loop that allows for multi-processing calls.
def consume ():
    reusableConsumer = DeserializingConsumer(getConfigs())
    reusableConsumer.subscribe(["myprototopic"])
    while (True):
        try:
            msg = reusableConsumer.poll(0.1)

            if msg is None:
                continue
            else:
                key = msg.key()
                value = msg.value()
                print("Received msg name: {}, fav food: {}, times eaten: {}".format(value.name, value.favoriteFood, value.timesEaten))

        except KeyboardInterrupt:
            break

    print("Closing Consumer")
    reusableConsumer.close()

if __name__ == '__main__':
    consoleParser = argparse.ArgumentParser(description='Parsing options for Python producer')
    consoleParser.add_argument('-t', dest="threads", required=True, help="How many threads do you want to allocate? NOTE:"
                                                                         "this utilizes multiprocessing, which is capped by the "
                                                                         "amount of cores are in the machine." +
                               "Your core count is:" + str(multiprocessing.cpu_count()))

    try:
        noOfThreads = int(consoleParser.parse_args().threads)
    except:
        print("You need to specify -t (number of cores to use, max: {}) ".format(str(multiprocessing.cpu_count())))

    elasticProcessingList =[]

    # Start producing through multiple cores
    for i in range(noOfThreads):
        thread = multiprocessing.Process(target=consume, name=str(i))
        elasticProcessingList.append(thread)
        thread.start()

    for i in elasticProcessingList:
        i.join()







