from confluent_kafka import Consumer
from time import sleep

conf = {'bootstrap.servers': 'pkc-lz6r3.northeurope.azure.confluent.cloud:9092',
        'group.id': 'mygroup',
        'sasl.mechanism' : 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username' : '6GF6RHWXMFQTZ624',
        'sasl.password' : 'iPaie4dfn3uUM/NhmHGnMlv2mIcOr+wIBIO9xuLJgBf+VZaZFldaBkcexvEpRXpK'
    }


cons = Consumer(conf)


cons.subscribe(["TH1","Etot","HVAC1","W1"])


while(True):


    ret = cons.consume()

    for r in ret:
        print("{} --> {}".format(r.topic(),str(r.value(),"utf8")))