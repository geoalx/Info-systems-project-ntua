from faker import Faker
from datetime import datetime,timedelta
from time import sleep
from tqdm import tqdm
from confluent_kafka import Producer

F = Faker()

class Sensor:

    def __init__(self, name, val_range, late=False):
        self.name = name
        self.late=late
        self.interval = timedelta(minutes=15)
        self.val_range = val_range
        self.value = float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)
        self.time = datetime.now()
        self.late_count = 0

    def generate(self):
        to_print = "{} | {}".format(self.time.strftime("%Y-%m-%d %H:%M"),round(self.value,2))
        self.time += self.interval
        self.value = float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)

        if self.late_count % 20 == 0 and self.late_count != 0 and self.late:
            value_2 = float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)
            late_time = self.time - timedelta(days=2)
            to_print_2 = "{} | {}".format(late_time.strftime("%Y-%m-%d %H:%M"),round(value_2,2))
            to_print += '$' + to_print_2

        if self.late_count % 120 == 0 and self.late_count != 0  and self.late:
            value_10 = float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)
            late_time = self.time - timedelta(days=10)
            to_print_10 = "{} | {}".format(late_time.strftime("%Y-%m-%d %H:%M"),round(value_10,2))   
            to_print += '$' + to_print_10

        
        self.late_count += 1
        return to_print


class SumSensor(Sensor):

    def __init__(self, name, val_range):
        super().__init__(name, val_range)
        self.value = 0.0
        self.interval = timedelta(days=1)


    def generate(self):
        to_print = "{} | {}".format(self.time.strftime("%Y-%m-%d %H:%M"),round(self.value,2))
        self.value += float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)
        self.time += self.interval
        return to_print

class MoveSensor(Sensor):

    def __init__(self, name):
        self.name = name
        self.count = 0
        self.time = datetime.now()
        self.ace_index = F.random.sample(range(0,96),4)

    def generate(self):
        if(self.count in self.ace_index):
            # self.time += timedelta(minutes=self.count*15)
            to_print = "{} | {}".format((self.time + timedelta(minutes=self.count*15)).strftime("%Y-%m-%d %H:%M"),1)
        else:
            to_print = "NO DATA"

        if self.count == 95:
            self.count, self.time = 0, self.time + timedelta(days=1)
            self.ace_index = F.random.sample(range(0,96),4)

        self.count += 1

        return to_print
        
        
        


if __name__ == "__main__":
    sensors = [Sensor("TH1", (12, 35)),Sensor("TH2", (12, 35)), Sensor("HVAC1", (0, 100)),Sensor("HVAC2", (0, 200)),Sensor("MiAC1", (0, 150)),Sensor("MiAC2", (0, 200)), SumSensor("Etot", (2600*24 - 1000, 2600*24 + 1000)),Sensor("W1",(0,1), late=True),SumSensor("Wtot",(100,120)),MoveSensor("Mov1")]
    
    headers_to_test = ["TH1","HVAC1","Etot","W1"]
    
    conf = {'bootstrap.servers': 'pkc-lz6r3.northeurope.azure.confluent.cloud:9092',
        'sasl.mechanism' : 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username' : '6GF6RHWXMFQTZ624',
        'sasl.password' : 'iPaie4dfn3uUM/NhmHGnMlv2mIcOr+wIBIO9xuLJgBf+VZaZFldaBkcexvEpRXpK'
    }


    producer = Producer(conf)

    producer.flush()

    for i in tqdm(range(125)):
        sleep(0.5)
        for sensor in sensors:
            with open("{}.txt".format(sensor.name), "a") as f:
                temp = sensor.generate()
                
                f.write(temp + "\n")

                if(sensor.name in headers_to_test):
                    for t in temp.split("$"):
                        producer.produce(topic=sensor.name, value=str(t))
                        producer.flush()
