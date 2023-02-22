from faker import Faker
from datetime import datetime,timedelta
from time import sleep
from tqdm import tqdm
from confluent_kafka import Producer

F = Faker()

class Sensor:
    '''
    Base class for Sensors. Each object corresponds to one sensor, and has a name and a value_range.
    By default, interval is 15 minutes, and late is false. The late parameter indicates whether or not
    to generate late-event-data.
    '''
    def __init__(self, name, val_range, late=False):
        self.name = name
        self.late=late
        self.interval = timedelta(minutes=15)
        self.val_range = val_range
        self.value = float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)
        self.time = datetime.now()
        self.late_count = 0    # used to find when to send late-event-data

    def generate(self):
        to_print = "{}|{}".format(self.time.strftime("%Y-%m-%d %H:%M"),round(self.value,2))
        self.time += self.interval
        self.value = float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)

        if self.late_count % 20 == 0 and self.late_count != 0 and self.late:   # add late event from 2 days before
            value_2 = float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)
            late_time = self.time - timedelta(days=2)
            to_print_2 = "{}|{}".format(late_time.strftime("%Y-%m-%d %H:%M"),round(value_2,2))
            to_print += '$' + to_print_2

        if self.late_count % 120 == 0 and self.late_count != 0  and self.late:  # add late event from 10 days before
            value_10 = float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)
            late_time = self.time - timedelta(days=10)
            to_print_10 = "{}|{}".format(late_time.strftime("%Y-%m-%d %H:%M"),round(value_10,2))   
            to_print += '$' + to_print_10

        
        self.late_count += 1
        return to_print


class SumSensor(Sensor):
    '''
    Sub-class of the previous Sensor class. It represents the sensors that measure cumulative data on a daily
    basis. The difference from the parent class is that the start with a value of 0 and the interval is 1 day
    instead of 15 minutes. Also, the generate function returns the previous value plus a random amount, while 
    in the Sensor class it returns a random value from val_range.
    '''

    def __init__(self, name, val_range):
        super().__init__(name, val_range)
        self.value = 0.0
        self.interval = timedelta(days=1)


    def generate(self):
        to_print = "{}|{}".format(self.time.strftime("%Y-%m-%d %H:%M"),round(self.value,2))
        self.value += float(F.random.randrange(self.val_range[0]*100, self.val_range[1]*100)/100)
        self.time += self.interval
        return to_print


class MoveSensor(Sensor):
    '''
    This class represents the moving sensor, which simply sends 1 if it detected motion in random times.
    '''
    def __init__(self, name):
        self.name = name
        self.count = 0    # used to determine when to send 1
        self.time = datetime.now()
        self.ace_index = F.random.sample(range(0,96),4)   # choose 4 random times to send 1

    def generate(self):
        if(self.count in self.ace_index):
            # self.time += timedelta(minutes=self.count*15)
            to_print = "{} | {}".format((self.time + timedelta(minutes=self.count*15)).strftime("%Y-%m-%d %H:%M"),1)
        else:
            to_print = "NO DATA"   # could also be None, will be filtered in next layer

        if self.count == 95:   # if one day passed, reset the counter and pick new random times to send 1
            self.count, self.time = 0, self.time + timedelta(days=1)
            self.ace_index = F.random.sample(range(0,96),4)

        self.count += 1

        return to_print
        
        
        


if __name__ == "__main__":
    sensors = [Sensor("TH1", (12, 35)),Sensor("TH2", (12, 35)), Sensor("HVAC1", (0, 100)),Sensor("HVAC2", (0, 200)),Sensor("MiAC1", (0, 150)),Sensor("MiAC2", (0, 200)), SumSensor("Etot", (2600*24 - 1000, 2600*24 + 1000)),Sensor("W1",(0,1), late=True),SumSensor("Wtot",(100,120)),MoveSensor("Mov1")]
    
    headers_to_test = ["TH1","HVAC1","Etot","W1"]
    
    # Producer Configuration
    local_conf = {'bootstrap.servers': 'localhost:9092'}

    # connect with the kafka broker as a producer
    producer = Producer(local_conf)

    producer.flush()

    for i in tqdm(range(125)):
        sleep(1)
        for sensor in sensors:
            with open("{}.txt".format(sensor.name), "a") as f:
                temp = sensor.generate()
                
                f.write(temp + "\n")
                for t in temp.split("$"):  # split in $ to find posible late events
                    # send the generated data to the appropriate kafka broker topic (channel)
                    producer.produce(topic=sensor.name, key="dummy", value=str(t))  
                    producer.flush()
