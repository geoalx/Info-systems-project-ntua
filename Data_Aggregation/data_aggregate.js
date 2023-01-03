// const Kafka = require('kafka-node');
const {KafkaStreams} = require("kafka-streams");
// const { KStream } = require('kafka-node');

// Consumer = Kafka.Consumer,
// client = new Kafka.KafkaClient()


// // Set up the Kafka consumer
// const consumer = new Consumer(
//   client,
//   [
//     {topic: 'TH1'}
//   ],
//   {
//     'bootstrap.servers': 'pkc-lz6r3.northeurope.azure.confluent.cloud:9092',
//     'group.id': 'mygroup',
//     'sasl.mechanism' : 'PLAIN',
//     'security.protocol': 'SASL_SSL',
//     'sasl.username' : '6GF6RHWXMFQTZ624',
//     'sasl.password' : 'iPaie4dfn3uUM/NhmHGnMlv2mIcOr+wIBIO9xuLJgBf+VZaZFldaBkcexvEpRXpK'
// }
// );


// const factory = new KafkaStreams({noptions: {
//   'bootstrap.servers': 'pkc-lz6r3.northeurope.azure.confluent.cloud:9092',
//   'group.id': 'mygroup',
//   'sasl.mechanisms' : 'GSSAPI',
//   'sasl.kerberos.service.name':'kafka',
//   'security.protocol': 'SASL_SSL',
//   //'security.protocol': "SASL_PLAINTEXT",
//   'sasl.username' : '6GF6RHWXMFQTZ624',
//   'sasl.password' : 'iPaie4dfn3uUM/NhmHGnMlv2mIcOr+wIBIO9xuLJgBf+VZaZFldaBkcexvEpRXpK'
// }});

const factory = new KafkaStreams({noptions: {
  'bootstrap.servers': 'localhost:9092',
  'group.id': 'mygroup'
}});

factory.on("error", (error) => {
    console.log("Error occured:", error.message);
});

const kstream = factory.getKStream("TH1")

kstream.forEach((message) => {
  console.log("key", message.key ? message.key.toString("utf8") : null);
  console.log("value", message.value ? message.value.toString("utf8") : null);
  console.log("partition", message.partition);
  console.log("size", message.size);
  console.log("offset", message.offset);
  console.log("timestamp", message.timestamp);
  console.log("topic", message.topic);
});

kstream.start();

// const ktable = factory.getKTable(/* .. */);

// kstream.merge(ktable).filter(/* .. */).map(/* .. */).reduce(/* .. */).to("output-topic");


