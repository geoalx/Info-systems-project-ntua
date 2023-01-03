// import org.apache.*;

// public class App {
//     public static void main(String[] args) throws Exception {
//         System.out.println("Hello, World!");
//     }
// }


import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        // set the stream configurations
        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        // define the processing topology here

        //subscribe to the topic TH1 and print the message
        builder.stream("TH1").foreach((key, value) -> System.out.println("key: " + key + " value: " + value));

        // build the topology and start streaming

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();


        // KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}