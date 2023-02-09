import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;


import java.util.Properties;

public class App2 {
    public static void main(String[] args) {
Properties props = new Properties();
        // set the stream configurations
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        System.out.println("Runnning");
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println("Running2");
        // define the input stream and subscribe to it
        KStream<String, String> inputStream = builder.stream("TH1");
        inputStream.peek((key, value) -> System.out.println("Received message: key = " + key + ", value = " + value));
        System.out.println("Running3");
        inputStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                System.out.println("Received message: key = " + key + ", value = " + value);
            }
        });

        System.out.println("Running4");

        //KafkaStreams streams = new KafkaStreams(builder.build(), props);
        System.out.println("Running5");
        //streams.start();
        System.out.println("Running6");
        Topology topology = builder.build();
        // shutdown hook to correctly close the streams application
        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}