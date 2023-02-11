import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import com.influxdb.client.*;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.*;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.influxdb.*;
import java.time.ZoneId;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Properties;

@SuppressWarnings("all")

public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        // set the stream configurations
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", 
        "wMqzuEl-JogXj1IrnQlwUDy9Gbv_15yvm5OQ8ua-qRvXAFdjEuJMEJ8rBEx2BqDQ6uoZ09ZfZNamhuZOrbpB6g==".toCharArray(),
        "info_sys",
        "info_sys_bucket"
        );

        WriteApi writeApi = client.makeWriteApi();
        
        StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        // define the input stream and subscribe to it
        KStream<String, String> inputStreamTH1 = builder.stream("TH1", Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new CustomTimestampExtractor()));
        // inputStreamTH1.groupByKey().;
        inputStreamTH1.mapValues(v -> v.split("\\|")[1]).peek((k,v)->{
            System.out.println("Received message: key = " + k + " value = " + v);
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String())).windowedBy(
            SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofDays(1), Duration.ofSeconds(5)))
            .aggregate(
            () ->  0.0D,
            (key, value, av) -> av + Double.parseDouble(value),
            Materialized.with(Serdes.String(), Serdes.Double())
            )
            .suppress(
                Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())).toStream().map(
                    (k,v) -> KeyValue.pair(k.key().toString(), v.toString())).peek((k,v) -> {
                        System.out.println("Aggregated key=" + k + ", and aggregated value=" + v);
                    });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // shutdown hook to correctly close the streams application
        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Double adder(Double a,String b){
        Double x = 0.0;
        try{
            x = Double.parseDouble(b);
        }
        catch(Exception e){
            System.out.println("Error occured during parsing " + b + " to double");
        }
        return a+x;
    }

    public static void peek_message(String key, String value, String measurement,WriteApi influxapi) {
            System.out.println("Received message: key = " + key + ", value = " + value);
            String[] parts = value.split("\\|");
            
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm", Locale.ENGLISH);
            LocalDateTime date = null;
            try {
                date = LocalDateTime.parse(parts[0], dtf);
            } catch (DateTimeParseException e) {
                System.out.print("Error occured during date parsing");
            }
            
            influxapi.writeRecord("info_sys_bucket", "info_sys", WritePrecision.MS, measurement+" value="+parts[1]+" "+Timestamp.valueOf(date).getTime());

    }
}