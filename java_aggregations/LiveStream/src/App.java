import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.ForeachAction;
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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;

public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        // set the stream configurations
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
        
        InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", 
        "URcO91Nga4QLixFaMMwVpFpeExDZ93Kt54jN51VxhD_iuhxawWuJZUSNjoWcztGHuZInyqG-nL5Ev94VD_L5uw==".toCharArray(),
        "info_sys",
        "info_sys"
        );

        WriteApi writeApi = client.makeWriteApi(WriteOptions.builder().flushInterval(5_000).build());
        

        
        System.out.println("Runnning");
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println("Running2");
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        // define the input stream and subscribe to it
        KStream<String, String> inputStream = builder.stream("TH1", Consumed.with(stringSerde, stringSerde));
        //inputStream.groupByKey().windowedBy(
            //SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(96), Duration.ofSeconds(192)));
        inputStream.peek((key, value) -> {
            System.out.println("Received message: key = " + key + ", value = " + value);
            String[] parts = value.split("\\|");
            System.out.println("Before timeparsing");
            // Instant time_parse = LocalDateTime.parse(
            //     parts[0] ,       
            //     DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm")  
            // )                                     
            // .atZone(ZoneId.of("America/Toronto"))                                     
            // .toInstant();
            SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm");
            Date date = null;
            try {
                date = dateFormat.parse(parts[0]);
            }
            catch (Exception e) {
                System.out.println("Error parsing date");
            }
            Instant time_parse = Instant.ofEpochMilli(date.getTime());
            System.out.println("AFter time parsing");
            Point point = Point.measurement("TH1").addField("value",Float.parseFloat(parts[1])).time(Instant.now(),WritePrecision.MS);
            writeApi.writePoint("info_sys", "info_sys", point);         
            });
        System.out.println("Running3");


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        System.out.println("Running5");
        streams.start();
        System.out.println("Running6");

        // shutdown hook to correctly close the streams application
        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}