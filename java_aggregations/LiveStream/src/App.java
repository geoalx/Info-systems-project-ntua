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
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;

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
    // class MyCountAndSum{
    //     private double sum_value;
    //     private long count_value;

    //     local class for aggregagtions that need average
    //     public MyCountAndSum(long c, double s) {
    //         sum_value = s;
    //         count_value = c;
    //     }

    //     public long getCount() {
    //         return count_value;
    //     }

    //     public double getSum() {
    //         return sum_value;
    //     }

    //     public void increment_counter() {
    //         count_value++;
    //     }
    //     public void add_value(double new_val) {
    //         sum_value += new_val;
    //     }
        
    // };

    // public static double peos = 0.0d;
    public static double prev_wtot = 0.0d;
    public static double prev_etot = 0.0d;
    public static int countTH1 = 0;
    public static int countTH2 = 0;
    public static int count = 0;
    public static int count2 = 0;
    public static int rcv_msg_count = 0;
    public static void main(String[] args) {
        Properties props = new Properties();
        // set the stream configurations
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", 
        "z2vYCdXn5etmn-o-OD8l50HuKUrNwjp8xoCsQVjx1GZxgNLmyvNZcnhbVd_x9s3_WoWs7aNoT9Q1gWsRNA3hfg==".toCharArray(),
        "info_sys_ntua",
        "info_sys_bucket"
        );

        WriteApi writeApi = client.makeWriteApi();
        
        StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        // define the input stream and subscribe to it

        // 15min sensors
        KStream<String, String> inputStreamTH1 = builder.stream("TH1", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamTH2 = builder.stream("TH2", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamHVAC1 = builder.stream("HVAC1", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamHVAC2 = builder.stream("HVAC2", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamMiAC1 = builder.stream("MiAC1", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamMiAC2 = builder.stream("MiAC2", Consumed.with(stringSerde, stringSerde));
        
        // 15min sensors with late events
        KStream<String, String> inputStreamW1 = builder.stream("W1", Consumed.with(stringSerde, stringSerde));

        //1day sensors
        KStream<String, String> inputStreamWtot = builder.stream("Wtot", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamEtot = builder.stream("Etot", Consumed.with(stringSerde, stringSerde));
        
        //Async move sensor
        KStream<String, String> inputStreamMov1 = builder.stream("Mov1", Consumed.with(stringSerde, stringSerde));

        //1 day sensors processing
        //Wtot sensor aggregations
        inputStreamWtot
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[WTOT] Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  0.0D,
        (key,value,av) -> AggDayDiffWtot(value),
        Materialized.with(Serdes.String(), Serdes.Double())

        )
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            System.out.println("[WTOT] Aggregated key=" + k + ", and aggregated value=" + v);
            count2++;
            System.out.println(count2 + " " +v);
        });

        // Etot sensor aggregations
        inputStreamEtot
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[ETOT] Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  0.0D,
        (key,value,av) -> AggDayDiffEtot(value),
        Materialized.with(Serdes.String(), Serdes.Double())
        )
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            System.out.println("[ETOT] Aggregated key=" + k + ", and aggregated value=" + v);
            count2++;
            System.out.println(count2 + " " +v);
        });

        // 15min sensors processing
        {

        inputStreamTH1
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[TH1] Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        //.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(10)))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        //.count()
        .aggregate(
        () -> { countTH1=0; return 0.0d;},
        (key,value,av) -> {   

            countTH1++;
            if(countTH1==0){
                return Double.parseDouble(value);
            }
            
            return (av*(countTH1-1)+ Double.parseDouble(value))/countTH1;   

        },
        Materialized
        .<String, Double, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-TH1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Double())
        .withLoggingDisabled() // disable caching and logging
        )
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString())
            )
        .peek((k,v) -> {
            System.out.println("[TH1] Aggregated key=" + k + ", and aggregated value=" + v);
        });

        inputStreamTH2
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[TH2] Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  {countTH2=0;return 0.0D;},
        (key,value,av) -> {

            countTH2++;


            if(countTH2==0){
                return Double.parseDouble(value);
            }
            
            return (av*(countTH2-1)+ Double.parseDouble(value))/countTH2;   

        },
        Materialized
        .<String, Double, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-TH2")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Double())
        .withLoggingDisabled() // disable caching and logging
        
        
        )
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            System.out.println("[TH2] Aggregated key=" + k + ", and aggregated value=" + v);
            count2++;
            System.out.println(count2 + " " +v);
        });

        inputStreamHVAC1
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[HVAC1] Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  0.0D,
        (key,value,av) -> adder(av,value),
        Materialized
        .<String, Double, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-HVAC1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Double())
        .withLoggingDisabled() // disable caching and logging
        ).toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            System.out.println("[HVAC1] Aggregated key=" + k + ", and aggregated value=" + v);
            count2++;
            System.out.println(count2 + " " +v);
        });

        inputStreamHVAC2
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[HVAC2] Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  0.0D,
        (key,value,av) -> adder(av,value),
        Materialized
        .<String, Double, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-HVAC2")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Double())
        .withLoggingDisabled() // disable caching and logging
        ).toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            System.out.println("[HVAC2] Aggregated key=" + k + ", and aggregated value=" + v);
            count2++;
            System.out.println(count2 + " " +v);
        });

        inputStreamMiAC1
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[MiAC1] Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  0.0D,
        (key,value,av) -> adder(av,value),
        Materialized
        .<String, Double, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-MiAC1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Double())
        .withLoggingDisabled() // disable caching and logging
        ).toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            System.out.println("[MiAC1] Aggregated key=" + k + ", and aggregated value=" + v);
            count2++;
            System.out.println(count2 + " " +v);
        });

        inputStreamMiAC2
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[MiAC2] Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  0.0D,
        (key,value,av) -> adder(av,value),
        Materialized
        .<String, Double, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-MiAC2")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Double())
        .withLoggingDisabled() // disable caching and logging
        ).toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            System.out.println("[MiAC2] Aggregated key=" + k + ", and aggregated value=" + v);
            count2++;
            System.out.println(count2 + " " +v);
        });

        inputStreamW1
        .mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            System.out.println("[W1]Received message: key = " + k + " value = " + v);
            rcv_msg_count++;
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(25)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  0.0D,
        (key,value,av) -> adder(av,value),
        Materialized
        .<String, Double, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-W1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Double())
        .withLoggingDisabled() // disable caching and logging
        ).toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            System.out.println("[W1] Aggregated key=" + k + ", and aggregated value=" + v);
            count2++;
            System.out.println(count2 + " " +v);
        });

    }
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // shutdown hook to correctly close the streams application
        // Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // Day difference operator
    public static Double AggDayDiffWtot(String curr){
        Double curr_num = 0.0;
        Double res = 0.0;
        try{
            curr_num = Double.parseDouble(curr);
            res = curr_num-prev_wtot;
            prev_wtot = curr_num;
        }
        catch(Exception e){
            System.out.println("Error occured during parsing " + curr + " to double");
        }
        return res;
    }

    public static Double AggDayDiffEtot(String curr){
        Double curr_num = 0.0;
        Double res = 0.0;
        try{
            curr_num = Double.parseDouble(curr);
            res = curr_num-prev_etot;
            prev_etot = curr_num;
        }
        catch(Exception e){
            System.out.println("Error occured during parsing " + curr + " to double");
        }
        return res;
    }
    
    // Add operator
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

    public static Double adderTH2(Double a,String b){
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
            
            influxapi.writeRecord("info_sys_bucket", "info_sys_ntua", WritePrecision.MS, measurement+" value="+parts[1]+" "+Timestamp.valueOf(date).getTime());

    }
}