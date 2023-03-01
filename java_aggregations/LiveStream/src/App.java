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
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
        "SfhKJc-PSWSDSK5LiQfjeQGVe09-TeONKC9E60xjqgci6tt_JFrIbZuEUN_IT8YxMpGOB1QyL7BQtnY54xuGxw==".toCharArray(),
        "info_sys_ntua",
        "info_sheesh_bucket"
        );

        WriteApi writeApi = client.makeWriteApi();
        
        StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        // define the input stream and subscribe to it

        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("prev-values"),
                Serdes.String(),
                Serdes.String()
            )
        );

        // 15min sensors
        KStream<String, String> inputStreamTH1 = builder.stream("TH1", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamTH2 = builder.stream("TH2", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamHVAC1 = builder.stream("HVAC1", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamHVAC2 = builder.stream("HVAC2", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamMiAC1 = builder.stream("MiAC1", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamMiAC2 = builder.stream("MiAC2", Consumed.with(stringSerde, stringSerde));
        
        //1day sensors
        KStream<String, String> inputStreamWtot = builder.stream("Wtot", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamEtot = builder.stream("Etot", Consumed.with(stringSerde, stringSerde));
        
        //Async move sensor
        KStream<String, String> inputStreamMov1 = builder.stream("Mov1", Consumed.with(stringSerde, stringSerde));
        
        // 15min sensors with late events
        KStream<String, String> inputStreamW1 = builder.stream("W1", Consumed.with(stringSerde, stringSerde));

        KStream<String,String>[] filteredW1 = inputStreamW1.transform(new LateEventFilter(),"prev-values").branch(
            (k,v) -> !v.contains("LATE"),
            (k,v) -> v.contains("LATE")
        );

        KStream<String, String> inputStreamW1_filtered = filteredW1[0];
        KStream<String, String> inputStreamW1_late = filteredW1[1];
 
        //Move sensor
        inputStreamMov1.
        peek((k,v) -> {
            peek_message(k, v, "MOV1", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  "0|0",
        (key,value,av) -> {
            String[] value_split = value.split("\\|");
            int x = Integer.parseInt(value_split[1]);
            x++;
            return value_split[0]+"|"+x;
        },Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-MOV1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled()
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map( (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            peek_message(k, v, "MOV1", writeApi, true);
        });


        //1 day sensors processing
        //Wtot sensor aggregations
        inputStreamWtot
        //.mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            peek_message(k, v, "WTOT", writeApi,false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  "0|0.0D",
        (key,value,av) -> {
            String[] value_split = value.split("\\|");
            return value_split[0]+"|"+AggDayDiffWtot(value_split[1]);
        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-WTOT")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled() // disable caching and logging

        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v)->{
            peek_message(k, v, "WTOT", writeApi, true);
        });

        // Etot sensor aggregations
        inputStreamEtot
        //.mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            peek_message(k, v, "ETOT", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  "0|0.0",
        (key,value,av) -> {
            String[] value_split = value.split("\\|");
            
            return value_split[0]+"|"+AggDayDiffEtot(value_split[1]);
        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-ETOT")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled() // disable caching and logging
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            peek_message(k, v, "ETOT", writeApi, true);
        });

        // 15min sensors processing
        {

        inputStreamTH1
        //.mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            peek_message(k, v, "TH1", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () -> { countTH1=0; return "0|0.0";},
        (key,value,av) -> {   

            String[] value_split = value.split("\\|");
            
            countTH1++;
            if(countTH1==0){
                return value;
            }
            
            String[] av_split = av.split("\\|");
            return value_split[0] + "|"+((Double.parseDouble(av_split[1])*(countTH1-1)+ Double.parseDouble(value_split[1]))/countTH1);   

        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-TH1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled() // disable caching and logging
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString())
            )
        .peek((k,v) -> {
            peek_message(k, v, "TH1", writeApi, true);
        });

        inputStreamTH2
        //.mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            peek_message(k, v, "TH2", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  {countTH2=0;return "0|0.0";},
        (key,value,av) -> {

            String[] value_split = value.split("\\|");
            
            countTH1++;
            if(countTH2==0){
                return value;
            }
            
            String[] av_split = av.split("\\|");
            return value_split[0] + "|"+((Double.parseDouble(av_split[1])*(countTH2-1)+ Double.parseDouble(value_split[1]))/countTH2);  

        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-TH2")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        //.withLoggingDisabled() // disable caching and logging
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            peek_message(k, v, "TH2", writeApi, true);
        });

        inputStreamHVAC1
        //.mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            peek_message(k, v, "HVAC1", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  "0|0.0",
        (key,value,av) -> {
            String[] value_split = value.split("\\|");
            String[] av_split = av.split("\\|");

            return value_split[0]+"|"+adder(Double.parseDouble(av_split[1]),value_split[1]);
        
        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-HVAC1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled() // disable caching and logging
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            peek_message(k, v, "HVAC1", writeApi, true);
        });

        inputStreamHVAC2
        //.mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            peek_message(k, v, "HVAC2", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  "0|0.0",
        (key,value,av) -> {
            
            String[] value_split = value.split("\\|");
            String[] av_split = av.split("\\|");
            return value_split[0]+"|"+adder(Double.parseDouble(av_split[1]),value_split[1]);
        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-HVAC2")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled() // disable caching and logging
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            peek_message(k, v, "HVAC2", writeApi, true);
        });

        inputStreamMiAC1
        //.mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            peek_message(k, v, "MiAC1", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->   "0|0.0",
        (key,value,av) -> {
            String[] value_split = value.split("\\|");
            String[] av_split = av.split("\\|");
            return value_split[0]+"|"+adder(Double.parseDouble(av_split[1]),value_split[1]);
        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-MiAC1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled() // disable caching and logging
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            peek_message(k, v, "MiAC1", writeApi, true);
        });

        inputStreamMiAC2
        //.mapValues(v -> v.split("\\|")[1])
        .peek((k,v)->{
            peek_message(k, v, "MiAC2", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  "0|0.0",
        (key,value,av) -> {
            String[] value_split = value.split("\\|");
            String[] av_split = av.split("\\|");
            return value_split[0]+"|"+adder(Double.parseDouble(av_split[1]),value_split[1]);
        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-MiAC2")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled() // disable caching and logging
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            peek_message(k, v, "MiAC2", writeApi, true);
        });


        inputStreamW1_late
        .mapValues((v)-> {String[] v_split = v.split("\\|"); return v_split[0]+"|"+v_split[1];})
        .peek((k,v)->{
            peek_message(k, v, "W1_LATE", writeApi, false);
        });

        inputStreamW1_filtered
        .peek((k,v)->{
            peek_message(k, v, "W1", writeApi, false);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofSeconds(10)).advanceBy(Duration.ofDays(1)))
        .aggregate(
        () ->  "0|0.0",
        (key,value,av) -> {
            String[] value_split = value.split("\\|");
            String[] av_split = av.split("\\|");
            return value_split[0]+"|"+adder(Double.parseDouble(av_split[1]),value_split[1]);
        },
        Materialized
        .<String, String, WindowStore<Bytes, byte[]>>as("windowed-aggregation-store-W1")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
        .withLoggingDisabled() // disable caching and logging
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(
            (k,v) -> KeyValue.pair(k.key().toString(), v.toString()))
        .peek((k,v) -> {
            peek_message(k, v, "W1", writeApi, true);
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

    public static void peek_message(String key, String value, String measurement,WriteApi influxapi,boolean isAgg) {
            String[] parts = value.split("\\|");
            String aggr_flag = "";
            if(isAgg){
                aggr_flag = "-AGG";
            }
            else{
                aggr_flag = "-RAW";
            }
            System.out.println("["+measurement+aggr_flag+"-INFLUX]"+" Received message: key = " + key + ", value = " + parts[1]);

            
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm", Locale.ENGLISH);
            LocalDateTime date = null;
            try {
                date = LocalDateTime.parse(parts[0], dtf);
            } catch (DateTimeParseException e) {
                System.out.print("Error occured during date parsing");
            }


            if(isAgg){
                influxapi.writeRecord("info_sheesh_bucket", "info_sys_ntua", WritePrecision.MS, measurement+",location=aggregated"+" value="+parts[1]+" "+Timestamp.valueOf(date).getTime());
            }
            else{
                influxapi.writeRecord("info_sheesh_bucket", "info_sys_ntua", WritePrecision.MS, measurement+",location=raw"+" value="+parts[1]+" "+Timestamp.valueOf(date).getTime());
            }
    }
}