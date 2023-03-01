
import org.apache.kafka.streams.KeyValue;

import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;


import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;



public class LateEventFilter implements TransformerSupplier<String, String, KeyValue<String, String>>{
    
    @Override
    public Transformer<String,String,KeyValue<String,String>> get() {
        return new LateEventFilterTransformer();
    }

    private static class LateEventFilterTransformer implements Transformer<String,String,KeyValue<String,String>>{
        private ProcessorContext context;
        private KeyValueStore<String, String> kvStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.kvStore = (KeyValueStore<String, String>) context.getStateStore("prev-values");
        }

        @Override
        public KeyValue<String,String> transform(String key, String value) {
            String storeKey = key + "-" + context.partition();
            String prevValue = kvStore.get(storeKey);
            
            String[] valueArray = value.split("\\|");
            String[] prevValueArray = null;

            try {
                prevValueArray = prevValue.split("\\|");
            } catch (NullPointerException e) {
                kvStore.put(storeKey, value);
                return new KeyValue<String,String>(key, value);
            }

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm", Locale.ENGLISH);
            LocalDateTime date_prev = null;
            LocalDateTime date_current = null;

            long prevTimestamp = 0;
            long currentTimestamp = 0;
            try {
                date_prev = LocalDateTime.parse(prevValueArray[0], dtf);
                date_current = LocalDateTime.parse(valueArray[0], dtf);
            } catch (DateTimeParseException e) {
                System.out.print("Error occured during date parsing");
            }

            prevTimestamp = Timestamp.valueOf(date_prev).getTime();
            currentTimestamp = Timestamp.valueOf(date_current).getTime();

            if (prevTimestamp - currentTimestamp > 259200000) {
                kvStore.put(storeKey, value);
                return new KeyValue<String,String>(key, value+"|LATE");
            } else {
                kvStore.put(storeKey, value);
                return new KeyValue<String,String>(key, value);
            }

        }

        @Override
        public void close() {
            // nothing to do
        }
        
    }


}