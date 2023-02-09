import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CustomTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm");
        try{
            Date parseDate = dateFormat.parse(record.value().toString().split("\\|")[0]);
            return (parseDate.getTime());
        }
        catch (Exception e){
            System.out.println("Error parsing date");
            return 0;
        }  
    }
}