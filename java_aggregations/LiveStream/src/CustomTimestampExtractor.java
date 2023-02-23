import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.sql.Timestamp;

public class CustomTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {

        String date = record.value().toString().split("\\|")[0];

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm", Locale.ENGLISH);
        LocalDateTime newdate = null;
        try {
            newdate = LocalDateTime.parse(date, dtf);
        } catch (DateTimeParseException e) {
            System.out.print("Error occured during date parsing");
        }

        long temp = Timestamp.valueOf(newdate).getTime();

        return temp + 7200000;
    }
}