package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StreamGenerator {

    public static void generateStream() {
        // Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Kafka topic to produce messages to
        String topic = "topic1";

        // CSV file path
        String csvFilePath = "stream.csv";

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Read CSV file and send each line as a message to Kafka
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            boolean firstLine = true;
            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue; // Skip the first line
                }

                // Split the CSV line by comma
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    // Extract temperature and timestamp
                    String timestampStr = parts[0];
                    double temperature = Double.parseDouble(parts[1]);


                    // Parse timestamp string to Date
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date timestamp = dateFormat.parse(timestampStr);

                    // Calculate delay in milliseconds
                    long delayMillis = timestamp.getTime() - System.currentTimeMillis();
                    if (delayMillis > 0) {
                        // Wait until the specified timestamp
                        TimeUnit.MILLISECONDS.sleep(delayMillis);
                    }

                    // Send temperature value as message to Kafka topic
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                    producer.send(record);
                }
            }
        } catch (IOException | ParseException | NumberFormatException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}
