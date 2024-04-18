package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class StreamGenerator2 {

    private static final String TOPIC_NAME = "topic3"; // Change this to your Kafka topic name
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Change this to your Kafka bootstrap servers


    public static void main(String[] args) {
        try (BufferedReader reader = new BufferedReader(new FileReader("stream3.csv"))) {
            Properties props = new Properties();
            props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                String line;
                reader.readLine(); // Skip header
                while ((line = reader.readLine()) != null) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, line);
                    producer.send(record).get();
                    System.out.println("Sent: " + line);
                    Thread.sleep(1000); // Sleep for 1 second between each message (adjust as needed)
                }
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
