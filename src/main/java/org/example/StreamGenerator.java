package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StreamGenerator {

    private static final String BOOTSTRAP_SERVER = "localhost:19092"; // Change this to your Kafka bootstrap servers
    private static final boolean SLOW = false;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(() -> processFile("Files/Input/stress2.csv", "stress"));
        executor.submit(() -> processFile("Files/Input/weight2.csv", "weight"));

        executor.shutdown();
    }

    private static void processFile(String fileName, String topic) {
        Properties props = createKafkaProperties();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName));
             KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String line;
            reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                sendRecord(producer, topic, line);
                if (SLOW){
                    Thread.sleep(1000); // Sleep between each message (adjust as needed)
                } else {
                    Thread.sleep(250); // Sleep between each message (adjust as needed)
                }

            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            System.err.println("Error in " + fileName + " processing: " + e.getMessage());
        }
    }

    private static Properties createKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private static void sendRecord(KafkaProducer<String, String> producer, String topic, String value)
            throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        producer.send(record).get();
        System.out.println("Sent: " + value);
    }
}