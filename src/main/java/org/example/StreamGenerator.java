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

    private static final String BOOTSTRAP_SERVERS = "localhost:29092"; // Change this to your Kafka bootstrap servers

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(() -> {
            try (BufferedReader reader = new BufferedReader(new FileReader("stress.csv"))) {
                Properties props = new Properties();
                props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    String line;
                    reader.readLine(); // Skip header
                    while ((line = reader.readLine()) != null) {
                        ProducerRecord<String, String> record = new ProducerRecord<>("stress", line);
                        producer.send(record).get();
                        System.out.println("Sent: " + line);
                        Thread.sleep(1000); // Sleep for 1 second between each message (adjust as needed)
                    }
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                System.err.println("Error in stress.csv processing: " + e.getMessage());
            }
        });

        executor.submit(() -> {
            try (BufferedReader reader = new BufferedReader(new FileReader("weight.csv"))) {
                Properties props = new Properties();
                props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    String line;
                    reader.readLine(); // Skip header
                    while ((line = reader.readLine()) != null) {
                        ProducerRecord<String, String> record = new ProducerRecord<>("weight", line);
                        producer.send(record).get();
                        System.out.println("Sent: " + line);
                        Thread.sleep(1000); // Sleep for 1 second between each message (adjust as needed)
                    }
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                System.err.println("Error in weight.csv processing: " + e.getMessage());
            }
        });

        executor.shutdown();
    }
}