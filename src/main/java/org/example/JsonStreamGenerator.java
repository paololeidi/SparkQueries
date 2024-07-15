package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;

public class JsonStreamGenerator {

    private static final String BOOTSTRAP_SERVERS = "localhost:29092"; // Change this to your Kafka bootstrap servers

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(() -> processFile("Files/Input/stress.json", "stress"));
        executor.submit(() -> processFile("Files/Input/weight.json", "weight"));

        executor.shutdown();
    }

    private static void processFile(String fileName, String topic) {
        Properties props = createKafkaProperties();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName));
             KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(reader);
            Iterator<JsonNode> elements = rootNode.elements();

            while (elements.hasNext()) {
                JsonNode element = elements.next();
                sendRecord(producer, topic, element.toString());
                Thread.sleep(500); // Sleep for 1 second between each message (adjust as needed)
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            System.err.println("Error in " + fileName + " processing: " + e.getMessage());
        }
    }

    private static Properties createKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
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