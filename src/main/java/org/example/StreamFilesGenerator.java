package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.List;

public class StreamFilesGenerator {
    private static final double MEAN = 15.00;
    private static final double STD_DEV = 4.00;

    public static void main(String[] args) {
        LocalDateTime start = LocalDateTime.now();
        List<StressRecord> stressRecords = new ArrayList<>();
        List<WeightRecord> weightRecords = new ArrayList<>();

        try (PrintWriter writer = new PrintWriter(new FileWriter("Files/Input/stress2.csv"))) {
            // Write header
            writer.println("timestamp,id,status,stressLevel");

            LocalDateTime timestamp = start;
            // Generate 50 rows
            for (int i = 0; i < 59; i++) {
                timestamp = timestamp.plusSeconds(1);
                int id = generateRandomNumber(1, 3);
                String status = generateRandomStatus();
                int stressLevel = generateRandomNumber(1, 8);

                // Create a stress record and add to the list
                StressRecord record = new StressRecord(timestamp, id, status, stressLevel);
                stressRecords.add(record);

                // Write row to CSV
                writer.printf("%s,%d,%s,%d%n",
                        timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                        id, status, stressLevel);
            }

            System.out.println("Stress CSV file generated successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to stress CSV file: " + e.getMessage());
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter("Files/Input/weight2.csv"))) {
            // Write CSV header
            writer.println("timestamp,id,weight");
            LocalDateTime timestamp = start;
            for (int i = 0; i < 59; i++) {
                timestamp = timestamp.plusSeconds(1);
                int id = generateRandomNumber(1, 3);
                double weight = generateWeight();

                // Create a weight record and add to the list
                WeightRecord record = new WeightRecord(timestamp, id, weight);
                weightRecords.add(record);

                // Write row to CSV
                writer.printf(Locale.US, "%s,%d,%.2f%n", timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), id, weight);
            }

            System.out.println("Weight CSV file generated successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to weight CSV file: " + e.getMessage());
        }

        // Write stress records to JSON file
        try (FileWriter writer = new FileWriter("Files/Input/stress2.json")) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(stressRecords, writer);

            System.out.println("Stress JSON file created successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to stress JSON file: " + e.getMessage());
        }

        // Write weight records to JSON file
        try (FileWriter writer = new FileWriter("Files/Input/weight2.json")) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(weightRecords, writer);

            System.out.println("Weight JSON file created successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to weight JSON file: " + e.getMessage());
        }
    }

    private static int generateRandomNumber(int min, int max) {
        Random random = new Random();
        return random.nextInt(max - min + 1) + min;
    }

    private static String generateRandomStatus() {
        String[] statuses = {"aaa", "bbb", "ccc"};
        Random random = new Random();
        int index = random.nextInt(statuses.length);
        return statuses[index];
    }

    private static double generateWeight() {
        Random random = new Random();
        return MEAN + STD_DEV * random.nextGaussian();
    }

    private static class StressRecord {
        private String timestamp;
        private int stressId;
        private String status;
        private int stressLevel;

        public StressRecord(LocalDateTime timestamp, int stressId, String status, int stressLevel) {
            this.timestamp = timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            this.stressId = stressId;
            this.status = status;
            this.stressLevel = stressLevel;
        }
    }

    private static class WeightRecord {
        private String timestamp;
        private int weightId;
        private double weight;

        public WeightRecord(LocalDateTime timestamp, int weightId, double weight) {
            this.timestamp = timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            this.weightId = weightId;
            this.weight = weight;
        }
    }
}