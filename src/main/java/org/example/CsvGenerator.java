package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class CsvGenerator {
    public static void main(String[] args) {
        String csvFile = "stress.csv";

        try (PrintWriter writer = new PrintWriter(new FileWriter(csvFile))) {
            // Write header
            writer.println("timestamp,id,status,stressLevel");

            LocalDateTime start = LocalDateTime.now();
            // Generate 50 rows
            for (int i = 0; i < 59; i++) {
                LocalDateTime timestamp = start.plusSeconds(i);
                int id = generateRandomNumber(1, 3);
                String status = generateRandomStatus();
                int stressLevel = generateRandomNumber(1, 8);

                // Write row to CSV
                writer.printf("%s,%d,%s,%d%n",
                        timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                        id, status, stressLevel);
            }

            System.out.println("CSV file generated successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
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
}
