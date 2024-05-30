package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;

public class CsvGenerator {
    private static final double MEAN = 15.00;
    private static final double STD_DEV = 4.00;

    public static void main(String[] args) {
        LocalDateTime start = LocalDateTime.now();

        try (PrintWriter writer = new PrintWriter(new FileWriter("stress.csv"))) {
            // Write header
            writer.println("timestamp,id,status,stressLevel");

            LocalDateTime timestamp = start;
            // Generate 50 rows
            for (int i = 0; i < 59; i++) {
                timestamp = timestamp.plusSeconds(1);
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

        try (FileWriter writer = new FileWriter("weight.csv")) {
            // Write CSV header
            writer.append("timestamp,id,weight\n");
            LocalDateTime timestamp = start;
            for (int i = 0; i < 59; i++) {
                timestamp = timestamp.plusSeconds(1);
                int id = generateRandomNumber(1, 3);
                double weight = generateWeight();

                // Write row to CSV
                writer.append(String.format(Locale.US,"%s,%d,%.2f\n", timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), id, weight));
            }

            System.out.println("CSV file created successfully.");
        } catch (IOException e) {
            e.printStackTrace();
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


}
