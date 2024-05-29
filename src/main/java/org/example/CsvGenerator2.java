package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;

public class CsvGenerator2 {
    private static final String FILE_NAME = "weight.csv";
    private static final int ROW_COUNT = 60;
    private static final double MEAN = 15.00;
    private static final double STD_DEV = 4.00;

    public static void main(String[] args) {
        try (FileWriter writer = new FileWriter(FILE_NAME)) {
            // Write CSV header
            writer.append("timestamp,id,weight\n");

            LocalDateTime start = LocalDateTime.now();
            for (int i = 0; i < 59; i++) {
                LocalDateTime timestamp = start.plusSeconds(i);
                int id = generateId();
                double weight = generateWeight();

                // Write row to CSV
                writer.append(String.format(Locale.US,"%s,%d,%.2f\n", timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), id, weight));
            }

            System.out.println("CSV file created successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateTimestamp() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return now.format(formatter);
    }

    private static int generateId() {
        Random random = new Random();
        return random.nextInt(3) + 1;  // Random number between 1 and 3
    }

    private static double generateWeight() {
        Random random = new Random();
        return MEAN + STD_DEV * random.nextGaussian();
    }
}
