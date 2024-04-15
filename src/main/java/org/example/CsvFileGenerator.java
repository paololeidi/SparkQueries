package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

public class CsvFileGenerator {

    public CsvFileGenerator(){}

    public static void generateCsvFile(){
        // Parameters
        int num_rows = 30;
        double mean_temperature = 25;
        double temperature_std_dev = 2; // Adjust as needed

        // Generate timestamps
        String[] timestamps = generateTimestamps(num_rows);

        // Sort timestamps
        Arrays.sort(timestamps);

        // Generate data and write to CSV file
        try (FileWriter writer = new FileWriter("stream.csv")) {
            // Write header
            writer.append("timestamp,temperature\n");

            // Generate and write data
            Random random = new Random();
            for (String timestamp : timestamps) {
                double temperature = mean_temperature + temperature_std_dev * random.nextGaussian();
                writer.append(String.format("%s,%.2f\n", timestamp, temperature));
            }

            System.out.println("CSV file generated successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String[] generateTimestamps(int num_rows) {
        String[] timestamps = new String[num_rows];
        // Generate timestamps within the next 5 minutes
        long currentTime = System.currentTimeMillis();
        Random random = new Random();
        for (int i = 0; i < num_rows; i++) {
            long randomTimeOffset = (long) (random.nextDouble() * 30 * 1000); // Random time within 5 minutes
            Date timestamp = new Date(currentTime + randomTimeOffset);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            timestamps[i] = dateFormat.format(timestamp);
        }
        return timestamps;
    }
}