package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

// Must use JAVA 11
public class SparkConnectToKafka {

    private static final String TOPIC = "stress";

    public static void main(String [] args) throws TimeoutException {

        final String master = args.length > 0 ? args[0] : "local[4]";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("ConnectToKafka")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", TOPIC)
                .load();

        Dataset<Row> windowed_temp = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic3")
                .load();

        Dataset<Row> decodedDF = df.selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'timestamp TIMESTAMP, id INT, status STRING, stressLevel INT') as decoded_data")
                .selectExpr(
                        "decoded_data.id as id",
                        "decoded_data.status as status",
                        "decoded_data.stressLevel as stressLevel",
                        "decoded_data.timestamp as timestamp");

        Dataset<Row> decodedDF2 = windowed_temp.selectExpr("CAST(value AS STRING) as data");

        Dataset<Row> result1 = decodedDF
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds"))
                .agg(avg("stressLevel")).alias("avg_stress");

        Dataset<Row> result2 = decodedDF
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds","5 seconds"))
                .agg(avg("stressLevel")).alias("avg_stress");

        Dataset<Row> result3 = decodedDF
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds","1 second"))
                .agg(avg("stressLevel")).alias("avg_stress");

        Dataset<Row> result4 = decodedDF
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds"),col("id"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result5 = decodedDF
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds","5 seconds"),col("id"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result6 = decodedDF
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds","1 second"),col("id"))
                .agg(max("stressLevel")).alias("max_stress");

        /*
        StreamingQuery query = result.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

         */

        StreamingQuery query = result6.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String line = value.toString();
                        line = line.replace("[", "")
                                .replace("]", "")
                                .replace(".0", "")
                                .replace(":00","");
                        System.out.println("Process " + line + " at time: " + Instant.now());
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output6.csv",true))) {
                            writer.write(line);
                            writer.newLine();
                        } catch (IOException e) {
                            e.printStackTrace();
                            // Handle IOException appropriately
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        ).start();

        StreamingQuery query2 = decodedDF2.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String line = value.toString();
                        System.out.println("Process w" + line + " at time: " + Instant.now());
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output2.csv",true))) {
                            writer.write(line);
                            writer.newLine();
                        } catch (IOException e) {
                            e.printStackTrace();
                            // Handle IOException appropriately
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        ).start();

        /*
        StreamingQuery query2 = decodedDF
                .groupBy(window(col("timestamp"), "30 seconds", "5 seconds"))
                .agg(avg("temperature")).alias("avg_temperature")
                .writeStream()
                .format("console")
                .start();
         */

        /*
        final StreamingQuery query = spark.sql(query1)
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .start()
        */

        /*
        CsvFileGenerator.generateCsvFile();
        StreamGenerator.generateStream();

         */

        try {
            query.awaitTermination();
            query2.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}