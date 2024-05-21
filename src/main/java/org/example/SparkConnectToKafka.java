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

        Dataset<Row> df2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic2")
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

        /*

        Dataset<Row> decodedDF = df.selectExpr("CAST(value AS STRING) as csv_data")
                .selectExpr("from_csv(csv_data, 'ID INT,  status STRING, stress_level INT, timestamp BIGINT') as decoded_data")
                .selectExpr("decoded_data.Id as ID",
                        "decoded_data.status as status",
                        "decoded_data.stress_level as stress_level",
                        "cast(from_unixtime(decoded_data.timestamp/1000) as timestamp) as ts");

        //decodedDF.createOrReplaceTempView("InputTable");


        //Query 1:  Write a continuous query that emits the max stress for each arm.
        //String query1 = "SELECT id, max(stress_level) as max_stress from InputTable group by id";
        Dataset<Row> stress = decodedDF
                .groupBy(col("id"))
                .max("stress_level")
                .select(col("id"), col("max(stress_level)"));

        //Query 2:  Write a continuous query that emits the max stress for each arm every 10 seconds

        Dataset<Row> win_stress = decodedDF
                .groupBy(col("id"), window(col("ts"), "10 seconds", "10 seconds").alias("window"))
                .max("stress_level")
                .select(col("id"), col("max(stress_level)"), col("window"));


        //Query 3: A continuous query that emits the average stress level between a pick (status==goodGrasped) and a place (status==placingGood).
        Dataset<Row> goodGrasped = decodedDF
                .where("status == 'good grasped'")
                .withWatermark("ts", "10 minutes")
                .select(col("id").alias("s1_id"), col("status").alias("s1_status"), col("stress_level").alias("s1_stress"),
                        col("ts").alias("s1_ts"), window(col("ts"), "10 seconds", "5 seconds").alias("s1_window"));

        Dataset<Row> placingGood = decodedDF
                .where("status == 'placing a good'")
                .withWatermark("ts", "10 minutes")
                .select(col("id").alias("s2_id"), col("status").alias("s2_status"), col("stress_level").alias("s2_stress"),
                        col("ts").alias("s2_ts"), window(col("ts"), "10 seconds", "5 seconds").alias("s2_window"));

        Dataset<Row> result = goodGrasped.join(placingGood, expr("s1_id = s2_id AND s1_ts < s2_ts AND s1_window.start = s2_window.start " +
                "AND s1_window.end = s2_window.end")).select(col("s1_id"), col("s2_id"), col("s1_stress"), col("s2_stress"),
                col("s1_ts"), col("s2_ts"), col("s1_window"), col("s2_window"), expr("(s1_stress + s2_stress)/2 as average"));

        StreamingQuery query =  win_stress.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

         */

        Dataset<Row> result = decodedDF
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds"), col("id"))
                .agg(max("stressLevel")).alias("max_stress_level");

        /*
        StreamingQuery query = result.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

         */

        StreamingQuery query = result.writeStream().foreach(
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
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.csv",true))) {
                            line = Instant.now()+","+line;
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