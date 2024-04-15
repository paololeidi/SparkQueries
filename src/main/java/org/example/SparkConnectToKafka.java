package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class SparkConnectToKafka {

    private static final String TOPIC = "topic1";

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

        Dataset<Row> decodedDF = df.selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'timestamp TIMESTAMP, temperature FLOAT') as decoded_data")
                .selectExpr(
                        "decoded_data.timestamp as timestamp",
                        "decoded_data.temperature as temperature");

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

        StreamingQuery query = decodedDF
                .withWatermark("timestamp", "5 seconds")
                .groupBy(window(col("timestamp"),"3 seconds"))
                .agg(avg("temperature")).alias("avg_temperature")
                .writeStream()
                .format("console")
                .start();

        // TODO write on kafka topic
        /*StreamingQuery query2 = decodedDF
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

        CsvFileGenerator.generateCsvFile();
        StreamGenerator.generateStream();

        try {
            query.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}