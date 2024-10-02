package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

// Must use JAVA 11
public class Queries {

    private static final boolean JOIN_FORMAT = false;

    public static void main(String [] args) throws TimeoutException {

        final String master = args.length > 0 ? args[0] : "local[4]";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("ConnectToKafka")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> stressStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "stress")
                .load();

        Dataset<Row> weightStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "weight")
                .load();

        Dataset<Row> stressStreamDecoded = stressStream.selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_json(data, 'stressTime TIMESTAMP, stressId INT, status STRING, stressLevel INT') as decoded_data")
                .selectExpr(
                        "decoded_data.stressId as stressId",
                        "decoded_data.status as status",
                        "decoded_data.stressLevel as stressLevel",
                        "decoded_data.stressTime as stressTime");

        stressStreamDecoded.printSchema();

        Dataset<Row> weightStreamDecoded = weightStream.selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_json(data, 'weightTime TIMESTAMP, weightId INT, weight FLOAT') as decoded_data")
                .selectExpr(
                        "decoded_data.weightId as weightId",
                        "decoded_data.weight as weight",
                        "decoded_data.weightTime as weightTime");

        Dataset<Row> result1 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result2 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds","5 seconds"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result3 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(session_window(col("stressTime"),"5 seconds"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result4 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds"),col("stressId"))
                .agg(max("stressLevel"));

        Dataset<Row> result5 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds","5 seconds"),col("stressId"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result6 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(session_window(col("stressTime"),"5 seconds"),col("stressId"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result7 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds"))
                .agg(min("stressLevel")).alias("min_stress");

        Dataset<Row> result8 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds","5 seconds"))
                .agg(min("stressLevel")).alias("min_stress");

        Dataset<Row> result9 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(session_window(col("stressTime"),"5 seconds"))
                .agg(min("stressLevel")).alias("min_stress");

        Dataset<Row> result10 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds"),col("stressId"))
                .agg(min("stressLevel")).alias("min_stress");

        Dataset<Row> result11 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds","5 seconds"),col("stressId"))
                .agg(min("stressLevel")).alias("min_stress");

        Dataset<Row> result12 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(session_window(col("stressTime"),"5 seconds"),col("stressId"))
                .agg(min("stressLevel")).alias("min_stress");

        Dataset<Row> result13 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"10 seconds"))
                .agg(avg("weight")).alias("avg_weight");

        Dataset<Row> result14 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"10 seconds","5 seconds"))
                .agg(avg("weight")).alias("avg_weight");

        Dataset<Row> result15 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(session_window(col("weightTime"),"5 seconds"))
                .agg(avg("weight")).alias("avg_weight");

        Dataset<Row> result16 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"10 seconds"),col("weightId"))
                .agg(avg("weight")).alias("avg_weight");

        Dataset<Row> result17 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"10 seconds","5 seconds"),col("weightId"))
                .agg(avg("weight")).alias("avg_weight");

        Dataset<Row> result18 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(session_window(col("weightTime"),"5 seconds"),col("weightId"))
                .agg(avg("weight")).alias("avg_weight");

        Dataset<Row> result19 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"10 seconds"))
                .count().alias("numberOfEvents");

        Dataset<Row> result20 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"10 seconds","5 seconds"))
                .count().alias("numberOfEvents");

        Dataset<Row> result21 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(session_window(col("weightTime"),"5 seconds"))
                .count().alias("numberOfEvents");

        Dataset<Row> result22 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"10 seconds"),col("weightId"))
                .count().alias("numberOfEvents");

        Dataset<Row> result23 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"10 seconds","5 seconds"),col("weightId"))
                .count().alias("numberOfEvents");

        Dataset<Row> result24 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(session_window(col("weightTime"),"5 seconds"),col("weightId"))
                .count().alias("numberOfEvents");

        // Apply watermarks on event-time columns
        Dataset<Row> stressWithWatermark = stressStreamDecoded.withWatermark("stressTime", "30 seconds");
        Dataset<Row> weightWithWatermark = weightStreamDecoded.withWatermark("weightTime", "30 seconds");

        Dataset<Row> join = stressWithWatermark.join(
                weightWithWatermark,
                expr(
                        "stressId = weightId AND " +
                                "weightTime >= stressTime - interval 5 seconds AND " +
                                "weightTime <= stressTime + interval 5 seconds "),
                "leftOuter"                 // can be "inner", "leftOuter", "rightOuter", "fullOuter", "leftSemi"
        ).select("stressTime", "stressId", "status", "stressLevel", "weightTime", "weight");


        StreamingQuery query = result4.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String line = value.toString().replace("[","").replace("]","");
                        System.out.println("line: "+line);
                        // Split the line using "," as separator
                        String[] tokens = line.split(",");
                        // Apply the replace functions only to the first token
                        tokens[0] = formatTimestamp(tokens[0]);
                        if (!JOIN_FORMAT){
                            tokens[1] = formatTimestamp(tokens[1]);
                        } else {
                            tokens[4] = formatTimestamp(tokens[4]);
                        }
                        // Build the full line again with tokens separated by ","
                        String modifiedLine = String.join(",", tokens);

                        System.out.println("Process " + modifiedLine + " at time: " + Instant.now());
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter("Files/Output/Queries/output4.csv",true))) {
                            writer.write(modifiedLine);
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

        try {
            query.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }

    @NotNull
    private static String formatTimestamp(String token) {
        return token
                .replace(".0", "");
    }
}