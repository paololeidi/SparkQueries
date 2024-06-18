package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

// Must use JAVA 11
public class Queries {

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
                .selectExpr("from_csv(data, 'stressTime TIMESTAMP, stressId INT, status STRING, stressLevel INT') as decoded_data")
                .selectExpr(
                        "decoded_data.stressId as stressId",
                        "decoded_data.status as status",
                        "decoded_data.stressLevel as stressLevel",
                        "decoded_data.stressTime as stressTime");

        Dataset<Row> weightStreamDecoded = weightStream.selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'weightTime TIMESTAMP, weightId INT, weight FLOAT') as decoded_data")
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
                .groupBy(window(col("stressTime"),"10 seconds","1 second"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result4 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds"),col("stressId"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result5 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds","5 seconds"),col("stressId"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result6 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds","1 second"),col("stressId"))
                .agg(max("stressLevel")).alias("max_stress");

        // Apply watermarks on event-time columns
        Dataset<Row> stressWithWatermark = stressStreamDecoded.withWatermark("stressTime", "30 seconds");
        Dataset<Row> weightWithWatermark = weightStreamDecoded.withWatermark("weightTime", "30 seconds");

        Dataset<Row> join = stressWithWatermark.join(
                weightWithWatermark,
                expr(
                        "stressId = weightId AND " +
                                "weightTime >= stressTime AND " +
                                "weightTime <= stressTime + interval 10 seconds "),
                "leftOuter"                 // can be "inner", "leftOuter", "rightOuter", "fullOuter", "leftSemi"
        );


        StreamingQuery query = join.writeStream().foreach(
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
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter("Files/Output/join.csv",true))) {
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

        try {
            query.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}