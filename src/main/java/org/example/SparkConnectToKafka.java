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
                .selectExpr("from_csv(data, 'timestamp TIMESTAMP, id INT, status STRING, stressLevel INT') as decoded_data")
                .selectExpr(
                        "decoded_data.id as id",
                        "decoded_data.status as status",
                        "decoded_data.stressLevel as stressLevel",
                        "decoded_data.timestamp as timestamp");

        Dataset<Row> weightStreamDecoded = stressStream.selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'timestamp TIMESTAMP, id INT, weight FLOAT') as decoded_data")
                .selectExpr(
                        "decoded_data.id as id",
                        "decoded_data.weight as weight",
                        "decoded_data.timestamp as timestamp");

        Dataset<Row> result1 = stressStreamDecoded
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds"))
                .agg(avg("stressLevel")).alias("avg_stress");

        Dataset<Row> result2 = stressStreamDecoded
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds","5 seconds"))
                .agg(avg("stressLevel")).alias("avg_stress");

        Dataset<Row> result3 = stressStreamDecoded
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds","1 second"))
                .agg(avg("stressLevel")).alias("avg_stress");

        Dataset<Row> result4 = stressStreamDecoded
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds"),col("id"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result5 = stressStreamDecoded
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds","5 seconds"),col("id"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result6 = stressStreamDecoded
                .withWatermark("timestamp", "2 seconds")
                .groupBy(window(col("timestamp"),"10 seconds","1 second"),col("id"))
                .agg(max("stressLevel")).alias("max_stress");



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

        StreamingQuery query2 = weightStreamDecoded.writeStream().foreach(
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

        try {
            query.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}