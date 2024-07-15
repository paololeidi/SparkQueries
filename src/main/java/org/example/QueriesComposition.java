package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.apache.spark.sql.functions.*;

// Must use JAVA 11
public class QueriesComposition {

    private static final boolean JOIN_FORMAT = false;
    private static final String STRESS_TOPIC = "stress";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String KSQL_BOOTSTRAP_SERVER = "localhost:29092";
    private static final boolean SEND_TO_KSQL = true;


    public static void main(String [] args) throws TimeoutException {

        final String master = args.length > 0 ? args[0] : "local[4]";

        String server = "";
        if (!SEND_TO_KSQL)
            server = BOOTSTRAP_SERVER;
        else
            server = KSQL_BOOTSTRAP_SERVER;

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("ConnectToKafka")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> stressStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", STRESS_TOPIC)
                .load();

        Dataset<Row> weightStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", "weight")
                .load();

        Dataset<Row> stressStreamDecoded = stressStream
                .selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'stressTime TIMESTAMP, stressId INT, status STRING, stressLevel INT') as decoded_data")
                .selectExpr(
                        "decoded_data.stressId as stressId",
                        "decoded_data.status as status",
                        "decoded_data.stressLevel as stressLevel",
                        "decoded_data.stressTime as stressTime");

        stressStreamDecoded.printSchema();

        Dataset<Row> weightStreamDecoded = weightStream
                .selectExpr("CAST(value AS STRING) as data")
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
                .groupBy(window(col("stressTime"),"2 seconds"),col("stressId"))
                .agg(max("stressLevel"));

        result4.printSchema();

        Dataset<Row> result5 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds","5 seconds"),col("stressId"))
                .agg(max("stressLevel")).alias("max_stress");

        Dataset<Row> result6 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"10 seconds","1 second"),col("stressId"))
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
                .groupBy(window(col("stressTime"),"10 seconds","1 second"))
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
                .groupBy(window(col("stressTime"),"10 seconds","1 second"),col("stressId"))
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
                .groupBy(window(col("weightTime"),"10 seconds","1 second"))
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
                .groupBy(window(col("weightTime"),"10 seconds","1 second"),col("weightId"))
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
                .groupBy(window(col("weightTime"),"10 seconds","1 second"))
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
                .groupBy(window(col("weightTime"),"10 seconds","1 second"),col("weightId"))
                .count().alias("numberOfEvents");

        // Second level queries

        Dataset<Row> outputStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", "output")
                .load();

        Dataset<Row> outputStreamDecoded = outputStream
                .selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'windowOpen TIMESTAMP, windowClose TIMESTAMP, id INT, maxStress INT') as decoded_data")
                .selectExpr(
                        "decoded_data.windowOpen as windowOpen",
                        "decoded_data.windowClose as windowClose",
                        "decoded_data.id as id",
                        "decoded_data.maxStress as maxStress");

        Dataset<Row> result4_2 = outputStreamDecoded
                .withWatermark("windowClose", "2 seconds")
                .groupBy(window(col("windowClose"),"5 seconds", "2 seconds"),col("id"))
                .agg(max("maxStress"));

        // Third level queries
        Dataset<Row> outputStream2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", "output2")
                .load();

        Dataset<Row> outputStreamDecoded2 = outputStream2
                .selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'windowOpen TIMESTAMP, windowClose TIMESTAMP, id INT, maxStress INT') as decoded_data")
                .selectExpr(
                        "decoded_data.windowOpen as windowOpen",
                        "decoded_data.windowClose as windowClose",
                        "decoded_data.id as id",
                        "decoded_data.maxStress as maxStress");

        Dataset<Row> result4_3 = outputStreamDecoded2
                .withWatermark("windowClose", "2 seconds")
                .groupBy(window(col("windowClose"),"5 seconds"),col("id"))
                .agg(max("maxStress"));


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


        /*
        String finalServer1 = server;
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

                        Properties props = new Properties();
                        props.put("bootstrap.servers", finalServer1);
                        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

                        System.out.println("Process " + modifiedLine + " at time: " + Instant.now());
                        ProducerRecord<String, String> record = new ProducerRecord<>("output", modifiedLine);
                        try {
                            producer.send(record).get();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        ).start();
        */

        String finalServer = server;
        StreamingQuery query2 = result4_2.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String line = value.toString().replace("[","").replace("]","");
                        System.out.println("line2222222222222222222222222222: "+line);
                        // Split the line using "," as separator
                        String[] tokens = line.split(",");
                        // Apply the replace functions only to the first token
                        tokens[0] = formatTimestamp(tokens[0]);
                        if (!JOIN_FORMAT){
                            tokens[1] = formatTimestamp(tokens[1]);

                        } else {
                            tokens[4] = formatTimestamp(tokens[4]);
                        }

                        String modifiedLine = "";
                        if (!SEND_TO_KSQL){
                            modifiedLine = String.join(",", tokens);
                        } else {
                            ObjectMapper objectMapper = new ObjectMapper();
                            ObjectNode objectNode = objectMapper.createObjectNode();

                            objectNode.put("windowClose", tokens[0]);
                            objectNode.put("windowOpen", tokens[1]);
                            objectNode.put("id", tokens[2]);
                            objectNode.put("maxStress", tokens[3]);

                            try {
                                modifiedLine = objectMapper.writeValueAsString(objectNode);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }


                        Properties props = new Properties();
                        props.put("bootstrap.servers", finalServer);
                        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

                        System.out.println("Process2222222222222222 " + modifiedLine + " at time: " + Instant.now());
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter("Files/Output/2/output4_2.csv",true))) {
                            writer.write(modifiedLine);
                            writer.newLine();
                        } catch (IOException e) {
                            e.printStackTrace();
                            // Handle IOException appropriately
                        }

                        ProducerRecord<String, String> record = new ProducerRecord<>("output2", modifiedLine);
                        try {
                            producer.send(record).get();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        ).start();


        /*
        StreamingQuery query3 = result4_3.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String line = value.toString().replace("[","").replace("]","");
                        System.out.println("line3333333333333333333333: "+line);
                        // Split the line using "," as separator
                        String[] tokens = line.split(",");
                        // Apply the replace functions only to the first token
                        tokens[0] = formatTimestamp(tokens[0]);
                        if (!JOIN_FORMAT){
                            tokens[1] = formatTimestamp(tokens[1]);
                            // create json for ksqlDB

                        } else {
                            tokens[4] = formatTimestamp(tokens[4]);
                        }
                        // Build the full line again with tokens separated by ","
                        String modifiedLine = String.join(",", tokens);

                        Properties props = new Properties();
                        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
                        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

                        System.out.println("Process3333333333333333333333 " + modifiedLine + " at time: " + Instant.now());
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter("Files/Output/3/output4_3.csv",true))) {
                            writer.write(modifiedLine);
                            writer.newLine();
                        } catch (IOException e) {
                            e.printStackTrace();
                            // Handle IOException appropriately
                        }

                        ProducerRecord<String, String> record = new ProducerRecord<>("output3", modifiedLine);
                        try {
                            producer.send(record).get();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        ).start();

         */

        try {
            //query.awaitTermination();
            query2.awaitTermination();
            //query3.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }


        spark.close();
    }

    @NotNull
    private static String formatTimestamp(String token) {
        return token
                .replace(".0", "")
                .replace(":00", "");
    }
}