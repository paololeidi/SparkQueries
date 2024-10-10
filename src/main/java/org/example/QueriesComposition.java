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
    private static final String BOOTSTRAP_SERVER = "localhost:19092";
    private static final String KSQL_BOOTSTRAP_SERVER = "localhost:19092";
    private static final boolean QUERY3_ON_KSQLDB = true;
    private static final boolean QUERY1 = true;
    private static final boolean QUERY2 = true;
    private static final boolean QUERY3 = false;
    public static final String OUTPUT_FILE_NAME = "Files/Output/Networks/flink-spark-spark.csv";


    public static void main(String [] args) throws TimeoutException {

        final String master = args.length > 0 ? args[0] : "local[4]";

        String server = "";
        if (!QUERY3_ON_KSQLDB)
            server = BOOTSTRAP_SERVER;
        else
            server = KSQL_BOOTSTRAP_SERVER;

        String finalServer = server;

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

        Dataset<Row> stressStreamDecoded = stressStream
                .selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'stressTime TIMESTAMP, stressId INT, status STRING, stressLevel INT') as decoded_data")
                .selectExpr(
                        "decoded_data.stressId as stressId",
                        "decoded_data.status as status",
                        "decoded_data.stressLevel as stressLevel",
                        "decoded_data.stressTime as stressTime");

        stressStreamDecoded.printSchema();



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
                .groupBy(window(col("windowClose"),"4 seconds", "2 seconds"),col("id"))
                .agg(max("maxStress"));

        StreamingQuery query2 = result4_2.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String[] tokens = getTokens(value);

                        String modifiedLine = "";
                        if (!QUERY3_ON_KSQLDB){
                            modifiedLine = String.join(",", tokens);
                        } else {
                            ObjectMapper objectMapper = new ObjectMapper();
                            ObjectNode objectNode = objectMapper.createObjectNode();

                            objectNode.put("windowOpen", tokens[0]);
                            objectNode.put("windowClose", tokens[1]);
                            objectNode.put("id", tokens[2]);
                            objectNode.put("maxStress", tokens[3]);

                            try {
                                modifiedLine = objectMapper.writeValueAsString(objectNode);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }


                        KafkaProducer<String, String> producer = getStringStringKafkaProducer(finalServer);

                        System.out.println("Process2222222222222222 " + modifiedLine + " at time: " + Instant.now());

                        ProducerRecord<String, String> record = new ProducerRecord<>("output2", modifiedLine);
                        if (QUERY2){
                            try {
                                producer.send(record).get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        ).start();

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
                .groupBy(window(col("windowClose"),"10 seconds"),col("id"))
                .agg(max("maxStress"));

        StreamingQuery query3 = result4_3.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String[] tokens = getTokens(value);
                        // Build the full line again with tokens separated by ","
                        String modifiedLine = String.join(",", tokens);

                        KafkaProducer<String, String> producer = getStringStringKafkaProducer(finalServer);

                        System.out.println("Process3333333333333333333333 " + modifiedLine + " at time: " + Instant.now());

                        ProducerRecord<String, String> record = new ProducerRecord<>("output3", modifiedLine);
                        if (QUERY3){
                            try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE_NAME,true))) {
                                writer.write(modifiedLine);
                                writer.newLine();
                            } catch (IOException e) {
                                e.printStackTrace();
                                // Handle IOException appropriately
                            }

                            try {
                                producer.send(record).get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        ).start();

        Dataset<Row> result4 = stressStreamDecoded
                .withWatermark("stressTime", "2 seconds")
                .groupBy(window(col("stressTime"),"2 seconds"),col("stressId"))
                .agg(max("stressLevel"));

        StreamingQuery query = result4.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String modifiedLine = getFormattedLine(value);

                        KafkaProducer<String, String> producer = getStringStringKafkaProducer(finalServer);

                        System.out.println("Process " + modifiedLine + " at time: " + Instant.now());
                        ProducerRecord<String, String> record = new ProducerRecord<>("output", modifiedLine);

                        if (QUERY1){
                            try {
                                producer.send(record).get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        ).start();
        try {
            query.awaitTermination();
            query2.awaitTermination();
            query3.awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }


        spark.close();
    }

    @NotNull
    private static String[] getTokens(Object value) {
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
        return tokens;
    }

    @NotNull
    private static String getFormattedLine(Object value) {
        String[] tokens = getTokens(value);
        // Build the full line again with tokens separated by ","
        String modifiedLine = String.join(",", tokens);
        return modifiedLine;
    }

    @NotNull
    private static KafkaProducer<String, String> getStringStringKafkaProducer(String finalServer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", finalServer);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    @NotNull
    private static String formatTimestamp(String token) {
        return token
                .replace(".0", "");
    }
}