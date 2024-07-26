package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
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

import static org.apache.spark.sql.functions.*;

// Must use JAVA 11
public class QueriesCompositionAutomatization {

    private static final boolean JOIN_FORMAT = false;
    private static final String STRESS_TOPIC = "stress";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String KSQL_BOOTSTRAP_SERVER = "localhost:9092";
    private static final boolean QUERY3_ON_KSQLDB = false;
    private static final boolean QUERY1 = true;
    private static final boolean QUERY2 = true;
    private static final boolean QUERY3 = true;
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
        String[] columns = new String[4];
        columns[0] = "windowOpen";
        columns[1] = "windowClose";
        columns[2] = "id";
        columns[3] = "maxStress";

        DataStreamWriter query2writer = createSlidingWindowQuery(
                spark,
                finalServer,
                "output",
                columns,
                columns,
                "4 seconds",
                "2 seconds",
                "id",
                "maxStress",
                QUERY3_ON_KSQLDB,
                "output2"
        );

        StreamingQuery query2 = query2writer.start();


        DataStreamWriter query3writer = createTumblingWindowQuery(
                spark,
                server,
                "output2",
                columns,
                columns,
                "10 seconds",
                "id",
                "maxStress",
                QUERY3_ON_KSQLDB,
                "output3"
        );

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
                        ProducerRecord<String, String> record = new ProducerRecord<>("query4", modifiedLine);

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

        Dataset<Row> result16 = weightStreamDecoded
                .withWatermark("weightTime", "2 seconds")
                .groupBy(window(col("weightTime"),"2 seconds"),col("weightId"))
                .agg(avg("weight")).alias("avg_weight");

        StreamingQuery query16 = result16.writeStream().foreach(
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
                        ProducerRecord<String, String> record = new ProducerRecord<>("query16", modifiedLine);

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

        Dataset<Row> outputQuery4Stream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", "query4")
                .load();

        Dataset<Row> outputQuery4StreamDecoded = outputQuery4Stream
                .selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'windowOpen TIMESTAMP, windowClose TIMESTAMP, id INT, maxStress INT') as decoded_data")
                .selectExpr(
                        "decoded_data.windowOpen as windowOpen4",
                        "decoded_data.windowClose as windowClose4",
                        "decoded_data.id as id4",
                        "decoded_data.maxStress as maxStress4");

        Dataset<Row> outputQuery16Stream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", "query16")
                .load();

        Dataset<Row> outputQuery16StreamDecoded = outputQuery16Stream
                .selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'windowOpen TIMESTAMP, windowClose TIMESTAMP, id INT, avgWeight FLOAT') as decoded_data")
                .selectExpr(
                        "decoded_data.windowOpen as windowOpen16",
                        "decoded_data.windowClose as windowClose16",
                        "decoded_data.id as id16",
                        "decoded_data.avgWeight as avgWeight16");

        Dataset<Row> join2 = outputQuery4StreamDecoded.join(
                outputQuery16StreamDecoded,
                expr(
                        "id4 = id16 AND " +
                                " windowClose4 = windowClose16 "),
                "inner"                 // can be "inner", "leftOuter", "rightOuter", "fullOuter", "leftSemi"
        ).select("windowOpen4", "windowClose4", "id4", "maxStress4", "avgWeight16");

        StreamingQuery joinQuery2 = join2.writeStream().foreach(
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
                        ProducerRecord<String, String> record = new ProducerRecord<>("join2", modifiedLine);

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
            query16.awaitTermination();
            joinQuery2.awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }


        spark.close();
    }

    private static DataStreamWriter createSlidingWindowQuery(
            SparkSession spark,
            String server,
            String sourceTopic,
            String[] inputColumns,
            String[] outputColumns,
            String windowDuration,
            String slideDuration,
            String groupByColumn,
            String maxColumn,
            boolean sendToKsqlDB,
            final String outputTopic) throws TimeoutException {
        Dataset<Row> outputStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", sourceTopic)
                .load();

        Dataset<Row> outputStreamDecoded = outputStream
                .selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'f1 TIMESTAMP, f2 TIMESTAMP, f3 INT, f4 INT') as decoded_data")
                .selectExpr(
                        "decoded_data.f1 as " + inputColumns[0],
                        "decoded_data.f2 as " + inputColumns[1],
                        "decoded_data.f3 as " + inputColumns[2],
                        "decoded_data.f4 as " + inputColumns[3]);

        Dataset<Row> result = outputStreamDecoded
                .withWatermark("windowClose", "2 seconds")
                .groupBy(window(col("windowClose"), windowDuration, slideDuration),col(groupByColumn))
                .agg(max(maxColumn));

        DataStreamWriter query2writer = result.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String[] tokens = getTokens(value);

                        String modifiedLine;

                        if (!sendToKsqlDB){
                            modifiedLine = String.join(",", tokens);
                        } else {
                            ObjectMapper objectMapper = new ObjectMapper();
                            ObjectNode objectNode = objectMapper.createObjectNode();

                            objectNode.put(outputColumns[0], tokens[0]);
                            objectNode.put(outputColumns[1], tokens[1]);
                            objectNode.put(outputColumns[2], tokens[2]);
                            objectNode.put(outputColumns[3], tokens[3]);

                            try {
                                modifiedLine = objectMapper.writeValueAsString(objectNode);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        KafkaProducer<String, String> producer = getStringStringKafkaProducer(server);

                        System.out.println("Process2222222222222222 " + modifiedLine + " at time: " + Instant.now());

                        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, modifiedLine);
                        try {
                            producer.send(record).get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        );
        return query2writer;
    }

    private static DataStreamWriter createTumblingWindowQuery(
            SparkSession spark,
            String server,
            String sourceTopic,
            String[] inputColumns,
            String[] outputColumns,
            String windowDuration,
            String groupByColumn,
            String maxColumn,
            boolean sendToKsqlDB,
            final String outputTopic) throws TimeoutException {
        Dataset<Row> outputStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", sourceTopic)
                .load();

        Dataset<Row> outputStreamDecoded = outputStream
                .selectExpr("CAST(value AS STRING) as data")
                .selectExpr("from_csv(data, 'f1 TIMESTAMP, f2 TIMESTAMP, f3 INT, f4 INT') as decoded_data")
                .selectExpr(
                        "decoded_data.f1 as " + inputColumns[0],
                        "decoded_data.f2 as " + inputColumns[1],
                        "decoded_data.f3 as " + inputColumns[2],
                        "decoded_data.f4 as " + inputColumns[3]);

        Dataset<Row> result = outputStreamDecoded
                .withWatermark("windowClose", "2 seconds")
                .groupBy(window(col("windowClose"), windowDuration),col(groupByColumn))
                .agg(max(maxColumn));

        DataStreamWriter query2writer = result.writeStream().foreach(
                new ForeachWriter() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true;
                    }
                    @Override
                    public void process(Object value) {
                        String[] tokens = getTokens(value);

                        String modifiedLine;

                        if (!sendToKsqlDB){
                            modifiedLine = String.join(",", tokens);
                        } else {
                            ObjectMapper objectMapper = new ObjectMapper();
                            ObjectNode objectNode = objectMapper.createObjectNode();

                            objectNode.put(outputColumns[0], tokens[0]);
                            objectNode.put(outputColumns[1], tokens[1]);
                            objectNode.put(outputColumns[2], tokens[2]);
                            objectNode.put(outputColumns[3], tokens[3]);

                            try {
                                modifiedLine = objectMapper.writeValueAsString(objectNode);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        KafkaProducer<String, String> producer = getStringStringKafkaProducer(server);

                        System.out.println("Process2222222222222222 " + modifiedLine + " at time: " + Instant.now());

                        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, modifiedLine);
                        try {
                            producer.send(record).get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    @Override public void close(Throwable errorOrNull) {
                    }
                }
        );
        return query2writer;
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