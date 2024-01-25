import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.Nullable;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.streaming.OpenChannelRequest;


public class MainApplication {
    static Dotenv dotenv = Dotenv.configure().load();

    static String appId = dotenv.get("APP_ID");
    static String kafkaInputTopic = dotenv.get("KAFKA_INPUT_TOPIC");
    static String brokerServers = dotenv.get("BROKER_SERVERS");
    static String snowflakeDb = dotenv.get("SNOWFLAKE_DB");
    static String snowflakeSchema = dotenv.get("SNOWFLAKE_SCHEMA");
    static String snowflakeTable = dotenv.get("SNOWFLAKE_TABLE");


    // generate random ID and append it to snowflake channel name
    // to ensure multiple instances of this app are not using the same channel name
    // A snowflake channel cannot be written to from multiple sources
    // but can send data to multiple tables
    static String uuid = UUID.randomUUID().toString().replace("-", "");
    static String channel_name = "MY_CHANNEL_" + uuid;

    public static void main(String[] args) throws Exception {
        //create kafka and snowflake properties
        Properties props = new Properties();
        Properties snowflake_props = new Properties();


        //create consumer properties
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, appId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put("max.poll.records", "1000"); // Set batch size to 1000 records


        //configure stream properties
        snowflake_props.put("user", dotenv.get("SNOWFLAKE_USER"));
        snowflake_props.put("url", "https://" + dotenv.get("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com:443");
        snowflake_props.put("private_key", dotenv.get("PRIVATE_KEY"));
        snowflake_props.put("role", dotenv.get("SNOWFLAKE_ROLE"));


        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        //Initialise snowflake ingest channel so we can call channel.close()
        //to gracefully close snowflake channel when app is done running or is terminated
        SnowflakeStreamingIngestChannel channel1 = new SnowflakeStreamingIngestChannel() {
            @Override
            public String getFullyQualifiedName() {
                return null;
            }
            @Override
            public String getName() {
                return null;
            }
            @Override
            public String getDBName() {
                return null;
            }
            @Override
            public String getSchemaName() {
                return null;
            }
            @Override
            public String getTableName() {
                return null;
            }
            @Override
            public String getFullyQualifiedTableName() {
                return null;
            }
            @Override
            public boolean isValid() {
                return false;
            }
            @Override
            public boolean isClosed() {
                return false;
            }
            @Override
            public CompletableFuture<Void> close() {
                return null;
            }
            @Override
            public InsertValidationResponse insertRow(Map<String, Object> map, @Nullable String s) {
                return null;
            }
            @Override
            public InsertValidationResponse insertRows(Iterable<Map<String, Object>> iterable, @Nullable String s) {
                return null;
            }
            @Override
            public String getLatestCommittedOffsetToken() {
                return null;
            }
        };


        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();
                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try (SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT")
                .setProperties(snowflake_props).build()) {

            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(kafkaInputTopic));

            // Create an open channel request on table TABLE_NAME, note that the corresponding
            // db/schema/table needs to be present on snowflake already
            OpenChannelRequest request1 = OpenChannelRequest.builder(channel_name)
                    .setDBName(snowflakeDb)
                    .setSchemaName(snowflakeSchema)
                    .setTableName(snowflakeTable)
                    .setOnErrorOption(
                            OpenChannelRequest.OnErrorOption.ABORT)  // Another ON_ERROR option is CONTINUE
                    .build();

            // Open a streaming ingest channel from the given client
            channel1 = client.openChannel(request1);

            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100)); //Increase duration time to poll more records
                for (ConsumerRecord<String, String> record : records) {

                    //Print out some metadata from the consumer
                    System.out.println("CONSUMER METADATA >>> Key: " + record.key() + ", Value: " + record.value() + " Partition: " + record.partition() + ", Offset:" + record.offset());

                    //assign consumer record to a variable for transformation if need be
                    String value = record.value();

                    Map<String, Object> row = new HashMap<>();

                    //populate map "row" with key/value pairs
                    //keys are snowflake table column names, values are to be inserted into the column for that key
                    row.put("column1", value);

                    //Send record to snowflake
                    InsertValidationResponse response = channel1.insertRow(row, String.valueOf(record.offset()));
                    if (response.hasErrors()) {
                        System.out.println("Error occurred pushing to snowflake >>> " + response.getInsertErrors().get(0).getException());
                    }
                    else {
                        System.out.println("Successfully sent record >>> " + row + " offset token : " + record.offset());
                    }
                }
            }
        }catch (WakeupException e) {
            System.out.println("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            System.out.println("Unexpected exception" + e);
        } finally {
            //close snowflake channel
            channel1.close().get();
            System.out.println("Snowflake Ingest complete");
            // flush and  consumer
            consumer.close(); // this will also commit the offsets if need be.
            System.out.println("The consumer is now gracefully closed.");
        }
    }
}
