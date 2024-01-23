# SnowflakeIngestKafkaDemo #

This repo contains source code demonstrating an ETL application that uses Snowflake ingest Java SDK 
to stream data from a kafka topic directly into a snowflake table.

### How do I get set up? ###
1. Clone this repo to your local machine.
2. Install [kafka](https://kafka.apache.org/quickstart) and create the necessary topics as seen in the sample.env file in this repo.
3. Build and run this app once you have your kafka zookeeper and server running.
4. Test the app by passing a sample record to the input topic and observe the data on snowflake.