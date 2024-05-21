import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka
from clickhouse_driver import Client
import json


class ParseKafkaMessage(beam.DoFn):
    def process(self, element):
        # Debug: Print raw message
        print(f"Received Kafka message: {element}")
        try:
            # Decode and parse the JSON message
            data = json.loads(element.value.decode("utf-8"))
            # Debug: Print parsed data
            print(f"Parsed message data: {data}")
            # Emit the parsed data for downstream processing
            yield data
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse JSON: {e}")


class WriteToClickHouse(beam.DoFn):
    def __init__(self, host, database, table, user, password):
        self.client = None
        self.host = host
        self.database = database
        self.table = table
        self.user = user
        self.password = password

    def start_bundle(self):
        # Initialize ClickHouse client
        self.client = Client(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def process(self, element):
        # Parameterized query for security and flexibility
        query = f"INSERT INTO {self.table} (device_id, timestamp, temperature, vibration) VALUES"
        values = (
            element["device_id"],
            element["timestamp"],
            element["temperature"],
            element["vibration"],
        )
        # Debug: Print values to be inserted
        print(f"Inserting into ClickHouse: {values}")
        # Use placeholders and execute_many for efficiency
        self.client.execute(f"{query} (%s, %s, %s, %s)", [values])
        # Print confirmation after each insertion
        print(f"Inserted into ClickHouse: {values}")

    def finish_bundle(self):
        # Disconnect the ClickHouse client
        if self.client:
            self.client.disconnect()


def run():
    # Create PipelineOptions object
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    # Use command-line arguments or environment variables for configuration
    kafka_bootstrap_servers = "localhost:9092"  # Default value
    kafka_topic = "iot_data"
    clickhouse_host = "localhost"  # Default value
    clickhouse_database = "default"  # Default value
    clickhouse_table = "iot_data"  # Default value
    clickhouse_user = "default"  # Default value
    clickhouse_password = "password"  # Default value

    # Increase consumer polling timeout
    consumer_config = {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": "beam_consumer",
        "session.timeout.ms": "10000",  # Set session timeout
        "max.poll.interval.ms": "10000"  # Set max poll interval
    }

    # Create the Beam pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromKafka"
            >> ReadFromKafka(
                consumer_config=consumer_config,
                topics=[kafka_topic],
                withConsumerPollingTimeout=10  # Increase polling timeout to 10 seconds
            )
            | "ParseKafkaMessage" >> beam.ParDo(ParseKafkaMessage())
            | "WriteToClickHouse"
            >> beam.ParDo(
                WriteToClickHouse(
                    host=clickhouse_host,
                    database=clickhouse_database,
                    table=clickhouse_table,
                    user=clickhouse_user,
                    password=clickhouse_password,
                )
            )
        )


if __name__ == "__main__":
    run()
