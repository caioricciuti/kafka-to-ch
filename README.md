# Data Engineering Project: Kafka to ClickHouse

This project demonstrates how to use Apache Kafka to stream IoT data into ClickHouse using both Apache Beam and Apache Spark. The project includes Docker configurations, data producer scripts, and data pipeline scripts.

## Project Structure

- `beam_to_clickhouse.py`: Apache Beam pipeline to read data from Kafka and write to ClickHouse.
- `clickhouse-jdbc-0.3.1.jar`: JDBC driver for ClickHouse.
- `create_table.sql`: SQL script to create the necessary table in ClickHouse.
- `docker-compose.yml`: Docker Compose file to set up Kafka, Zookeeper, and ClickHouse.
- `iot_data_producer.py`: Python script to produce simulated IoT data to Kafka.
- `spark_to_clickhouse.py`: Apache Spark pipeline to read data from Kafka and write to ClickHouse.

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- Python 3.7+ installed
- Virtualenv (optional, but recommended)

### 1. Setting Up the Environment

1. **Clone the Repository**:
    ```sh
    git clone https://github.com/caioricciuti/kafka-to-ch
    cd kafka-to-ch
    ```

2. **Create a Virtual Environment** (optional, but recommended):
    ```sh
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install Python Dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

### 2. Setting Up Docker Containers

1. **Start Kafka, Zookeeper, and ClickHouse**:
    ```sh
    docker-compose up -d
    ```

2. **Create ClickHouse Table**:
    - Access the ClickHouse client:
      ```sh
      docker exec -it <clickhouse-container-id> clickhouse-client
      ```
    - Run the `create_table.sql` script to create the necessary table:
      ```sql
      CREATE TABLE iot_data (
          device_id UInt64,
          timestamp UInt64,
          temperature Float64,
          vibration Float64
      ) ENGINE = MergeTree() ORDER BY timestamp;
      ```

### 3. Running the Data Producer

1. **Run the IoT Data Producer**:
    ```sh
    python iot_data_producer.py
    ```

### 4. Running the Apache Beam Pipeline

1. **Run the Beam Pipeline**:
    ```sh
    python beam_to_clickhouse.py
    ```

### 5. Running the Apache Spark Pipeline

1. **Submit the Spark Job**:
    ```sh
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars clickhouse-jdbc-0.3.1.jar spark_to_clickhouse.py
    ```

## Files Description

### `beam_to_clickhouse.py`
This script sets up an Apache Beam pipeline to read messages from a Kafka topic, parse them, and write them to a ClickHouse database.

### `clickhouse-jdbc-0.3.1.jar`
This is the JDBC driver for ClickHouse, required for the Spark job to connect and write data to ClickHouse.

### `create_table.sql`
SQL script to create the necessary table in ClickHouse to store IoT data.

### `docker-compose.yml`
Docker Compose configuration to set up Kafka, Zookeeper, and ClickHouse services.

### `iot_data_producer.py`
Python script that simulates IoT devices by producing random data and sending it to a Kafka topic.

### `spark_to_clickhouse.py`
This script sets up an Apache Spark streaming job to read messages from a Kafka topic, parse them, and write them to a ClickHouse database.

## Notes

- Ensure that all services (Kafka, Zookeeper, ClickHouse) are running properly using Docker.
- Adjust the configurations (like Kafka topic, ClickHouse credentials) as necessary.
- Monitor the logs for any errors during execution.

## Troubleshooting

- **Kafka Issues**: Ensure Kafka and Zookeeper are running and the Kafka topic is correctly created.
- **ClickHouse Issues**: Ensure ClickHouse is running and accessible, and the table schema matches the data being inserted.
- **Python Dependencies**: Ensure all required Python packages are installed.

## License

This project is licensed under the MIT License.

## Acknowledgments

- Apache Beam
- Apache Spark
- ClickHouse
- Kafka

