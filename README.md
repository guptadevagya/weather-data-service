# Weather Data Service

[](https://www.google.com/search?q=https://github.com/%3CYOUR_USERNAME%3E/%3CYOUR_REPO%3E/actions/workflows/ci.yml)

A fault-tolerant gRPC service for retrieving time-series weather data, featuring an event-driven ingestion pipeline via **Apache Kafka**, an optimized Cassandra schema, and a scalable, multi-container deployment using Docker.

---

## ✨ Key Features

- **Event-Driven Architecture**: Ingests data in real-time using an **Apache Kafka** topic, decoupling data production from consumption.
- **gRPC API**: A well-defined, strongly-typed API for querying weather data using Protocol Buffers.
- **Distributed Database**: Utilizes a 3-node **Apache Cassandra** cluster for a fault-tolerant, distributed data store.
- **Data Processing**: Employs **Apache Spark** for efficient one-time preprocessing and loading of station metadata.
- **Fault Tolerance & Consistency**: The server's Kafka consumer writes to Cassandra with `ConsistencyLevel.ONE`, while gRPC reads use `ConsistencyLevel.THREE`, ensuring `R + W > RF` for strong consistency.
- **Containerized**: Fully containerized with **Docker** and orchestrated with **Docker Compose** for easy setup and deployment.

---

## 🏗️ Architecture

This system uses an event-driven model for data ingestion. A producer script publishes weather records to a Kafka topic. The gRPC server contains a background Kafka consumer that reads from this topic and writes the data to a 3-node Cassandra cluster. Clients can then query the processed data via gRPC endpoints.

```
+----------+      +---------+      +-----------------+      +-----------+
| Producer | ---> |  Kafka  | ---> |  gRPC Server &  | ---> | Cassandra |
| (Script) |      | (Topic) |      | Kafka Consumer  |      |  (DB)     |
+----------+      +---------+      +-----------------+      +-----------+
```

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Installation & Setup

1.  **Clone the repository:**

    ```sh
    git clone https://github.com/<YOUR_USERNAME>/<YOUR_REPO>.git
    cd <YOUR_REPO>
    ```

2.  **Build and start all services:**
    This command starts the gRPC server, the 3-node Cassandra cluster, Zookeeper, and Kafka.

    ```sh
    docker-compose up --build -d
    ```

3.  **Wait for services and load the data:**
    Wait about a minute for the cluster to initialize. Then, run the producer script to publish the weather data to Kafka. The server will automatically consume and insert it into Cassandra.

    ```sh
    docker-compose exec kafka python3 clients/producer.py
    ```

---

## ⚙️ How to Use

Data is ingested via the producer and queried via the gRPC client scripts.

### 1\. Ingesting Data

To load or add data, run the `producer.py` script. This script reads from `weather.parquet` and publishes each record to the `weather_data` Kafka topic.

### 2\. Querying Data

To query the data stored in Cassandra, use the gRPC client scripts.

**Example: Get Maximum Temperature for a Station**

```sh
docker-compose exec weather-db-1 python3 clients/client_station_max.py USR0000WDDG
```

**Expected Output:**

```
344
```

### Available gRPC Query Methods

- **`StationSchema()`**: Retrieves the CQL schema for the `stations` table.
- **`StationName(station_id)`**: Gets the official name for a given station ID.
- **`StationMax(station_id)`**: Returns the highest `tmax` value ever recorded for a station.

---

## 🔧 Technical Details

### Cassandra Schema Design

The `stations` table is designed for efficient time-series queries.

- **Partition Key (`id`)**: Groups all records for a single weather station.
- **Clustering Key (`date`)**: Orders records chronologically within a partition.
- **Static Column (`name`)**: Stores the station name only once per partition, avoiding data duplication.

### Consistency Levels

- **Data Ingestion (via Kafka Consumer)**: Writes to Cassandra use `ConsistencyLevel.ONE`. This ensures high availability for data ingestion, as a write will succeed even if only one replica is available.
- **Data Reading (via gRPC)**: Reads from Cassandra use `ConsistencyLevel.THREE`. With a replication factor (RF) of 3, this guarantees that a query returns the most up-to-date data by querying all replicas, satisfying `R + W > RF` (3 + 1 \> 3).
