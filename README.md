# ✈️ Real-Time Flight Data Streaming & Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue)
![Apache Superset](https://img.shields.io/badge/Apache%20Superset-BI%20Dashboard-teal)
![Docker](https://img.shields.io/badge/Docker-Containerization-2496ED)

## 📌 Project Overview
This project is an end-to-end **Real-Time Event-Driven Data Engineering Pipeline**. It simulates live commercial flight data, streams it securely with high throughput, processes the events in real-time, and visualizes the insights on an interactive BI dashboard. 

The architecture is designed to reflect industry standards for handling fast-moving data, utilizing containerization for seamless deployment and environment isolation.

## 🏗️ Architecture & Data Flow

1. **Data Ingestion (Producer):** A Python-based simulator generates live flight events (Flight ID, Airline, Origin, Destination, Status, Delay Minutes, Event Time).
2. **Event Streaming (Apache Kafka):** Events are published to a Kafka topic (`flight-updates`), decoupling the data generation from data processing and acting as a robust message broker. Zookeeper is used for cluster management.
3. **Stream Processing (Consumer):** A Python consumer subscribes to the Kafka topic, processes the incoming JSON payloads, and ingests them into the database with sub-second latency.
4. **Data Storage (PostgreSQL):** A relational database optimized for time-series and transactional data stores the live flight status.
5. **Data Visualization (Apache Superset):** A custom-built Superset container connects directly to PostgreSQL to serve live KPIs and charts (e.g., Busiest Routes, Average Delay per Airline, Live Flight Feed).

## 🛠️ Technology Stack
* **Language:** Python
* **Message Broker:** Apache Kafka & Zookeeper
* **Database:** PostgreSQL
* **BI & Analytics:** Apache Superset
* **Infrastructure:** Docker & Docker Compose (Custom Bridge Network: `airline-net`)

## 📊 Dashboard Insights
The Superset dashboard includes the following visualizations:
* **Live Flight Feed:** A raw record table updating in real-time sorted by the latest `event_time`.
* **Airlines Performance:** A horizontal bar chart displaying average delay minutes per airline.
* **Busiest Routes:** Aggregation of origin and destination to track high-traffic flight paths.
* **Flight Status Breakdown:** A pie chart showing the real-time ratio of `ON_TIME`, `DELAYED`, and `CANCELLED` flights.

## 🚀 How to Run the Project

### Prerequisites
Make sure you have [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed on your machine.

### Step-by-Step Guide

**1. Clone the repository:**
```bash
git clone [https://github.com/your-username/Airline-Streaming-Project.git](https://github.com/your-username/Airline-Streaming-Project.git)
cd Airline-Streaming-Project
