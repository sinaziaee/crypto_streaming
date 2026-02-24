# 🚀 Crypto Real-Time Streaming & ML Platform (Hopefully I will finish this one :)

A real-time crypto market data platform built around the **Kraken WebSocket API**, **Kafka (Redpanda)**, and a streaming analytics pipeline, with a roadmap toward **ML-based price movement inference**, **MLflow model registry**, and **cloud infrastructure using Terraform and AWS**.

This project is designed as a production-style system to demonstrate real-time data ingestion, stream processing, feature engineering, observability, and scalable ML architecture.

---

## 🏗️ Architecture (Phase 1)

Kraken WebSocket (v2)
→ WS Ingestor (Python)
→ Kafka (Confluent Kafka)
→ Stream Consumers (analytics / features / debug)
→ (Future) ML Inference Service

---

## ✨ Features

- Real-time price ingestion from Kraken WebSocket API
- Kafka-based streaming pipeline (using Confluent Kafka locally)
- Separate topics for raw market data:
  - `crypto.ticker.raw`
  - `crypto.trade.raw`
- Async, reconnect-safe WebSocket ingestor
- Kafka consumers for debugging and analytics
- Partitioning by symbol for scalability
- Ingestion timestamp for end-to-end latency measurement

---

## 🧭 Roadmap

### Phase 2 – Streaming Analytics
- Windowed aggregations (1s / 10s / 1m)
- Feature engineering (returns, volatility, spread, volume)
- Output to `crypto.features.*` topics
- Persist to ClickHouse or TimescaleDB
- Grafana dashboards

### Phase 3 – ML Platform
- Historical data ingestion
- Feature store
- Model training for:
  - Price direction
  - Volatility
  - Short-term returns
- Online inference consumer
- MLflow model registry

### Phase 4 – Production & Infrastructure
- Dockerize all services
- Terraform for AWS infrastructure
- Datadog for logs and metrics
- CI/CD pipeline
- Schema validation and DLQ topics

---

## 🛠️ Tech Stack

- Python 3.10+
- Kraken WebSocket API v2
- Kafka (Confluent Kafka for local development)
- aiokafka, websockets, asyncio
- Docker / Docker Compose
- (Planned) ClickHouse / TimescaleDB, Grafana, MLflow, Terraform, AWS, Datadog

---

## 🚦 Getting Started (Local)

### 1) Start Kafka (Confluent Kafka)

```bash
docker compose up -d
```

### 2) Dev setup (uv + pre-commit)

```bash
bash scripts/dev-setup.sh
```
