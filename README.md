# Nexus-Quant: Event-Driven Distributed Algorithmic Trading Platform

[![Java 21](https://img.shields.io/badge/Java-21%20(Loom)-ED8B00?logo=openjdk&logoColor=white)](https://openjdk.org/projects/loom/)
[![Spring Boot 3](https://img.shields.io/badge/Spring%20Boot-3.3.0-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Rust](https://img.shields.io/badge/Rust-2021%20Edition-black?logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![gRPC](https://img.shields.io/badge/gRPC-Protobuf-244f5c?logo=grpc&logoColor=white)](https://grpc.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)

Nexus-Quant is a high-throughput, low-latency distributed algorithmic trading system architected to process real-time market data streams and execute sub-millisecond risk evaluation and order workflows.

---

## Architecture Overview

```
                      ┌───────────────────────────────────────┐
                      │          Apache Kafka Cluster         │
                      │         (Topic: market-ticks)         │
                      └──────────────────┬────────────────────┘
                                         │
                                         ▼
                 ┌─────────────────────────────────────────────────┐
                 │       Java 21 Spring Boot 3 Orchestrator        │
                 │                                                 │
                 │  • Project Loom Virtual Threads Concurrency     │
                 │  • Non-blocking Spring Kafka Message Listener   │
                 │  • Real-time Price Threshold Detection          │
                 │  • Non-blocking gRPC Client (BlockingStub on VT)│
                 └───────────────────────┬─────────────────────────┘
                                         │
                                         │ gRPC (EvaluateRisk)
                                         │ Target Latency: < 5ms
                                         ▼
                 ┌─────────────────────────────────────────────────┐
                 │          Rust Risk Compute Sidecar              │
                 │                                                 │
                 │  • High-performance Tonic gRPC Server           │
                 │  • Multi-worker tokio::spawn Task Concurrency   │
                 │  • 10,000-Path Monte Carlo VaR Simulation       │
                 │  • Box-Muller Geometric Brownian Motion (GBM)   │
                 │  • O(N) Quickselect Value-at-Risk Extraction    │
                 │  • Benchmark Latency: ~924 μs (< 1 ms)          │
                 └─────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Main Orchestrator** | **Java 21**, **Spring Boot 3.3**, **Spring Kafka**, **WebFlux** | Ingests market ticks, checks trading thresholds, and orchestrates trading workflows on **Virtual Threads (Project Loom)** without OS-thread blocking. |
| **Compute Sidecar** | **Rust (Edition 2021)**, **Tokio**, **Tonic**, **Prost** | Sub-millisecond compute engine calculating 10,000-path Monte Carlo Value-at-Risk (VaR). |
| **Inter-Service Protocol** | **gRPC / Protocol Buffers (proto3)** | Ultra low-overhead binary RPC communication over HTTP/2. |
| **Messaging Spine** | **Apache Kafka & Zookeeper** | Resilient, distributed event streaming pipeline for market ticks and order events. |
| **Containerization** | **Docker & Docker Compose** | Multi-stage production container builds for local dev and cloud deployment. |

---

## Project Structure

```
Nexus-Quant/
├── proto/
│   └── risk_engine.proto          # Shared gRPC protobuf contract
├── orchestrator/                  # Java 21 Spring Boot 3 Main Service
│   ├── src/main/java/com/trading/orchestrator/
│   │   ├── client/
│   │   │   └── RiskGrpcClient.java           # Loom-friendly gRPC client
│   │   ├── config/
│   │   │   └── KafkaConsumerConfig.java      # Virtual Thread Kafka Listener Factory
│   │   ├── grpc/                             # Generated Protobuf & gRPC stubs
│   │   ├── model/
│   │   │   ├── MarketTick.java               # Immutable market data record
│   │   │   └── OrderStatus.java              # Order lifecycle state
│   │   ├── service/
│   │   │   └── MarketDataConsumerService.java# Event consumer & threshold evaluator
│   │   └── OrchestratorApplication.java
│   ├── src/main/resources/
│   │   └── application.yml                   # Configuration with Loom & Kafka properties
│   ├── src/test/java/                        # Unit & concurrency tests
│   ├── Dockerfile
│   └── pom.xml
├── risk-sidecar/                  # Rust Tokio/Tonic Compute Engine
│   ├── src/
│   │   └── main.rs                # Multi-worker Monte Carlo VaR simulator & Tonic server
│   ├── build.rs                   # Automatic proto compilation
│   ├── Cargo.toml
│   └── Dockerfile
├── docker-compose.yml             # Full stack orchestration (Zookeeper, Kafka, Orchestrator, Sidecar)
├── .gitignore
└── README.md
```

---

## Performance Benchmark

The Rust Monte Carlo VaR simulation was verified with concurrent worker tasks across 10,000 paths:

```bash
cd risk-sidecar
cargo test --release -- --nocapture
```

```text
running 1 test
Test Simulation: VaR(95%)=379.8869, is_approved=true, latency=924μs
test tests::test_monte_carlo_var_simulation ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

* **Target Latency:** `< 5 ms`
* **Achieved Benchmark Latency:** **~924 μs (< 1 ms)**

---

## Quickstart & Deployment

### 1. Run via Docker Compose
To launch the entire platform (Kafka, Zookeeper, Rust Risk Sidecar, Java Orchestrator):

```bash
docker compose up --build
```

### 2. Run Locally for Development

#### A. Start Kafka & Zookeeper
```bash
docker compose up -d zookeeper kafka
```

#### B. Start Rust Risk Sidecar
```bash
cd risk-sidecar
cargo run --release
```
*Listens on `0.0.0.0:50051`.*

#### C. Start Java Orchestrator
```bash
cd orchestrator
mvn spring-boot:run
```
*Listens on `http://localhost:8080`.*

---

## Market Data Tick Format

Publish tick events to the `market-ticks` Kafka topic:

```json
{
  "portfolio_id": "TECH_PORTFOLIO_1",
  "symbol": "AAPL",
  "price": 175.50,
  "volume": 250.0,
  "timestamp": 1715000000000
}
```

*When `price >= 150.00` (configurable), the Java Orchestrator triggers an RPC to the Rust compute sidecar to simulate 10,000 price paths and determine trade approval.*

---

## License
MIT License.
