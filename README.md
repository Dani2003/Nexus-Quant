# Nexus-Quant: Event-Driven Distributed Algorithmic Trading Platform

[![Java 21](https://img.shields.io/badge/Java-21%20(Loom)-ED8B00?logo=openjdk&logoColor=white)](https://openjdk.org/projects/loom/)
[![Spring Boot 3](https://img.shields.io/badge/Spring%20Boot-3.3.0-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Rust](https://img.shields.io/badge/Rust-2021%20Edition-black?logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![gRPC](https://img.shields.io/badge/gRPC-Protobuf-244f5c?logo=grpc&logoColor=white)](https://grpc.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)

Nexus-Quant is an event-driven, distributed algorithmic trading platform designed for high-throughput market tick ingestion, sub-millisecond risk evaluation, and resilient order lifecycle management.

---

## Architectural Architecture

```
                      ┌────────────────────────────────────────┐
                      │          Apache Kafka Cluster          │
                      │  • Topic: market-ticks                 │
                      │  • Topic: trade-orders                 │
                      │  • Topic: risk-rejections              │
                      └──────────────────┬─────────────────────┘
                                         │
                                         ▼ (Manual Acknowledgment upon success)
                 ┌──────────────────────────────────────────────────┐
                 │       Java 21 Spring Boot 3 Orchestrator         │
                 │                                                  │
                 │  • Project Loom Virtual Threads Container        │
                 │  • Reliable Processing-Tied Acknowledgment       │
                 │  • Strict Input Validation (finite price/size)   │
                 │  • Idempotency Guard (Deduplication on Replay)   │
                 │  • Bounded Concurrency Semaphore (Overload Guard)│
                 │  • Blocking gRPC Stub yielding Carrier Threads   │
                 │  • Enforced gRPC Deadlines & Fail-Closed Policy  │
                 └───────────────────────┬──────────────────────────┘
                                         │
                                         │ gRPC (EvaluateRisk)
                                         │ Enforced Deadline: 50ms
                                         ▼
                 ┌──────────────────────────────────────────────────┐
                 │          Rust Risk Compute Sidecar               │
                 │                                                  │
                 │  • Tonic gRPC Server (Async I/O Event Loop)      │
                 │  • tokio::task::spawn_blocking Offload           │
                 │  • Multi-Core Rayon Parallelism across CPU cores │
                 │  • 10,000-Path Monte Carlo VaR Simulation        │
                 │  • Explicit LONG & SHORT Position Modeling       │
                 │  • O(N) Quickselect Value-at-Risk Extraction     │
                 │  • Multi-threaded Benchmark: 10 concurrent       │
                 │    simulations completed in 3ms total            │
                 └──────────────────────────────────────────────────┘
```

---

## Key Resilience & Concurrency Guarantees

### 1. Reliable Kafka Acknowledgment
- **Processing-Tied Commit**: The consumer does not use detached fire-and-forget submission. Each tick is processed on a Virtual Thread, and `acknowledgment.acknowledge()` is called **only after** validation, risk evaluation, and order/rejection publishing succeed.
- **Controlled Error Recovery**: RPC timeouts or sidecar downtime trigger Spring Kafka's `DefaultErrorHandler` with exponential backoff and routing to Dead Letter Queues (DLQ) rather than silently dropping ticks.

### 2. Enforced gRPC Deadline & Fail-Closed Overload Policy
- **Deadline Enforcement**: `blockingStub.withDeadlineAfter(grpcDeadlineMs, TimeUnit.MILLISECONDS)` prevents hung connections.
- **Fail-Closed Design**: If the risk sidecar is unreachable or times out, the trade is rejected with `RISK_ENGINE_UNAVAILABLE` or `RISK_ENGINE_DEADLINE_EXCEEDED` rather than unsafely executed.
- **Bounded Concurrency**: A concurrency limiter (`Semaphore`) caps simultaneous in-flight risk checks, triggering an explicit overload rejection policy when capacity is saturated.

### 3. Dedicated CPU Parallelism in Rust
- **Tokio & Rayon Separation**: Pure CPU compute loops (Box-Muller variate generation and price path integration) are executed on Rayon's threadpool via `tokio::task::spawn_blocking`. This prevents CPU starvation on Tokio's async reactor threads.
- **Position Handling**: Supports both **LONG** (`trade_size > 0`) and **SHORT** (`trade_size < 0`) positions, correctly modeling drawdowns from upward price movement on short exposure.

### 4. Idempotency & Replay Protection
- An in-memory bounded LRU deduplication cache detects duplicate tick deliveries from Kafka partition rebalances and skips re-evaluating risk, avoiding duplicate orders.

---

## Benchmark & Load Verification

Tested on an 8-core machine with 10 concurrent requests (each simulating 10,000 paths):

```bash
cd risk-sidecar
cargo test --release -- --nocapture
```

```text
running 3 tests
SHORT test: VaR=391.8839, approved=true, latency=1595μs
LONG test: VaR=389.3897, approved=true, latency=1761μs
test tests::test_long_position_var ... ok
test tests::test_short_position_var ... ok
Concurrent task finished: VaR=390.48, approved=true, latency=2752μs
Concurrent task finished: VaR=400.94, approved=true, latency=1703μs
Concurrent task finished: VaR=382.55, approved=true, latency=1099μs
...
10 Concurrent Monte Carlo requests completed in 3ms
test tests::test_concurrent_load_performance ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
```

---

## Quickstart & Docker Compose

Launch the complete stack (Zookeeper, Kafka, Rust Risk Sidecar, Java Orchestrator):

```bash
docker compose up --build
```

### Market Tick Ingestion Payload
Publish ticks to `market-ticks`:
```json
{
  "portfolio_id": "MAIN_FUND",
  "symbol": "AAPL",
  "price": 175.50,
  "volume": 250.0,
  "timestamp": 1715000000000
}
```

*When `price >= 150.00` (configurable), the orchestrator triggers an RPC to the Rust sidecar. Approved trades are published to `trade-orders`, while rejected trades are published to `risk-rejections`.*
