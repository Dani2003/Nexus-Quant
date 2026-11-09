# Nexus-Quant: Event-Driven Distributed Algorithmic Trading Platform

[![Java 21](https://img.shields.io/badge/Java-21%20(Loom)-ED8B00?logo=openjdk&logoColor=white)](https://openjdk.org/projects/loom/)
[![Spring Boot 3](https://img.shields.io/badge/Spring%20Boot-3.3.0-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Rust](https://img.shields.io/badge/Rust-2021%20Edition-black?logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![gRPC](https://img.shields.io/badge/gRPC-Protobuf-244f5c?logo=grpc&logoColor=white)](https://grpc.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)

Nexus-Quant is an event-driven, distributed algorithmic trading platform designed for high-throughput market tick ingestion, sub-millisecond risk evaluation, resilient order lifecycle management, and durable decision auditability.

---

## Architecture

```
                      ┌────────────────────────────────────────┐
                      │          Apache Kafka Cluster          │
                      │  • market-ticks      (Inbound stream)  │
                      │  • trade-orders      (Approved orders) │
                      │  • trade-executions  (Broker fills)    │
                      │  • risk-rejections   (Audit breaches)  │
                      │  • market-ticks.DLT  (Dead letter)     │
                      └──────────────────┬─────────────────────┘
                                         │
                                         ▼ (Manual Acknowledgment upon success)
                 ┌──────────────────────────────────────────────────┐
                 │       Java 21 Spring Boot 3 Orchestrator         │
                 │                                                  │
                 │  • Project Loom Virtual Threads Container        │
                 │  • Reliable Processing-Tied Acknowledgment       │
                 │  • Post-Success Deduplication & Audit Storage    │
                 │  • Bounded Concurrency Semaphore (Overload Guard)│
                 │  • Synchronous gRPC Stub on Virtual Threads      │
                 │  • Enforced 50ms Deadlines & Fail-Closed Policy  │
                 │  • Order Execution Simulator & Fill Reports      │
                 │  • Reactive Spring WebFlux REST & SSE Stream     │
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

## End-to-End Reliability Guarantees

### 1. Reliable Kafka Acknowledgment
- **Strict Post-Success Commit**: Offsets are committed via `acknowledgment.acknowledge()` **only after** validation, risk evaluation, and synchronous broker event publication succeed.
- **Post-Success Idempotency**: Ticks are only marked as processed in the idempotency cache after durable outcome recording. If a transient RPC failure occurs, the tick is retried by Kafka rather than falsely suppressed as a duplicate.
- **Dead Letter Topic (DLT)**: `DeadLetterPublishingRecoverer` routes poisoned records or records exhausting retries to `market-ticks.DLT` with full diagnostic headers.

### 2. Enforced gRPC Deadline & Fail-Closed Overload Policy
- **Deadline Enforcement**: `blockingStub.withDeadlineAfter(50, TimeUnit.MILLISECONDS)` prevents hung connections.
- **Fail-Closed Design**: If the risk sidecar is unreachable or times out, the trade is rejected with `RISK_ENGINE_UNAVAILABLE` or `RISK_ENGINE_DEADLINE_EXCEEDED` rather than unsafely executed.
- **Bounded Concurrency**: A concurrency limiter (`Semaphore`) caps simultaneous in-flight risk checks, triggering an explicit overload rejection policy when capacity is saturated.

### 3. Dedicated CPU Parallelism in Rust
- **Tokio & Rayon Separation**: Pure CPU compute loops (Box-Muller variate generation and price path integration) are executed on Rayon's threadpool via `tokio::task::spawn_blocking`. This prevents CPU starvation on Tokio's async reactor threads.
- **Position Handling**: Supports both **LONG** (`trade_size > 0`) and **SHORT** (`trade_size < 0`) positions, correctly modeling drawdowns from upward price movement on short exposure.

---

## Reactive WebFlux API & Real-Time SSE

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/api/v1/market/tick` | `POST` | Ingests a single market tick into Kafka. |
| `/api/v1/market/simulate` | `POST` | Starts a synthetic multi-asset Brownian motion stream on a Virtual Thread. |
| `/api/v1/market/simulate/stop` | `POST` | Stops active market tick simulation. |
| `/api/v1/audit/decisions` | `GET` | Queries historical risk evaluation decisions and VaR metrics. |
| `/api/v1/audit/orders` | `GET` | Queries approved trade orders. |
| `/api/v1/audit/executions` | `GET` | Queries order execution fill reports. |
| `/api/v1/stream/events` | `GET` | **Server-Sent Events (SSE)** real-time stream of ticks, approvals, and fills. |

---

## Benchmark & Load Verification

Tested on an 8-core machine with 10 concurrent requests (each simulating 10,000 paths):

```bash
cd risk-sidecar
cargo test --release -- --nocapture
```

```text
running 3 tests
SHORT test: VaR=395.0526, approved=true, latency=1908μs
LONG test: VaR=380.8795, approved=true, latency=1924μs
test tests::test_long_position_var ... ok
test tests::test_short_position_var ... ok
...
10 Concurrent Monte Carlo requests completed in 3ms total
test tests::test_concurrent_load_performance ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
```

---

## Quickstart & Docker Compose

Launch the complete stack (Zookeeper, Kafka, Rust Risk Sidecar, Java Orchestrator):

```bash
docker compose up --build
```
