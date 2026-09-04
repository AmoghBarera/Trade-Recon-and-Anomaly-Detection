# TradeRecon: Complete Project Guide (Interview & Developer Reference)

TradeRecon is a real-time, event-driven Trade Reconciliation Engine designed to match and validate trade executions against trade confirmations, while ensuring PnL consistency. 

This document serves as a comprehensive guide for both users and developers. It is specifically designed to provide deep technical context, covering the architecture, tech stack, and the **"Why" behind every technical decision** (highly useful for system design discussions and interviews).

---

## 1. Project Overview & Functionalities

TradeRecon subscribes to streaming trade data to ensure that internal records (Executions) perfectly match external clearing records (Confirmations) and end-of-day financial snapshots (PnL).

### Core Functionalities
*   **Real-time Streaming**: Consumes asynchronous execution and confirmation data from Kafka topics.
*   **Dynamic Tolerance Checking**: Validates that price, quantity, and timestamps are within acceptable asset-class specific bounds (e.g., Equities tolerate a 0.5% price deviation, FX tolerates 0.1%).
*   **PnL Consistency**: Re-calculates expected PnL `(Price * Quantity) - Commission` and compares it against reported financial snapshots.
*   **Anomaly Detection**: Uses an Unsupervised Machine Learning model (Isolation Forest) to score trades for suspicious behaviors based on historical patterns *before* standard tolerance checks are applied.
*   **Severity Tiering & Escalations**: Classifies mismatches into `INFO`, `WARNING`, and `CRITICAL` tiers based on the magnitude of the tolerance breach, writing critical failures to an Escalation database.
*   **Reporting & Metrics**: Provides a real-time web UI (Flask) for filtering and viewing trades, CSV exports, and exposes operational metrics via Prometheus for Grafana dashboards.

---

## 2. Architecture & Tech Stack

### Tech Stack
*   **Language**: Python 3.10
*   **Streaming**: Apache Kafka (`confluent-kafka`)
*   **Database**: SQLite (`SQLAlchemy` ORM)
*   **Web Framework**: Flask (Jinja2 templates, TailwindCSS styling)
*   **Machine Learning**: `scikit-learn` (Isolation Forest), `pandas`, `numpy`
*   **Observability**: `prometheus_client`
*   **Containerization**: Docker & Docker Compose

### System Architecture
1.  **Data Producers (`kafka/producer.py`)**: Simulates upstream trading systems by pushing JSON payloads to Kafka topics (`executions`, `confirmations`, `pnl_snapshot`).
2.  **Consumers (`app/consumer.py`)**: Background threads that subscribe to Kafka topics and route payloads to the reconciliation engine.
3.  **Reconciliation Engine (`app/reconcile.py`)**: The brain of the application. It locks trade states, applies the ML anomaly scoring, executes dynamic asset-class tolerance rules, calculates severity, and persists the final `ReconciliationResult` and `Escalation` to SQLite.
4.  **Web Interface (`app/main.py` & `app/report_generator.py`)**: A Flask application that queries the SQLite database to generate HTML reports, CSV downloads, and exposes a `/metrics` endpoint for Prometheus.

---

## 3. Deep Dive: Technical Decisions & System Design (Interview Prep)

If asked about the architecture in an interview, refer to these critical design decisions:

### A. Why Apache Kafka?
*   **High Throughput & Low Latency**: Trade environments generate thousands of messages per second. Kafka handles massive throughput significantly better than REST APIs or traditional message brokers like RabbitMQ.
*   **Decoupling**: The trading desks (Producers) don't need to know if the Reconciliation engine is up or down. They simply fire-and-forget to Kafka.
*   **Replayability (Event Sourcing)**: Kafka retains messages (unlike typical pub/sub). If the SQLite database drops or the Engine crashes, we can rewind the consumer group offset and reprocess the trades to restore the exact state.

### B. Scalability & Partitioning Strategy
*   **Horizontal Scaling**: If trade volume spikes, we can spin up multiple instances of the `TradeDataConsumer` in the same Kafka **Consumer Group**. Kafka will automatically balance the partitions across our consumers.
*   **Message Ordering**: To ensure an execution and its corresponding confirmation go to the *same* consumer thread (preventing race conditions), messages are partitioned by `trade_id` using a consistent hashing algorithm at the producer level.

### C. Concurrency and Thread Safety
*   **I/O Bound Nature**: Since reading from Kafka and writing to SQLite are I/O bound tasks, Python's multithreading is effective here despite the Global Interpreter Lock (GIL). 
*   **Shared State Protection**: Multiple consumers read executions and confirmations simultaneously. They store partial trades in an in-memory dictionary. To prevent Race Conditions (e.g., consumer A writes the execution while consumer B writes the confirmation at the exact same millisecond), this dictionary is protected by a Python `threading.Lock`.

### D. Why Isolation Forest for Anomaly Detection?
*   **Unsupervised Learning**: In financial reconciliation, we rarely have perfectly labeled "fraud" or "bad trade" datasets to train on. Isolation Forest is unsupervised; it isolates anomalies by randomly partitioning features. 
*   **Performance**: It works exceptionally well on high-dimensional tabular data (price, quantity, time drift) and is highly computationally efficient (O(n) time complexity) for real-time scoring compared to neural networks or standard clustering models.

### E. Database Strategy
*   **SQLite for Local State**: Currently using SQLite for simplicity and portability in this demonstration. 
*   **Future Migration (Production)**: In a real-world scenario, the SQLAlchemy ORM makes it trivial to swap SQLite for PostgreSQL by simply changing the connection string (`DB_URL`). PostgreSQL would resolve SQLite's concurrent write limitations.

### F. Observability (Prometheus & Grafana)
*   **Why Prometheus?**: Traditional logging is hard to parse for performance trends. Prometheus scrapes time-series metrics. We use `Counters` for total processed/mismatched trades, `Gauges` for the current in-memory store size and open escalations, and `Histograms` for latency percentiles.

---

## 4. Design Patterns & Best Practices Utilized

1.  **Producer-Consumer Pattern**: Kafka completely separates the data producers from the data processors.
2.  **Strategy Pattern (Configuration)**: By moving tolerances into a `yaml` file, the Engine dynamically applies the correct strategy (tolerance thresholds) based on the incoming data's `asset_class`, rather than hard-coding massive `if/else` blocks.
3.  **Parameterized Testing**: Used `pytest.mark.parametrize` to run the engine through hundreds of CSV-defined edge cases (e.g., verifying a 0.5% breach vs a 0.51% breach) in a single DRY (Don't Repeat Yourself) test block.

---

## 5. Change Log & Project Evolution

This section tracks all major modifications made to the repository since the baseline engine was established.

### Change 1: Implementation of Anomaly Detection
*   **What was done**: Added `app/anomaly_detector.py` to train an `IsolationForest` on historical `.csv` data on startup. The model scores real-time trades, saving `anomaly_score` and `is_anomalous` to the DB. Added the `traderecon_anomalies_total` metric.
*   **Impact**: Enhances the system's ability to catch "fat-finger" errors or systemic execution delays that might technically pass tolerance but mathematically represent extreme statistical outliers.

### Change 2: Configurable Asset Class Tolerances
*   **What was done**: Introduced `config/tolerances.yaml`. Refactored `app/utils.py` to support percentage-based tolerances. The Engine now looks up the incoming trade's `asset_class` and applies the specific profile (e.g., tighter tolerances for FX than Fixed Income).
*   **Impact**: Made the engine scalable for multi-asset trading desks rather than acting as a single-asset monolithic checker. Prevented hardcoding logic into the Python application layer.

### Change 3: Mismatch Severity Tiering
*   **What was done**: Overhauled mismatch logic. Mismatches are assigned a `breach_factor`. Breaches `< 2x` are `INFO`, `2x-5x` are `WARNING`, and `> 5x` or PnL failures are `CRITICAL`. Created a new `Escalation` SQL table, a UI dropdown filter, and a `traderecon_open_escalations` Prometheus gauge.
*   **Impact**: Triage automation. In high-volume environments, support teams suffer from alert fatigue. By classifying breaches, we filter out noise (`INFO`) and force immediate attention on `CRITICAL` failures via a dedicated SQL Escalations table and Prometheus alerts.
