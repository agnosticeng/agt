# 🚀 SQL-Driven ETL Engine based on ClickHouse


This project turns [ClickHouse](https://clickhouse.com/) into a high-performance, SQL-native transformation engine for modern data pipelines.

ClickHouse isn't just a fast analytics database — it supports 50+ input/output formats, integrates with many external databases, and is highly extensible through user-defined functions (UDFs). This flexibility allows us to build powerful ETL workflows entirely in SQL, using ClickHouse itself as the execution engine.

All transformation logic is expressed in SQL and executed inside a local or embedded ClickHouse instance. The result is a fast, scalable, and easily deployable engine for both batch and micro-batch processing.

---

## 🧠 Key Concepts

### 🔧 Variables (`vars`)

Each task carries a set of **key-value pairs** called **`vars`**, which can be used throughout the processing pipeline.

- **Global vars** can be defined at runtime using CLI flags (e.g. `--var environment=prod`) and are available to all tasks.
- **Task vars** can be **produced dynamically** by source and processors:
  - Each **source** query result set's row will generate a **task** whose `vars` will be generate from row's columns values. 
  - If the **last query in a processor** returns a row, its columns will be added as new `vars` to the task.
  - These vars can then be used by downstream processors via Go templating (e.g. `{{ .some_var }}`).

This makes it easy to:
- Inject global context or runtime parameters into the pipeline
- Share computed values between processing stages
- Influence task behavior based on intermediate results


---

## 🔗 Source

A **source** is an arbitrary SQL query that generates tasks. Each row returned by the query becomes an individual **task**, which flows through the pipeline.

- A source query can be executed **once**, or **repeated at a fixed interval** to produce **micro-batches** of tasks over time.
- This enables both one-off jobs and continuous ingestion in streaming-style workflows (e.g., via a Kubernetes job or cron).
- All columns from the source query are available to processors as **Go template variables**.
  - For example, if the source returns a column `block_number`, it can be used in processor SQL as:  
    ```sql
    SELECT * FROM actions WHERE block_number = '{{ .block_number }}'
    ```
---

## ⚙️ Processor

A **processor** is a SQL-based transformation stage. It receives a task, executes logic, and may modify, enrich, or delay the task.

### 🧩 `apply`

- Executes a **list of SQL queries** for each task
- Can be run with **configurable concurrency**
- After the final query:
  - If a row is returned, its column values are **merged into the task’s `vars`**

### ⛓️ `sequence`

- Ensures tasks are **emitted in original order**, even if they arrive or finish out-of-order
- Useful when task order is significant (e.g., for event replays or stream joins)

### 🧮 `accumulate`

The **`accumulate`** processor processes tasks one by one, executing SQL queries for each individual task in the batch. 

The flow is as follows:

1. **On the first task of a batch**:
   - Execute a **"Setup" query** (e.g., to create or initialize a buffer table).
   
2. **For each subsequent input task**:
   - Execute all **"Queries"** (e.g., append each task to the buffer table).

3. **At the end of the batch** (when either a **size limit** or **time duration** is reached):
   - Execute a **"Teardown" query** (e.g., to finalize the batch or clean up resources).

> **Note:** A custom SQL query can be provided to determine the **"size"** of the batch (e.g., based on row count or time).

> **Note** Just like `apply`, if the last query returns a row, it is used to **add or update task `vars`**

This approach is ideal for tasks like consolidating or buffering data before further processing, ensuring that SQL queries are executed on each task while maintaining control over when batch operations take place.

---

## ✅ Benefits

- 💾 **All logic in SQL** — no custom DSL or separate orchestrators
- 📦 **Embeddable or standalone** — run as a binary, job, or service
- ⚡ **Fast & scalable** — built on ClickHouse’s columnar engine
- 🔁 **Deterministic & testable** — transformations are declarative, versionable, and reproducible
- 🔍 **Context-aware** — dynamically injects and updates variables (`vars`) across pipeline stages

---