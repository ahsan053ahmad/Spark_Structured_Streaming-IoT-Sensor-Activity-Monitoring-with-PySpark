# Spark_Structured_Streaming-IoT-Sensor-Activity-Monitoring-with-PySpark

This repository contains my submission for an advanced **Spark Structured Streaming** assignment using PySpark in a **Databricks notebook**. The goal of the assignment was to simulate real-time stream processing using sensor data from IoT devices, with tasks that explored everything from file streaming to real-time aggregations, joins, and visualizations using Apache Spark.

---

### Business Problem

As real-time data from IoT devices becomes more common, organizations need scalable solutions to process streaming data on the fly. This assignment demonstrates how **Spark Structured Streaming** can be used to process continuous streams of motion sensor data collected from smartphones and smartwatches, identify user activities, and generate useful aggregations and summaries in near real-time.

The data used simulated realistic use-cases where multiple devices collect orientation sensor data from users performing various physical activities like walking, standing, or climbing stairs.

---

### Dataset Overview

We used a pre-loaded subset of the **Heterogeneity Human Activity Recognition (HHAR)** dataset, originally available from the UCI Machine Learning Repository.

- Format: JSON files, each simulating one chunk of streaming data
- Location: `dbfs:/databricks-datasets/definitive-guide/data/activity-data/`
- Key fields:
  - `Arrival_Time`, `Creation_Time`, `Device`, `Model`, `User`, `gt` (activity label)
  - Sensor readings along `x`, `y`, and `z` axes

---

### Project Objectives

This project was designed to:

- Load and explore streaming data using **Spark Structured Streaming APIs**
- Apply various transformations and real-time aggregations on streaming DataFrames
- Create **temporary in-memory streaming SQL tables** for querying
- Visualize streaming outputs using periodic snapshots
- Join streaming and static DataFrames
- Understand Spark partitions, shuffles, and streaming query lifecycle

---

### Solution Approach

**1. Initial Exploration**
- Viewed the directory structure with `dbutils.fs.ls()`
- Counted total and `.json` files; computed total size
- Loaded data as a **static DataFrame** to inspect schema and contents

**2. Static Analysis & Profiling**
- Explored schema, data types, nulls, and summary statistics
- Grouped and counted activity/device combinations
- Validated partition counts and adjusted `spark.sql.shuffle.partitions`

**3. Streaming Setup**
- Defined schema from staticDF
- Initialized streaming DataFrame with `maxFilesPerTrigger = 1`
- Wrote output to memory sink using `writeStream.format("memory")`

**4. Streaming Aggregations**
Performed multiple transformations using:
- `groupBy()` – Counts of rows
- `cube()` and `rollup()` – Hierarchical/multidimensional aggregations
- `filter()` – For stairs-related activities, and for users 'a' and 'b'
- `agg()` – Averages of sensor coordinates grouped by `Model` and `gt`
- `join()` – Static vs Streaming average comparison

**5. Streaming Query Management**
- Used `.isActive`, `.name`, `.id`, and `.status` to monitor queries
- Printed outputs in real-time using `spark.sql(...).show()` in a timed loop
- Stopped queries after each task to manage memory

---

### Business Value

This assignment showcases key capabilities of Spark for real-world streaming use cases:

- **IoT stream processing**: Handling continuous sensor data with low latency
- **Real-time aggregation**: Summarizing sensor activity by user, device, and model
- **Memory-efficient querying**: Avoiding large `.collect()` calls with memory sinks
- **Feature extraction**: Calculating averages and identifying behavioral trends
- **Data fusion**: Joining batch (historical) data with streaming data

---

### Challenges Encountered

- Waiting for streaming queries to initialize before querying outputs
- Avoiding memory overload from `.append` mode with large streaming inputs
- Understanding the implications of shuffle partitions in streaming workloads
- Formatting join outputs and label naming across static/streaming queries

---
