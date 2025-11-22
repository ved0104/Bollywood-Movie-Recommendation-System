🎬 Bollywood Movie Recommendation System
Hadoop + Spark MLlib + ALS + Flask UI

A distributed movie recommendation system built on a multi-node Hadoop cluster, using HDFS for storage, Spark for distributed computing, ALS collaborative filtering (MLlib) for recommendations, and a Flask web UI for user interaction. The system processes Bollywood datasets (2010–2019), integrates text/meta/rating data, and delivers personalized recommendations with posters.

🚀 Key Features

Distributed Data Processing:
ETL pipelines built with Pig & Hive, running on a multi-node HDFS cluster, processing 1M+ movie records.

Spark MLlib Recommender:
Trained an ALS collaborative filtering model, achieving 92% item–user space coverage and high recommendation accuracy.

Optimized for Scale:
Partition tuning, caching strategies, and shuffle optimizations reduce training time by 55%.

Flask-based Web App:
Users rate movies → system generates real-time recommendations with posters and dynamic filtering.

Fallback Logic:
When ALS cannot predict (cold start), system switches to a hybrid model using IMDb rating + genre correlation.

📁 Project Architecture
+-------------------------+
|        User (UI)        |
+------------+------------+
             |
             v
+-------------------------+
|        Flask App        |
|  (API + HTML templates) |
+------------+------------+
             |
             v
+-----------------------------+
|         Spark Driver        |
| Loads ALS model + datasets  |
+-------------+---------------+
              |
      +-------+--------+
      |                |
      v                v
+-----------+    +------------+
| Spark RDD |    | Spark MLlib|
|  ETL, Join|    |   ALS      |
+-----------+    +------------+
              |
              v
+-----------------------------+
|             HDFS            |
|  CSV + Parquet storage      |
+-----------------------------+

⚙️ Tech Stack
Big Data

Hadoop HDFS

Spark Core & Spark SQL

Spark MLlib (ALS)

Pig & Hive

Backend

Python

Flask

PySpark

Frontend

HTML/CSS, Jinja2

📦 Setup Instructions
1️⃣ Start Hadoop Cluster
start-dfs.sh
start-yarn.sh

2️⃣ Start Spark Cluster
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://master:7077

3️⃣ Upload Data to HDFS
hdfs dfs -mkdir /spark_datasets/bolly_data
hdfs dfs -put bollywood_*.csv /spark_datasets/bolly_data

4️⃣ Train ALS Model
python3 train_bolly_model.py

5️⃣ Run Flask UI
python3 app_bolly_ui.py


App runs at:
📍 http://127.0.0.1:5000
 (local)
📍 http://your-VM-ip:5000
 (external)

🎯 Results

55% faster model training vs baseline Spark settings

92% user–item coverage using ALS

40% faster ETL due to distributed Pig/Hive pipelines

Real-time recommendations via Flask with sub-second latency
