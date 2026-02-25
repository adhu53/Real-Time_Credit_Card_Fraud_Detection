**#Real-Time Credit Card Fraud Alerting System#**

#Architecture Flow#
MySQL → Python Producer → Kafka (credit_card_transactions) → Spark Streaming Fraud Detection → Kafka (fraud_alerts) → Python Fraud Consumer → Block Card + Send Email
(See Image)

 =================================================
 
 STEP 1 – Start Kafka & Zookeeper (Docker Compose)
 
 =================================================
>>vi docker-compose.yml
=====================
version: "3.8"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
     - "9092:9092"
     - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      
==============================================

>>docker compose up -d

 ============================
 
 STEP 2 – Create Kafka Topics
 
 ============================

2a. Create Transaction Topic

>>docker exec -it kafka \
  kafka-topics --bootstrap-server kafka:9092 \
  --create --topic credit_card_transactions \
  --partitions 1 --replication-factor 1

=============================

2b. Create Fraud Alert Topic

>>docker exec -it kafka \
  kafka-topics --bootstrap-server kafka:9092 \
  --create --topic fraud_alerts \
  --partitions 1 --replication-factor 1

===============================
2c. Verify Consumer

>>docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic credit_card_transactions \
  --from-beginning

 ==================================
 
 STEP 3 – Create Spark Docker Image
 
 ==================================
>>vi Dockerfile
=============
FROM apache/spark:3.5.1
USER root
RUN pip3 install pandas pyarrow>=4.0.0
USER spark

 ====================================
 
 STEP 4 – Build & Run Spark Container
 
 ====================================
>>docker build -t spark-pandas .

>>docker run -dit \
  --name spark \
  --network hadoop_default \
  -v /home/hadoop/jobs:/opt/spark/jobs \
  spark-pandas \
  bash

 ==========================
 
 STEP 5 – Submit Spark Job
 
 ==========================
>>docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark/jobs/kafka_consumer.py
===================================
Note: Before running, delete old checkpoint:
docker exec -it spark rm -rf /tmp/fraud_pandas_state_checkpoint
===============================================================

 =============================
 
 STEP 6 – Verify Fraud Alerts
 
 =============================
>>docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic fraud_alerts \
  --from-beginning

=============

MySQL Setup

=============
CREATE TABLE transactions (
    Transaction_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    card_number VARCHAR(20),
    amount DECIMAL(10,2),
    Transcation_status VARCHAR(20),
    time_stamp DATETIME
);

CREATE TABLE card_details(
    card_number VARCHAR(20) PRIMARY KEY,
    username VARCHAR(20),
    card_status VARCHAR(20),
    useremail_id VARCHAR(30)
);

 ==============================
 
 insert few records for testing 
 
 ==============================

#insert into card_details(card_number,username,card_status,useremail_id) values("4434-2421-2321-9994","Adarsh M D","Inactive","useremail@gmail.com");
- insert into transactions(card_number,amount,Transcation_status,time_stamp) values("3452-3263-4341-1415",2000,"Invalid",now());
- insert into transactions(card_number,amount,Transcation_status,time_stamp) values("3231-3321-4311-1123",1000,"Valid",now());
- insert into transactions(card_number,amount,Transcation_status,time_stamp) values("3232-3434-4321-1235",3000,"Invalid",now());
- insert into transactions(card_number,amount,Transcation_status,time_stamp) values("6565-4343-3242-1313",4000,"Online_Enabled",now());
- insert into transactions(card_number,amount,Transcation_status,time_stamp) values("5454-3423-4545-7676",11000,"Invalid",now());
- insert into transactions(card_number,amount,Transcation_status,time_stamp) values("8772-3443-5421-1656",200,"Merch_Disabled",now());
- insert into transactions(card_number,amount,Transcation_status,time_stamp) values("4343-3352-5545-3331",390,"Invalid",now());
==================================================================================================================================

 ==========================================
 
  STEP 7 – Python Producer (MySQL → Kafka) 
  
 ==========================================
File: python-to-kafka.py

- Reads new transactions from MySQL
- Publishes to credit_card_transactions topic
- Runs every 2 seconds
>> python3 python-to-kafka.py

 ========================================
 
 STEP 9 – Fraud Detection Logic (Spark)
 
 ========================================
Fraud Rule:

If 3 consecutive Invalid transactions occur within 5 minutes
→ Send alert
→ Reset state

=======================================

STEP 10 – Fraud Consumer + Email Alert

======================================

File: kafka_fraud_consumer.py

- Listens to fraud_alerts
- Blocks card in MySQL
- Sends Email via Gmail SMTP
====================================
Configure the following in kafka_fraud_consumer.py

SENDER_EMAIL = "yourgmail@gmail.com"
SENDER_PASSWORD = "gmail_app_password"

>>python3 kafka_fraud_consumer.py
=====================================

===============

##How to Run###

===============

1.docker compose up -d
2.Create Kafka topics
3.Start Spark container
4.Submit Spark job
5.Run python-to-kafka.py
6.Run kafka_fraud_consumer.py
7.Insert test data into MySQL

