# Real-Time-Vehicle-Fleet-Analytics
The Aim of the Proposed Work “Real Time &amp; Batch Moving Vehicle Fleet Analytics“ is to grab the different kind of data from Fleet vehicles and to form a database which in turn helpful for real-time and batch analytical processing
# Purpose
Keeping track of each driver running vehicles of a company is a daunting task .
The purpose of Fleet Management in a business is to :
To ensure vehicles of a business are operating smoothly
To improve performance and efficiency
For Safety measures
Predictive Maintenance
To keep operation costs at a minimum
To maintain compliance with government regulations
To Provide better customer experience
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/95f2f92b-b15f-4c08-a31e-9f461f99cce7)

![kafka server details](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/714a4756-004f-42c1-9ab0-201886ba8554)

# Data Flow Pipeline
1 Data ingestion and acquisition is done through Sqoop/Shell/Spark JDBC and NIFI, 
2 Queued into distributed Kafka queue, 
3 Enriched using Spark streaming engine, 
4 Pushed into Elastic search full document search store NOSQL, 
5 Analyzed in real-time using Kibana 
6 Batch analytics will be done using Hive such as
#  Raw Layer load -> 
#  Filtration & Curation Load -> 
# Data Governance & Masking -> 
# ETL & Metrics -> 
# Dimension Modeling -> 
# Aggregation
# Conceptual Architecture (Realtime):
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/267913ef-68ce-4ca2-8295-f3808e0e0c0e)

# Data lake Reference Architecture
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/4af359dd-59df-4fcc-9b42-db390e2d9e55)

# Technical Architecture (Realtime)
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/b6c1b50c-ba63-495e-8ec5-72b169ba141c)

#  Outcome & Results
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/3ee6b23e-2ee0-43c1-9897-b869c446249e)

# How to run this Project
1 Login to VMWare –
2 Extraction of Source code & data
3 DataSet:
Below files are kept in the given location
/home/hduser/fleet
Timesheet.csv, drivers.csv, truck_info.csv, truck_event_text_partition_backup.csv and
truck_event_partition.csv
Ensure the below services are running, if not start the Following services:
 Hadoop:
start-all.sh
Start the Zookeeper Coordination service:
zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties
Start the Nifi Service:
nifi.sh start
Open the below url from Firefox:
http://localhost:8082/nifi
Start Elastic search & Kibana:
nohup /usr/local/elasticsearch/bin/elasticsearch &
nohup /usr/local/kibana/bin/kibana
# Batch data load
Drivers update their timesheet and miles driven info on daily basis in the MYSQL RDBMS.
Create and Insert Timesheet and Driver table data into Mysql as per the below commands:
create database fleetdb;
use fleetdb;
Timesheet Table:
CREATE TABLE timesheet(driverId INTEGER NOT NULL ,week INTEGER NOT NULL,hourslogged INTEGER
NOT NULL,mileslogged INTEGER NOT NULL);
source /home/hduser/fleet/timesheet_inserts.txt
Driver Table:
CREATE TABLE driver(driverId INTEGER NOT NULL PRIMARY KEY,name VARCHAR(19) NOT NULL,ssn
INTEGER NOT NULL,location VARCHAR(34) NOT NULL,certified VARCHAR(1) NOT NULL,wageplan
VARCHAR(5) NOT NULL);
source /home/hduser/fleet/drivers_inserts.txt;
# Realtime Data streaming using shell script, Kafka, NIFI, Spark, Elastic search and Kibana
Start Kafka Server
1.Start the Zookeeper Coordination service
zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties
2. Ensure kafka is installed and configured, if already done, go to step 3 directly.
sudo rm -r /usr/local/kafka
cd /home/hduser/fleet
tar xvzf kafka_2.11-0.10.0.1.tgz
sudo mv kafka_2.11-0.10.0.1 /usr/local/kafka

3. Start the Kafka server
kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
4.Create a topic (with one replica and one partition)
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic truckevents1
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic fleetanalysticsout
Check whether the events data started flowing in the kafka consumer.
kafka-console-consumer.sh --zookeeper localhost:2181 --topic truckevents1
![kafka server details](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/0bcb800a-43d6-45d9-a97e-6734f81d8db6)

Start NIFI (If not started already)
1. Run the web ui
http://localhost:8082/nifi
Overall NIFI Flow explanation –
Read the data from the files created in streaming fashion from the simulate script, replace the event text from
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/59f229dc-2733-481d-8a50-7764c7e92e35)
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/1d161a83-d883-4b79-96a0-3d8ad1f6d285)
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/a923586c-8e6d-4bc2-b170-5c8e168891c4)
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/9b310d59-cc33-45c9-be96-90265ff5b424)

# Elastic Search:
Execute the below commands to create Elastic Search indexes:
Open Linux Terminal and execute the below curl http requests to delete an existing index or create a
new index.
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/761a1823-cadd-4496-90cf-10b784eb8de5)
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/e5f8fcdc-0cdd-4c5d-bb65-9b15afc7e497)
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/b0dbaf72-919f-493e-9cd4-3e9b52e9a508)
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/6f6e8c38-8a8a-4b3c-be5d-46a324242f47
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/c95fe209-be94-4f4e-a112-9fc577c4b616)
![image](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/e0f7383e-1088-4f72-9e21-25a485d2650d)

![Elastic Serach](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/4fae0b80-0a9a-4e49-84c9-db5f3505abb1)







![1st](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/1d532633-0d4b-4416-9a8c-bfe9e2c73b0b)

![2nd](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/e15c6b5d-c72b-4692-829b-ceb3d1157b23)
![3rd](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/c2cb9ad7-a709-444d-aa97-ad9d9c2a1dbb)



![4th](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/62820411-a0f1-4957-8de8-986e1f153b76)



![1](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/3140703c-51de-4bab-b3da-5c19afb8555d)


![2](https://github.com/hellosigh/Real![Database Details](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/107a3890-3c51-4370-9ea3-31774697bb4f)
-Time-Vehicle-Fleet-Analytics/assets/124508230/2fb40162-640b-4088-9abf-2e90ade122c0)


![table details](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/21896234-784d-4895-af60-3efd6248e642)
![table details2](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/a7d0bf75-9ca8-4f71-8e0d-a0fe14b92af7)


![list of file in LFS](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/9b3f4537-e6fa-42b1-9da0-ca02fc7047da)


![service details](https://github.com/hellosigh/Real-Time-Vehicle-Fleet-Analytics/assets/124508230/e5448d43-7bde-4ee9-8bdf-9956173605ad)


