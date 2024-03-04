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
















