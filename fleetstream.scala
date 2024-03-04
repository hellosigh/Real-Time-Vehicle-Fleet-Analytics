package org.inceptez.fleet
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.elasticsearch.spark.streaming._
import org.apache.spark.sql.Row;
import org.elasticsearch.spark._
import java.util.Date
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.elasticsearch.spark.streaming._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._

// updates, scd, audit, masking, datalake zones

case class events(driverId: String, truckId: String, eventType: String, longitude: Float,latitude:Float, routeName: String, eventts: String,miles: String);
//Define Case classes for events and elastic search rdds to impose schema on the data hence we can register as dataframes.
// events case class is defined to impose structure on the streaming data that we get from kafka and register as dataframes, then as a temp view for performing sql queries on it  

case class rddtoes (driverId: String,truckId: String,eventType: String,hrs :String,miles :String, total: String); 

case class drivedetailrddtoes (driverId: Integer,drivername:String,location:String,certified:String,truckId: Integer, eventType: String,tothrs :Float,totmiles :Float,numevents:Integer, curmiles: Integer,avgdriven: Integer);
case class driveevents(driverid: Integer, truckid: Integer, eventtype: String, longitude: String,latitude:String,location:String, drivername:String, routename: String, eventdate: Date);
object fleetstream {
println("**Starting the FLEET STREAMING PROGRAM****");
def main(args:Array[String])
{

println("**Main program begins****")

//  Initialize Spark configuration object including Elastic search nodes and port info.
  val spark = SparkSession
			.builder()
			.appName("FLEET REALTIME").config("es.nodes", "localhost").config("es.port","9200")
.config("es.index.auto.create","true")
////.config("es.mapping.id","driverId")
.config("es.nodes", "127.0.0.1")
.config("es.port", "9200")
.config("hive.metastore.uris","thrift://localhost:9083")
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
.enableHiveSupport()
.master("local[*]")
.getOrCreate()
println("**Initializing Spark context, Streaming Context, SQL context and Hive Contexts****")
//  Initialize Spark context.
spark.sqlContext.setConf("spark.sql.shuffle.partitions","4")
val sparkcontext=spark.sparkContext

// Show only errors in the console
sparkcontext.setLogLevel("error")

//  Initialize Spark Streaming context stream once in 10 seconds.

val ssc = new StreamingContext(sparkcontext, Seconds(10))

println("**Defining structure type for reading column name and defining schema explicitly****")

val driveSchema = StructType(Array(
    StructField("driverId", IntegerType, true),
    StructField("name", StringType, true),
    StructField("ssn", IntegerType, true),
    StructField("location", StringType, true),
    StructField("certified", StringType, true),
    StructField("wage", StringType, true)));

val tsSchema = StructType(Array(
    StructField("driverId", IntegerType, true),
    StructField("week", IntegerType, true),
    StructField("hours", IntegerType, true),
    StructField("miles", IntegerType, true)));

println("**Spark SQL Operations starts reading data from sqoop imported locations****")

val confile="/home/hduser/fleet/fleet_db_connection.prop"

val tbl="(select driverid,name,certified,wageplan,week,hourslogged,mileslogged,case when certified='Y' then hourslogged*25 else mileslogged*0.5 end as dollartopay from (select t.driverid,d.name,d.certified,d.wageplan,t.week,sum(t.hourslogged) hourslogged,sum(t.mileslogged) mileslogged from timesheet t inner join driver d on t.driverid=d.driverid group by t.driverid,d.name,d.certified,d.wageplan,t.week) temp) tbl1"

val lb=1;
val ub=10000;
val part=4;
val driverdetailts = org.inceptez.generic.framework.UDFs.getRdbmsData(spark.sqlContext,"fleetdb",tbl,"driverid",s"$confile",lb,ub,part)

///driverdetailts.cache();
driverdetailts.createOrReplaceTempView("drivedetailstshive");
driverdetailts.show(2,false)

//Read Driver and Timesheet info from Hive table

val driverdetail = spark.sql("select driverId,name,ssn,location,certified,wageplan as wage from fleetdb.driver_scd1");
val drivinginfo = spark.sql("select driverid,sum(hourslogged) hrs,sum(mileslogged) miles from fleetdb.timesheet group by driverid");

//driverdetail.cache();
//drivinginfo.cache();


drivinginfo.createOrReplaceTempView("drivingsummary");
driverdetail.createOrReplaceTempView("driverinfo");

// Checkpoint the lineages for streaming RDDs helps in case of failure of driver

ssc.checkpoint("file:/tmp/ckptdir/ckpt")


println("**Kafka config started****")

// Define Direct Kafka related Params

val kafkaParams = Map[String, Object](
"bootstrap.servers" -> "localhost:9092",
"key.deserializer" -> classOf[StringDeserializer],
"value.deserializer" -> classOf[StringDeserializer],
"group.id" -> "kafkatest1",
"auto.offset.reset" -> "latest"
)

val topics = Array ("truckevents1")

println("** steam rdd created as kafka reads the from truckevents1 topic ****")

val stream = KafkaUtils.createDirectStream[String, String](
ssc,
PreferConsistent,
Subscribe[String, String](topics, kafkaParams)
)


println("**Read the kafka streaming data and parse using Spark Core functions****")

// Read the kafka streaming data and parse using Spark Core functions

val kafkastream = stream.map(record => (record.key, record.value))

// Read only the message value and leave the header, split based on , delimiter and filter all blank messages if any

val inputStream = kafkastream.map(rec => rec._2).map(line => line.split(",")).filter { x => x.length>5 }

//inputStream.print

println("**Iterate on each and every dstream micro batch rdds and apply events case class to impose dataframe and convert as temp table****")

println("**Create driverstreamtable from kafka streaming data and create tables to find driving patterns, events with latitude/longitude****")

// Iterate on each and every dstream micro batch rdds and apply events case class to impose dataframe and convert as temp table
// Create driverstreamtable from kafka streaming data and create tables to find driving patterns, events with latitude/longitude
import spark.implicits._;
inputStream.foreachRDD(rdd => {
  
  if(!rdd.isEmpty)
            {
rdd.map(x => x.mkString(","))
val df = rdd.map(x => events(x(0).toString(),x(1).toString(), x(2).toString, x(3).toFloat,x(4).toFloat,x(5).toString,x(6).toString,x(7).toString));
val df1 = df.toDF()
df1.createOrReplaceTempView("driverstreamtable");

val drivingpattern = spark.sql("""select distinct driverId,truckId,eventType,routeName,eventts from driverstreamtable 
                                  where eventType<>'Perfect Driving'""");
drivingpattern.show
val driveevents1 = spark.sql("""select distinct a.driverId driverid,a.truckId truckid,a.eventType eventype,a.longitude,a.latitude,
  concat(a.latitude,',',a.longitude) location,b.name drivername, a.routeName routename,a.eventts eventdate,a.miles 
  from driverstreamtable a 
  inner join 
  driverinfo b 
  on a.driverId=b.driverId 
  where eventType<>'Perfect Driving' """);
driveevents1.show();

val drivingpatterncnt = spark.sql("""select a.driverId,a.truckId,a.eventType,b.hrs,b.miles,count(1) total 
  from driverstreamtable a inner join drivingsummary b 
  on a.driverId=b.driverId 
  where a.eventType<>'Perfect Driving' 
  group by a.driverId,a.truckId,a.eventType,b.hrs,b.miles""" );

val drivingpatterdetails = spark.sql("""select c.driverId,c.name,c.location,c.certified,a.truckId,a.eventType,b.hrs total_hrs,
  b.miles total_miles,count(1) number_of_events,cast(sum(a.miles) as Int) current_miles, 
  cast(sum(a.miles)/count(1) as Int) avg_driven 
  from driverstreamtable a inner join drivingsummary b 
  on a.driverId=b.driverId 
  left join driverinfo c 
  on a.driverId=c.driverId 
  where a.eventType<>'Perfect Driving' 
  group by c.driverId,c.name,c.location,c.certified,a.truckId,a.eventType,b.hrs,b.miles""" );

// Print complete value of all columns, printing only first 2 rows 
drivingpattern.show(20,false);
drivingpatterncnt.show(20,false);
drivingpatterdetails.show(20,false);
drivingpatterdetails.printSchema();
drivingpatterncnt.printSchema()
//driveevents1.show(2,false);

println("**Write the computed data to 3 Elastic Search indexes created seperately, if not created auto create will happen****")

// Write the computed data to totally 3 Elastic Search indexes- Convert the DataFrame to RDD (as DF cant be writtern directly to ES) and apply 
// case class and write to ES indexes created manually, if not created auto create will happen.

println("**Write to drivingpattern es for identifying cumulative realtime analytics of drivers driving the vehicles  ****")
 
//val drivingpatternrdd = drivingpatterncnt.rdd;
//val drivingpatternrddes = drivingpatternrdd.map(x => rddtoes(x(0).toString , x(1).toString().toString ,x(2).toString,x(3).toString,x(4).toString,x(5).toString));

drivingpatterncnt.saveToEs("drivingpattern/driver")
//drivingpatternrddes.saveToEs("drivingpattern/driver")  

println("**Write to drivingpattern detail es for identifying detailed realtime analytics of drivers driving the vehicles  ****")

val drivingpatterndetrdd = drivingpatterdetails.rdd;
val drivingpatterndetrddes = drivingpatterndetrdd.map(x => drivedetailrddtoes(x(0).toString.toInt , x(1).toString() ,x(2).toString,x(3).toString,x(4).toString.toInt,x(5).toString,x(6).toString.toFloat,x(7).toString.toFloat,x(8).toString.toInt,x(9).toString.toInt,x(10).toString().toInt));
drivingpatterdetails.saveToEs("drivingpatterndet/driver")
//drivingpatterndetrddes.saveToEs("drivingpatterndet/driver")

println("**Write to drive events es for identifying geo locations of where our drivers driving the vehicles  ****")

val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val driveevents1rdd = driveevents1.rdd;
val driveevents1rddes = driveevents1rdd.map(x => driveevents(x(0).toString.toInt , x(1).toString().toInt ,x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString,x(7).toString,format.parse(x(8).toString)));
driveevents1.printSchema()
driveevents1rddes.saveToEs("driveevents1/drivedocs")

println("Streaming data written into Elastic Search")

println("Continue running to read new events from kafka")
            }
})

ssc.start()
ssc.awaitTermination()

}
}