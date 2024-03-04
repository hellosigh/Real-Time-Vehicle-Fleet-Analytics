package org.inceptez.fleet
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;
import org.inceptez.generic.framework.UDFs._;

//Iteration 1
// Ensure the tables (driver,timestamp) are created and initial data loaded in mysql fleetdb database
//update driver set updts=current_timestamp;
// Ensure the source data is kept in the below location for truck events and 
///home/hduser/fleet/truck_events.csv
///home/hduser/fleet/trucks_info.csv
// Run this Spark job to get the 

//Iteration 2
//MYSQL
		//update driver set updts=current_timestamp - interval 25 hour;
		//insert into driver values (101,'Dave Patton',977706052,'3028 A- St.','Y','hours',current_timestamp);
//update driver set updts=current_timestamp,location='#35, Livingston ave, Piscatway, NJ' where driverid=11;

object fleetbatchscd {

	def main(args:Array[String]) {
		println("**Main program begins****")

		//  Initialize Spark configuration object including Elastic search nodes and port info.
		val spark=SparkSession.builder().appName("Sample sql app").master("local[*]")
		.config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
		.config("spark.eventLog.dir", "file:////tmp/spark-events")
		.config("spark.eventLog.enabled", "true")
		.config("hive.metastore.uris","thrift://localhost:9083")
		.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
		.enableHiveSupport().getOrCreate();

		spark.sparkContext.setLogLevel("error")

		val sqlc=spark.sqlContext;
		println("**Initializing Spark context, Streaming Context, SQL context and Hive Contexts****")
		//  Initialize Spark context.

		val sparkcontext=spark.sparkContext

		// Show only errors in the console
		sparkcontext.setLogLevel("error")

		import org.apache.spark.sql.types._

		val eventsSchema = StructType(Array(
				StructField("driverId", IntegerType, true),
				StructField("truckid", IntegerType, true),
				StructField("driving", StringType, true),
				StructField("lat", StringType, true),
				StructField("long", StringType, true),
				StructField("route", StringType, true),
				StructField("ts", TimestampType, true),
				StructField("distance", IntegerType, true)));

		import org.apache.spark.sql.functions._;

		// Data aquisition from Transient (LFS) to Raw zone (HDFS)
		val rawroutedf=spark.read.option("delimiter",",").schema(eventsSchema).option("header","false")
				.csv("file:///home/hduser/fleet/truck_events.csv")

				println(rawroutedf.count())
				println(rawroutedf.show(20))

				// Raw Zone (As is data)-> To provide oppurtunity for all other Consumers to consume the data/Archival
				val dfdt=rawroutedf.withColumn("dt", date_format(col("ts"),"yyyy-mm-dd"))
				dfdt.write.mode("overwrite").option("compression", "gzip").partitionBy("dt").csv("hdfs://localhost:54310/user/hduser/fleetraw/")

				// Curated  Zone

				val cleansedroutedf=spark.read.schema(eventsSchema).option("delimiter",",").option("header","false").
				option("quote","\"").option("escape","-").option("ignoreLeadingWhiteSpace","true").
				option("ignoreTrailingWhiteSpace","true").option("nullValue","na").option("nanValue","0").
				option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss.SSSZZ").option("dateFormat","yyyy-MM-dd").
				option("maxCharsPerColumn","100").option("mode","dropmalformed").csv("file:///home/hduser/fleet/truck_events.csv")
				.na.drop(Array("driverId")).dropDuplicates()

				println(cleansedroutedf.count())
				println(cleansedroutedf.show(20))

				cleansedroutedf.createOrReplaceTempView("routeview")
// I will use the above temp view later.

				//2. Read data from Database
				
				val confile="/home/hduser/fleet/fleet_db_connection.prop"

		val lb1=1;
		val ub1=2000;
		val part1=2;
		val timesheetdata = org.inceptez.generic.framework.UDFs.getRdbmsData(spark.sqlContext,"fleetdb","timesheet","driverid",s"$confile",lb1,ub1,part1)

				spark.sql("""create database if not exists fleetdb""")

				if (!timesheetdata.rdd.isEmpty())
				{
					//loading data in the Raw Zone
					timesheetdata.coalesce(1).write.mode("overwrite").orc("hdfs://localhost:54310/tmp/raw/timesheetdata")

					// cleaning the data and load into the Curated/Trusted Zone  

					val tsdeduplicated=timesheetdata.dropDuplicates()
					
					// Create spark temp view for further usage
					tsdeduplicated.createOrReplaceTempView("timesheetdatatbl")

					// Write the deduplicated timesheet data to Hive
					spark.sql("create database if not exists fleetdb")
					writeHive(tsdeduplicated,"fleetdb","timesheet","p","week","overwrite")
					println("Printing the loaded hive data")
					spark.sql("""select * from fleetdb.timesheet""").show(20,false)
				}


		/// Lets deal with the Driver info
		
		val lb=1;
		val ub=1000;
		val part=2;

		// Domain Data load with Dimension modeling (SCD1 - Do a Insert if target key doesn't match else update) 
		
		// Create source mysql and target hive temp views 
		val driversql1="(select * from fleetdb.driver where updts >= current_timestamp- interval 24 hour) tmp"
				val newdata = org.inceptez.generic.framework.UDFs.getRdbmsData(spark.sqlContext,"fleetdb",driversql1,"driverid",s"$confile",lb,ub,part)
				newdata.cache

				spark.sql("""create table if not exists fleetdb.driver_scd1(driverId int ,name string,ssn int,location string,certified VARCHAR(1),
				  wageplan VARCHAR(5),updts timestamp,curdt date)
						row format delimited fields terminated by ','
						stored as textfile""")

						val histdata1=readHive(spark.sqlContext,"fleetdb","driver_scd1")
						histdata1.printSchema()
						//Creating a dummy df histdata for overwriting the same table selected, which gives the below error if we dont do it
						//org.apache.spark.sql.AnalysisException: Cannot insert overwrite into table that is also being read from.;
						var histdata=histdata1;
		if (histdata1.count()>0)
		{
			histdata1.write.mode("overwrite").parquet("hdfs://localhost:54310/tmp/raw/histdata");
			histdata=spark.read.option("header",true).option("inferschema", true).parquet("hdfs://localhost:54310/tmp/raw/histdata")
			histdata.createOrReplaceTempView("initialdata")
		}
		println(histdata.count)

		println(newdata.count)
		import org.apache.spark.sql.functions._          
		val newdatacurdt=newdata.withColumn("curdt", current_date())

		newdatacurdt.createOrReplaceTempView("incremental")
		spark.sql("""select * from incremental""").show
		//write.saveAsTable("default.driver_scd1")

// Applying SCD1 logic
		//println(histdata.count)
		try {
			//Incremental Load
			if (histdata.count > 0 ) {
				println("Historical data is available for scd1")
				val historydf = spark.sql("select driverid,name,ssn,location,certified,wageplan,updts from initialdata")
				historydf.show
				println("printing new data for reference")
				val newdf = spark.sql("select driverid,name,ssn,location,certified,wageplan,updts from incremental")
				newdf.show
				if (!newdf.rdd.isEmpty()) {
					println("History excluded data for scd1")
					// history 200 driver, incremental 100 driver -> 40 updated old, 60 new not matching , 140 (history) - 100
					// 50 new not matching 
					val historydffiltered = spark.sql("""select i.driverid,i.name,i.ssn,i.location,i.certified,i.wageplan,i.updts from initialdata i
							left outer join incremental o on i.driverid = o.driverid where o.driverid is null""")
							historydffiltered.show(20,false)
							println("History+New data")
							val newdf = historydffiltered.union(newdata).toDF().withColumn("curdt", current_date())
							newdf.show

							//newdf.coalesce(1).write.mode("overwrite").csv("file:///home/hduser/scd1newdata")
							//spark.sql("""truncate table fleetdb.driver_scd1""")
							newdf.write.mode("overwrite").saveAsTable("fleetdb.driver_scd1")

							// Applying Data Governance of Masking for Sensitive fields
							println("Data masking")
							//	spark.udf.register("posmask",org.inceptez.generic.framework.UDFs.posUDF _)
							spark.udf.register("hashmask",org.inceptez.generic.framework.UDFs.hashmask _) 
							spark.udf.register("rangemask",org.inceptez.generic.framework.UDFs.posmask _) 
							newdf.createOrReplaceTempView("driverscd1")
							val maskdf=spark.sql("""select driverid,name,rangemask(cast(ssn as string),4,9,"*") as ssnmasked,
							  hashmask(location) as locationmasked,
							  certified,wageplan,updts,curdt 
							  from driverscd1""")
							maskdf.show(10,false)
							maskdf.write.mode("overwrite").saveAsTable("fleetdb.driver_scd1_masked")
				}
			} 
			else {
				println("No Historical data to process for scd1 , loading new data as history")
				newdatacurdt.write.mode("overwrite").saveAsTable("fleetdb.driver_scd1")
			}
		} 
		catch {
		case a: org.apache.spark.sql.AnalysisException => {
			println(s"Exception Occured $a")
		}
		case unknown: Exception => println(s"Unknown exception: $unknown")
		}

// SCD2 -> Retain the history
		
		val driversql="(select * from fleetdb.driver where updts >= current_timestamp- interval 24 hour) tmp"
				val newdata1 = org.inceptez.generic.framework.UDFs.getRdbmsData(spark.sqlContext,"fleetdb",driversql,"driverid",s"$confile",lb,ub,part)
				newdata1.cache

				import org.apache.spark.sql.functions._          
				val newdatacurdt1=newdata1.withColumn("curdt", current_date())

				newdatacurdt1.createOrReplaceTempView("incremental")

				//select * from fleetdb.driver where updts >= current_timestamp - interval 24 hour;
				//update fleetdb.driver set updts=current_timestamp - interval 1 day  where driverid<20;
				//update fleetdb.driver set updts=current_timestamp  where driverid>20;
				spark.sql("""create external table if not exists fleetdb.driver_scd2
						(driverId int,ver int ,name string,ssn int,location string,certified VARCHAR(1),wageplan VARCHAR(5),updts timestamp,curdt date)
						row format delimited fields terminated by ','
						location 'hdfs://localhost:54310/user/hduser/driver_scd2'
						""")          

						val histdata2=readHive(spark.sqlContext,"fleetdb","driver_scd2")
						histdata2.printSchema()
						var histdata12=histdata2;
		histdata12.createOrReplaceTempView("initialdata2")
		if (! histdata2.rdd.isEmpty())
		{
			histdata2.write.mode("overwrite").parquet("hdfs://localhost:54310/tmp/raw/histdata2")
			val histdata12=spark.read.option("header",true).option("inferschema", true).parquet("hdfs://localhost:54310/tmp/raw/histdata2")
			histdata12.createOrReplaceTempView("initialdata2")
		}

		println(histdata12.count)
		println(newdata1.count)

		spark.sql("""select * from incremental""").show


		try {
			//							histdata12.createOrReplaceTempView("histmax")

			val historymax=spark.sql("select driverid,max(ver) as maxver from initialdata2 group by driverid")
			historymax.createOrReplaceTempView("histmax")


			println("printing new data for reference")
			val newdf2 = spark.sql("""select i.driverid,row_number() over(partition by i.driverid order by i.driverid)+coalesce(maxver,0) as ver,
					i.name,i.ssn,i.location,i.certified,i.wageplan,concat(year(i.updts),'-',month(i.updts),'-',day(i.updts),' ',hour(i.updts),':',minute(i.updts),':',second(i.updts)) as updts 
					from incremental
					i left outer join histmax b on i.driverid=b.driverid""")
					newdf2.show

					if (! newdf2.rdd.isEmpty()) {
						newdf2.withColumn("curdt", current_date()).write.mode("append").csv("hdfs://localhost:54310/user/hduser/driver_scd2")
					}

		} 
		catch {
		case a: org.apache.spark.sql.AnalysisException => {
			println(s"Exception Occured $a")
		}
		case unknown: Exception => println(s"Unknown exception: $unknown")
		}

		// Data Transformation, Metrics Derivation and Aggregation

		val truckinfodf=spark.read.option("delimiter",",").option("header","true").option("inferschema","true").
				option("maxCharsPerColumn","100").option("mode","dropmalformed").csv("file:///home/hduser/fleet/trucks_info.csv")
				.na.drop(Array("truckid")).dropDuplicates()  
				truckinfodf.createOrReplaceTempView("truckinfo")


				val predictivemaintenancedf=spark.sql("""
						select ti.truckid,case when summiles>10000 and odometer >150000 and product="Gasoline" then "maintenance"  
						else "no maintenance" end as predict_maintenance,
						case when mpg>=30 then "efficient" else "high cost" end as fuel_efficiency,summiles,odometer,product
						from truckinfo as ti
						left outer join (select driverid as truckid,sum(mileslogged) as summiles from timesheetdatatbl group by driverid) as ts
						on (ti.truckid=ts.truckid)
						""")
						predictivemaintenancedf.show(false)

						predictivemaintenancedf.write.mode("overwrite").saveAsTable("fleetdb.maintenancevehicleinfo")

						predictivemaintenancedf.createOrReplaceTempView("vehicleinfo")



						val highriskdrivepatterndf=spark.sql("""select ds.driverid,m.truckid,ds.ssn,m.predict_maintenance,
								m.fuel_efficiency,summiles,odometer
								from fleetdb.driver_scd1 as ds left outer join vehicleinfo as m 
								on(ds.driverid=m.truckid)            
								""")
								highriskdrivepatterndf.show(false)            

								highriskdrivepatterndf.write.mode("overwrite").saveAsTable("fleetdb.highriskdrivepatterndf")

								val vehiclestatespeed=spark.sql("""select driverid,truckid,distance,
										case when prev_miles > next_miles then concat("speed decreased to ",prev_miles-next_miles) else concat("vehicle is in steady speed of ",prev_miles-next_miles) end as speed_decrease,
										case when prev_miles < next_miles then concat("speed increased to ",next_miles-prev_miles) else concat("vehicle is in steady speed of ",prev_miles-next_miles) end as speed_increase,
										case when distance=0 then "vehicle is in idle state" end as idle_speed
										from ( 
										select driverid,truckid,distance,
										lag(distance) over(partition by driverid order by ts) as prev_miles,
										lead(distance) over(partition by driverid order by ts) as next_miles from routeview) as temp""")
										vehiclestatespeed.show(50,false);
		vehiclestatespeed.write.mode("overwrite").saveAsTable("fleetdb.vehicle_state_speed")

              

	}

}
























