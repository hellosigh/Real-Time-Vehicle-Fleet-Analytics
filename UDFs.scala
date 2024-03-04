package org.inceptez.generic.framework

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.io.FileInputStream
import java.util.Properties


object UDFs {

def readHive(sqlc: SQLContext,sch:String,tbl:String):DataFrame={
  
  sqlc.read.table(s"$sch"+"."+s"$tbl")
}  

def writeHive(df:org.apache.spark.sql.DataFrame,sch:String,tbl:String,part:String,cols:String,mode:String)={
  if(part == "p")
    {
    println("creating partition table")
  df.write.mode(s"$mode").partitionBy(s"$cols").saveAsTable(s"$sch"+"."+s"$tbl")
    }
  else
  df.write.mode(s"$mode").saveAsTable(s"$sch"+"."+s"$tbl")  
}
  
  /* function to connect and pull data from mysql database */
def getRdbmsData(sqlc: SQLContext,DatabaseName: String, TableName: String, PartitionColumn:String, ConnFile: String,lb:Int,ub:Int,part:Int):
DataFrame= {
val conn=new Properties()
val probFile= new FileInputStream(s"$ConnFile")
conn.load(probFile)
/* Reading mysql server connection detail from property file */
val Driver=conn.getProperty("driver")
val Host =conn.getProperty("host")
val Port =conn.getProperty("port")
val User =conn.getProperty("user")
val Pass =conn.getProperty("pass")
val url="jdbc:mysql://"+Host+":"+Port+"/"+s"$DatabaseName"
val df=sqlc.read.format("jdbc")
.option("driver",s"$Driver")
.option("url",s"$url")
.option("user",s"$User")
.option("lowerBound",s"$lb")
.option("upperBound",s"$ub")
.option("numPartitions",s"$part")
.option("partitionCol",s"$PartitionColumn")
.option("password",s"$Pass")
.option("dbtable",s"$TableName").load()
return df;
}

 
  def emailUDF = udf {
    email: String => email.replaceAll("(?<=@)[^.]+(?=\\.)", "*****")

  }
  def cellUDF = udf {
      cell:String => cell.replaceAll("[^0-9]","")
  }
  def ageUDF = udf {
        age:String => val md = java.security.MessageDigest.getInstance("SHA-1")
          val ha = new sun.misc.BASE64Encoder().encode(md.digest(age.getBytes))
      ha
  }

 // 140-333-4444
 // 1403334444, 5
 // 14033XXXXX
  //substring will start from 0

  def posUDF = udf {
    (cell: String, pos :Int) => cell.substring(0, cell.length - pos).concat("X" * pos)
    }
  
  def hashmask(indata:String):Int=
    return indata.trim().hashCode()
  def posmask(data:String,st:Int,ed:Int,sym:String):String={
    var maskstr=""
    var tmp=ed
    val dsize=data.size
    if(dsize<ed)
    {
      tmp=dsize
    }
    
    for (i <- 1 to tmp-st)
    {
      maskstr=maskstr+sym
      //9-5-> 4 ****
    }
    val pre=data.substring(0,st)
    val pos=data.substring(tmp,dsize)
    
    return pre+maskstr+pos
  }
}



















