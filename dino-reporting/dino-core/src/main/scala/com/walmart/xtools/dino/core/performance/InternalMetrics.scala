package com.walmart.xtools.dino.core.performance

import com.walmart.xtools.dino.core.api.BaseDinoSparkJob.createSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.walmart.xtools.dino.core.spi.job.DinoStreamingJob
import com.walmart.xtools.dino.core.utils.DinoJobCommon
import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO

import scala.collection.mutable
import org.apache.spark.sql.functions.{expr, struct, to_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable.ListBuffer

@SerialVersionUID(1113799434508676097L)
class InternalMetrics extends DinoStreamingJob {
  override def getJobName: String = "internalmetrics"
  override def getSourceMap: Map[String, String] = Map("source.1" -> "KafkaDStreamConnector")
  // override def getSinkMap: Map[String, String] = Map("sink.1" -> "KafkaBatchConnector")
  override def getSinkMap: Map[String, String] = Map("sink.1" -> "KairosDStreamConnector")
  override def execute(dataframeMap: mutable.Map[String, DataFrame]) : List[(DataFrame,String)] = {
    //Business logic with given Data Frames

    val kafkaDF = dataframeMap("source.1")
    println("*********************************************************************")
    kafkaDF.printSchema()
    println("Record count: " + kafkaDF.count())
    println("*********************************************************************")

    /*val path = "/Users/vn0xghh/Desktop/test123"

    val schema = (new StructType).add("requestcount", IntegerType)
      .add("error_count", IntegerType).add("response_status", IntegerType)
      .add("ExecutionTime", IntegerType).add("SessionTime", IntegerType)
      .add("channel", StringType).add("appname", StringType)
      .add("sourcename", StringType).add("APIName", StringType)
      .add("host", StringType).add("timestamp", StringType)

    val peopleDF = sparkSession.read.option("multiline", "true").schema(schema).json(path)
    peopleDF.show()*/

    processDataFrame(kafkaDF)

    //Saving a Result DF to single Sink.
    List(kafkaDF -> "sink.1")
  }

  def processDataFrame(df : DataFrame) : Any = {
    val kafkaDf =  df.select(
      df("requestcount"),
      df("error_count"),
      df("response_status"),
      df("ExecutionTime"),
      df("SessionTime"),
      df("channel"),
      df("appname"),
      df("sourcename"),
      df("APIName"),
      df("host"),
      df("timestamp")
    )
    /*df("Browser"),
          df("DeviceType"),
          df("Device"),
          df("appver"),
          df("envtype")*/

    /*val dfMap: Map[String,String] = Map("request" ->"requestcount","error" -> "error_count",
      "response" ->"response_status", "execute" -> "ExecutionTime", "session" -> "SessionTime"," +
      "channel" ->"channel", "app" -> "appname", "source" -> "sourcename",
      "api" ->"APIName", "host" -> "host", "timestamp" -> "timestamp",
      "browser" ->"Browser", "devicetype" -> "DeviceType", "device" -> "Device" ,
      "appver" ->"appver", "env" -> "envtype" )*/






    print("printing dataframe...")
    df.show()
    val tagsMap = new java.util.HashMap[String, String]()

    implicit val encoder = org.apache.spark.sql.Encoders.kryo[List[KairosDTO]]



    val kafkardd = kafkaDf.rdd
    val kafkarddhead = kafkaDf.head()

    //print(kafkaDf.rdd.count())
    //kafkardd.foreach(println)

    val res = kafkardd.map(x => x).collect().mkString

    val jsonstr = "[" + res + "]"

    print(kafkarddhead)

    print(kafkarddhead.fieldIndex("timestamp"))
    print(kafkardd.count())
    var uDF = kafkardd.collect.foreach{ r => {
      val metrics = new ListBuffer[KairosDTO]
      // !IMPORTANT! this multiply by 1000 is temporary until site-speed-kafka-plugin is updated
      //val timestamp = r.getAs[Long]("timestamp")*1000
      val timestamp = 1

      //r.getAs[Long](kafkarddhead.fieldIndex("timestamp"))

      //val timestamp =  r.getAs[Long](kafkarddhead.fieldIndex("timestamp")) * 1000


      // ================================================================
      // create tags map

      tagsMap.put("channel", r(kafkarddhead.fieldIndex("channel")).toString)
      tagsMap.put("AppName", r(kafkarddhead.fieldIndex("appname")).toString)
      tagsMap.put("SourceName", r(kafkarddhead.fieldIndex("sourcename")).toString)
      tagsMap.put("APIName", r(kafkarddhead.fieldIndex("APIName")).toString)

      /*if(!r(kafkarddhead.fieldIndex("Browser")).toString().equals("")){
        tagsMap.put("Browser", r(kafkarddhead.fieldIndex("Browser")).toString)
      }   */
      if(df.columns.contains("Browser"))         {
        tagsMap.put("Browser", r(kafkarddhead.fieldIndex("Browser")).toString)
      }
      if(df.columns.contains("DeviceType")) {
        tagsMap.put("DeviceType", r(kafkarddhead.fieldIndex("DeviceType")).toString)
      }
      if(df.columns.contains("Device")) {
        tagsMap.put("Device", r(kafkarddhead.fieldIndex("Device")).toString)
      }
      tagsMap.put("host", r(kafkarddhead.fieldIndex("host")).toString)
      if(df.columns.contains("appver")) {
        tagsMap.put("appver", r(kafkarddhead.fieldIndex("appver")).toString)
      }
      if(df.columns.contains("envtype")) {
        tagsMap.put("envtype", r(kafkarddhead.fieldIndex("envtype")).toString)
      }
      // ================================================================

      print("size of r =" + r.size)

      for (i <- 0 to r.size-1) {
        //val name = r.schema.fieldNames(i)
        val name = kafkarddhead.get(i).toString()
        var value = ""

        if (r(i)!=null) {
          value = DinoJobCommon.makeUrlKairosSafe(r(i).toString)
        }

        // if the field name is not tags, url, or resultLink it is a metric field


        if(!tagsMap.containsKey(name) && !name.equals("timestamp")){
          metrics += new KairosDTO(name, value, timestamp, tagsMap, 15780000)
        }
      }
     metrics.toList
    }
    }
    //var a = 0



    /*val updatedDF = kafkaDf.foreachPartition(iter => {
      iter.map(r => {

        val metrics = new ListBuffer[KairosDTO]
       // !IMPORTANT! this multiply by 1000 is temporary until site-speed-kafka-plugin is updated
        val timestamp = r.getAs[Long]("timestamp")*1000

        // ================================================================
        // create tags map

        tagsMap.put("channel", r(r.fieldIndex("channel")).toString)
        tagsMap.put("AppName", r(r.fieldIndex("AppName")).toString)
        tagsMap.put("SourceName", r(r.fieldIndex("SourceName")).toString)
        tagsMap.put("APIName", r(r.fieldIndex("APIName")).toString)



        if(!r(r.fieldIndex("Browser")).toString().equals("")){
          tagsMap.put("Browser", r(r.fieldIndex("Browser")).toString)
        }
        if(!r(r.fieldIndex("DeviceType")).toString().equals("")){
          tagsMap.put("DeviceType", r(r.fieldIndex("DeviceType")).toString)
        }
        if(!r(r.fieldIndex("Device")).toString().equals("")){
          tagsMap.put("Device", r(r.fieldIndex("Device")).toString)
        }
        tagsMap.put("host", r(r.fieldIndex("host")).toString)
        if(!r(r.fieldIndex("appver")).toString().equals("")){
          tagsMap.put("appver", r(r.fieldIndex("appver")).toString)
        }
        if(!r(r.fieldIndex("envtype")).toString().equals("")){
          tagsMap.put("envtype", r(r.fieldIndex("envtype")).toString)
        }
        // ================================================================

        for (i <- 0 to r.length-1) {
          val name = r.schema.fieldNames(i)
          var value = ""

          if (r(i)!=null) {
            value = DinoJobCommon.makeUrlKairosSafe(r(i).toString)
          }

          // if the field name is not tags, url, or resultLink it is a metric field


          if(!tagsMap.containsKey(name) && !name.equals("timestamp")){
            metrics += new KairosDTO(name, value, timestamp, tagsMap, 15780000)
          }
        }
        metrics.toList
      })
    })*/

  }
}
