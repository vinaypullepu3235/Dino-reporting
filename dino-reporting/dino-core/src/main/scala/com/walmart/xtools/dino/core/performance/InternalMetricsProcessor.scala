package com.walmart.xtools.dino.core.performance

import com.walmart.xtools.dino.core.api.BaseDinoSparkJob.createSparkSession
import com.walmart.xtools.dino.core.utils.DinoJobCommon
import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class InternalMetricsProcessor {
  def ProcessMetrics(df : DataFrame): ListBuffer[KairosDTO] ={
    var metrics = new ListBuffer[KairosDTO]

    val tagsMap = new java.util.HashMap[String, String]()

    implicit val encoder = org.apache.spark.sql.Encoders.kryo[List[KairosDTO]]




    val kafkaDf = df

    val kafkardd = kafkaDf.rdd
    val kafkarddhead = kafkaDf.head()
    var uDF = kafkardd.collect.foreach{ r => {

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
      tagsMap.put("host", r(kafkarddhead.fieldIndex("host")).toString)

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

      if(df.columns.contains("appver")) {
        tagsMap.put("appver", r(kafkarddhead.fieldIndex("appver")).toString)
      }
      if(df.columns.contains("envtype")) {
        tagsMap.put("envtype", r(kafkarddhead.fieldIndex("envtype")).toString)
      }
      // ================================================================ 

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
      //metrics.toList
    }
      metrics.toList

    }

    return metrics
  }
}
