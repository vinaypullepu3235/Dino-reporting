package com.walmart.xtools.dino.core.performance

import org.apache.spark.sql.DataFrame
import com.walmart.xtools.dino.core.spi.job.DinoStreamingJob
import com.walmart.xtools.dino.core.utils.DinoJobCommon
import scala.collection.mutable
import org.apache.spark.sql.functions.{expr, struct, to_json}
import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.ListBuffer


@SerialVersionUID(1113799434508676096L)
class FEPerformance extends DinoStreamingJob with Serializable {

  //1113799434508676095L

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  override def getJobName: String = "feperformance"

  override def getSourceMap: Map[String, String] = Map("source.1" -> "KafkaDStreamConnector")


  override def getSinkMap: Map[String, String] = Map("sink.1" -> "KairosDStreamConnector")

  override def execute(dataFrameMap: mutable.Map[String, DataFrame]): List[(DataFrame, String)] = {

    //Business logic with given Data Frames

    val kafkaDF = dataFrameMap("source.1")
    println("*********************************************************************")
    kafkaDF.printSchema()
    println("Record count: " + kafkaDF.count())
    println("*********************************************************************")

    val processedDF = processDataFrame(kafkaDF)

    //Saving a Result DF to single Sink.
    List(processedDF -> "sink.1")


  }

  def processDataFrame(df: DataFrame): DataFrame = {
    if(df.columns.contains("timestamp") && df.columns.contains("testPlanId")){
    val kafkaDf =  df.select(
      expr("performance.*"),
      expr("asset.*"),
      df("timestamp"),
      df("runId"),
      df("url"),
      df("resultLink"),
      df("projectName"),
      df("testPlanId"),
      to_json(struct("tags.*")).alias("tags")
    ).where(df("timestamp").isNotNull)
    implicit val encoder = org.apache.spark.sql.Encoders.kryo[List[KairosDTO]]

      val updatedDF = kafkaDf.mapPartitions(iter => {
        iter.map(r => {
          val metrics = new ListBuffer[KairosDTO]
          // !IMPORTANT! this multiply by 1000 is temporary until site-speed-kafka-plugin is updated
          val timestamp = r.getAs[Long]("timestamp")*1000
          val runId = r.getAs[Long]("runId")
          // ================================================================
          // create tags map
          val tagsMap = DinoJobCommon.createTagsMap(r.getAs[String]("tags"))
          tagsMap.put("url", DinoJobCommon.makeUrlKairosSafe(r.getAs[String]("url")))
          tagsMap.put("projectName", r.getAs[String]("projectName"))
          // ================================================================
          for (i <- 0 to r.length-1) {
            val name = r.schema.fieldNames(i)
            var value = ""

            if (r(i)!=null) {
              value = DinoJobCommon.makeUrlKairosSafe(r(i).toString)
            }

            // if the field name is not tags, url, or resultLink it is a metric field
            if (!name.equals("tags") && !name.equals("url") && !name.equals("projectName")) {
              if (name.equals("resultLink")) {
                metrics += new KairosDTO(name + "_" + runId, value, timestamp, tagsMap, 15780000)
              }
              else {
                metrics += new KairosDTO(name, value, timestamp, tagsMap, 15780000)
              }
            }
          }
          metrics.toList
        })
      })

      updatedDF.toDF() }else {
      null
    }

  }



}
