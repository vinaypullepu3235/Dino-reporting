package com.walmart.xtools.dino.core.performance

import com.fasterxml.jackson.databind.ObjectMapper
import com.walmart.xtools.dino.core.spi.job.DinoStreamingJob
import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

@SerialVersionUID(1113799434508676095L)
class BEPerformance extends DinoStreamingJob with Serializable {


  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  override def getJobName: String = "beperformance"

  override def getSourceMap: Map[String, String] = Map("source.1" -> "KafkaDStreamConnector")


  override def getSinkMap: Map[String, String] = Map("sink.1" -> "KairosDStreamConnector")

  override def execute(dataFrameMap: mutable.Map[String, DataFrame]): List[(DataFrame, String)] = {

    //Business logic with given Data Frames
   try{
    val kafkaDF = dataFrameMap("source.1")
    println("*********************************************************************")
    kafkaDF.printSchema()
    println("Record count: " + kafkaDF.count())
    println("*********************************************************************")

    val processedDF = processDataFrame(kafkaDF)

    //Saving a Result DF to single Sink.
    List(processedDF -> "sink.1")

  }catch {
      case ex: Exception => import java.io.{PrintWriter, StringWriter}
      {val msg = "Exception in processDataFrame"
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))
        println("Exception in processDataFrame"+sw.toString)
        LOGGER.error(msg, ex)
        throw new Exception(msg, ex)
      }
    }


  }

  val addPercentileMetrics = (data: String,
                           timestamp: Long,
                           tagsMap: java.util.Map[String, String],
                           metrics: ListBuffer[KairosDTO])   => {
    val objectMapper = new ObjectMapper
    val percentilesNode = objectMapper.readTree(data)
    val fields = percentilesNode.fieldNames()
    val iter = percentilesNode.fieldNames
    while (iter.hasNext) {
      val name = iter.next
      val value = percentilesNode.get(name).asText
      metrics += new KairosDTO(name, value, timestamp, tagsMap, 15780000)
    }
  }

  val processDataFrame = (df: DataFrame) => {

    if (df.columns.contains("timestamp") && df.columns.contains("testPlanId")) {
      val kafkaDf = df.select(
        df("*"),
        df("timestamp"),
        df("runId"),
        df("channel"),
        df("transactionName"),
        df("status"),
        df("testPlanId"),
        to_json(struct("percentiles.*")).alias("extractedPercentiles")
      )
      implicit val encoder = org.apache.spark.sql.Encoders.kryo[List[KairosDTO]]

      val updatedDF = kafkaDf.mapPartitions(iter => {

        iter.map(r => {
          val metrics = new ListBuffer[KairosDTO]
          try {
            val timestamp = r.getAs[String]("timestamp").toLong
            val transactionName = r.getAs[String]("transactionName")

            // create tags map
            val tagsMap = new java.util.HashMap[String, String]()
            tagsMap.put("runId", r.getAs[Long]("runId").toString)
            tagsMap.put("channel", r.getAs[String]("channel"))
            tagsMap.put("transactionName", transactionName)
            tagsMap.put("testPlanId", r.getAs[Long]("testPlanId").toString)
            tagsMap.put("status", r.getAs[String]("status"))

            if(r.schema.fieldNames.contains("extractedPercentiles")) {
              addPercentileMetrics(r.getAs[String]("extractedPercentiles"), timestamp, tagsMap, metrics)
            }

            for (i <- 0 to r.length - 1) {
              val name = r.schema.fieldNames(i)
              var value = ""
              if (r(i) != null) {
                 value = r(i).toString
              }

              // if the field name is not tags, url, or resultLink it is a metric field
              if (!name.equals("percentiles")  && !name.equals("extractedPercentiles") && !name.equals("runId") && !name.equals("channel") && !name.equals("transactionName") && !name.equals("testPlanId") && !name.equals("status") && !name.equals("timestamp")) {
                  metrics += new KairosDTO(name, value, timestamp, tagsMap, 15780000)
              }
            }
          }
          catch {
            case ex: Exception => import java.io.{PrintWriter, StringWriter}
            {val msg = "Exception in processDataFrame"
              val sw = new StringWriter
              ex.printStackTrace(new PrintWriter(sw))
              println("Exception in processDataFrame"+sw.toString)
              LOGGER.error(msg, ex)
              throw new Exception(msg, ex)
            }
          }
          metrics.toList
        })
      }) // --> RDD[List[KairosDTO]]
       updatedDF.toDF()
      }else{
        null
       }
   }


}
