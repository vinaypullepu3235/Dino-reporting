package com.walmart.xtools.dino.core.job.test.jobs

import com.walmart.xtools.dino.core.spi.job.DinoStreamingJob
import org.apache.spark.sql.DataFrame
import scala.collection.mutable

class TestJob extends DinoStreamingJob {

  override def getJobName: String = "TestJob"

  override def getSourceMap: Map[String, String] = Map("source.1" -> "TestBatchConnector")

  override def getSinkMap: Map[String, String] = Map("sink.1" -> "TestBatchConnector")

  override def execute(dataFrameMap: mutable.Map[String, DataFrame]): List[(DataFrame, String)] = {

    val kafkaDF = dataFrameMap("source.1")

    println("*********************************************************************")
    kafkaDF.printSchema()
    println("Record count: " + kafkaDF.collect())
    println("*********************************************************************")

    //Saving a Result DF to single Sink.
    List(kafkaDF -> "sink.1")

  }
}
