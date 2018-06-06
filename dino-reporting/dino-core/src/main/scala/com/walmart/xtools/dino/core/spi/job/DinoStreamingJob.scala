package com.walmart.xtools.dino.core.spi.job

import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait DinoStreamingJob {

  // --TODO Do the validation for the job params
  // def validate(config: Config) : SparkJobValidation

  def getJobName: String

  def getSourceMap: Map[String, String]

  def getSinkMap: Map[String, String]

  def execute(dataFrameMap: mutable.Map[String, DataFrame]): List[(DataFrame, String)]= {

    //default implementation: can handle one source and multiple sinks
    val sourceDF = dataFrameMap(getSourceMap.head._1)
    var sinkList = new ListBuffer[(DataFrame, String)]

    for (sink <- getSinkMap){
      sinkList += (sourceDF->sink._1)
    }

    sinkList.toList
  }

}

