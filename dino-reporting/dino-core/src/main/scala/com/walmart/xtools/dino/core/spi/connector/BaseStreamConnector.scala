package com.walmart.xtools.dino.core.spi.connector

import org.apache.spark.sql.{DataFrame, SparkSession}

trait BaseStreamConnector {

  def getFormat: String

  def readStream(sparkSession: SparkSession,
                 configs: collection.mutable.Map[String, String]): DataFrame = {
    sparkSession
        .readStream
        .format(getFormat)
        .options(configs)
        .load()
  }

  def writeStream(dataFrame: DataFrame,
                  configs: collection.mutable.Map[String, String]): Unit = {
    dataFrame
        .writeStream
        .format(getFormat)
        .options(configs)
        .start
        .awaitTermination

  }
}
