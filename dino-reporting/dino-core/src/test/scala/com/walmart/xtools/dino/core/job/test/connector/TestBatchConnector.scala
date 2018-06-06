package com.walmart.xtools.dino.core.job.test.connector

import com.walmart.xtools.dino.core.spi.connector.{BaseBatchConnector, ConnectorRegister}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.Map


class TestBatchConnector extends BaseBatchConnector with ConnectorRegister {

  override def getName: String = "TestBatchConnector"

  override def getFormat: String = "test"

  override def read(sparkSession: SparkSession, configs: Map[String, String]): DataFrame = {

    val testList = List("one", "two", "three", "four", "five", "six")
    import sparkSession.implicits._
    sparkSession.sparkContext.parallelize(testList).toDF()

  }

  override def write(dataFrame: DataFrame, configs: Map[String, String]): Unit = {
    dataFrame
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("path","dino-core/src/test/resources/output")
        .saveAsTable("person")
  }

}
