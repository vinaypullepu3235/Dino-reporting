package com.walmart.xtools.dino.core.spi.connector

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait BaseBatchConnector {

  def getFormat: String

  def read(sparkSession: SparkSession,
           configs: collection.mutable.Map[String, String]): DataFrame = {

    val format = getFormatFromConfigs(configs)

    val dataFrame = sparkSession
        .read
        .format(format)
        .options(configs)
        .load()
    /**
      * Determine the Temporary view Name. Precedence takes place to value assigned to "sql.view.name".
      */
    if (configs.contains("sql.view.name")) {
      dataFrame.createOrReplaceTempView(configs("sql.view.name"))
    }
    dataFrame
  }

  def write(dataFrame: DataFrame,
            configs: collection.mutable.Map[String, String]): Unit = {


      val saveMode = if(configs.contains("SaveMode")) {
        configs("SaveMode").toLowerCase match {
          case "append" => SaveMode.Append
          case "overwrite" => SaveMode.Overwrite
          case "errorifexists" => SaveMode.ErrorIfExists
          case "ignore" => SaveMode.Ignore
          case _ => SaveMode.ErrorIfExists
        }
      } else {
        SaveMode.ErrorIfExists
      }

    val format = getFormatFromConfigs(configs)

      dataFrame
        .write
        .format(format)
        .options(configs)
        .mode(saveMode)
        .save()

  }

  private def getFormatFromConfigs(configs: collection.mutable.Map[String, String]): String ={

    val format = if (getFormat.equalsIgnoreCase("generic")) {

      if (configs.contains("format")){
        if (ConnectorType.isProvidedConnector(configs("format"))){
          throw new Exception("Please use the provided connectors instead of generic connector.")
        }
        configs("format")
      }
      else
        throw new Exception("format does not exists in config.")
    }
    else
    getFormat

    if (configs.contains("format"))
      configs.remove("format")

    format
  }

}
