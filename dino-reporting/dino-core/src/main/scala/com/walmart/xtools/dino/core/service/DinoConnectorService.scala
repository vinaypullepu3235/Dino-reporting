package com.walmart.xtools.dino.core.service


import java.util.ServiceLoader

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import com.walmart.xtools.dino.core.utils.SparkCCMConfigurator
import com.walmart.xtools.dino.core.spi.connector.{BaseBatchConnector, BaseDStreamConnector, BaseStreamConnector, ConnectorRegister}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

case class DinoConnectorService(sparkSession: SparkSession,
                                  sparkCCMConfigurator: SparkCCMConfigurator,
                                  sourceMap: Map[String, String],
                                  sinkMap: Map[String, String]) {


  def read: scala.collection.mutable.Map[String, Any] = {

    var inputDFMap = scala.collection.mutable.Map[String, Any]()
    var dStreamFound = false
    var structuredStreamingFound = false

    // Iterate through all Sources and create map of all readers. Each source is connector with Id and type of
    // the connector . Ex: ("source.1" -> "KafkaDStreamConnector")

    val path = "/Users/vn0xghh/Desktop/test123"
    /*val intCountRDD = sparkSession.sparkContext.wholeTextFiles(path).map(x => x._2)
    intCountRDD.collect().foreach(println)*/

    val schema = (new StructType).add("requestcount", IntegerType)
      .add("error_count", IntegerType).add("response_status", IntegerType)
      .add("ExecutionTime", IntegerType).add("SessionTime", IntegerType)
      .add("channel", StringType).add("appname", StringType)
      .add("sourcename", StringType).add("APIName", StringType)
      .add("host", StringType).add("timestamp", StringType)
      /*.add("Browser", StringType).add("DeviceType", StringType)
      .add("Device", StringType).add("appver", StringType)
      .add("envtype", StringType)*/


    val intCountRDD = sparkSession.sparkContext.wholeTextFiles(path).map(x => x._2)

    val res = intCountRDD.map(x => x).collect().mkString

    val jsonstr = "[" + res + "]"

    val rdd = sparkSession.sparkContext.parallelize(Seq(jsonstr))
    val intCountDF = sparkSession.read.json(rdd)

    //val intCountDF = sparkSession.read.option("multiline", "true").schema(schema).json(path)
    intCountDF.show()
    print("Printing dataframe in dinoconnectorservice complete...")

    for (source <- sourceMap) {

      /** Fetch connector specific configs from CCM */
      val configs = sparkCCMConfigurator.getPropertiesWithId(source._1, source._2).asScala

      DinoConnectorService.lookupDinoConnector(source._2).newInstance() match {

        case baseBatchConnector: BaseBatchConnector =>
          inputDFMap += (source._1 -> baseBatchConnector.read(sparkSession, configs))

        case baseDStreamConnector: BaseDStreamConnector =>
          if (dStreamFound)
            throw new Exception("More than 1 KafkaDstreams are not allowed")
          else if (structuredStreamingFound) {
            throw new Exception("Structured Streaming source is already found. Structured Streaming and Discretized streaming in single job is not allowed")
          }
          else {
            //inputDFMap += (source._1 -> baseDStreamConnector.read(sparkSession, configs))
            inputDFMap += (source._1 -> intCountDF)
            dStreamFound = true
          }

        case baseStreamConnector: BaseStreamConnector =>
          if (dStreamFound ) {
            throw new Exception("Discretized Streaming source is already found. Structured Streaming and DStream in single job is not allowed")
          } else {
            inputDFMap += (source._1 -> baseStreamConnector.readStream(sparkSession, configs))
            structuredStreamingFound = true
          }

        case _ => throw new Exception(s"Unknown method read for connector id ${source._1} of type $source._2")
      }

    }
    inputDFMap
  }

  def write(sinkDataFrameList: List[(DataFrame, String)] ) : Unit = {

    for (sinkDataFrameElement <- sinkDataFrameList) {

      val sinkDataFrame = sinkDataFrameElement._1
      val sinkId = sinkDataFrameElement._2
      val sinkType = sinkMap(sinkId)

      /** Fetch Write connector parameters using CCM Util library. */
      val configs = sparkCCMConfigurator.getPropertiesWithId(sinkId,sinkType).asScala

      DinoConnectorService.lookupDinoConnector(sinkType).newInstance() match {
        case baseBatchConnector:  BaseBatchConnector => baseBatchConnector.write(sinkDataFrame, configs)
        case baseStreamConnector: BaseStreamConnector => baseStreamConnector.writeStream(sinkDataFrame, configs)
        case baseDStreamConnector: BaseDStreamConnector => baseDStreamConnector.writeStream(sinkDataFrame, configs)
        case _ => throw new Exception(s"Unknown method read for connector id ${sinkId} of type ${sinkType}")
      }
    }

  }

}

object DinoConnectorService {

  private def getContextOrSparkClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader

  private val loader = getContextOrSparkClassLoader

  def lookupDinoConnector(connectorType: String): Class[_] = {
    try {

      val serviceLoader = ServiceLoader.load(classOf[ConnectorRegister], loader)

      serviceLoader.asScala.filter(_.getName.equalsIgnoreCase(connectorType)).toList match {

        // the provider format did not match any given registered aliases
        case Nil =>
          try {
            Try(loader.loadClass(connectorType)) match {
              case Success(dataSource) =>
                // Found the data source using fully qualified path
                dataSource
              case Failure(error) =>
                throw new ClassNotFoundException(
                  s"Failed to find data source: $connectorType. Please find how to add new connectors here ",
                  error)
            }
          } catch {
            case e: Exception => throw e
          }
        case head :: Nil =>
          // there is exactly one registered alias
          head.getClass
        case sources =>
          // There are multiple registered aliases for the input. If there is single datasource
          // that has "org.apache.spark" package in the prefix, we use it considering it is an
          // internal datasource within Spark.
          val sourceNames = sources.map(_.getClass.getName)
          val internalSources = sources.filter(_.getClass.getName.startsWith("com.walmart.xtools.dino.core.connector"))
          if (internalSources.lengthCompare(1) == 0) {
            println(s"Multiple sources found for $connectorType (${sourceNames.mkString(", ")}), " +
              s"defaulting to the internal datasource (${internalSources.head.getClass.getName}).")
            internalSources.head.getClass
          } else {
            throw new ClassNotFoundException(s"Multiple sources found for $connectorType" +
              s"(${sourceNames.mkString(", ")}), please specify the fully qualified class name.")
          }
      }
    } catch {
      case e: Exception => throw e
    }
  }
}

