package com.walmart.xtools.dino.core.api

//import org.apache.calcite.avatica.ColumnMetaData.StructType
import com.walmart.xtools.dino.core.performance.InternalMetricsProcessor
import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

//import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import com.walmart.xtools.dino.core.utils.SparkCCMConfigurator
import com.walmart.xtools.dino.core.service.DinoStreamingJobService
import com.walmart.xtools.dino.core.service.DinoConnectorService
import com.walmart.xtools.dino.core.service.ConfigSourceType
import com.walmart.xtools.dino.core.performance

import scala.collection.mutable
import com.typesafe.config.ConfigFactory
import com.walmart.xtools.dino.core.service.ConfigSourceType._
import com.walmart.xtools.dino.core.utils.DinoJobCommon
import org.slf4j.{Logger, LoggerFactory}


object BaseDinoSparkJob {



  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    try {

    // Parse input arguments
    //val (jobAppName, configSourceType) = parseArgs(args)

    val jobAppName = "internalmetrics"
    val configSourceType = ConfigSourceType.LOCAL
    LOGGER.info("Argument found. Job Name: " + jobAppName + "Config Source Type" + configSourceType)

    /** Initialize configuration parameters from CCM Helper Library or the local properties and the system properties */
    val sparkCCMConfigurator = getCCMConfigurator(jobAppName, configSourceType)

    // Get Job service
    val jobService = getJobService(jobAppName)


    // Execute the Spark Job
    executeSparkJob(sparkCCMConfigurator, jobService)

  }catch {
      case ex: Exception => {
        print(ex.getMessage())
        val msg = "Exception in the main of BaseDinoSparkJob "
        LOGGER.error(msg, ex)
        System.exit(1)
      }
    }

  }

  private def executeSparkJob(sparkCCMConfigurator: SparkCCMConfigurator,
                              jobService: DinoStreamingJobService) = {

    val (sourceMap, sinkMap) = getConnectorMaps(jobService)

    // Create Spark Session
    val sparkSession: SparkSession = try createSparkSession(sparkCCMConfigurator)
    catch {
      case ex: Throwable => throw ex
    }
    //val sparkSession: SparkSession = createSparkSession(sparkCCMConfigurator)


    // Create Connector Service
    val connectorService = DinoConnectorService(sparkSession, sparkCCMConfigurator, sourceMap, sinkMap)

    // Read input DataFrames including KafkaInputDStream
    val inputDFMap = connectorService.read

    var kafkaDStream: Tuple2[String, InputDStream[ConsumerRecord[String, String]]] = null
    var dataFrameMap = scala.collection.mutable.Map[String, DataFrame]()

    //Find whether source map contains Kafka DStream
    for (inputDFTuple <- inputDFMap) {
      inputDFTuple._2 match {
        case kafkaInputDStream: InputDStream[ConsumerRecord[String, String]] =>
          kafkaDStream = inputDFTuple._1 -> kafkaInputDStream
        case dataFrame: DataFrame =>
          dataFrameMap += (inputDFTuple._1 -> dataFrame)
      }
    }

    if (kafkaDStream != null) {

      // Remove dStream Tuple
      inputDFMap -= kafkaDStream._1

      // TODO: Call handle Kafka DStream
      handleKafkaDStream(sparkSession,
        kafkaDStream,
        dataFrameMap,
        sparkCCMConfigurator,
        jobService,
        connectorService,
        sourceMap,
        sinkMap)

    } else {
      // Handle Data Frames - Batch DataFrames and Structured Streaming
      val sinkDataFrameList  = jobService.execute(dataFrameMap)


      // TODO: Save ResultDFList
      // saveDataFrame(resultDFList, sparkSession, sparkCCMConfigurator, sinkMap)
      connectorService.write(sinkDataFrameList)

    }

  }

  private def getConnectorMaps(jobService: DinoStreamingJobService) = {
    // Get Source Map
    val sourceMap = jobService.getSources

    // Get Sink Map
    val sinkMap = jobService.getSinks

    // Make sure Job has at least one source and one sink before proceeding.
    if (sourceMap.isEmpty || sinkMap.isEmpty) throw new Exception("Please provided at least 1 Source and 1 Sink map")

    (sourceMap, sinkMap)
  }

  private def parseArgs(args: Array[String]): (String, ConfigSourceType.Value) = {

    LOGGER.info("Parsing arguments: " + args.mkString(","))

    // Throw error if minimum arguments are not passed.
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: BaseDinoSparkJob <Spark job Name> <Configuration Type(--ccm/--local)")
    }

    // Make sure Spark Job name is Alphanumeric
    val ordinary=(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
    def isAlphanumeric(s:String)=s.forall(ordinary.contains(_))

    val jobAppName = {
      if (isAlphanumeric(args(0)))
        args(0)
      else
        throw new IllegalArgumentException("Job Name " +
          "should be a String")
    }

    // Configuration Source Type can be either "--ccm" or "--local"
    val configSourceType = args(1).toLowerCase match {
      case "--ccm" => ConfigSourceType.CCM
      case "--local" => ConfigSourceType.LOCAL
      case _ => throw new IllegalArgumentException("--ccm or --local are only valid Configuration Source Types")
    }

    (jobAppName, configSourceType)

  }

  private def handleKafkaDStream(sparkSession: SparkSession,
                                 discretizedStreamReaderMap: (String, InputDStream[ConsumerRecord[String, String]]),
                                 dataFrameMap: scala.collection.mutable.Map[String, DataFrame],
                                 sparkCCMConfigurator: SparkCCMConfigurator,
                                 jobService: DinoStreamingJobService,
                                 connectorService: DinoConnectorService,
                                 sourceMap: Map[String, String],
                                 sinkMap: Map[String, String]): Unit = {

    val sourceId = discretizedStreamReaderMap._1
    val kafkaDStream = discretizedStreamReaderMap._2
    import sparkSession.implicits._

    //Read mock data and add rdds into KafkaDStream here
    //val df = sparkSession.read.json("/Users/vn0xghh/Desktop/test123")
    //sparkSession.read.json("/Users/vn0xghh/Desktop/test123").printSchema()



    /*var df: DataFrame = sparkSession.read.option("multiline","true").schema(schema).json(path).toDF()
    df.show()*/

    //kafkaDStream.map(u => r.)


    kafkaDStream.foreachRDD { rdd : RDD[ConsumerRecord[String, String]] =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      import sparkSession.implicits._

      val df = sparkSession.read.json(rdd.map(record => record.value).toDS)

      /**
        * Send the source DF to Job class to run the business logic and get the result DataFrame.
        */
      val sinkDataFrameList = jobService.execute(dataFrameMap = dataFrameMap + (sourceId -> df))

      connectorService.write(sinkDataFrameList)

      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }

    kafkaDStream.context.start()
    kafkaDStream.context.awaitTermination()

  }

  private def getJobService(jobName: String) = DinoStreamingJobService(jobName)


  /**
    * Creates spark session using configs from SparkCCMConfigurator, Helper library to fetch CCM configs.
    *
    * @param sparkCCMConfigurator : CCM configurator
    * @return Returns Spark Session
    */
  private def createSparkSession(sparkCCMConfigurator: SparkCCMConfigurator): SparkSession = {


    import scala.collection.JavaConverters._
    /** Fetch spark session related configs. */
    val configs = sparkCCMConfigurator.getPropertiesWithId("spark", "spark")
    val sparkconf = new SparkConf().setAll(configs.asScala)
                /*.setAppName("internalmetrics").setMaster("local")*/

    //.setMaster("local[*]").setAppName("internalmetrics")
    //.setAll(configs.asScala)
    //val sparkconf = new SparkConf().setAppName("internalmetrics").setMaster("local")

    /*val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("internalmetrics")
      .getOrCreate()*/



    val sparkSession = SparkSession.builder.config(sparkconf).getOrCreate()






    println("spark session default configurations")
    sparkSession.conf.getAll foreach println
    sparkSession



  }

  /*private def getconf(): SparkConf = {
    try {
      val conf = new SparkConf().setAppName("internalmetrics").setMaster("local[*]")
      conf
    }
    catch {
      case ex: Exception => {
        val msg = "Exception in the main of BaseDinoSparkJob "
        LOGGER.error(msg, ex)
      }
    }
  }*/

  /**
    * Fetch CCM Configgs and returns SparkCCMConfigurator based on the configSourceType
    *
    * @param sparkJobName     Spark Job Name. Ex: CartKakfaToCassandra
    * @param configSourceType Source type of configuration parameters. Ex: "--ccm" or "--local"
    * @return SparkCCMConfigurator
    */
  private def getCCMConfigurator(sparkJobName: String,
                                 configSourceType: ConfigSourceType.Value): SparkCCMConfigurator = {

    import scala.collection.JavaConverters._
    configSourceType match {
      case ConfigSourceType.CCM =>
        val sparkCCMConfigurator = new SparkCCMConfigurator(sparkJobName)
        sparkCCMConfigurator

      case ConfigSourceType.LOCAL =>
        val configs = ConfigFactory.load(DinoJobCommon.getBaseEnvDirectory+sparkJobName)
        var configProps: mutable.Map[String, String] = collection.mutable.Map[String, String]()
        configs.entrySet().asScala.map(keyVal => {
          val sysPropValue = System.getProperty(keyVal.getKey)
          val value = if(sysPropValue!=null) sysPropValue else keyVal.getValue.unwrapped().toString
          configProps += (keyVal.getKey -> value)
        })

        println(configProps.asJava)

        val sparkCCMConfigurator = new SparkCCMConfigurator(configProps.asJava, sparkJobName)
        sparkCCMConfigurator

      case _ =>
        throw new IllegalArgumentException("Valid Configuration Source Types are \n --ccm \n --local")
    }
  }
}


/** Case class for converting RDD to DataFrame */
case class Record[K,V](
                        topic: String = null,
                        partition: Int = 0,
                        offset: Long = 0L,
                        timestamp: Long = 0L,
                        checksum: Long = 0L,
                        serializedKeySize: Int = 0,
                        serializedValueSize: Int = 0,
                        key: K = null,
                        value: V = null)