package com.walmart.xtools.dino.core.job.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, PrivateMethodTester}
import com.walmart.xtools.dino.core.utils.SparkCCMConfigurator
import com.walmart.xtools.dino.core.api.BaseDinoSparkJob
import com.walmart.xtools.dino.core.service.ConfigSourceType
import com.walmart.xtools.dino.core.service.DinoStreamingJobService

class BaseDinoSparkJobTest extends FunSuite
  with BeforeAndAfterAll
  with PrivateMethodTester
  with Logging {

  var ccmSparkCCMConfigurator: SparkCCMConfigurator = _
  var localSparkCCMConfigurator: SparkCCMConfigurator = _

  var baseDinoSparkJob = BaseDinoSparkJob
  val jobAppName: String = "TestJob"

  val ccmConfigType  = ConfigSourceType.CCM
  val localConfigType = ConfigSourceType.LOCAL

  var localSparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    System.setProperty("com.walmart.platform.config.runOnEnv", "default")

    ccmSparkCCMConfigurator = new SparkCCMConfigurator()

    localSparkSession = SparkSession
      .builder()
      .master("local[3]")
      .config("spark.sql.warehouse.dir", "dino-core/src/test/resources/output")
      .getOrCreate()

  }

  test("Parsing input Arguments") {
    val inputArguments = Array("TestJob", "--ccm")
    val parseArgs = PrivateMethod[(String, ConfigSourceType.Value) ]('parseArgs)

    val (actualAppName, actualConfigSourceType): (String, ConfigSourceType.Value)  = baseDinoSparkJob invokePrivate
      parseArgs(inputArguments)

    assert(actualAppName === jobAppName)
    assert(ccmConfigType === actualConfigSourceType)
  }

  test("Parsing Wrong input Arguments") {
    val inputArguments1 = Array("")
    val inputArguments2 = Array("ba&2 --ccm")
    val parseArgs = PrivateMethod[(String, ConfigSourceType.Value) ]('parseArgs)

    assertThrows[IllegalArgumentException] {
      val (actualAppName1, actualConfigSourceType1): (String, ConfigSourceType.Value)  = baseDinoSparkJob invokePrivate
        parseArgs(inputArguments1)
      val (actualAppName2, actualConfigSourceType2): (String, ConfigSourceType.Value)  = baseDinoSparkJob invokePrivate
        parseArgs(inputArguments2)
    }
  }

  test("Create Configurator using Local type") {
    val getCCMConfigurator = PrivateMethod[SparkCCMConfigurator]('getCCMConfigurator)
    val localSparkCCMConfigurator: SparkCCMConfigurator = baseDinoSparkJob invokePrivate getCCMConfigurator(jobAppName, localConfigType)
    val sparkkConfigs = localSparkCCMConfigurator.getPropertiesWithId("spark", "spark")

    assert(localSparkCCMConfigurator.isInstanceOf[SparkCCMConfigurator])
    assert(sparkkConfigs.get("spark.app.name") === jobAppName)
    assert(sparkkConfigs.get("spark.cores.max") === "2" )
  }


  test("Get Job Service") {
    val getJobService = PrivateMethod[DinoStreamingJobService]('getJobService)
    val jobService: DinoStreamingJobService = baseDinoSparkJob invokePrivate getJobService(jobAppName)
    assert(jobService.isInstanceOf[DinoStreamingJobService])
  }

  test("Create Spark Session using Local Configs") {
    val getCCMConfigurator = PrivateMethod[SparkCCMConfigurator]('getCCMConfigurator)
    val localSparkCCMConfigurator: SparkCCMConfigurator = baseDinoSparkJob invokePrivate getCCMConfigurator(jobAppName,
      ConfigSourceType.LOCAL)

    val createSparkSession = PrivateMethod[SparkSession]('createSparkSession)
    val sparkSession: SparkSession = baseDinoSparkJob invokePrivate createSparkSession(localSparkCCMConfigurator)
    assert(sparkSession.conf.get("spark.app.name") === "TestJob")
  }

  test("Get Connector Maps")  {
    val getJobService = PrivateMethod[DinoStreamingJobService]('getJobService)
    val jobService: DinoStreamingJobService = baseDinoSparkJob invokePrivate getJobService(jobAppName)

    val getConnectorMap = PrivateMethod[(Map[String, String], Map[String, String])]('getConnectorMaps)
    val (sourceMap, sinkMap) = baseDinoSparkJob invokePrivate getConnectorMap(jobService)

    assert(sourceMap.head === ("source.1" -> "TestBatchConnector"))
    assert(sinkMap.head === ("sink.1" ->  "TestBatchConnector"))
  }

  test("Execute the job") {
    val getJobService = PrivateMethod[DinoStreamingJobService]('getJobService)
    val jobService: DinoStreamingJobService = baseDinoSparkJob invokePrivate getJobService(jobAppName)

    val executeSparkJob = PrivateMethod[Unit]('executeSparkJob)
    val getCCMConfigurator = PrivateMethod[SparkCCMConfigurator]('getCCMConfigurator)

    val localSparkCCMConfigurator: SparkCCMConfigurator = baseDinoSparkJob invokePrivate getCCMConfigurator(jobAppName,
      ConfigSourceType.LOCAL)
    baseDinoSparkJob invokePrivate executeSparkJob(localSparkCCMConfigurator, jobService)

    val df = localSparkSession.read.table("person")
    df.printSchema()
    df.show(10)
    assert(df.count() === 6)

  }

}
