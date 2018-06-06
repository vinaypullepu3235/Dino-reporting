package com.walmart.xtools.dino.core.connector

import com.walmart.xtools.dino.core.spi.connector.{BaseDStreamConnector, ConnectorRegister}
import org.apache.spark.sql.DataFrame
import com.walmart.xtools.dino.core.utils.KairosSupport
import com.walmart.xtools.dino.core.performance
import com.walmart.xtools.dino.core.performance.InternalMetricsProcessor

class KairosDStreamConnector extends BaseDStreamConnector with ConnectorRegister {

  override def getName: String = "KairosDStreamConnector"

  override def writeStream(dataFrame: DataFrame,
                  configs: collection.mutable.Map[String, String]): Unit = {
    import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO

    implicit val encoder = org.apache.spark.sql.Encoders.kryo[List[KairosDTO]]
    //val intprocessor  =  new InternalMetricsProcessor()
    if (dataFrame!=null) {

      /*val metrics = intprocessor.ProcessMetrics(dataFrame)
      val rdd = dataFrame.rdd

      rdd.foreach{
        r => {
          print(r.toString())
          print(r(1).toString()
        }
      }*/



      val kairosDS = dataFrame.as[List[KairosDTO]]
      //val kairosDS = metrics.toList
      //metrics.

      //dataFrame.rdd
      KairosSupport.saveDataPointsBatch(configs("kairosUrl"), 1000, kairosDS.rdd)
      //KairosSupport.saveDataPointsBatch(configs("kairosUrl"), 1000, dataFrame.rdd)
    }

  }

}
