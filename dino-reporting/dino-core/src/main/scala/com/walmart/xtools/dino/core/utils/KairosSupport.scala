package com.walmart.xtools.dino.core.utils


import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO
import org.apache.spark.streaming.dstream.DStream
import com.walmart.xtools.dino.core.utils.kairos.util.impl.KairosDataLoaderImpl
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object KairosSupport {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def saveDStreamOfDataPoints(kairosEndPoint: String,
                              batchSize: Int,
                              kairosDTOs: DStream[KairosDTO]): Unit =
    kairosDTOs.foreachRDD(rdd => saveDataPoints(kairosEndPoint, batchSize, rdd))

  def saveDataPoints(kairosEndPoint: String,
                      batchSize: Int,
                      rdd: RDD[KairosDTO]): Unit = {

    rdd.foreachPartition(kairosDataPointIterator => {
      try {
        val kairosDataLoader = new KairosDataLoaderImpl(kairosEndPoint)
        var metricList = new java.util.ArrayList[KairosDTO]()
        if (kairosDataPointIterator != null) {
          kairosDataPointIterator.foreach(record => {
            if (record != null) {
              metricList.add(record)
            }
          }
          )
          kairosDataLoader.saveDataPoints(metricList)
        }
      }
      catch {
        case ex: Exception => {
          val msg = "Exception in saveDataPoints"
          LOGGER.error(msg, ex)
          throw new Exception(msg, ex)
        }
      }
    })

  }

  def saveDataPointsBatch(kairosEndPoint: String,
                     batchSize: Int,
                     rdd: RDD[List[KairosDTO]]): Unit = {

    if (rdd != null && !rdd.isEmpty()){
      rdd.foreachPartition(kairosDataPointIterator => {
        try {
          val kairosDataLoader = new KairosDataLoaderImpl(kairosEndPoint)
          var metricList = new java.util.ArrayList[KairosDTO]()
          if (kairosDataPointIterator != null) {
            kairosDataPointIterator.foreach(record => {
              if (record != null) {
                metricList.addAll(record.asJava)
              }
            }
            )
            kairosDataLoader.saveDataPoints(metricList)
          }
        }
        catch {
          case ex: Exception => import java.io.{PrintWriter, StringWriter}
          {
            val msg = "Exception in saveDataPointsBatch"
            val sw = new StringWriter
            ex.printStackTrace(new PrintWriter(sw))
            println("Exception in saveDataPointsBatch"+sw.toString)
            LOGGER.error(msg, ex)
            throw new Exception(msg, ex)
          }
        }
      })

    }
  }



}
