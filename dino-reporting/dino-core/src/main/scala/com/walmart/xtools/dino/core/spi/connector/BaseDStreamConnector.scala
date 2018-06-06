package com.walmart.xtools.dino.core.spi.connector

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.walmart.xtools.dino.core.utils.kairos.util.KairosDTO

trait BaseDStreamConnector {


  def read(sparkSession: SparkSession,
           configs: collection.mutable.Map[String, String]): InputDStream[ConsumerRecord[String, String]] = {


    val sparkStreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(Integer.parseInt(configs("batch.duration"))))

    val topics = Array(configs("topic"))



    val kafkaInputStream = KafkaUtils.createDirectStream[String, String](
      sparkStreamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, configs))

    kafkaInputStream
  }

  def writeStream(dataFrame: DataFrame,
                  configs: collection.mutable.Map[String, String]): Unit = {


  }

}
