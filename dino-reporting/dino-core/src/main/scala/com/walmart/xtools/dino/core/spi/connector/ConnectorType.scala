package com.walmart.xtools.dino.core.spi.connector

/** Enum for Connector types. */
object ConnectorType extends Enumeration {
  type ConnectorType = Value
  val Kafka = Value("kafka")
  val Solr = Value("solr")
  val Cassandra = Value("org.apache.spark.sql.cassandra")
  val Jdbc = Value("jdbc")

  def isProvidedConnector(s: String) = values.exists(_.toString == s)

}
