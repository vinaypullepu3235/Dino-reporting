package com.walmart.xtools.dino.core.utils

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.{Logger, LoggerFactory}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.sql.Row

object DinoJobCommon {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  val ENV = "env"
  val FULL_ENV = "com.walmart.platform.config.runOnEnv"
  val DEFAULT_ENV = "default"

  val ENVIRONMENT_CONFIG_DIR = "environmentConfig/"

  /**
    * Retrieving base directory based on the run on environment flag
    *
    * @return String 	Based directory that has the configs based on the run on environment system property
    * @throws Exception
    */
  @throws(classOf[Exception])
  def getBaseEnvDirectory: String = {
    try {
      var baseEnvDir: String = ENVIRONMENT_CONFIG_DIR
      if (System.getProperty(FULL_ENV) != null ) {
          baseEnvDir = baseEnvDir + System.getProperty(FULL_ENV) + "/"
      }else {
          baseEnvDir = baseEnvDir + DEFAULT_ENV + "/"
      }
      return baseEnvDir
    }
    catch {
      case e: Exception => {
        val msg = "Exception inside getBaseEnvDirectory"
        LOGGER.error(msg, ExceptionUtils.getStackTrace(e));
        throw new Exception(msg)
      }
    }
  }

  def parseLong(r: Row, name: String): Long = {
    val value = r(r.fieldIndex(name)).toString
    var result: Option[Long] = None
    try {
      result = Some(value.toLong)
    } catch {
      case nfe: NumberFormatException => {
        println("== ERROR ==> Failed to parse " + name + ": " + value)
        throw new IllegalArgumentException(name + " field should be long")
      }
    }
    result.get
  }

  def makeUrlKairosSafe(data: String): String = {
    var httpIndex = data.indexOf("http://");
    if (httpIndex >= 0) {
      // this is url data
      var url = data.substring(httpIndex + 7);
      if (url.indexOf(":") > 0) {
        var slashIndex = url.indexOf("/")
        if (slashIndex >= 0) {
          url.split(":")(0) + url.substring(slashIndex)
        } else {
          url.split(":")(0)
        }
      }
      else url
    }
    else data // not url data
  }



  def createTagsMap(json: String): java.util.Map[String, String] = {
    val objectMapper = new ObjectMapper
    val tagsMap = new java.util.HashMap[String, String]()
    val tagsNode = objectMapper.readTree(json)
    val iter = tagsNode.fieldNames
    while (iter.hasNext) {
      val tagName = iter.next
      tagsMap.put(tagName, tagsNode.get(tagName).asText)
    }
    tagsMap
  }

}
