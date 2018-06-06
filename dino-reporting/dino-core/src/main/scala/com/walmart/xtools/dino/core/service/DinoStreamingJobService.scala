package com.walmart.xtools.dino.core.service

import java.util.ServiceLoader

import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import com.walmart.xtools.dino.core.spi.job.DinoStreamingJob

case class DinoStreamingJobService(jobName: String) {
  private lazy val providingClass: Class[_] = lookupDinoStreamingJob()

  private val loader: ClassLoader = getContextOrSparkClassLoader

  private def getContextOrSparkClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader

  def getJobName: String = {
    providingClass.newInstance() match {
      case s: DinoStreamingJob => s.getJobName
      case _ => throw new Exception("getJobName is not a supporting operation ")
    }
  }

  def getSources: Map[String, String] = {
    providingClass.newInstance() match {
      case s: DinoStreamingJob => s.getSourceMap
      case _ => throw new Exception("getSourceMap is not a supporting operation ")
    }
  }

  def getSinks: Map[String, String] = {
    providingClass.newInstance() match {
      case s: DinoStreamingJob => s.getSinkMap
      case _ => throw new Exception("getSinkMap is not a supporting operation ")
    }
  }

  def execute(dataFrameMap: mutable.Map[String, DataFrame]): List[(DataFrame, String)] = {
    providingClass.newInstance() match {
      case s: DinoStreamingJob => s.execute(dataFrameMap = dataFrameMap)
      case _ => throw new Exception("execute is not a supporting operation ")
    }
  }

  private def lookupDinoStreamingJob(): Class[_] = {
    try {
      val serviceLoader = ServiceLoader.load(classOf[DinoStreamingJob], loader)
      serviceLoader.asScala.filter(_.getJobName.equalsIgnoreCase(jobName)).toList match {
        // the provider format did not match any given registered aliases
        case Nil =>
          try {
            Try(loader.loadClass(jobName)) match {
              case Success(jobClass) =>
                // Found the data source using fully qualified path
                jobClass
              case Failure(error) =>
                throw new ClassNotFoundException(
                  s"Failed to locate $jobName Job. ",
                  error)
            }
          } catch {
            case e: Exception => throw e
          }
        case head :: Nil =>
          // there is exactly one registered alias
          head.getClass
        case sources =>
          // Multiple Jobs find with same name.
          val sourceNames = sources.map(_.getClass.getName)
          throw new ClassNotFoundException(s"Multiple sources found for $jobName" +
              s"(${sourceNames.mkString(", ")}), please specify the fully qualified class name.")
      }
    } catch {
      case e: Exception => throw e
    }
  }

}
