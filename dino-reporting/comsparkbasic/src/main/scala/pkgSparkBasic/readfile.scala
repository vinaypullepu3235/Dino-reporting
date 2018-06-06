package pkgSparkBasic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object readfile {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local")
      .setAppName("myapp")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/Users/vn0xghh/Desktop/Tags")

    rdd.foreach(println)
  }
}
