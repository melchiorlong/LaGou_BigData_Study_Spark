package com.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName.init).set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val pairs: RDD[(String, Int)] = sc.parallelize(List(("a", 5), ("a", 1), ("b", 6), ("b", 3), ("c", 2)))
    pairs.reduceByKey((a, b) => {
      a > b match {
        case true => a
        case false => b
      }})
      .collectAsMap()
      .foreach(println)

    sc.stop()

  }
}
