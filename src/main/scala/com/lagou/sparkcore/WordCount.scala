package com.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("WordCount").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("hdfs://192.168.80.121:9000/wcinput/wc.txt")
    //    val lines: RDD[String] = sc.textFile("data/wc.txt")
    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
    sc.stop()


  }
}
