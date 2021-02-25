package com.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountNew {

  def main(args: Array[String]): Unit = {


    // 1、创建SparkContext
    val conf = new SparkConf().setMaster("local").setAppName("wc").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 2、读本地文件
    val lines: RDD[String] = sc.textFile("data/wc.txt")

    // 3、RDD转换
    val words: RDD[String] = lines.flatMap(line => line.split("\\s+"))
    val wordMap: RDD[(String, Int)] = words.map(x => (x, 1))
    val result: RDD[(String, Int)] = wordMap.reduceByKey(_ + _)

    // 4、输出
    result.foreach(println)

    // 5、关闭SparkContext
    sc.stop()

    // 6、打包，使用spark-submit提交集群运行

  }
}
