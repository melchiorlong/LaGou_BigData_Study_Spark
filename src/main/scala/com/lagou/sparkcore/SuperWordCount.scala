package com.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将单词全部转换为小写，
 * 去除标点符号(难)，
 * 去除停用词(难)；
 * 最后按照 count 值降序保存到文件，
 * 同时将全部结果保存到MySQL(难)；
 * 标点符号和停用词可以自定义。
 */

object SuperWordCount {

  val stopWords = "in on to from by a an the is are were was i we you your he his some any of as can it each".split("\\s+")
  val punctuation = "[\\)\\.,:;'!\\?]"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("data/swc.dat")

    val result: RDD[(String, Int)] = lines.flatMap(_.split("\\s+"))
      .map(_.toLowerCase())
      .map(_.replaceAll(punctuation, ""))
      .filter(word => !stopWords.contains(word) && word.nonEmpty)
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    result.saveAsTextFile("I:\\LaGou_BigData_Study_Spark\\data\\superWC")


    sc.stop()
  }
}
