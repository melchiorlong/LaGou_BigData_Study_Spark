package com.lagou.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

object SparkPi {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkContext
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName.init).set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val slices = if (args.length > 0) args(0).toInt else 10
    val N = 1000000000
    // 生成RDD 转换RDD
    val n: Double = sc.makeRDD(1 to N, slices).map(idx => {
      val (x, y) = (random, random)
      if ((x * x + y * y) <= 1) 1 else 0
    }).sum()

    // 输出结果
    val pi = 4.0 * n / N
    println(s"Pi = $pi")

    // 关闭sc
    sc.stop()

  }
}
