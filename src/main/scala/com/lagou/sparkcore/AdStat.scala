package com.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求
 * 1、统计每一个省份点击TOP3的广告ID
 * 2、统计每一个省份每一个小时的TOP3广告ID
 *
 * 1562085629599	Hebei	Shijiazhuang	564	1
 * 1562085629621	Hunan	Changsha	14	6
 *
 */

object AdStat {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)


    val lines: RDD[String] = sc.textFile("data/advert.log")

    val stat1RDD: RDD[(String, String)] = lines.map { line =>
      val fields: Array[String] = line.split("\\s+")
      (fields(1), fields(4))
    }

    // 按省份广告为key汇总
    val reduce1RDD: RDD[((String, String), Int)] = stat1RDD.map { case (province, adid) => ((province, adid), 1) }
      .reduceByKey(_ + _)

    // 对以上汇总信息求Top3
    reduce1RDD.map { case ((province, adid), count) => (province, (adid, count)) }
      .groupByKey()
      .mapValues(buf => buf.toList.sortWith(_._2 > _._2).take(3).map(_._1).mkString("; "))
      .foreach(println)

    // 统计每一个省份每一个小时的TOP3广告ID
    lines.map {
      line =>
        val fields: Array[String] = line.split("\\s+")
        ((getHour(fields(0)), fields(1), fields(4)), 1)
    }.reduceByKey(_ + _)
      .map { case ((hour, province, adid), count) => ((province, hour),(adid, count)) }
      .groupByKey()
      .mapValues(buf => buf.toList.sortWith(_._2 > _._2).take(3).map(_._1).mkString(","))
      .foreach(println)


    sc.stop()
  }

  def getHour(timeStamp: String): Int = {
    import org.joda.time.DateTime
    val dt = new DateTime(timeStamp.toLong)
    dt.getHourOfDay
  }


}
