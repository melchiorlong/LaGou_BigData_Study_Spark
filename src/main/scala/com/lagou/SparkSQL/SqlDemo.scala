package com.lagou.SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Info(Id: String, tags: String)

object SqlDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[*]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("warn")

    import spark.implicits._

    // 准备数据
    val arr: Array[String] = Array("1 1,2,3", "2 2,3", "3 1,2")
    val rdd: RDD[Info] = sc.makeRDD(arr)
      .map { line =>
        val fields: Array[String] = line.split("\\s+")
        Info(fields(0), fields(1))
      }
    val ds: Dataset[Info] = spark.createDataset(rdd)
    ds.createOrReplaceTempView("t1")

    // 用sql处理数据 此处为Hive的语法即 HQL
    spark.sql(
      """
        |select id, tag
        | from t1
        | lateral view explode(split(tags, ",")) as tag
        |""".stripMargin).show()

    // 用SparkSQL的语法
    spark.sql(
      """
        |select id, explode(split(tags, ",")) tag
        | from t1
        |""".stripMargin).show()


    spark.close()
  }
}
