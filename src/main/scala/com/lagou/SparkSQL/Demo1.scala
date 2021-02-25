package com.lagou.SparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Demo1 {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .config("spark.some.config.option", "some-value")
      .config("spark.testing.memory", "2147480000")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    val df1: DataFrame = spark.read.csv("data/people1.csv")
    df1.printSchema()
    df1.show()

    val df2: DataFrame = spark.read.csv("data/people2.csv")
    df2.printSchema()
    df2.show()

    println("******************************************************************************")
    // 定义参数

    val df3: DataFrame = spark.read
      .options(Map(("header", "true"), ("inferschema", "true")))
      .csv("data/people1.csv")
    df3.printSchema()
    df3.show()

    val df4: DataFrame = spark.read
      .option("header","true")
      .option("delimiter",";")
      .option("inferschema","true")
      .csv("data/people2.csv")
    df4.printSchema()
    df4.show()


    spark.close()

  }
}
