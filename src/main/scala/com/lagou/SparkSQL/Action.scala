package com.lagou.SparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}

object Action {
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

    val df1: DataFrame = spark.read
      .options(Map(("header", "true"), ("inferschema", "true")))
      .csv("data/emp.dat")

    df1.printSchema()

    df1.toJSON.show(false)
    println(df1.head())
    println(df1.head(2).mkString("Array(", ", ", ")"))




    spark.close()

  }
}
