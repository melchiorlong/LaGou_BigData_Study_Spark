package com.lagou.SparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TransformationDemo {
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

    val df1: DataFrame = spark.read
      .options(Map(("header", "true"), ("inferschema", "true")))
      .csv("data/emp.dat")

    df1.printSchema()

    df1.map(row => row.getInt(0)).show()

    // randomSplit(与RDD类似，将DF、DS按给定参数分成多份)
    val Array(dfx, dfy, dfz) = df1.randomSplit(Array(0.5, 0.6, 0.7))
    println(dfx.count)
    println(dfy.count)
    println(dfz.count)

    // 取10行数据生成新的DS
    val df2: Dataset[Row] = df1.limit(10)
    df2.show()

    // distinct 去重
    val df3: Dataset[Row] = df1.union(df1)
    println(df3.distinct().count())

    // 返回全部列统计

    df1.describe().show()

    // 返回部分列统计
    df1.describe("sal").show()
    df1.describe("sal", "comm").show()

    df1.cache()

    df1.createOrReplaceTempView("t1")
    spark.sql("select * from t1")
    spark.catalog.cacheTable("t1")
    spark.catalog.uncacheTable("t1")


    spark.close()

  }
}
