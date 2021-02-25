package com.lagou.SparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}

object InputOutputDemo {
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

    // parquet
    val df1: DataFrame = spark.read.load("data/users.parquet")
    df1.show()

    println("******************************************************")

    import spark._
    sql(
      """
        |create or replace temporary view users
        | using parquet
        | options (path "data/users.parquet")
        |""".stripMargin)

    sql(
      """
        |select * from users
        |""".stripMargin).show()

//    df1.write
//      .mode("overwrite")
//      .save("data/parquet")

    // json

    val df2: DataFrame = spark.read.format("json").load("data/emp.json")

    df2.show()

    sql(
      """
        |create or replace temporary view emp
        | using json
        | options (path "data/emp.json")
        |""".stripMargin)


    sql(
      """
        |select * from emp
        |""".stripMargin).show()

    // JDBC
//    val jdbcDf: DataFrame = spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://192.168.80.123:3306/ebiz?useSSL=false")
//      .option("user", "root")
//      .option("password", "12345678")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("dbtable", "lagou_product_info")
//      .load()
//
//    jdbcDf.show()

    spark.close()

  }
}
