package com.lagou.SparkSQL

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object UDFRegistration {
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
    import spark.sql

    val data = List(("scala", "author1"), ("spark", "author2"),
      ("hadoop", "author3"), ("hive", "author4"),
      ("strom", "author5"), ("kafka", "author6"))

    val df = data.toDF("title", "author")
    df.createTempView("books")

    // 定义scala函数并注册
    def len1(str:String): Int = str.length
    spark.udf.register("len1", len1 _)

    // 使用udf
    sql(
      """
        |select title, author, len1(title) from books
        |""".stripMargin).show()
    sql(
      """
        |select title, author from books
        |where len1(title) > 5
        |""".stripMargin).show()

    println("********************************************************")

    // DSL方式
    df.filter("len1(title) > 5").show()
    val len2: UserDefinedFunction = udf(len1 _)
    df.select($"title", $"author", len2($"title")).show()
    df.filter(len2($"title") > 4).show()

    println("********************************************************")

    // 不适用UDF
    val ds: Dataset[(String, String, Int)] = df.map { case Row(title: String, author: String) =>
      (title, author, title.length)
    }

    ds.show()


    spark.close()
  }
}
