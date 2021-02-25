package com.lagou.SparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.rand

case class Sales(id: Int, name1: String, sales: Double, discount: Double, name2: String, stime: String)

case class SalesBuffer(var sales2019: Double, var sales2020: Double)

class TypeSafeUDAF extends Aggregator[Sales, SalesBuffer, Double] {
  // 定义初值
  override def zero: SalesBuffer = SalesBuffer(0.0, 0.0)

  // 分区内合并
  override def reduce(buffer: SalesBuffer, input: Sales): SalesBuffer = {
    val sales: Double = input.sales
    val year: String = input.stime.take(4)
    year match {
      case "2019" => buffer.sales2019 += sales
      case "2020" => buffer.sales2020 += sales
      case _ => println("Error")
    }
    buffer
  }

  // 分区间合并
  override def merge(buffer1: SalesBuffer, buffer2: SalesBuffer): SalesBuffer = {
    //    buffer1.sales2019 += buffer2.sales2019
    //    buffer1.sales2020 += buffer2.sales2020
    //    buffer1
    SalesBuffer(buffer1.sales2019 + buffer2.sales2019, buffer1.sales2020 + buffer2.sales2020)
  }

  // 最终计算
  override def finish(reduction: SalesBuffer): Double = {
    println(s"evaluate : ${reduction.sales2019}, ${reduction.sales2020}")
    if (reduction.sales2019.abs < 0.00000001) 0
    else (reduction.sales2020 - reduction.sales2019) / reduction.sales2020
  }

  // 定义编码器
  override def bufferEncoder: Encoder[SalesBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object SafeUDAF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Demo1")
      .config("spark.some.config.option", "some-value")
      .config("spark.testing.memory", "2147480000")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val sales = Seq(
      Sales(1, "Widget Co", 1000.00, 0.00, "AZ", "2019-01-02"),
      Sales(2, "Acme Widgets", 2000.00, 500.00, "CA", "2019-02-01"),
      Sales(3, "Widgetry", 1000.00, 200.00, "CA", "2020-01-11"),
      Sales(4, "Widgets R Us", 2000.00, 0.0, "CA", "2020-02-19"),
      Sales(5, "Ye Olde Widgete", 3000.00, 0.0, "MA", "2020-02-28"))

    import spark.implicits._


    val ds: Dataset[Sales] = spark.createDataset(sales)
    ds.show()
    ds.select(rand())

    val rate: TypedColumn[Sales, Double] = new TypeSafeUDAF().toColumn.name("rate")

    ds.select(rate).show()

    spark.close()
  }
}
