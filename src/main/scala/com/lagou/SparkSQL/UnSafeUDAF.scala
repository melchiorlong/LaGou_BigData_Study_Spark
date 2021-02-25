package com.lagou.SparkSQL

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}



class TypeUnsafeUDAF extends UserDefinedAggregateFunction {
  // 定义输入数据类型
  override def inputSchema: StructType = new StructType().add("sales", DoubleType).add("saleDate", StringType)

  // 定义数据缓存的类型
  override def bufferSchema: StructType = new StructType().add("year2019", DoubleType).add("year2020", DoubleType)

  // 定义最终返回结果的类型
  override def dataType: DataType = DoubleType

  // 对于相同的结果 是否有相同的数据
  override def deterministic: Boolean = true

  // 数据缓存的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
    buffer.update(1, 0.0)
  }

  // 分区内合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 取出销售量
    val sales: Double = input.getAs[Double](0)
    val saleDate: String = input.getAs[String](1).take(4)

    saleDate match {
      case "2019" => buffer(0) = buffer.getAs[Double](0) + sales
      case "2020" => buffer(1) = buffer.getAs[Double](1) + sales
      case _ => println("Error")
    }
  }

  // 分区间合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    // (2020年的合计值 – 2019年的合计值) / 2019年的合计值
    println(s"evaluate : ${buffer.getDouble(0)}, ${buffer.getDouble(1)}")
    if (buffer.getDouble(0).abs < 0.000000001) 0
    else (buffer.getDouble(1) - buffer.getDouble(0)) / buffer.getDouble(0)
  }
}


object UDAFRegistration {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Demo1")
      .config("spark.some.config.option", "some-value")
      .config("spark.testing.memory", "2147480000")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    val sales = Seq(
      (1, "Widget Co", 1000.00, 0.00, "AZ", "2019-01-02"),
      (2, "Acme Widgets", 2000.00, 500.00, "CA", "2019-02-01"),
      (3, "Widgetry", 1000.00, 200.00, "CA", "2020-01-11"),
      (4, "Widgets R Us", 2000.00, 0.0, "CA", "2020-02-19"),
      (5, "Ye Olde Widgete", 3000.00, 0.0, "MA", "2020-02-28"))

    val salesDF: DataFrame = spark.createDataFrame(sales).toDF("id", "name", "sales", "discount", "state", "saleDate")

    salesDF.createTempView("sales")

    val userFunc = new TypeUnsafeUDAF
    spark.udf.register("userFunc", userFunc)

    spark.sql("select userFunc(sales, saleDate) as rate from sales").show()


    spark.close()

  }
}
