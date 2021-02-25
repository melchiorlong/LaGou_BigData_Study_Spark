package com.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable


object TopN {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName.init).set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val N = 9

    // 生成数据
    val random = scala.util.Random
    val scores: immutable.IndexedSeq[String] = (1 to 50).flatMap { idx =>
      (1 to 2000).map { id =>
        f"group$idx%2d,${random.nextInt(100000)}"
      }
    }

    val scoresRDD: RDD[(String, Int)] = sc.makeRDD(scores).map { line =>
      val fields: Array[String] = line.split(",")
      (fields(0), fields(1).toInt)
    }
    scoresRDD.cache()

    // TopN的实现

    // 通过groupByKey实现
    // 需要将每个分区的每个group的全部数据做shuffle
    scoresRDD.groupByKey()
      .mapValues(buf => buf.toList.sorted.takeRight(N).reverse)
      .sortByKey()
      .collect.foreach(println)

    println("*****************************************************")

    // TopN的优化
    scoresRDD.aggregateByKey(List[Int]())(
      (lst, score) => (lst :+ score).sorted.takeRight(N),
      (lst1, lst2) => (lst1 ++ lst2).sorted.takeRight(N)
    ).mapValues(_.reverse)
      .sortByKey()
      .collect.foreach(println)

      scoresRDD.hashCode()



    // 关闭SparkContext
    sc.stop()

  }
}
