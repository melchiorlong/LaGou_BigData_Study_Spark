package com.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object FindFriends {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("data/fields.dat")


    val friendsRDD: RDD[(String, Array[String])] = lines.map { line =>
      val fields: Array[String] = line.split(",")
      val userid: String = fields(0).trim
      val friends: Array[String] = fields(1).trim.split("\\s+")
      (userid, friends)
    }

    // 方法1 利用笛卡尔积求两两相交的好友，然后出去多余的数据
    friendsRDD.cartesian(friendsRDD)
      .filter { case ((id1, _), (id2, _)) => id1 < id2 }
      .map { case ((id1, friend1), (id2, friend2)) =>
        ((id1, id2), friend1.toSet & friend2.toSet)
        //        ((id1, id2), friend1.intersect(friend2).sorted.toBuffer)
      }
      .sortByKey()
      .collect().foreach(println)


    println("***************************************")

    // 方法2 不适用笛卡尔积
    // 核心思想，将数据变形，将任意userid 的好友，无序组合，然后将组合作为key后，groupByKey

    friendsRDD.flatMapValues(friends => friends.combinations(2))
      .map(x => (x._2.mkString(","), Set(x._1)))
      .reduceByKey(_ union _)
      .sortByKey()
      .collect().foreach(println)


  }
}
