package com.chandler.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object lesson06_rdd_over {

  def main(args: Array[String]): Unit = {
    //综合应用算子
    //topN 分组取TopN
    //同月份中
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val file = sc.textFile("data/topdata.txt")
    //2019-6-1  39
    val data = file.map(row => row.split(" ")).map(arr => {
      val arrays = arr(0).split("-")
//      (year, month, day, temperature)
      (arrays(0).toInt, arrays(1).toInt, arrays(2).toInt, arr(1).toInt)
    })

    data.foreach(println)

    //分组取topN
    //错误
//    val sorted = data.sortBy(t4 => (t4._1, t4._2, t4._4), false)
//    val reduced = sorted.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).reduceByKey((x, y) => if (y > x) y else x)
//    val maped = reduced.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
//    val grouped = maped.groupByKey()
//    grouped.foreach(println)

//    data.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).combineByKey(
//      k => k,
//      (c:Int, k) => if(c > k) c else k,
//      (c1, c2) =>
//    )





//    val grouped = data.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
//    grouped.foreach(println)
//
//    val res = grouped.mapValues(arr => {
//      val map = new mutable.HashMap[Int, Int]()
//      arr.foreach(element => {
//        if (map.get(element._1).getOrElse(0) < element._2) {
//          map.put(element._1, element._2)
//        }
//      })
//      map.toList.sorted(new Ordering[(Int, Int)] {
//        override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2)
//      })
//    })
//
//    res.foreach(println)


  }

}
