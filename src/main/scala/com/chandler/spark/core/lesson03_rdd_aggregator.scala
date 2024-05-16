package com.chandler.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object lesson03_rdd_aggregator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(List(
      ("zhangsan", 234),
      ("zhangsan", 5667),
      ("zhangsan", 343),
      ("lisi", 212),
      ("lisi", 44),
      ("lisi", 33),
      ("wangwu", 535),
      ("wangwu", 22)
    ))

    //key value -> 一组
    val group = data.groupByKey()
    group.foreach(println)

    //行列转换

    val flatmap = group.flatMap(x => x._2.map(y => (x._1, y)))
    flatmap.foreach(println)
    println("---" * 30)
    val flatmapvalues = group.flatMapValues(e => e.iterator)
    flatmapvalues.foreach(println)
    println("---" * 30)
    group.mapValues(e => e.toList.sorted.take(2)).foreach(println)
    println("---" * 30)
    group.flatMapValues(e => e.toList.sorted.take(2)).foreach(println)

    println("-----------------------------------------sum, count, min, max, avg-------------------------------------------------")

    val sum = data.reduceByKey(_ + _)
    sum.foreach(println)
    println("-----------------------------------------")
    //求最大值
    data.reduceByKey((oldValue, newValue) => if (oldValue > newValue) oldValue else newValue).foreach(println)
    println("-----------------------------------------")
    //求count
    val count = data.map(x => (x._1, 1)).reduceByKey(_ + _)
    count.foreach(println)
    println("-----------------------------------------")
    //求avg
    val beforeAvg = sum.join(count)
    val avg = beforeAvg.mapValues(e => e._1 / e._2)
    avg.foreach(println)
    println("-----------------------------------------")
    //如何优化?
    val avg2 = data.combineByKey(
      //第一个函数解决第一条记录的问题
      (value: Int) => (value, 1),
      //第二个函数解决shuffle之前，怎么本地合并
      (oldValue: (Int, Int), newValue: Int) => (oldValue._1 + newValue, oldValue._2 + 1),
      //最终合并
      (oldValue: (Int, Int), newValue: (Int, Int)) => (oldValue._1 + newValue._1, oldValue._2 + newValue._2)
    ).mapValues(e => e._1 / e._2)
    avg2.foreach(println)


    while(true) {

    }
  }
}
