package com.chandler.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object lesson02_rdd_sort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    //PV, UV
    //需求: 根据数据计算各网站的PV, UV，同时，只显示top5
    val file = sc.textFile("data/user-behavior.csv", 5)
    println("------------------PV-------------------")

    val pair = file.map(line => (line.split("\\|")(3), 1))

    val reduce = pair.reduceByKey(_ + _)
    val swapRes = reduce.map(_.swap)
    val sortedRDD = swapRes.sortByKey(false)
    val res = sortedRDD.map(_.swap)
    val pv = res.take(10)
    pv.foreach(println)

    println("------------------UV-------------------")

    //需求：同一个用户同一个App只能算一个UV
    val userKeys = file.map(line => {
      val strs = line.split("\\|")
      (strs(3), strs(1))
    })

    val key = userKeys.distinct()
    val pairx = key.map(k => (k._1, 1))
    val uvreduce = pairx.reduceByKey(_ + _)
    val uvSorted = uvreduce.sortBy(_._2, false)
    val uvRes = uvSorted.take(10)
    uvRes.foreach(println)

    //上面一共产生了6个job, 为什么？
    while(true) {

    }
  }

}
