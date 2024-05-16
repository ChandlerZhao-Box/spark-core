package com.chandler.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object lesson05_rdd_gaoji {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

//    val data = sc.parallelize(1 to 100)
//    //抽样
//    println(s"----------------------")
//    data.sample(false, 0.1, 222).foreach(println)
//    println(s"----------------------")
//    data.sample(false, 0.1, 222).foreach(println)
//    println(s"----------------------")
//    data.sample(false, 0.1, 220).foreach(println)


    //分区调优
    val data = sc.parallelize(1 to 10, 5)
    val data1 = data.mapPartitionsWithIndex((pIndex, pIter) => {
      pIter.map(e => (pIndex, e))
    })

//    val repartition = data1.repartition(4)
    val repartition = data1.coalesce(10, false)
    val res = repartition.mapPartitionsWithIndex((pIndex, pIter) => {
      pIter.map(e => (pIndex, e))
    })
    data1.foreach(println)
    println("---" * 30)
    res.foreach(println)
    println(s"data partitions: ${data1.getNumPartitions}")
    println(s"repartition partitions: ${repartition.getNumPartitions}")

    while(true) {

    }

  }

}
