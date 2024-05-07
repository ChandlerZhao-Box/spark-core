package com.chandler.lesson01

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val fileRDD = sc.textFile("data/testdata.txt")

    val words = fileRDD.flatMap(x => {
      x.split(" ")
    })

    val pairWord = words.map(x => new Tuple2(x, 1))

    val res = pairWord.reduceByKey((x, y) => {
      x + y
    })

    val fanzhuan = res.map(x => (x._2, 1))
    val resOver = fanzhuan.reduceByKey(_ + _)

    resOver.foreach(println)
    println("="*50)
    res.foreach(println)
  }
}
