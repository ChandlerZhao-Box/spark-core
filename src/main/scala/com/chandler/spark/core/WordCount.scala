package com.chandler.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val fileRDD = sc.textFile("data/testdata.txt", 100)

    val words = fileRDD.flatMap(x => {
      x.split(" ")
    })

    val pairWord = words.map(x => new Tuple2(x, 1))

    val res = pairWord.reduceByKey((x, y) => {
      x + y
    })
    //(chandler,1)
    //(hello,2)
    //(123,1)
    //(world,1)
    //(test,1)
//
    val fanzhuan = res.map(x => (x._2, 1))
    val resOver = fanzhuan.reduceByKey(_ + _)
    //(1,4)
    //(2,1)
//    resOver.foreach(println)
    println("="*50)
//    res.foreach(println)

    Thread.sleep(1000000)
  }
}
