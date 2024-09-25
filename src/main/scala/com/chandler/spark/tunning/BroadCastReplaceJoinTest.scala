package com.chandler.spark.tunning

import org.apache.spark.{SparkConf, SparkContext}

object BroadCastReplaceJoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val nameRDD = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 19),
      ("lisi", 20),
      ("test", 12)
    ))

    val scoreRDD = sc.parallelize(Array[(String, Int)](
      ("zhangsan", 100),
      ("lisi", 200),
      ("test", 300)
    ))

    val nameInfos = nameRDD.collectAsMap()
    val bc = sc.broadcast(nameInfos)

    scoreRDD.map(tp => {
      val map = bc.value
      if(map.contains(tp._1)) {
        (tp._1, (tp._2, map(tp._1)))
      } else {
        (tp._1, (tp._2, null))
      }
    }).foreach(println)


//    nameRDD.join(scoreRDD).foreach(println)
  }
}
