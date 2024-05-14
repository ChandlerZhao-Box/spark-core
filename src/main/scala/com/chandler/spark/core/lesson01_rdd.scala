package com.chandler.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object lesson01_rdd {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listRDD = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))

    listRDD.foreach(println)
  }
}
