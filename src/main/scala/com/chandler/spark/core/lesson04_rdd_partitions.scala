package com.chandler.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object lesson04_rdd_partitions {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(1 to 10, 2)

    //外关联 sql查询
    val res = data.map(value => {
      println("---- conn to mysql ---")
      println(s"-----select $value ------")
      println("----- close conn ------")
      value + "selected"
    })

    res.foreach(println)
    println("----"*30)

    val res02 = data.mapPartitionsWithIndex(
      (pIndex, iter) => {
        val lb = new ListBuffer[String] //lb是内存的，这个危险！ 根据之前源码发现，spark 是一个pipeline的迭代器嵌套模式，数据不会在内存积压。
        //数据不会造成内存积压的情况
        println(s"---$pIndex ---- conn mysql")
        while (iter.hasNext) {
          val value = iter.next()
          println(s"-----select $value ------")
          lb += (value + "selected")
        }
        println("----- close conn ------")
        lb.iterator
      }
    )
    res02.foreach(println)

    println("----"*30)
    val res03 = data.mapPartitionsWithIndex(
      (pIndex, iter) => {
        new Iterator[String] {
          println(s"---$pIndex ---- conn mysql ------")
          override def hasNext: Boolean = {
            if (iter.hasNext) {
              true
            } else {
              println(s"---$pIndex ---- close conn ------")
              false
            }
          }

          override def next(): String = {
            val value = iter.next()
            println(s"-----select $value ------")
            value + "selected"
          }
        }
      }
    )

    res03.foreach(println)

  }
}
