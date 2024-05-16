package com.chandler.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object lesson01_rdd {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listRDD = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))

    val res = listRDD.map((_, 1)).reduceByKey(_ + _).map(_._1)

    println("*"*100)
    //面向数据集

    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(List(3, 4, 5, 6, 7))
    //差集: 提供了一个方法： 有方向的
//    val subtractRDD = rdd1.subtract(rdd2)
//    subtractRDD.foreach(println)
//    val intersectionRes = rdd1.intersection(rdd2)
//    intersectionRes.foreach(println)

    //如果数据，不需要区分每一条记录归属于哪个分区，间接的，这样的数据不需要partitioner，不需要shuffle
    //因为shuffle的寓意：洗牌 ===》 面向每一条记录计算他的分区号
    //如果有行为，不需要区分记录，本地IO拉取数据，那么这种直接IO一定比先partition计算，shuffle落文件，最后再IO拉取速度快！
//    val cartesian = rdd1.cartesian(rdd2)
//    println(rdd1.partitions.size)
//    println(rdd2.partitions.size)
//    println(cartesian.partitions.size)
//
//    cartesian.foreach(println)

    //Union
//    println(rdd1.partitions.size)
//    println(rdd2.partitions.size)
//    val unionRDD = rdd1.union(rdd2)
//    println(unionRDD.partitions.size)
//    unionRDD.map((_,1))
//    unionRDD.foreach(println)

    val kv1 = sc.parallelize(List(
      ("zhangsan", 11),
      ("zhangsan", 12),
      ("lisi", 13),
      ("wangwu", 14)
    ))

    val kv2 = sc.parallelize(List(
      ("zhangsan", 21),
      ("zhangsan", 22),
      ("lisi", 23),
      ("zhaoliu", 24)
    ))

    val cogroup = kv1.cogroup(kv2)
    cogroup.foreach(println)

    //所有的关联操作来自于cogroup
    val joinRDD = kv1.leftOuterJoin(kv2)
    joinRDD.foreach(println)


    while(true) {

    }


  }

}
