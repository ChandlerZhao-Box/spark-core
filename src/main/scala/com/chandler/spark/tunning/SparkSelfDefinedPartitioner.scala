package com.chandler.spark.tunning

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object SparkSelfDefinedPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array[(Int, String)](
      (1, "zhangsan"),
      (2, "lisi"),
      (3, "test"),
      (4, "aaa"),
      (5, "bb"),
      (6, "ccc"),

    ))

    println(s"rdd1 partition length = ${rdd1.getNumPartitions}")
    //注意: 自定义分区器需要作用在K, V格式的RDD上
    val rdd2 = rdd1.partitionBy(new Partitioner {
      //指定生成的新RDD有几个分区
      override def numPartitions: Int = 6

      //当前一条数据进来之后，将这条数据去往哪个分区
      override def getPartition(key: Any): Int = {
        key.toString.toInt % numPartitions
      }
    })

    rdd2.mapPartitionsWithIndex((index, iter) => {
      iter.map(tp => {
        s"partition == ${index}, tp = ${tp}"
      })
    }).foreach(println)

  }

}
