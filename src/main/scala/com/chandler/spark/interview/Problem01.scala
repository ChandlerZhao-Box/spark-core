package com.chandler.spark.interview

import org.apache.spark.sql.SparkSession

object Problem01 {

  def main(args: Array[String]): Unit = {

    // 创建 SparkSession
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ReadCSV")
      .getOrCreate()

    // 读取 CSV 文件
    spark.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("data/task01.csv")
      .withColumnRenamed("_c0", "name")
      .withColumnRenamed("_c1", "time")
      .createOrReplaceTempView("device_fact")

    //需求: 每个设备上传的数据之间间隔5秒内，算做一个周期，划分周期
    val selectRes = spark.sql(
      """
        | with time_diff_table as ( select *,
        |                     lag(time) over (order by time) as prev_time,
        |                     to_timestamp(time)-to_timestamp(lag(time) over (partition by name order by time)) as diff_value,
        |                     case when unix_timestamp(time)-unix_timestamp(lag(time) over (partition by name order by time)) <= 5 then 0
        |                     else 1
        |                     end as time_diff
        |                     from device_fact
        |                     order by name,time),
        | sum_table as (select *, sum(time_diff) over (partition by name order by time) as group_id from time_diff_table)
        | select name, min(time) as start_time, max(time) as end_time from sum_table group by name,group_id
        |""".stripMargin)

    selectRes.show()
  }

}
