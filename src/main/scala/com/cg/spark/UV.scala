package com.cg.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object UV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UV").setMaster("local")
    // 根据SparkConf创建一个SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    // 3.加载数据文件, 获取初始RDD
    val lines: RDD[String] = sc.textFile("d:/PVUV.txt")
    // 4.对每一行内容切分
    val ips = lines.flatMap(line => line.split(",")).map(x => x(0))
    // 5.对所有ip去重统计
    val result: Long = ips.distinct().count()

    println(result)
    
    sc.stop()
  }
}