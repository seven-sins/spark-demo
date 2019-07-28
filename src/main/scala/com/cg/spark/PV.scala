package com.cg.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PV").setMaster("local")
    // 根据SparkConf创建一个SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    // 3.加载数据文件, 获取初始RDD
    val lines: RDD[String] = sc.textFile("d:/PVUV.txt")
    // 4.统计
    val result: Long = lines.count()
    
    println(result)
    
    sc.stop()
  }
}