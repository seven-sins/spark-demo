package com.cg.spark.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TopN {
    def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local")
    // 根据SparkConf创建一个SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    // 3.加载数据文件, 获取初始RDD
    val lines: RDD[String] = sc.textFile("d:/PVUV.txt")
    // 4.对每一行内容切分
    val lineArray: RDD[Array[String]] = lines.map(line => line.split(","))
    // 5.过滤不符合条件的数据
    val filterRDD: RDD[Array[String]] = lineArray.filter(arr => arr.length > 3)
    // 6.获取所有的url，并统计, 每出现一次记为1 (url, 1)        以ip为key
    val urlAndOne: RDD[(String, Int)] = filterRDD.map(arr => arr(0)).map(url => (url, 1))
    // 7.对相同的url进行聚合统计
    val urlCounts: RDD[(String, Int)] = urlAndOne.reduceByKey((a, b) => a + b)
    // 8.对统计的结果降序排序
    val sortedRDD: RDD[(String, Int)] = urlCounts.sortBy(url => url._2, false)
    // 9.取出前5名
    val finalResult: Array[(String, Int)] = sortedRDD.take(5)
    // 10.输出
    finalResult.foreach(el => println(el))
    
    sc.stop()
  }
}