package com.cg.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCountWithScala {
  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象, 指定appName以及Master
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    // 2.创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    // 3.加载数据文件, 初始化RDD
    val lines: RDD[String] = sc.textFile("d:/1.txt")
    // 4.对每一行文本切分压平, 获取所有单词
    val words: RDD[String] = lines.flatMap(line => line.split(" "))
    // 5.对所有单词进行统计, 每出现一次记为(word, 1)
    val pairs: RDD[(String, Int)] = words.map(word => (word, 1))
    // 6.对相同的单词进行聚合统计
    val wordCount: RDD[(String, Int)] = pairs.reduceByKey((a, b) => a + b)
    // 7.输出或保存结果
    wordCount.foreach(item => (println(item)))
    // 释放资源
    sc.stop();
  }
}