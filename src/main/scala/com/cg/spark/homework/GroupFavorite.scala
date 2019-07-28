package com.cg.spark.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GroupFavorite {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Favorite").setMaster("local")
    // 根据SparkConf创建一个SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    // 3.加载数据文件, 获取初始RDD
    val lines: RDD[String] = sc.textFile("d:/PVUV.txt")
    // 4.对每一行内容切分
    //val lineArray: RDD[Array[String]] = lines.map(line => line.split(","))
    
    //[{{ip:xxx, row:[]}, count: 1}]
    val rows: RDD[((String, String), Int)] = lines.map(line => {
      /**
       * 0:ip
       * 1:地区
       * 2:日期
       * 3:userId
       * 4:token
       * 5:地址
       * 6:action
       */
      val arr: Array[String] = line.split(",")
      ((arr(0), arr(5)), 1)
    })
    // 5.对ip和地址聚合统计           两个元组, 第一个元组是(String, Array[String]), 第二个是Int
    val counts: RDD[((String, String), Int)] = rows.reduceByKey((a, b) => a + b)
    // 6.对地址分组
    val groupRDD: RDD[(String, Iterable[((String, String), Int)])] = counts.groupBy(el => el._1._2)
    // 7.对分组后的结果进行排序
    val sortedRDD: RDD[(String, List[((String, String), Int)])] = groupRDD.mapValues(iter => iter.toList.sortBy(el => el._2).reverse)
    
    val result: Array[(String, List[((String, String), Int)])] = sortedRDD.collect()
    
    result.foreach(item => println(item.toString()))
    
    sc.stop()
  }
}
/* 返回数据
(www.qq.com,List(((175.178.6.199,www.qq.com),4), ((165.78.61.199,www.qq.com),2), ((165.178.61.199,www.qq.com),2)))

(www.163.com,List(((165.18.6.199,www.163.com),2), ((165.118.6.199,www.163.com),1)))

(www.jd.com,List(((165.78.6.9,www.jd.com),2), ((165.178.6.9,www.jd.com),2)))

(www.baidu.com,List(((165.8.6.199,www.baidu.com),4), ((165.138.6.199,www.baidu.com),3), ((165.178.6.39,www.baidu.com),2), ((105.18.6.199,www.baidu.com),2), ((165.178.6.199,www.baidu.com),1), ((105.178.6.199,www.baidu.com),1)))

(www.google.com,List(((165.18.6.199,www.google.com),4), ((163.178.6.199,www.google.com),3), ((165.178.6.29,www.google.com),2), ((165.178.6.119,www.google.com),1)))
*/