package com.cg.spark.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import scala.collection.mutable.HashMap

object GroupFavorite2 {
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
    // 5.地址和ip进行聚合统计
    val counts: RDD[((String, String), Int)] = rows.reduceByKey((a, b) => a+ b)
    // 获取所有地址
    val ipaddress: Array[String] = rows.map(el => el._1._1).distinct().collect()
    
    val partitioner = new IpaddressPartitioner(ipaddress)
    val partitionerRDD: RDD[((String, String), Int)] = counts.partitionBy(partitioner)
    
    val result: RDD[((String, String), Int)] = partitionerRDD.mapPartitions(iter => {
      iter.toList.sortBy(el => el._2).reverse.take(3).iterator
    })
    
    val finalResult: Array[((String, String), Int)] = result.collect()
    finalResult.foreach(item => println(item))
    
    sc.stop()
  }
}

class IpaddressPartitioner(ipaddress:Array[String]) extends Partitioner {
  
  val rules: HashMap[String, Int] = new HashMap[String, Int]()
  var index = 0
  for(ip <- ipaddress){
    rules.put(ip, index)
    index += 1
  }
  /**
   * 根据key返回分区编号
   */
  def getPartition(key: Any): Int = {
    // 获取ip地址
    val ipaddr: String = key.asInstanceOf[(String, String)]._1
    rules(ipaddr)
  }
  
  /**
   * 指定分区数量
   */
  def numPartitions: Int = ipaddress.length
}


/* 返回数据
((165.78.61.199,www.qq.com),2)
((165.8.6.199,www.baidu.com),4)
((165.18.6.199,www.google.com),4)
((165.18.6.199,www.163.com),2)
((105.18.6.199,www.baidu.com),2)
((165.178.6.9,www.jd.com),2)
((165.178.6.39,www.baidu.com),2)
((165.178.6.29,www.google.com),2)
((165.178.6.199,www.baidu.com),1)
((165.178.6.119,www.google.com),1)
((165.178.61.199,www.qq.com),2)
((163.178.6.199,www.google.com),3)
((105.178.6.199,www.baidu.com),1)
((165.78.6.9,www.jd.com),2)
((175.178.6.199,www.qq.com),4)
((165.138.6.199,www.baidu.com),3)
((165.118.6.199,www.163.com),1)
 */
