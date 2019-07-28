package com.cg.spark.homework

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import scala.collection.mutable.HashMap

object GroupFavorite3 {
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
    // 获取所有地址
    val ipaddress: Array[String] = rows.map(el => el._1._1).distinct().collect()
    val partitioner = new IpaddressPartitioner3(ipaddress)
    
    // 5.地址和ip进行聚合统计
    val counts: RDD[((String, String), Int)] = rows.reduceByKey(partitioner, (a, b) => a+ b)
    
    val result: RDD[((String, String), Int)] = counts.mapPartitions(iter => {
      iter.toList.sortBy(el => el._2).reverse.iterator
    })
    
    val finalResult: Array[((String, String), Int)] = result.collect()
    finalResult.foreach(item => println(item))
    
    
    sc.stop()
  }
}
class IpaddressPartitioner3(ipaddress:Array[String]) extends Partitioner {
  
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