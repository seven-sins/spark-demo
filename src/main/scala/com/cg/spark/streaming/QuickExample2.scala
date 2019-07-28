package com.cg.spark.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

object QuickExample2 {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new StreamingContext(conf, Seconds(1))
    // TODO: 修改日志输入
    sc.sparkContext.setLogLevel("WARN")
    
//    sc.checkpoint("./updateStateByKey")
    
    val lines: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 9999)
    // 数据切分
    val words: DStream[String] = lines.flatMap(line => line.split(" "))
    // 对每个单词加1
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
    // 聚合统计
    // val wordCounts: DStream[(String, Int)] = pairs.reduceByKey((a, b) => a+b )                    5秒计算一次, 统计10秒数据
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKeyAndWindow((a:Int, b:Int) => a + b, Seconds(20), Seconds(5))
    
    val result: DStream[(String, Int)] = wordCounts.transform(rdd => {
      val sorted: RDD[(String, Int)] = rdd.sortBy(x => x._2, false)
      
      sorted
    })
    
    
    // 输出结果
    result.print()
    
    // 开启流失计算
    sc.start()
    
    // 等待终止退出
    sc.awaitTermination()
  }
}