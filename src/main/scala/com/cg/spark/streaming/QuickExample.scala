package com.cg.spark.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream

object QuickExample {
  
  def updateFunc(newValues:Seq[Int], oldValues:Option[Int]): Option[Int] = {
    // 将新的值添加到旧的值上
    val newCount = oldValues.getOrElse(0) + newValues.sum
    // Option.apply(newCount)
    Some(newCount)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new StreamingContext(conf, Seconds(1))
    // TODO: 修改日志输入
    sc.sparkContext.setLogLevel("WARN")
    
    sc.checkpoint("./updateStateByKey")
    
    val lines: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 9999)
    // 数据切分
    val words: DStream[String] = lines.flatMap(line => line.split(" "))
    // 对每个单词加1
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
    // 聚合统计
    // val wordCounts: DStream[(String, Int)] = pairs.reduceByKey((a, b) => a+b )
    val wordCounts: DStream[(String, Int)] = pairs.updateStateByKey(updateFunc)
    // 输出结果
    wordCounts.print()
    
    // 开启流失计算
    sc.start()
    
    // 等待终止退出
    sc.awaitTermination()
  }
}














